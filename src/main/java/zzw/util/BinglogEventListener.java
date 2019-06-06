package zzw.util;

import static com.github.phantomthief.scope.Scope.runWithNewScope;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Optional.ofNullable;
import static zzw.model.BaseChangedRow.BinlogType.DELETE;
import static zzw.model.BaseChangedRow.BinlogType.INSERT;
import static zzw.model.BaseChangedRow.BinlogType.UPDATE;
import static zzw.util.BinlogUtils.toColumnType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.util.ThrowableRunnable;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import zzw.model.BaseChangedRow;
import zzw.model.DeletedRow;
import zzw.model.InsertedRow;
import zzw.model.UpdatedRow;

/**
 * @author zhangzhewei
 */
public class BinglogEventListener implements BinaryLogClient.EventListener {

    private static final Logger logger = LoggerFactory.getLogger(BinglogEventListener.class);

    private static final int TABLE_MAP_EVENT_CACHE_SIZE = 1000_0000;

    /**
     * 之所以使用cache结构，是因为，tableId不光在DDL时会涨，还有表结构缓存失效重新打开后也会涨
     * 具体参考：http://blog.chinaunix.net/uid-26896862-id-3329896.html
     * 我们的DBA会在散表时使用更大的table_definition_cache和table_open_cache
     * 这里的cache是一个保底措施，别出现内存泄露了，所以缓存最大值会设置很大，基本上不会出现业务有影响的情况
     */
    private final Cache<Long, String> tableIdMap = CacheBuilder.newBuilder() //
            .maximumSize(TABLE_MAP_EVENT_CACHE_SIZE) //
            .build();

    private final Map<String, TableMapEventData> tableMapMapping = new ConcurrentHashMap<>();

    private final BinlogCache holder;
    private final Map<String, BinlogResolver> resolvers;
    private final ExecutorService executorService;
    private final Map<BinlogResolver, ExecutorService> forceSingleThreadPools;

    public BinglogEventListener(BinlogCache holder, Map<String, BinlogResolver> resolvers,
            ExecutorService executorService,
            Map<BinlogResolver, ExecutorService> forceSingleThreadPools) {
        this.holder = holder;
        this.resolvers = resolvers;
        this.executorService = executorService;
        this.forceSingleThreadPools = forceSingleThreadPools;
    }

    @Override
    public void onEvent(Event event) {
        if (event.getHeader() instanceof EventHeaderV4) {
            EventHeaderV4 eventHeaderV4 = event.getHeader();
            long timestamp = eventHeaderV4.getTimestamp();

            if (event.getData() instanceof UpdateRowsEventData) {

                UpdateRowsEventData updateRowsEventData = event.getData();

                TableMapEventData tableMapEventData = getTableMapEventData(
                        updateRowsEventData.getTableId());
                if (tableMapEventData == null) {
                    logger.warn("MISSING TABLE MAP EVENT DATA, POSITION WRONG:{}", event);
                    return;
                }
                dealUpdateEventData(tableMapEventData, updateRowsEventData, timestamp);
            } else if (event.getData() instanceof DeleteRowsEventData) {
                DeleteRowsEventData deleteRowsEventData = event.getData();
                TableMapEventData tableMapEventData = getTableMapEventData(
                        deleteRowsEventData.getTableId());
                if (tableMapEventData == null) {
                    logger.warn("MISSING TABLE MAP EVENT DATA, POSITION WRONG:{}", event);
                    return;
                }
                dealDeleteEventData(tableMapEventData, deleteRowsEventData, timestamp);
            } else if (event.getData() instanceof WriteRowsEventData) {
                WriteRowsEventData writeRowsEventData = event.getData();
                TableMapEventData tableMapEventData = getTableMapEventData(
                        writeRowsEventData.getTableId());
                if (tableMapEventData == null) {
                    logger.warn("MISSING TABLE MAP EVENT DATA, POSITION WRONG:{}", event);
                    return;
                }
                dealWriteEventData(tableMapEventData, writeRowsEventData, timestamp);
            } else if (event.getData() instanceof TableMapEventData) {
                TableMapEventData tableMapEventData = event.getData();
                putTableMapEventData(tableMapEventData);
            }
        }
    }

    private void dealWriteEventData(TableMapEventData tableMapEventData,
            WriteRowsEventData writeRowsEventData, long timestamp) {
        String database = tableMapEventData.getDatabase();
        String table = tableMapEventData.getTable();
        if (needResolveThisTable(table)) {
            writeRowsEventData.getRows().stream() //
                    .map(row -> new InsertedRow(database, table, timestamp,
                            () -> buildRows(tableMapEventData, row), //
                            () -> buildColumnType(tableMapEventData), //
                            () -> buildUnsingedColumns(tableMapEventData))) //
                    .forEach(insertedRow -> resolvers.forEach((name, r) -> {
                        if (!r.acceptTableName(insertedRow.getTableName())) {
                            return;
                        }
                        if (!r.acceptType().contains(INSERT)) {
                            return;
                        }
                        startUriRunning(
                                () -> asyncRunResolver(r, () -> loggedRun(name, r, insertedRow)));
                    }));
        }
    }

    private void dealDeleteEventData(TableMapEventData tableMapEventData,
            DeleteRowsEventData deleteRowsEventData, long timestamp) {
        String database = tableMapEventData.getDatabase();
        String table = tableMapEventData.getTable();
        if (needResolveThisTable(table)) {
            deleteRowsEventData.getRows().stream() //
                    .map(row -> new DeletedRow(database, table, timestamp,
                            () -> buildRows(tableMapEventData, row), //
                            () -> buildColumnType(tableMapEventData), //
                            () -> buildUnsingedColumns(tableMapEventData))) //
                    .forEach(deletedRow -> resolvers.forEach((name, r) -> {
                        if (!r.acceptTableName(deletedRow.getTableName())) {
                            return;
                        }
                        if (!r.acceptType().contains(DELETE)) {
                            return;
                        }
                        startUriRunning(
                                () -> asyncRunResolver(r, () -> loggedRun(name, r, deletedRow)));
                    }));
        }
    }

    public static <X extends Throwable> void startUriRunning(ThrowableRunnable<X> runnable)
            throws X {
        runWithNewScope(runnable);
    }

    private void dealUpdateEventData(TableMapEventData tableMapEventData,
            UpdateRowsEventData updateRowsEventData, long timestamp) {
        String database = tableMapEventData.getDatabase();
        String table = tableMapEventData.getTable();
        if (needResolveThisTable(table)) {
            updateRowsEventData.getRows().stream() //
                    .map(row -> new UpdatedRow(database, table, timestamp, //
                            () -> buildRows(tableMapEventData, row.getKey()), //
                            () -> buildRows(tableMapEventData, row.getValue()), //
                            () -> buildColumnType(tableMapEventData),
                            () -> buildUnsingedColumns(tableMapEventData))) //
                    .forEach(updatedRow -> resolvers.forEach((name, r) -> {
                        if (!r.acceptTableName(updatedRow.getTableName())) {
                            return;
                        }
                        if (!r.acceptType().contains(UPDATE)) {
                            return;
                        }
                        asyncRunResolver(r, () -> loggedRun(name, r, updatedRow));
                    }));
        }
    }

    private void loggedRun(String beanName, BinlogResolver r, BaseChangedRow updatedRow) {
        String type = updatedRow.getType().name().toLowerCase();
        String tag = type + "." + beanName + "/" + holder.getHost() + ":" + holder.getPort();
        String bizName = holder.getBizName();
        try {
            r.onUpdate(updatedRow);
        } catch (Throwable e) {
            logger.error("fail to deal {} sql:{}, handler:{}, tag:{}, bizName:{}", type, updatedRow,
                    r, tag, bizName, e);
        }
    }

    private void asyncRunResolver(BinlogResolver resolver, Runnable runnable) {
        ofNullable(forceSingleThreadPools.get(resolver)) //
                .orElse(executorService) //
                .execute(runnable);
    }

    private Map<String, ColumnType> buildColumnType(TableMapEventData tableMapEvent) {
        Map<String, ColumnType> result = new HashMap<>();
        String database = tableMapEvent.getDatabase();
        String table = tableMapEvent.getTable();
        byte[] types = tableMapEvent.getColumnTypes();
        int[] metadata = tableMapEvent.getColumnMetadata();
        Set<String> columns = holder.getColumns(database, table);
        if (columns.size() != types.length) {
            throw new IllegalStateException(
                    format("invalid meta data, database:%s, table:%s, columns:%s", database, table,
                            columns));
        }
        // see https://github.com/google/guava/wiki/IdeaGraveyard#countingindexed-iterator
        Iterator<String> iterator = columns.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            String column = iterator.next();
            result.put(column, toColumnType(types[i], metadata[i]));
        }
        return unmodifiableMap(result);
    }

    private Set<String> buildUnsingedColumns(TableMapEventData tableMapEventData) {
        String database = tableMapEventData.getDatabase();
        String table = tableMapEventData.getTable();
        return holder.getUnsignedColumns(database, table);
    }

    /**
     * 检查下目前的resolver是否会处理这个表的binlog，如果没人会处理干脆直接跳过就好了
     */
    private Map<String, Serializable> buildRows(TableMapEventData tableMapEventData,
            Serializable[] row) {
        // database
        String database = tableMapEventData.getDatabase();
        //表名
        String table = tableMapEventData.getTable();
        //列对应的value
        Map<String, Serializable> map = new HashMap<>();

        Set<String> columns = getColumnsWithRateRetry(database, table, row.length);
        if (columns.size() != row.length) {
            throw new IllegalStateException(
                    format("invalid meta data, database:%s, table:%s, columns:%s, rows:%s",
                            database, table, columns, Arrays.toString(row)));
        }

        // see https://github.com/google/guava/wiki/IdeaGraveyard#countingindexed-iterator
        Iterator<String> iterator = columns.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            String column = iterator.next();
            map.put(column, row[i]);
        }
        return unmodifiableMap(map);
    }

    private Set<String> getColumnsWithRateRetry(String database, String table,
            int expectColumnNum) {
        Set<String> columns = holder.getColumns(database, table);
        if (columns.size() != expectColumnNum) {
            columns = refreshMetaCache(database, table);
        }
        return columns;
    }

    private Set<String> refreshMetaCache(String database, String table) {
        holder.clearTableMetaCache(database, table);
        return holder.getColumns(database, table);
    }

    @Nullable
    private TableMapEventData getTableMapEventData(long tableId) {
        String schema = tableIdMap.getIfPresent(tableId);
        if (schema == null) {
            return null;
        }
        return tableMapMapping.get(schema);
    }

    private void putTableMapEventData(TableMapEventData data) {
        String schema = data.getDatabase() + "." + data.getTable();
        tableMapMapping.put(schema, data);
        tableIdMap.put(data.getTableId(), schema);
    }

    /**
     * 检查下目前的resolver是否会处理这个表的binlog，如果没人会处理干脆直接跳过就好了
     */
    private boolean needResolveThisTable(String tableName) {
        if (resolvers == null || resolvers.isEmpty()) {
            return false;
        }
        return resolvers.values().stream().anyMatch(r -> r.acceptTableName(tableName));
    }
}
