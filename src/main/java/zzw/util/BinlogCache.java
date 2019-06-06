package zzw.util;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static java.util.Map.Entry;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;
import static org.slf4j.LoggerFactory.getLogger;
import static zzw.util.BinlogUtils.getTableColumns;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.github.phantomthief.tuple.TwoTuple;

/**
 * @author zhangzhewei
 */
public final class BinlogCache {

    private static final Logger logger = getLogger(BinlogCache.class);

    private final String host;
    private final int port;
    private final String binlogUserName;
    private final String binlogPassword;
    private final String bizName;
    private final Map<TwoTuple<String, String>, Map<String, Boolean>> tableInfoCache = new ConcurrentHashMap<>();

    public BinlogCache(String host, int port, String binlogUserName, String binlogPassword,
            String bizName) {
        this.host = host;
        this.port = port;
        this.binlogUserName = binlogUserName;
        this.binlogPassword = binlogPassword;
        this.bizName = bizName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getBizName() {
        return bizName;
    }

    void clearTableMetaCache(String database, String table) {
        tableInfoCache.remove(tuple(database, table));
    }

    public Set<String> getColumns(String database, String table) {
        return getTableInfoCache(database, table).keySet();
    }

    Set<String> getUnsignedColumns(String database, String table) {
        return getTableInfoCache(database, table).entrySet().stream() //
                .filter(Entry::getValue) //
                .map(Entry::getKey) //
                .collect(toSet());
    }

    private Map<String, Boolean> getTableInfoCache(String database, String table) {
        TwoTuple<String, String> key = tuple(database, table);
        Map<String, Boolean> result = tableInfoCache.get(key);
        if (result != null) {
            return result;
        }
        return tableInfoCache.computeIfAbsent(key, tuple -> getTableColumns(host, port,
                binlogUserName, binlogPassword, tuple.getFirst(), tuple.getSecond(), "joda"));
    }

    @Override
    public String toString() {
        return reflectionToString(this, SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinlogCache that = (BinlogCache) o;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

}
