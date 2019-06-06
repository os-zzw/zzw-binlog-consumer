package zzw.model;

import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static zzw.model.BaseChangedRow.BinlogType.UPDATE;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

/**
 * @author zhangzhewei
 */
public class UpdatedRow extends BaseChangedRow {

    private final Supplier<Map<String, Serializable>> before;

    public UpdatedRow(String database, String tableName, long timestamp,
            Supplier<Map<String, Serializable>> before, Supplier<Map<String, Serializable>> after,
            Supplier<Map<String, ColumnType>> columnType,
            Supplier<Set<String>> unsignedColumns) {
        super(database, tableName, timestamp, after, columnType, unsignedColumns);
        this.before = lazy(before);
    }

    /**
     * @return 返回修改前的记录内容
     */
    public Map<String, Serializable> getBefore() {
        return before.get();
    }

    @Override
    public BinlogType getType() {
        return UPDATE;
    }
}
