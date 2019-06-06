package zzw.model;

import static zzw.model.BaseChangedRow.BinlogType.DELETE;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

/**
 * @author zhangzhewei
 */
public class DeletedRow extends BaseChangedRow {

    public DeletedRow(String database, String tableName, long timestamp,
            Supplier<Map<String, Serializable>> before,
            Supplier<Map<String, ColumnType>> columnType, Supplier<Set<String>> unsignedColumns) {
        super(database, tableName, timestamp, before, columnType, unsignedColumns);
    }

    @Override
    public BinlogType getType() {
        return DELETE;
    }
}
