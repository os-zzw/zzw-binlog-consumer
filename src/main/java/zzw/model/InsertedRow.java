package zzw.model;


import static zzw.model.BaseChangedRow.BinlogType.INSERT;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

/**
 * @author zhangzhewei
 */
public class InsertedRow extends BaseChangedRow {

    public InsertedRow(String database, String tableName, long timestamp,
            Supplier<Map<String, Serializable>> after, Supplier<Map<String, ColumnType>> columnType,
            Supplier<Set<String>> unsignedColumns) {
        super(database, tableName, timestamp, after, columnType, unsignedColumns);
    }

    @Override
    public BinlogType getType() {
        return INSERT;
    }
}
