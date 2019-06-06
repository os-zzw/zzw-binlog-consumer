package zzw.model;

import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

/**
 * @author zhangzhewei
 */
public abstract class BaseChangedRow {

    private final String database;
    private final String tableName;
    private final long timestamp;
    private final Supplier<Map<String, Serializable>> affectedRow;
    private final Supplier<Map<String, ColumnType>> columnTypes;
    private final Supplier<Set<String>> unsignedColumns;

    BaseChangedRow(String database, String tableName, long timestamp,
            Supplier<Map<String, Serializable>> affectedRow,
            Supplier<Map<String, ColumnType>> columnTypes,
            Supplier<Set<String>> unsignedColumns) {
        this.database = database;
        this.tableName = tableName;
        this.timestamp = timestamp;
        this.affectedRow = lazy(affectedRow);
        this.columnTypes = lazy(columnTypes);
        this.unsignedColumns = lazy(unsignedColumns);
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public abstract BinlogType getType();

    public Map<String, ColumnType> getColumnTypes() {
        return columnTypes.get();
    }

    /**
     * @return 如果是 unsigned 的，返回 {@code true}，其余情况返回 {@code false}
     */
    public boolean isUnsigned(@Nonnull String column) {
        checkNotNull(column);
        checkArgument(getColumnTypes().containsKey(column), "invalid column name [%s]", column);
        return unsignedColumns.get().contains(column);
    }

    /**
     * 受影响的行
     *
     * 对于 {@link InsertedRow}
     * 和 {@link UpdatedRow} ，是修改后的内容
     * 对于 {@link DeletedRow} 是被删除的内容
     */
    public Map<String, Serializable> getAffectedRow() {
        return affectedRow.get();
    }

    @Override
    public String toString() {
        return reflectionToString(this, SHORT_PREFIX_STYLE);
    }

    /**
     * Binlog类型
     */
    public enum BinlogType {
        UPDATE, INSERT, DELETE
    }
}
