package zzw.util;

import static org.apache.commons.lang3.StringUtils.isNumeric;

import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nonnull;

import zzw.model.BaseChangedRow;


/**
 * @author zhangzhewei
 */
public interface BinlogResolver {

    EnumSet<BaseChangedRow.BinlogType> ALL_TYPES = EnumSet.allOf(BaseChangedRow.BinlogType.class);

    /**
     * 为row based binlog回调准备
     *
     * <strong>CAUTION:</strong>
     * 1.如果表字段有datetime类型的数据，请不要使用，因为可能涉及到时区转换问题
     * 2.对大多数默认设置sync_binlog=0时，binlog收到trx可能还没commit，这时候如果去读也许会读不到
     * http://dev.mysql.com/doc/refman/5.7/en/binary-log.html
     * 3.对于json类型的数据，请使用{@link com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary#parseAsString}去解析
     * 4.对于<REPLACE INTO>操作，会产生两条binlog，一条<DELETE>，一条<INSERT>。所以请尽量使用<INSERT ON DUPLICATE KEY>语法代替
     */
    void onUpdate(BaseChangedRow row) throws Exception;

    /**
     * WARNING: 仅在row binlog下有效
     * 散表情况请配合使用 {@link #getNoShardTableName(String)}
     */
    boolean acceptTableName(String tableName);

    /**
     * @return 消费互斥组，当返回非{@code null}时，使用这个作为唯一标志做IDC内互斥消费
     */
    default String consumerGroup() {
        return null;
    }

    static String getNoShardTableName(String tableName) {
        int index = tableName.lastIndexOf("_");
        if (index < 0 || index == tableName.length() - 1
                || !isNumeric(tableName.substring(index + 1))) {
            return tableName;
        }
        return tableName.substring(0, index);
    }

    /**
     * 尽量使用本方法做类型过滤，而不是 {@link #onUpdate} 时自己过滤
     * 这样的好处是统计会少一些无关的操作
     */
    @Nonnull
    default Set<BaseChangedRow.BinlogType> acceptType() {
        return ALL_TYPES;
    }
}
