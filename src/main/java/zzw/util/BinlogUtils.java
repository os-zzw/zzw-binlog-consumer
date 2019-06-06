package zzw.util;

import static com.github.shyiko.mysql.binlog.event.deserialization.ColumnType.ENUM;
import static com.github.shyiko.mysql.binlog.event.deserialization.ColumnType.SET;
import static com.github.shyiko.mysql.binlog.event.deserialization.ColumnType.byCode;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.StringUtils.endsWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author zhangzhewei
 */
public class BinlogUtils {

    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final long POOLED_MAX_TIME_MINUTES = 10;
    private static final int MAX_POOL_SIZE = 3;
    private static final String MANAGED_MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String PROP_KEY_DATA_SOURCE_NAME = "dataSourceName";

    private static final LoadingCache<BinlogDataSourceCacheKey, HikariDataSource> DATA_SOURCE_CACHE = CacheBuilder
            .newBuilder() //
            .expireAfterWrite(POOLED_MAX_TIME_MINUTES, MINUTES) //
            .<BinlogDataSourceCacheKey, HikariDataSource> removalListener(
                    notification -> notification.getValue().close()) //
            .build(new CacheLoader<BinlogDataSourceCacheKey, HikariDataSource>() {

                @Override
                public HikariDataSource load(BinlogDataSourceCacheKey key) {
                    Properties properties = new Properties();
                    if (key.bizName != null) {
                        properties.put(PROP_KEY_DATA_SOURCE_NAME, key.bizName);
                    }
                    properties.put("user", key.user);
                    properties.put("password", key.pass);

                    HikariConfig config = new HikariConfig();
                    config.setDriverClassName(MANAGED_MYSQL_DRIVER);
                    config.setJdbcUrl(
                            format("jdbc:mysql://%s:%d?useSSL=false", key.host, key.port));
                    config.setDataSourceProperties(properties);
                    config.setMaximumPoolSize(MAX_POOL_SIZE);
                    config.setIdleTimeout(MINUTES.toMillis(POOLED_MAX_TIME_MINUTES));
                    config.setMinimumIdle(0);
                    return new HikariDataSource(config);
                }
            });

    /**
     * @return 对应表的所有列名，按照顺序，value为是否为unsigned类型
     */
    static Map<String, Boolean> getTableColumns(String host, int port, String user, String pass,
            String database, String tableName, String bizName) {
        HikariDataSource dataSource = DATA_SOURCE_CACHE.getUnchecked(
                new BinlogDataSourceCacheKey(bizName, host, port, user, pass, database));
        try (Connection connection = dataSource.getConnection();
                ResultSet colSet = connection.getMetaData().getColumns(database, null, tableName,
                        null)) {
            LinkedHashMap<String, Boolean> columns = new LinkedHashMap<>();
            while (colSet.next()) {
                String column = colSet.getString(COLUMN_NAME);
                boolean unsigned = endsWith(colSet.getString(TYPE_NAME), "UNSIGNED");
                columns.put(column, unsigned);
            }
            checkArgument(!columns.isEmpty(),
                    "NO COLUMNS FOUND FOR %s@%s:%s/%s(%s), no SELECT grants?", user, host, port,
                    database, tableName);
            return unmodifiableMap(columns);
        } catch (Throwable e) {
            throwIfUnchecked(e);
            throw new RuntimeException("fail to get meta:[" + bizName + "]" + host + ":" + port
                    + "/" + database + ", user:" + user + ", table:" + tableName, e);
        }
    }

    static ColumnType toColumnType(int typeCode, int meta) {
        /*
          CSOFF: MagicNumberCheck
          copy from {@link com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer}
         */
        // mysql-5.6.24 sql/log_event.cc log_event_print_value (line 1980)
        typeCode = typeCode & 0xFF;
        if (typeCode == ColumnType.STRING.getCode()) {
            if (meta >= 256) {
                int meta0 = meta >> 8;
                if ((meta0 & 0x30) != 0x30) {
                    typeCode = meta0 | 0x30;
                } else {
                    // mysql-5.6.24 sql/rpl_utility.h enum_field_types (line 278)
                    if (meta0 == ENUM.getCode() || meta0 == SET.getCode()) {
                        typeCode = meta0;
                    }
                }
            }
        }
        // CSON: MagicNumberCheck
        return byCode(typeCode);
    }

    // fully hashCode and equals for all fields.
    private static class BinlogDataSourceCacheKey {

        private final String bizName;
        private final String host;
        private final int port;
        private final String user;
        private final String pass;
        private final String database;

        BinlogDataSourceCacheKey(String bizName, String host, int port, String user, String pass,
                String database) {
            this.bizName = bizName;
            this.host = host;
            this.port = port;
            this.user = user;
            this.pass = pass;
            this.database = database;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BinlogDataSourceCacheKey that = (BinlogDataSourceCacheKey) o;
            return port == that.port && Objects.equals(bizName, that.bizName)
                    && Objects.equals(host, that.host) && Objects.equals(user, that.user)
                    && Objects.equals(pass, that.pass) && Objects.equals(database, that.database);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bizName, host, port, user, pass, database);
        }
    }
}
