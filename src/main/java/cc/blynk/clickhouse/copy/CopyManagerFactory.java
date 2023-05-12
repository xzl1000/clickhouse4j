package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouse4jDataSource;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public final class CopyManagerFactory {

    private CopyManagerFactory() {
    }

    public static CopyManager create(Connection connection) throws SQLException {
        return new CopyManagerImpl(connection);
    }

    public static CopyManager create(ClickHouse4jDataSource dataSource) throws SQLException {
        return create(dataSource.getConnection());
    }

    public static CopyManager create(Connection connection,
                                     Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        return new CopyManagerImpl(connection, additionalDBParams);
    }

    public static CopyManager create(ClickHouse4jDataSource dataSource,
                                     Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        return create(dataSource.getConnection(), additionalDBParams);
    }
}
