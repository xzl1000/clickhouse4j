package cc.blynk.clickhouse;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public final class ClickHouse4jDataSource {

    private final ClickHouseDriver driver = new ClickHouseDriver();
    protected final String url;
    private PrintWriter printWriter;
    private int loginTimeoutSeconds = 0;
    private ClickHouseProperties properties;

    public ClickHouse4jDataSource(String url) {
        this(url, new ClickHouseProperties());
    }

    public ClickHouse4jDataSource(String url, Properties info) {
        this(url, new ClickHouseProperties(info));
    }

    public ClickHouse4jDataSource(String url, ClickHouseProperties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must be not null");
        }
        this.url = url;
        try {
            this.properties = ClickhouseJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public ClickHouseConnection getConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        return driver.connect(url, properties.withCredentials(username, password));
    }

    public String getHost() {
        return properties.getHost();
    }

    public int getPort() {
        return properties.getPort();
    }

    public String getDatabase() {
        return properties.getDatabase();
    }

    public String getUrl() {
        return url;
    }

    public ClickHouseProperties getProperties() {
        return properties;
    }

    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeoutSeconds = seconds;
    }

    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

}
