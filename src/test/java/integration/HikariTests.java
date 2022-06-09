package integration;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import org.junit.jupiter.api.Test;
import integration.util.TestSettings;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HikariTests {

    @Test
    public void testOpenConnectionWithMysqlUrl() throws SQLException, ClassNotFoundException {

        // Make sure that MySql driver class is loaded and registered at DriverManager
        Class.forName("com.mysql.cj.jdbc.Driver");

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("aws-proxy-jdbc:mysql://" + TestSettings.mysqlServerName + "/" + TestSettings.mysqlDatabase);
        ds.setUsername(TestSettings.mysqlUser);
        ds.setPassword(TestSettings.mysqlPassword);

        Connection conn = ds.getConnection();

        assertTrue(conn instanceof HikariProxyConnection);
        HikariProxyConnection hikariConn = (HikariProxyConnection)conn;

        assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
        ConnectionWrapper connWrapper = (ConnectionWrapper)hikariConn.unwrap(Connection.class);
        assertTrue(connWrapper.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

        assertTrue(conn.isValid(10));
        conn.close();
    }

    @Test
    public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException, ClassNotFoundException {

        // Make sure that MySql driver class is loaded and registered at DriverManager
        Class.forName("com.mysql.cj.jdbc.Driver");

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        HikariDataSource ds = new HikariDataSource();
        ds.setDataSourceClassName("software.aws.rds.jdbc.proxydriver.ds.ProxyDriverDataSource");
        ds.setUsername(TestSettings.mysqlUser);
        ds.setPassword(TestSettings.mysqlPassword);
        ds.addDataSourceProperty("targetDataSourceClassName", "com.mysql.cj.jdbc.MysqlDataSource");

        Properties targetDataSourceProps = new Properties();
        targetDataSourceProps.setProperty("serverName", TestSettings.mysqlServerName);
        targetDataSourceProps.setProperty("databaseName", TestSettings.mysqlDatabase);
        ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

        Connection conn = ds.getConnection();

        assertTrue(conn instanceof HikariProxyConnection);
        HikariProxyConnection hikariConn = (HikariProxyConnection)conn;

        assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
        ConnectionWrapper connWrapper = (ConnectionWrapper)hikariConn.unwrap(Connection.class);
        assertTrue(connWrapper.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

        assertTrue(conn.isValid(10));
        conn.close();
    }
}
