package software.aws.rds.jdbc.proxydriver;

import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.ds.ProxyDriverDataSource;
import software.aws.rds.jdbc.proxydriver.util.TestSettings;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataSourceTests {

    @Test
    public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException, ClassNotFoundException {

        // Make sure that MySql driver class is loaded and registered at DriverManager
        Class.forName("com.mysql.cj.jdbc.Driver");

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        ProxyDriverDataSource ds = new ProxyDriverDataSource();

        ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

        Properties targetDataSourceProps = new Properties();
        targetDataSourceProps.setProperty("serverName", TestSettings.mysqlServerName);
        ds.setTargetDataSourceProperties(targetDataSourceProps);

        Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

        assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);

        conn.close();
    }

    @Test
    public void testOpenConnectionWithMysqlUrl() throws SQLException, ClassNotFoundException {

        // Make sure that MySql driver class is loaded and registered at DriverManager
        Class.forName("com.mysql.cj.jdbc.Driver");

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        ProxyDriverDataSource ds = new ProxyDriverDataSource();
        ds.setJdbcUrl("jdbc:mysql://" + TestSettings.mysqlServerName + "/");

        Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

        assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);

        conn.close();
    }

    @Test
    public void testOpenConnectionWithPostgresqlDataSourceClassName() throws SQLException, ClassNotFoundException {

        if(!org.postgresql.Driver.isRegistered()) {
            org.postgresql.Driver.register();
        }

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        ProxyDriverDataSource ds = new ProxyDriverDataSource();

        ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

        Properties targetDataSourceProps = new Properties();
        targetDataSourceProps.setProperty("serverName", TestSettings.postgresqlServerName);
        targetDataSourceProps.setProperty("databaseName", TestSettings.postgresqlDatabase);
        ds.setTargetDataSourceProperties(targetDataSourceProps);

        Connection conn = ds.getConnection(TestSettings.postgresqlUser, TestSettings.postgresqlPassword);

        assertTrue(conn instanceof org.postgresql.PGConnection);

        conn.close();
    }

    @Test
    public void testOpenConnectionWithPostgresqlUrl() throws SQLException, ClassNotFoundException {

        if(!org.postgresql.Driver.isRegistered()) {
            org.postgresql.Driver.register();
        }

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        ProxyDriverDataSource ds = new ProxyDriverDataSource();
        ds.setJdbcUrl("jdbc:postgresql://" + TestSettings.postgresqlServerName + "/" + TestSettings.postgresqlDatabase);

        Connection conn = ds.getConnection(TestSettings.postgresqlUser, TestSettings.postgresqlPassword);

        assertTrue(conn instanceof org.postgresql.PGConnection);

        conn.close();
    }
}
