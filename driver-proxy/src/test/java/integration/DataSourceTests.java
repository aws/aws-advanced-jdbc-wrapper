/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.util.TestSettings;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.ds.ProxyDriverDataSource;

@Disabled
public class DataSourceTests {

  @BeforeAll
  public static void setup() throws SQLException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");
    Class.forName("org.mariadb.jdbc.Driver");

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }
  }

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", TestSettings.mysqlServerName);
    targetDataSourceProps.setProperty("databaseName", TestSettings.mysqlDatabase);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

    assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);
    assertEquals(conn.getCatalog(), TestSettings.mysqlDatabase);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMysqlUrl() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setJdbcUrl(
        "jdbc:mysql://" + TestSettings.mysqlServerName + "/" + TestSettings.mysqlDatabase);

    Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

    assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);
    assertEquals(conn.getCatalog(), TestSettings.mysqlDatabase);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMariaDbDataSourceClassName() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();

    ds.setTargetDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setUrlPropertyName("url");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "url",
        "jdbc:mysql://" + TestSettings.mysqlServerName + "/" + TestSettings.mysqlDatabase + "?permitMysqlScheme");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

    assertTrue(conn instanceof org.mariadb.jdbc.Connection);
    assertEquals(conn.getCatalog(), TestSettings.mysqlDatabase);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMariaDbUrl() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setJdbcUrl(
        "jdbc:mysql://" + TestSettings.mysqlServerName + "/" + TestSettings.mysqlDatabase + "?permitMysqlScheme");

    Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

    assertTrue(conn instanceof org.mariadb.jdbc.Connection);
    assertEquals(conn.getCatalog(), TestSettings.mysqlDatabase);

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
