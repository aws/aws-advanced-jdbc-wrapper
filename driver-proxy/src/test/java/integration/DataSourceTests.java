/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.util.TestSettings;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.ds.ProxyDriverDataSource;

@Disabled
public class DataSourceTests {

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassName()
      throws SQLException, ClassNotFoundException {

    // Make sure that MySql driver class is loaded and registered at DriverManager
    Class.forName("com.mysql.cj.jdbc.Driver");

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    ProxyDriverDataSource ds = new ProxyDriverDataSource();

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", TestSettings.mysqlServerName);
    targetDataSourceProps.setProperty("databaseName", TestSettings.mysqlDatabase);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

    assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMysqlUrl() throws SQLException, ClassNotFoundException {

    // Make sure that MySql driver class is loaded and registered at DriverManager
    Class.forName("com.mysql.cj.jdbc.Driver");

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setJdbcUrl(
        "jdbc:mysql://" + TestSettings.mysqlServerName + "/" + TestSettings.mysqlDatabase);

    Connection conn = ds.getConnection(TestSettings.mysqlUser, TestSettings.mysqlPassword);

    assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithPostgresqlDataSourceClassName()
      throws SQLException, ClassNotFoundException {

    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    ProxyDriverDataSource ds = new ProxyDriverDataSource();

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", TestSettings.postgresqlServerName);
    targetDataSourceProps.setProperty("databaseName", TestSettings.postgresqlDatabase);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn =
        ds.getConnection(TestSettings.postgresqlUser, TestSettings.postgresqlPassword);

    assertTrue(conn instanceof org.postgresql.PGConnection);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithPostgresqlUrl() throws SQLException, ClassNotFoundException {

    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setJdbcUrl(
        "jdbc:postgresql://"
            + TestSettings.postgresqlServerName
            + "/"
            + TestSettings.postgresqlDatabase);

    Connection conn =
        ds.getConnection(TestSettings.postgresqlUser, TestSettings.postgresqlPassword);

    assertTrue(conn instanceof org.postgresql.PGConnection);

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
