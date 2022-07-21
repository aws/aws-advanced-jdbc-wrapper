/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.standard.mysql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;

public class HikariTests extends StandardMysqlBaseTest {

  @Test
  public void testOpenConnectionWithMysqlUrl() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(getUrl());
    ds.setUsername(STANDARD_MYSQL_USERNAME);
    ds.setPassword(STANDARD_MYSQL_PASSWORD);

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  @Disabled //TODO: check
  public void testOpenConnectionWithMysqlDataSourceClassName()
      throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setDataSourceClassName("software.aws.rds.jdbc.proxydriver.ds.ProxyDriverDataSource");
    ds.setUsername(STANDARD_MYSQL_USERNAME);
    ds.setPassword(STANDARD_MYSQL_PASSWORD);
    ds.addDataSourceProperty("targetDataSourceClassName", "com.mysql.cj.jdbc.MysqlDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_MYSQL_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_MYSQL_DB);
    ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
