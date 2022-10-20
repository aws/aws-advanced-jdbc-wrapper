/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.container.standard.mysql.mariadbdriver;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class HikariTests extends MariadbStandardMysqlBaseTest {

  @Test
  public void testOpenConnectionWithMysqlUrl() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(getUrlMariadbDriver());
    ds.setUsername(STANDARD_USERNAME);
    ds.setPassword(STANDARD_PASSWORD);

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(org.mariadb.jdbc.Connection.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());

    // Configure the connection pool:
    ds.setUsername(STANDARD_USERNAME);
    ds.setPassword(STANDARD_PASSWORD);

    // Configure AwsWrapperDataSource:
    ds.addDataSourceProperty("jdbcProtocol", "jdbc:mysql:");
    ds.addDataSourceProperty("databasePropertyName", "databaseName");
    ds.addDataSourceProperty("portPropertyName", "port");
    ds.addDataSourceProperty("serverPropertyName", "serverName");

    // Specify the driver-specific data source for AwsWrapperDataSource:
    ds.addDataSourceProperty("targetDataSourceClassName", "org.mariadb.jdbc.MariaDbDataSource");

    // Configuring MysqlDataSource:
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    targetDataSourceProps.setProperty(
        "url", "jdbc:mysql://" + STANDARD_HOST + ":" + STANDARD_PORT + "/" + STANDARD_DB + "?permitMysqlScheme");
    ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(org.mariadb.jdbc.Connection.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
