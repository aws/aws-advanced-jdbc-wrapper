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

package integration.container.standard.mysql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.awslabs.jdbc.wrapper.ConnectionWrapper;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
  @Disabled // TODO: check
  public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setDataSourceClassName("com.amazon.awslabs.jdbc.ds.ProxyDriverDataSource");
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
