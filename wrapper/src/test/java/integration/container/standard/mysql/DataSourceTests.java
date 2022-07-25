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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.awslabs.jdbc.ds.ProxyDriverDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DataSourceTests extends StandardMysqlBaseTest {

  @BeforeAll
  public static void setup() throws SQLException, ClassNotFoundException {
    Class.forName("org.mariadb.jdbc.Driver");
  }

  @Test
  @Disabled // TODO: check
  public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_MYSQL_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  @Disabled // TODO: check
  public void testOpenConnectionWithMysqlUrl() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setJdbcUrl("jdbc:mysql://" + STANDARD_MYSQL_HOST + "/" + STANDARD_MYSQL_DB);

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  @Disabled // TODO: check
  public void testOpenConnectionWithMariaDbDataSourceClassName() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();

    ds.setTargetDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setUrlPropertyName("url");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "url",
        "jdbc:mysql://" + STANDARD_MYSQL_HOST + "/" + STANDARD_MYSQL_DB + "?permitMysqlScheme");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof org.mariadb.jdbc.Connection);
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  @Disabled // TODO: check
  public void testOpenConnectionWithMariaDbUrl() throws SQLException {
    ProxyDriverDataSource ds = new ProxyDriverDataSource();
    ds.setJdbcUrl(
        "jdbc:mysql://" + STANDARD_MYSQL_HOST + "/" + STANDARD_MYSQL_DB + "?permitMysqlScheme");

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof org.mariadb.jdbc.Connection);
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
