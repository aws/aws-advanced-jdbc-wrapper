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

package integration.container.aurora.mysql.mysqldriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.util.SimpleJndiContextFactory;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class AuroraMysqlDataSourceTest extends MysqlAuroraMysqlBaseTest {
  public static String mysqlProtocolPrefix = "jdbc:mysql://";
  
  @Test
  public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(mysqlProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    
    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", MYSQL_CLUSTER_URL);
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn = ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD)) {
      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameAndCredentialProperties() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(mysqlProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", MYSQL_CLUSTER_URL);
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    targetDataSourceProps.setProperty("user", AURORA_MYSQL_USERNAME);
    targetDataSourceProps.setProperty("password", AURORA_MYSQL_PASSWORD);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingProtocol() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", MYSQL_CLUSTER_URL);
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingServer() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(mysqlProtocolPrefix);
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingUser() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(mysqlProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", MYSQL_CLUSTER_URL);
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection("", AURORA_MYSQL_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingPassword() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(mysqlProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", MYSQL_CLUSTER_URL);
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(AURORA_MYSQL_USERNAME, ""));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingPropertyNames() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(mysqlProtocolPrefix);

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", MYSQL_CLUSTER_URL);
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD));
  }

  @Test
  public void testOpenConnectionWithMysqlUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setJdbcUrl("jdbc:mysql://" + MYSQL_CLUSTER_URL + "/" + AURORA_MYSQL_DB);

    try (final Connection conn = ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD)) {
      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlWithCredentials() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    ds.setJdbcUrl(
        mysqlProtocolPrefix
            + MYSQL_CLUSTER_URL
            + ":" + AURORA_MYSQL_PORT + "/"
            + AURORA_MYSQL_DB
            + "?user=" + AURORA_MYSQL_USERNAME
            + "&password=" + AURORA_MYSQL_PASSWORD);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlWithPort() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    ds.setJdbcUrl(mysqlProtocolPrefix + MYSQL_CLUSTER_URL + ":" + AURORA_MYSQL_PORT
        + "/" + AURORA_MYSQL_DB);

    try (final Connection conn = ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD)) {
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlAndProperties() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("databaseName", "proxy-driver-test-db");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    ds.setJdbcUrl(mysqlProtocolPrefix + MYSQL_CLUSTER_URL + "/" + AURORA_MYSQL_DB);

    try (final Connection conn = ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD)) {
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlMissingPropertyNames() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    ds.setJdbcUrl(mysqlProtocolPrefix + MYSQL_CLUSTER_URL + "/" + AURORA_MYSQL_DB);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD));
  }

  @Test
  public void testConnectionWithUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL + ":" + AURORA_MYSQL_PORT + "/" + AURORA_MYSQL_DB);

    try (final Connection conn = ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD)) {
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithUrlWithCredentials() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(
        DB_CONN_STR_PREFIX
            + MYSQL_CLUSTER_URL
            + ":" + AURORA_MYSQL_PORT + "/"
            + AURORA_MYSQL_DB
            + "?user=" + AURORA_MYSQL_USERNAME
            + "&password=" + AURORA_MYSQL_PASSWORD);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithUrlMissingPort() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL + "/" + AURORA_MYSQL_DB);

    try (final Connection conn = ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD)) {
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithUrlMissingUser() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL + ":" + AURORA_MYSQL_PORT + "/");

    assertThrows(
        SQLException.class,
        () -> ds.getConnection("", AURORA_MYSQL_PASSWORD));
  }

  @Test
  public void testConnectionWithUrlMissingPassword() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL + ":" + AURORA_MYSQL_PORT + "/");

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(AURORA_MYSQL_USERNAME, ""));
  }

  @Test
  public void testConnectionWithUrlMissingPropertyNames() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL + ":" + AURORA_MYSQL_PORT + "/" + AURORA_MYSQL_DB);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD));
  }

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", MYSQL_CLUSTER_URL);
    targetDataSourceProps.setProperty("databaseName", AURORA_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    final Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    final InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    final AwsWrapperDataSource dsFromJndiLookup = (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    assertNotNull(dsFromJndiLookup);

    assertNotSame(ds, dsFromJndiLookup);
    final Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();
    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    try (final Connection conn = dsFromJndiLookup.getConnection(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD)) {
      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), AURORA_MYSQL_DB);

      assertTrue(conn.isValid(10));
    }
  }
}
