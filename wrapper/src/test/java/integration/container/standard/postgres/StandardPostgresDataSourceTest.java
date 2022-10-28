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

package integration.container.standard.postgres;

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
import org.postgresql.util.PSQLException;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class StandardPostgresDataSourceTest extends StandardPostgresBaseTest {

  public static String postgresProtocolPrefix = "jdbc:postgresql://";

  @Test
  public void testConnectionWithDataSourceClassName() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameAndCredentialProperties() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    targetDataSourceProps.setProperty("user", STANDARD_USERNAME);
    targetDataSourceProps.setProperty("password", STANDARD_PASSWORD);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingProtocol() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingServer() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingDatabase() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingUser() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection("", STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingPassword() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, ""));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingPropertyNames() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    ds.setTargetDataSourceProperties(targetDataSourceProps);
    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_WRITER + "/" + STANDARD_DB);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlWithCredentials() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    ds.setJdbcUrl(
        postgresProtocolPrefix
            + STANDARD_WRITER
            + ":" + STANDARD_PORT + "/"
            + STANDARD_DB
            + "?user=" + STANDARD_USERNAME
            + "&password=" + STANDARD_PASSWORD);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlWithPort() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setPortPropertyName("port");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    ds.setJdbcUrl(
        postgresProtocolPrefix
        + STANDARD_WRITER
        + ":" + STANDARD_PORT + "/"
        + STANDARD_DB);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlAndProperties() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("databaseName", "proxy-driver-test-db");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_WRITER + "/" + STANDARD_DB);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlMissingPropertyNames() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_WRITER + "/" + STANDARD_DB);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlMissingDatabase() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    ds.setTargetDataSourceProperties(targetDataSourceProps);
    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_WRITER + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(
        DB_CONN_STR_PREFIX
        + STANDARD_WRITER
        + ":" + STANDARD_PORT + "/"
        + STANDARD_DB);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithUrlWithCredentials() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(
        DB_CONN_STR_PREFIX
            + STANDARD_WRITER
            + ":" + STANDARD_PORT + "/"
            + STANDARD_DB
            + "?user=" + STANDARD_USERNAME
            + "&password=" + STANDARD_PASSWORD);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithUrlMissingPort() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_WRITER + "/" + STANDARD_DB);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testConnectionWithUrlMissingDatabase() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_WRITER + ":" + STANDARD_PORT + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithUrlMissingUser() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_WRITER + ":" + STANDARD_PORT + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection("", STANDARD_PASSWORD));
  }

  @Test
  public void testConnectionWithUrlMissingPassword() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_WRITER + ":" + STANDARD_PORT + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_USERNAME, ""));
  }

  @Test
  public void testConnectionWithDataSourceClassNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_WRITER);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
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

    try (final Connection conn =
             dsFromJndiLookup.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }
}
