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
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithDataSourceClassNameAndCredentialProperties() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    targetDataSourceProps.setProperty("user", STANDARD_POSTGRES_USERNAME);
    targetDataSourceProps.setProperty("password", STANDARD_POSTGRES_PASSWORD);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection();

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingProtocol() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingServer() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingDatabase() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingUser() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection("", STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingPassword() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, ""));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingPropertyNames() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrl() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    ds.setTargetDataSourceProperties(targetDataSourceProps);
    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_POSTGRES_HOST + "/" + STANDARD_POSTGRES_DB);

    Connection conn = ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlWithCredentials() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    ds.setJdbcUrl(
        postgresProtocolPrefix
            + STANDARD_POSTGRES_HOST
            + ":" + STANDARD_POSTGRES_PORT + "/"
            + STANDARD_POSTGRES_DB
            + "?user=" + STANDARD_POSTGRES_USERNAME
            + "&password=" + STANDARD_POSTGRES_PASSWORD);

    Connection conn = ds.getConnection();

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlWithPort() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    ds.setJdbcUrl(
        postgresProtocolPrefix
        + STANDARD_POSTGRES_HOST
        + ":" + STANDARD_POSTGRES_PORT + "/"
        + STANDARD_POSTGRES_DB);

    Connection conn = ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlAndProperties() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("databaseName", "proxy-driver-test-db");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_POSTGRES_HOST + "/" + STANDARD_POSTGRES_DB);

    Connection conn = ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlMissingPropertyNames() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_POSTGRES_HOST + "/" + STANDARD_POSTGRES_DB);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameUsingUrlMissingDatabase() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    ds.setTargetDataSourceProperties(targetDataSourceProps);
    ds.setJdbcUrl(postgresProtocolPrefix + STANDARD_POSTGRES_HOST + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithUrl() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(
        DB_CONN_STR_PREFIX
        + STANDARD_POSTGRES_HOST
        + ":" + STANDARD_POSTGRES_PORT + "/"
        + STANDARD_POSTGRES_DB);

    Connection conn = ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithUrlWithCredentials() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(
        DB_CONN_STR_PREFIX
            + STANDARD_POSTGRES_HOST
            + ":" + STANDARD_POSTGRES_PORT + "/"
            + STANDARD_POSTGRES_DB
            + "?user=" + STANDARD_POSTGRES_USERNAME
            + "&password=" + STANDARD_POSTGRES_PASSWORD);

    Connection conn = ds.getConnection();

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithUrlMissingPort() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_POSTGRES_HOST + "/" + STANDARD_POSTGRES_DB);

    Connection conn = ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testConnectionWithUrlMissingDatabase() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_POSTGRES_HOST + ":" + STANDARD_POSTGRES_PORT + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithUrlMissingUser() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_POSTGRES_HOST + ":" + STANDARD_POSTGRES_PORT + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection("", STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithUrlMissingPassword() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setPortPropertyName("port");
    ds.setJdbcUrl(DB_CONN_STR_PREFIX + STANDARD_POSTGRES_HOST + ":" + STANDARD_POSTGRES_PORT + "/");

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, ""));
  }

  @Test
  public void testConnectionWithUrlMissingPropertyNames() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcUrl(
        DB_CONN_STR_PREFIX
        + STANDARD_POSTGRES_HOST
        + ":" + STANDARD_POSTGRES_PORT + "/"
        + STANDARD_POSTGRES_DB);

    assertThrows(
        PSQLException.class,
        () -> ds.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD));
  }

  @Test
  public void testConnectionWithDataSourceClassNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(postgresProtocolPrefix);
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_POSTGRES_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_POSTGRES_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Hashtable<String, Object> env = new Hashtable<String, Object>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    AwsWrapperDataSource dsFromJndiLookup = (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    assertNotNull(dsFromJndiLookup);

    assertNotSame(ds, dsFromJndiLookup);
    Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();
    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    Connection conn = dsFromJndiLookup.getConnection(STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);

    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));
    assertEquals(conn.getCatalog(), STANDARD_POSTGRES_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
