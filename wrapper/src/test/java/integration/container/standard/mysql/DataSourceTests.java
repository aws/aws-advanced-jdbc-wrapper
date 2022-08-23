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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import integration.util.SimpleJndiContextFactory;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class DataSourceTests extends StandardMysqlBaseTest {

  @BeforeAll
  public static void setup() throws SQLException, ClassNotFoundException {
    Class.forName("org.mariadb.jdbc.Driver");
  }

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_MYSQL_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMysqlUrl() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");
    ds.setJdbcUrl("jdbc:mysql://" + STANDARD_MYSQL_HOST + "/" + STANDARD_MYSQL_DB);

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_MYSQL_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_MYSQL_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Hashtable<String, Object> env = new Hashtable<String, Object>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    AwsWrapperDataSource dsFromJndiLookup = (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    if (dsFromJndiLookup == null) {
      fail();
    }

    assertNotSame(ds, dsFromJndiLookup);
    Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();
    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    Connection conn = dsFromJndiLookup.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMariaDbDataSourceClassName() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setUrlPropertyName("url");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "url",
        "jdbc:mysql://" + STANDARD_MYSQL_HOST + "/" + STANDARD_MYSQL_DB + "?permitMysqlScheme");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(org.mariadb.jdbc.Connection.class));
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMariaDbUrl() throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcUrl("jdbc:mariadb://" + STANDARD_MYSQL_HOST + "/" + STANDARD_MYSQL_DB + "?permitMysqlScheme");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    Connection conn = ds.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(org.mariadb.jdbc.Connection.class));
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testOpenConnectionWithMariaDbDataSourceClassNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setUrlPropertyName("url");
    ds.setUserPropertyName("user");
    ds.setPasswordPropertyName("password");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "url",
        "jdbc:mysql://" + STANDARD_MYSQL_HOST + "/" + STANDARD_MYSQL_DB + "?permitMysqlScheme");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Hashtable<String, Object> env = new Hashtable<String, Object>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    AwsWrapperDataSource dsFromJndiLookup = (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    if (dsFromJndiLookup == null) {
      fail();
    }

    assertNotSame(ds, dsFromJndiLookup);
    Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();
    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    Connection conn = dsFromJndiLookup.getConnection(STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(org.mariadb.jdbc.Connection.class));
    assertEquals(conn.getCatalog(), STANDARD_MYSQL_DB);

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
