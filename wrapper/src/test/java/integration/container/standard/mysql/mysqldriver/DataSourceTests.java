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

package integration.container.standard.mysql.mysqldriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class DataSourceTests extends MysqlStandardMysqlBaseTest {

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassName() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_HOST);
    targetDataSourceProps.setProperty("databaseName", STANDARD_DB);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testOpenConnectionWithMysqlUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcUrl("jdbc:mysql://" + STANDARD_HOST + "/" + STANDARD_DB);

    try (final Connection conn = ds.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testOpenConnectionWithMysqlDataSourceClassNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", STANDARD_HOST);
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

    try (final Connection conn = dsFromJndiLookup.getConnection(STANDARD_USERNAME, STANDARD_PASSWORD)) {
      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));
      assertEquals(conn.getCatalog(), STANDARD_DB);

      assertTrue(conn.isValid(10));
    }
  }
}
