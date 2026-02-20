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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.cache.CacheConnection;
import software.amazon.jdbc.plugin.cache.CachedResultSet;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.VALKEY_CACHE)
@DisableOnTestFeature({TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
@Order(26)

public class DataRemoteCachePluginTests {

  @AfterEach
  public void afterEach() throws Exception {
    // Clear the static connection pool registry to prevent test pollution
    CacheConnection.clearEndpointPoolRegistry();
  }

  @TestTemplate
  public void testSuccessfulQueryCachingWithAuth() throws SQLException {
    try (Connection conn = createCacheEnabledConnection(null, 0)) {
      String tableName = "testTable";
      createTestTable(conn, tableName);

      // Insert test data
      conn.createStatement().execute("insert into " + tableName + " (id, name) values (1, 'name1')");
      conn.createStatement().execute("/*+ CACHE_PARAM(ttl=300s) */ insert into " + tableName + " (id, name) values "
          + "(2, 'name2')");
      conn.createStatement().execute("/*+ CACHE_PARAM(ttl=300s) */ insert into " + tableName + " (id, name) values "
          + "(3, 'name3')");

      // Query without cache hint - verify initial data
      Statement statement = conn.createStatement();
      ResultSet resultSet = statement.executeQuery("select id, name from " + tableName);
      validateRow(resultSet, 1, "name1");
      validateRow(resultSet, 2, "name2");
      validateRow(resultSet, 3, "name3");
      assertFalse(resultSet.next()); // no more data

      // Update data
      conn.createStatement().execute("update " + tableName + " set id=id*10");
      conn.createStatement().execute("/* +CACHE_PARAM(ttl=300s) */ update " + tableName + " set name=concat('name', "
          + "id)");

      // Query without cache hint - should hit database
      Statement testStatement = conn.createStatement();
      ResultSet testResultSet = testStatement.executeQuery("select id, name from " + tableName);
      assertFalse(testResultSet.isWrapperFor(CachedResultSet.class));
      verifyResultSetForTestTable(testResultSet);

      // Query with cache hint - populate cache
      ResultSet cachedResultSet = executeQueryWithCacheHint(testStatement, "select id, name from " + tableName);
      assertTrue(cachedResultSet.isWrapperFor(CachedResultSet.class));
      verifyResultSetForTestTable(cachedResultSet);

      // Query again with cache hint - should come from cache
      ResultSet cachedResultSet2 = executeQueryWithCacheHint(testStatement, "select id, name from " + tableName);
      assertTrue(cachedResultSet2.isWrapperFor(CachedResultSet.class));
      verifyResultSetForTestTable(cachedResultSet2);

      // Cache the result of an empty query
      ResultSet cachedResultSet3 = executeQueryWithCacheHint(testStatement, "select id, name from " + tableName + " "
          + "where id = 4");
      assertTrue(cachedResultSet3.isWrapperFor(CachedResultSet.class));
      assertFalse(cachedResultSet3.next());

      // Insert new data
      conn.createStatement().execute("insert into " + tableName + " (id, name) values (4, 'name4')");

      // Previously cached empty result is fetched (cache hit)
      ResultSet cachedResultSet4 = executeQueryWithCacheHint(testStatement, "select id, name from " + tableName + " "
          + "where id = 4");
      assertTrue(cachedResultSet4.isWrapperFor(CachedResultSet.class));
      assertFalse(cachedResultSet4.next()); // Still empty from cache

      // Cleanup
      conn.createStatement().execute("delete from " + tableName);
      dropTestTable(conn, tableName);
    }
  }

  @TestTemplate
  public void testWrongAuthFallsBackToDatabase() throws SQLException {
    try (Connection conn = createCacheEnabledConnection("wrong-password", 0)) {
      String tableName = "testTableAuth";
      createTestTable(conn, tableName);
      conn.createStatement().execute("insert into " + tableName + " (id, name) values (1, 'test1')");

      Statement statement = conn.createStatement();

      // First query with cache hint - should fall back to database
      ResultSet rs1 = executeQueryWithCacheHint(statement, "select id, name from " + tableName + " where id = 1");
      validateRow(rs1, 1, "test1");
      assertFalse(rs1.next());

      // Second query - should still hit database (not cache)
      ResultSet rs2 = executeQueryWithCacheHint(statement, "select id, name from " + tableName + " where id = 1");
      validateRow(rs2, 1, "test1");
      assertFalse(rs2.next());

      // Delete the data
      conn.createStatement().execute("delete from " + tableName + " where id = 1");

      // Query after deletion - should return empty (proves cache wasn't used)
      ResultSet rs3 = executeQueryWithCacheHint(statement, "select id, name from " + tableName + " where id = 1");
      assertTrue(rs3.isWrapperFor(CachedResultSet.class));
      assertFalse(rs3.next()); // Should be empty

      dropTestTable(conn, tableName);
    }
  }

  @TestTemplate
  public void testNoAuthConnection() throws SQLException {
    // Use the second Valkey instance (no-auth)
    List<TestInstanceInfo> cacheInstances = TestEnvironment.getCurrent().getInfo().getValkeyServerInfo().getInstances();
    if (cacheInstances.size() < 2) {
      return; // Skip test if no-auth instance not available
    }

    try (Connection conn = createCacheEnabledConnection(null, 1)) {
      String tableName = "testTableNoAuth";
      createTestTable(conn, tableName);
      conn.createStatement().execute("insert into " + tableName + " (id, name) values (1, 'noauth1')");

      Statement statement = conn.createStatement();

      // First query with cache hint - should work without auth
      ResultSet rs1 = executeQueryWithCacheHint(statement, "select id, name from " + tableName + " where id = 1");
      assertTrue(rs1.isWrapperFor(CachedResultSet.class));
      validateRow(rs1, 1, "noauth1");
      assertFalse(rs1.next());

      // Second query - should come from cache
      ResultSet rs2 = executeQueryWithCacheHint(statement, "select id, name from " + tableName + " where id = 1");
      assertTrue(rs2.isWrapperFor(CachedResultSet.class));
      validateRow(rs2, 1, "noauth1");
      assertFalse(rs2.next());

      // Delete the data
      conn.createStatement().execute("delete from " + tableName + " where id = 1");

      // Query after deletion - should return cached data
      ResultSet rs3 = executeQueryWithCacheHint(statement, "select id, name from " + tableName + " where id = 1");
      assertTrue(rs3.isWrapperFor(CachedResultSet.class));
      validateRow(rs3, 1, "noauth1"); // Should still have data from cache
      assertFalse(rs3.next());

      dropTestTable(conn, tableName);
    }
  }

  private void verifyResultSetForTestTable(ResultSet testResultSet) throws SQLException {
    assertTrue(testResultSet.next());
    assertEquals(10, testResultSet.getObject(1));
    assertEquals("name10", testResultSet.getObject(2));
    assertTrue(testResultSet.next());
    assertEquals(20, testResultSet.getObject(1));
    assertEquals("name20", testResultSet.getObject(2));
    assertTrue(testResultSet.next());
    assertEquals(30, testResultSet.getObject(1));
    assertEquals("name30", testResultSet.getObject(2));
  }

  private Properties getCacheEnabledProperties(@Nullable String cachePassword, int cacheInstanceIndex) {
    List<TestInstanceInfo> cacheInstances = TestEnvironment.getCurrent().getInfo().getValkeyServerInfo().getInstances();
    TestInstanceInfo instance = cacheInstances.get(cacheInstanceIndex);
    final String cacheEndpoint = instance.getHost() + ":" + instance.getPort();

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "dataRemoteCache");
    props.setProperty("cacheEndpointAddrRw", cacheEndpoint);
    props.setProperty("cacheUseSSL", "false");

    // Only set credentials if using auth-enabled instance (index 0)
    if (cacheInstanceIndex == 0) {
      props.setProperty("cacheUsername", TestEnvironment.getCurrent().getInfo().getValkeyServerUsername());
      if (cachePassword != null) {
        props.setProperty("cachePassword", cachePassword);
      } else {
        props.setProperty("cachePassword", TestEnvironment.getCurrent().getInfo().getValkeyServerPassword());
      }
    }
    // For no-auth instance (index 1), don't set username/password
    return props;
  }

  private Connection createCacheEnabledConnection(@Nullable String cachePassword,
      int cacheInstanceIndex) throws SQLException {
    return DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(),
        getCacheEnabledProperties(cachePassword, cacheInstanceIndex));
  }

  private void createTestTable(Connection conn, String tableName) throws SQLException {
    conn.createStatement().execute("drop table if exists " + tableName);
    conn.createStatement().execute(
        "create table " + tableName + " (id int not null primary key, name varchar(100))");
  }

  private void dropTestTable(Connection conn, String tableName) throws SQLException {
    conn.createStatement().execute("drop table " + tableName);
  }

  private ResultSet executeQueryWithCacheHint(Statement statement, String query) throws SQLException {
    return statement.executeQuery("/* +CACHE_PARAM(ttl=300s) */ " + query);
  }

  private void validateRow(ResultSet rs, int expectedId, String expectedName) throws SQLException {
    assertTrue(rs.next());
    assertEquals(expectedId, rs.getObject(1));
    assertEquals(expectedName, rs.getObject(2));
  }
}
