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
import static software.amazon.jdbc.plugin.cache.CacheConnection.CACHE_RW_ENDPOINT_ADDR;

import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnTestFeature;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.cache.CacheConnection;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.VALKEY_CACHE)
@Order(25)
@Tag("caching")

public class SpringCachingTests {

  @AfterEach
  public void afterEach() throws Exception {
    // Clear the static connection pool registry to prevent test pollution
    CacheConnection.clearEndpointPoolRegistry();
  }

  @TestTemplate
  public void testQueryCachingWithAuth() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource(null, 0, true, false));

    // Query from the database directly and populate the cache from database result
    int rnd = new Random().nextInt(100);
    List<Object> res = executeQueryWithCacheHint(jdbcTemplate, rnd);
    assertEquals(1, res.size());
    assertEquals(rnd, res.get(0));

    // Query the same result again from the cache
    List<Object> res2 = executeQueryWithCacheHint(jdbcTemplate, rnd);
    assertEquals(1, res2.size());
    assertEquals(rnd, res2.get(0));
  }

  @TestTemplate
  public void testWrongAuthFallsBackToDatabase() throws Exception {
    // Use WRONG cache credentials
    JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource("wrong-password", 0, true, false));

    // Use randomness to avoid collision with other tests
    int rnd = new Random().nextInt(100) + 1000;

    // First query - should fall back to database (cache auth fails)
    List<Object> res1 = executeQueryWithCacheHint(jdbcTemplate, rnd);
    assertEquals(1, res1.size());
    assertEquals(rnd, res1.get(0));

    // Second query - should still hit database (not cache)
    List<Object> res2 = executeQueryWithCacheHint(jdbcTemplate, rnd);
    assertEquals(1, res2.size());
    assertEquals(rnd, res2.get(0));

    // Now query with a different value - if cache was working, this would return the old value
    // But since cache auth failed, it should return the new value from DB
    int newRnd = rnd + 500;
    List<Object> res3 = executeQueryWithCacheHint(jdbcTemplate, newRnd);
    assertEquals(1, res3.size());
    assertEquals(newRnd, res3.get(0)); // Should get new value, proving cache wasn't used
  }

  @TestTemplate
  public void testNoAuthConnection() {
    // Use the second Valkey instance (no-auth)
    List<TestInstanceInfo> cacheInstances = TestEnvironment.getCurrent().getInfo().getValkeyServerInfo().getInstances();
    if (cacheInstances.size() < 2) {
      return; // Skip test if no-auth instance not available
    }

    // Use no-auth instance (index 1), no credentials
    JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource(null, 1, false, false));

    // Use randomness to avoid collision with other tests
    int rnd = new Random().nextInt(100) + 2000;

    // First query - should work without auth and populate cache
    List<Object> res1 = executeQueryWithCacheHint(jdbcTemplate, rnd);
    assertEquals(1, res1.size());
    assertEquals(rnd, res1.get(0));

    // Allow async write to complete
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Second query - should come from cache
    List<Object> res2 = executeQueryWithCacheHint(jdbcTemplate, rnd);
    assertEquals(1, res2.size());
    assertEquals(rnd, res2.get(0));

    // Query with different value - should get new value (proves cache is working correctly)
    int newRnd = rnd + 100;
    List<Object> res3 = executeQueryWithCacheHint(jdbcTemplate, newRnd);
    assertEquals(1, res3.size());
    assertEquals(newRnd, res3.get(0));

    // Query original value again - should still be cached
    List<Object> res4 = executeQueryWithCacheHint(jdbcTemplate, rnd);
    assertEquals(1, res4.size());
    assertEquals(rnd, res4.get(0));
  }

  @TestTemplate
  public void testQueryCachingWithTLS() {
    List<TestInstanceInfo> cacheInstances = TestEnvironment.getCurrent().getInfo().getValkeyServerInfo().getInstances();
    if (cacheInstances.size() < 3) {
      return;
    }

    JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource(null, 2, true, true));
    String tableName = "springTestTLS";
    createTestTable(jdbcTemplate, tableName);

    jdbcTemplate.execute("insert into " + tableName + " (id, name) values (1, 'tls1')");

    // First query - populate cache
    List<Object[]> res1 = executeTableQueryWithCacheHint(jdbcTemplate, tableName, 1);
    assertEquals(1, res1.size());
    assertEquals(1, res1.get(0)[0]);
    assertEquals("tls1", res1.get(0)[1]);

    // Allow async cache write to complete (TLS handshake on first write)
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Second query - should come from cache
    List<Object[]> res2 = executeTableQueryWithCacheHint(jdbcTemplate, tableName, 1);
    assertEquals(1, res2.size());
    assertEquals("tls1", res2.get(0)[1]);

    // Delete from database
    jdbcTemplate.execute("delete from " + tableName + " where id = 1");

    // Query after deletion - should return cached data
    List<Object[]> res3 = executeTableQueryWithCacheHint(jdbcTemplate, tableName, 1);
    assertEquals(1, res3.size());
    assertEquals("tls1", res3.get(0)[1]);

    dropTestTable(jdbcTemplate, tableName);
  }

  @TestTemplate
  public void testTLSWithoutAuth() {
    List<TestInstanceInfo> cacheInstances = TestEnvironment.getCurrent().getInfo().getValkeyServerInfo().getInstances();
    if (cacheInstances.size() < 4) {
      return;
    }

    JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource(null, 3, false, true));
    String tableName = "springTestTLSNoAuth";
    createTestTable(jdbcTemplate, tableName);

    jdbcTemplate.execute("insert into " + tableName + " (id, name) values (1, 'tlsnoauth1')");

    // First query - populate cache
    List<Object[]> res1 = executeTableQueryWithCacheHint(jdbcTemplate, tableName, 1);
    assertEquals(1, res1.size());
    assertEquals("tlsnoauth1", res1.get(0)[1]);

    // Allow async cache write to complete (TLS handshake on first write)
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Second query - should come from cache
    List<Object[]> res2 = executeTableQueryWithCacheHint(jdbcTemplate, tableName, 1);
    assertEquals(1, res2.size());
    assertEquals("tlsnoauth1", res2.get(0)[1]);

    // Delete from database
    jdbcTemplate.execute("delete from " + tableName + " where id = 1");

    // Query after deletion - should return cached data
    List<Object[]> res3 = executeTableQueryWithCacheHint(jdbcTemplate, tableName, 1);
    assertEquals(1, res3.size());
    assertEquals("tlsnoauth1", res3.get(0)[1]);

    dropTestTable(jdbcTemplate, tableName);
  }

  private DataSource getDataSource(@Nullable String cachePassword, int cacheInstanceIndex,
      boolean includeCredentials, boolean useTls) {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("software.amazon.jdbc.Driver");
    dataSource.setUrl(ConnectionStringHelper.getWrapperUrl());
    dataSource.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    dataSource.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.LOGGER_LEVEL.name, "ALL");
    props.setProperty(PropertyDefinition.PLUGINS.name, "remoteQueryCache");

    // Fetch the cache server information
    List<TestInstanceInfo> cacheInstances = TestEnvironment.getCurrent().getInfo().getValkeyServerInfo().getInstances();
    final String cacheEndpoint =
        cacheInstances.get(cacheInstanceIndex).getHost() + ":" + cacheInstances.get(cacheInstanceIndex).getPort();
    props.setProperty(CACHE_RW_ENDPOINT_ADDR.name, cacheEndpoint);
    props.setProperty("cacheUseSSL", String.valueOf(useTls));
    if (useTls) {
      props.setProperty("cacheTlsCaCertPath", "test/resources/certs/ca.crt");
    }

    // Isolate cache keys per driver iteration and test suite to prevent cross-pollution
    String driverPrefix = "S" + TestEnvironment.getCurrent().getCurrentDriver().name();
    props.setProperty("cacheKeyPrefix", driverPrefix);

    // Only set credentials if requested (for auth-enabled instance)
    if (includeCredentials) {
      props.setProperty("cacheUsername", TestEnvironment.getCurrent().getInfo().getValkeyServerUsername());
      if (cachePassword != null) {
        props.setProperty("cachePassword", cachePassword);
      } else {
        props.setProperty("cachePassword", TestEnvironment.getCurrent().getInfo().getValkeyServerPassword());
      }
    }

    dataSource.setConnectionProperties(props);
    return dataSource;
  }

  private List<Object> executeQueryWithCacheHint(JdbcTemplate jdbcTemplate, int value) {
    String sqlFind = "/*+ CACHE_PARAM(ttl=60s) */ SELECT ?";
    return jdbcTemplate.query(
        con -> {
          PreparedStatement ps = con.prepareStatement(sqlFind);
          ps.setString(1, Integer.toString(value));
          return ps;
        },
        (rs, rowNum) -> rs.getInt(1));
  }

  private void createTestTable(JdbcTemplate jdbcTemplate, String tableName) {
    jdbcTemplate.execute("drop table if exists " + tableName);
    jdbcTemplate.execute("create table " + tableName + " (id int not null primary key, name varchar(100))");
  }

  private void dropTestTable(JdbcTemplate jdbcTemplate, String tableName) {
    jdbcTemplate.execute("drop table " + tableName);
  }

  private List<Object[]> executeTableQueryWithCacheHint(JdbcTemplate jdbcTemplate, String tableName, int id) {
    String sql = "/*+ CACHE_PARAM(ttl=60s) */ SELECT id, name FROM " + tableName + " WHERE id = ?";
    return jdbcTemplate.query(
        con -> {
          PreparedStatement ps = con.prepareStatement(sql);
          ps.setInt(1, id);
          return ps;
        },
        (rs, rowNum) -> new Object[]{rs.getInt(1), rs.getString(2)});
  }
}
