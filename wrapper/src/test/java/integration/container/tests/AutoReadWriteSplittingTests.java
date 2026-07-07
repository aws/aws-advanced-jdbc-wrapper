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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnNumOfInstances(min = 2)
@EnableOnDatabaseEngineDeployment({
    DatabaseEngineDeployment.AURORA,
    DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER,
    DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@MakeSureFirstInstanceWriter
@Order(27)
public class AutoReadWriteSplittingTests {

  private static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  private static final Logger LOGGER =
      Logger.getLogger(AutoReadWriteSplittingTests.class.getName());

  private Properties getProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name,
        String.valueOf(TimeUnit.SECONDS.toMillis(3)));
    props.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name,
        String.valueOf(TimeUnit.SECONDS.toMillis(10)));
    PropertyDefinition.PLUGINS.set(props, "sqlParser,autoReadWriteSplitting");
    return props;
  }

  private Properties getBalancingProps(final boolean includeWriter) {
    final Properties props = getProps();
    props.setProperty("queryLevelLoadBalancing", "true");
    props.setProperty("readerHostSelectorStrategy", "roundRobin");
    if (includeWriter) {
      props.setProperty("loadBalancingIncludeWriter", "true");
    }
    return props;
  }

  @TestTemplate
  public void test_selectRoutesToReader() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // Execute a SELECT — should automatically route to reader
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT 1")) {
        rs.next();
      }

      final String currentInstanceId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerInstanceId, currentInstanceId,
          "SELECT should have routed to a reader instance");
    }
  }

  @TestTemplate
  public void test_nonSelectRoutesToWriter() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      // queryInstanceId carries a /*@keep*/ hint, so it does not change routing.
      // A fresh connection starts on the writer.
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // A DML/DDL statement must run on the writer (non-SELECT is never routed to a reader).
      // The table declares a primary key because RDS Multi-AZ clusters enforce
      // sql_require_primary_key, which rejects tables without one.
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(
            "CREATE TEMPORARY TABLE IF NOT EXISTS auto_rw_test (id INT PRIMARY KEY)");
      }

      final String afterDmlInstanceId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerInstanceId, afterDmlInstanceId,
          "DML should run on the writer instance");
    }
  }

  @TestTemplate
  public void test_writerHintOverridesSelect() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // SELECT with /*@writer*/ hint — should stay on writer
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("/*@writer*/ SELECT 1")) {
        rs.next();
      }

      final String currentInstanceId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerInstanceId, currentInstanceId,
          "/*@writer*/ hint should keep query on writer instance");
    }
  }

  @TestTemplate
  public void test_readerHintOnPreparedStatement() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // PreparedStatement with /*@reader*/ hint
      try (PreparedStatement stmt = conn.prepareStatement("/*@reader*/ SELECT 1");
          ResultSet rs = stmt.executeQuery()) {
        rs.next();
      }

      final String currentInstanceId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerInstanceId, currentInstanceId,
          "/*@reader*/ hint should route to reader instance");
    }
  }

  @TestTemplate
  public void test_transactionAlwaysUsesWriter() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // Start a transaction
      conn.setAutoCommit(false);

      // SELECT inside transaction — should stay on writer
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT 1")) {
        rs.next();
      }

      final String currentInstanceId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerInstanceId, currentInstanceId,
          "Queries inside a transaction should stay on writer");

      conn.commit();
      conn.setAutoCommit(true);
    }
  }

  /**
   * With query-level load balancing enabled, consecutive read queries should rotate across
   * multiple reader instances (roundRobin strategy). Requires at least two readers.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  public void test_queryLevelLoadBalancing_rotatesAcrossReaders() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getBalancingProps(false))) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      final Set<String> observed = new HashSet<>();
      for (int i = 0; i < 8; i++) {
        // Each routed SELECT triggers a per-query balancing switch before it runs.
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1")) {
          rs.next();
        }
        // queryInstanceId carries a /*@keep*/ hint, so it reports the current instance
        // without perturbing routing.
        observed.add(auroraUtil.queryInstanceId(conn));
      }

      assertTrue(observed.size() > 1,
          "Query-level load balancing should rotate across multiple readers, but only saw: "
              + observed);
      assertFalse(observed.contains(writerInstanceId),
          "Load balancing without loadBalancingIncludeWriter should not route reads to the writer");
    }
  }

  /**
   * With query-level load balancing and loadBalancingIncludeWriter enabled, the writer instance
   * is also an eligible balancing candidate and should be reached over enough iterations.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  public void test_queryLevelLoadBalancing_includeWriter() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getBalancingProps(true))) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      final Set<String> observed = new HashSet<>();
      for (int i = 0; i < 15; i++) {
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1")) {
          rs.next();
        }
        observed.add(auroraUtil.queryInstanceId(conn));
      }

      assertTrue(observed.contains(writerInstanceId),
          "With loadBalancingIncludeWriter enabled, the writer should be used for some reads. "
              + "Observed: " + observed);
    }
  }

  /**
   * Query-level load balancing must not switch connections inside a transaction: all statements
   * in the transaction must run on a single instance.
   */
  @TestTemplate
  public void test_queryLevelLoadBalancing_suppressedInTransaction() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getBalancingProps(false))) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      conn.setAutoCommit(false);
      try {
        final Set<String> observed = new HashSet<>();
        for (int i = 0; i < 4; i++) {
          try (Statement stmt = conn.createStatement();
              ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.next();
          }
          observed.add(auroraUtil.queryInstanceId(conn));
        }

        assertEquals(1, observed.size(),
            "Load balancing must not switch connections inside a transaction. Observed: "
                + observed);
        assertTrue(observed.contains(writerInstanceId),
            "A transaction started with autocommit off should stay on the writer");
      } finally {
        conn.commit();
        conn.setAutoCommit(true);
      }
    }
  }
}
