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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;

/**
 * Integration tests for the SQL-driven {@code autoReadWriteSplitting} plugin.
 *
 * <p>This class both reuses the shared topology-based read/write splitting suite (by extending
 * {@link ReadWriteSplittingTests} with an {@code autoReadWriteSplitting} configuration) and adds
 * auto-specific tests that assert SQL-based routing behavior (routing hints, per-statement reader
 * routing, transaction pinning, and query-level reader rotation). Routing on {@code setReadOnly} is
 * still supported via the fallback signal, and the diagnostic {@code queryInstanceId} query carries
 * a {@code /*@keep* /} hint so it does not trigger a switch. Base tests that execute un-hinted raw
 * read SQL and expect it to stay on the current connection are disabled here, because SQL-based
 * routing intentionally reroutes such reads.
 */
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
@Order(13)
public class AutoReadWriteSplittingTests extends ReadWriteSplittingTests {

  @Override
  protected Properties getProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "sqlParser,autoReadWriteSplitting");
    return props;
  }

  @Override
  protected Properties getPropsWithFailover() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "failover,efm2,sqlParser,autoReadWriteSplitting");
    return props;
  }

  @TestTemplate
  @Disabled("SQL routing reroutes 'START TRANSACTION READ ONLY' before the transaction is active, "
      + "so this setReadOnly-oriented expectation does not apply to autoReadWriteSplitting.")
  @Override
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException {
  }

  @TestTemplate
  @Disabled("SQL routing may reroute the post-write SELECT to a reader (subject to replication lag), "
      + "so this setReadOnly-oriented expectation does not apply to autoReadWriteSplitting.")
  @Override
  public void test_setReadOnlyTrueInTransaction() throws SQLException {
  }

  @TestTemplate
  @Disabled("SQL routing reroutes the raw SELECT to a reader, changing the stale-statement semantics "
      + "this test relies on; not applicable to autoReadWriteSplitting.")
  @Override
  public void test_executeWithOldConnection() throws SQLException {
  }

  @TestTemplate
  @Disabled("With autocommit disabled the connection is pinned to preserve the transaction, so "
      + "setReadOnly(false) does not switch back to the writer here; this setReadOnly-oriented "
      + "expectation does not apply to autoReadWriteSplitting.")
  @Override
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse() throws SQLException {
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
  public void test_transactionOnWriterStaysOnWriter() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      // A fresh connection starts on the writer.
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // Start a transaction on the writer.
      conn.setAutoCommit(false);

      // A read inside the transaction stays pinned to the current (writer) connection instead of
      // routing to a reader. This verifies transaction pinning, NOT that transactions must use the
      // writer -- see test_transactionOnReaderStaysOnReader for a valid transaction on a reader.
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT 1")) {
        rs.next();
      }

      final String currentInstanceId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerInstanceId, currentInstanceId,
          "A transaction started on the writer should stay pinned to the writer");

      conn.commit();
      conn.setAutoCommit(true);
    }
  }

  @TestTemplate
  public void test_transactionOnReaderStaysOnReader() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // A transaction on a reader connection is perfectly valid. Establish a reader first.
      conn.setReadOnly(true);
      final String readerInstanceId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerInstanceId, readerInstanceId,
          "setReadOnly(true) should route to a reader instance");

      // Start a transaction on the reader.
      conn.setAutoCommit(false);

      // A read inside the transaction stays pinned to the reader; being in a transaction must not
      // force a switch to the writer.
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT 1")) {
        rs.next();
      }

      final String currentInstanceId = auroraUtil.queryInstanceId(conn);
      assertEquals(readerInstanceId, currentInstanceId,
          "A transaction on a reader connection should stay on that reader, not switch to the writer");

      conn.commit();
      conn.setAutoCommit(true);
      conn.setReadOnly(false);
    }
  }

  /**
   * With query-level load balancing enabled, a PreparedStatement holding a plain read is expected
   * to rotate reader-to-reader on each re-execution (statement recreation). This verifies that the
   * re-execution path selects fresh readers rather than staying pinned to the reader chosen at
   * prepare time. Requires at least three instances (one writer plus two readers) so that a
   * rotation between distinct readers is observable.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  public void test_reExecutedPreparedStatementRotatesAcrossReaders() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

    final Properties props = getProps();
    // Enable per-query reader balancing and pick a deterministic rotation strategy so successive
    // re-executions of the same PreparedStatement visit different readers.
    props.setProperty("queryLevelLoadBalancing", "true");
    props.setProperty("readerHostSelectorStrategy", "roundRobin");

    try (final Connection conn = DriverManager.getConnection(url, props)) {
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // A plain instance-id SELECT (no routing hint) is classified as a read and routed to a
      // reader. Re-executing the same PreparedStatement should rebind onto a fresh reader.
      final Set<String> observedReaders = new HashSet<>();
      try (PreparedStatement stmt = conn.prepareStatement(auroraUtil.getInstanceIdSql())) {
        for (int i = 0; i < 6; i++) {
          try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(), "instance-id query returned no rows");
            final String instanceId = rs.getString(1);
            assertNotEquals(writerInstanceId, instanceId,
                "a read PreparedStatement must never rotate onto the writer");
            observedReaders.add(instanceId);
          }
        }
      }

      assertTrue(observedReaders.size() >= 2,
          "re-executed PreparedStatement should rotate across at least two distinct readers, "
              + "but only observed: " + observedReaders);
    }
  }
}
