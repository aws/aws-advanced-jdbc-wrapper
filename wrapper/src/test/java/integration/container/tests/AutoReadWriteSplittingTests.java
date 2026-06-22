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
import java.util.Properties;
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
@EnableOnDatabaseEngineDeployment(integration.DatabaseEngineDeployment.AURORA)
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
      final String writerInstanceId = auroraUtil.queryInstanceId(conn);

      // Execute a SELECT first to switch to reader
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT 1")) {
        rs.next();
      }

      String readerInstanceId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerInstanceId, readerInstanceId);

      // Execute DML — should route back to writer
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS auto_rw_test (id INT)");
        stmt.executeUpdate("INSERT INTO auto_rw_test (id) VALUES (1)");
      }

      final String afterDmlInstanceId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerInstanceId, afterDmlInstanceId,
          "DML should have routed back to writer instance");
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
}
