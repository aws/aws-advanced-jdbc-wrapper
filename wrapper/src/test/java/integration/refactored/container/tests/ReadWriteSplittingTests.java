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

package integration.refactored.container.tests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import integration.refactored.DatabaseEngine;
import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestInstanceInfo;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.ProxyHelper;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnDatabaseEngine;
import integration.refactored.container.condition.EnableOnDatabaseEngineDeployment;
import integration.refactored.container.condition.EnableOnNumOfInstances;
import integration.refactored.container.condition.EnableOnTestFeature;
import integration.refactored.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.util.SqlState;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnNumOfInstances(min = 2)
@EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
@DisableOnTestFeature({TestEnvironmentFeatures.PERFORMANCE, TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
@MakeSureFirstInstanceWriter
public class ReadWriteSplittingTests {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingTests.class.getName());
  private static final AuroraTestUtility auroraUtil =
      new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());

  protected static Properties getProps() {
    return TestEnvironment.isAwsDatabase() ? getAuroraProps() : getNonAuroraProps();
  }

  protected static Properties getProxiedPropsWithFailover() {
    if (TestEnvironment.isAwsDatabase()) {
      final Properties props = getAuroraPropsWithFailover();
      AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
          "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo()
              .getInstanceEndpointSuffix());
      return props;
    } else {
      // No extra properties needed
      return getNonAuroraProps();
    }
  }

  protected static Properties getProxiedProps() {
    if (TestEnvironment.isAwsDatabase()) {
      final Properties props = getAuroraProps();
      AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
          "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo()
              .getInstanceEndpointSuffix());
      return props;
    } else {
      // No extra properties needed
      return getNonAuroraProps();
    }
  }

  protected static Properties getDefaultPropsNoPlugins() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    return props;
  }

  protected static Properties getAuroraProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "auroraHostList,readWriteSplitting");
    return props;
  }

  protected static Properties getAuroraPropsWithFailover() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover");
    return props;
  }

  protected static Properties getNonAuroraProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.set(props,
        "true");
    return props;
  }

  protected String getUrl() {
    if (TestEnvironment.isAwsDatabase()) {
      return ConnectionStringHelper.getWrapperUrl();
    } else {
      final List<TestInstanceInfo> instances =
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances();
      return getCommaDelimitedHostUrl(instances);
    }
  }

  protected String getProxiedUrl() {
    if (TestEnvironment.isAwsDatabase()) {
      return ConnectionStringHelper.getProxyWrapperUrl();
    } else {
      final List<TestInstanceInfo> instances =
          TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances();
      return getCommaDelimitedHostUrl(instances);
    }
  }

  private String getCommaDelimitedHostUrl(final List<TestInstanceInfo> instances) {
    return DriverHelper.getWrapperDriverProtocol()
        + instances.get(0).getEndpoint() + ","
        + instances.get(1).getEndpoint() + "/"
        + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
        + DriverHelper.getDriverRequiredParameters();
  }

  @TestTemplate
  public void test_connectToWriter_switchSetReadOnly() throws SQLException {
    final String url = getUrl();

    LOGGER.finest("Connecting to url " + url);
    try (final Connection conn = DriverManager.getConnection(url, getProps())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      conn.setReadOnly(true);
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(true);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
  public void test_connectToReader_setReadOnlyTrueFalse() throws SQLException {
    final String url = getUrl();

    LOGGER.finest("Connecting to url " + url);
    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("writerConnectionId: " + writerConnectionId);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("readerConnectionId: " + readerConnectionId);
      assertNotEquals(writerConnectionId, readerConnectionId);

      conn.setReadOnly(false);
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
      assertNotEquals(readerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
  public void test_connectToReaderCluster_setReadOnlyTrueFalse() throws SQLException {
    final String url = getUrl();
    LOGGER.finest("Connecting to url " + url);
    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("writerConnectionId: " + writerConnectionId);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      conn.setReadOnly(false);
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
      assertNotEquals(readerConnectionId, writerConnectionId);
    }
  }

  @TestTemplate
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      stmt.execute("START TRANSACTION READ ONLY");
      stmt.executeQuery("SELECT 1");

      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      conn.setAutoCommit(false);
      stmt.executeQuery(
          // TODO: can we replace it with something less database specific?
          "SELECT COUNT(*) FROM information_schema.tables");

      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }


  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL})
  public void test_setReadOnlyTrueInTransaction() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {
      final AuroraTestUtility auroraUtil =
          new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
      stmt1.executeUpdate(
          "CREATE TABLE test_readWriteSplitting_readOnlyTrueInTransaction "
              + "(id int not null primary key, text_field varchar(255) not null)");
      stmt1.execute("SET autocommit = 0");

      final Statement stmt2 = conn.createStatement();
      stmt2.executeUpdate(
          "INSERT INTO test_readWriteSplitting_readOnlyTrueInTransaction "
              + "VALUES (1, 'test_field value 1')");

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      stmt2.execute("COMMIT");
      final ResultSet rs =
          stmt2.executeQuery(
              "SELECT count(*) from test_readWriteSplitting_readOnlyTrueInTransaction");
      rs.next();
      assertEquals(1, rs.getInt(1));

      conn.setReadOnly(false);
      stmt2.execute("SET autocommit = 1");
      stmt2.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyTrue_oneHost() throws SQLException {


    // Use static host list with only one host
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.set(props, "true");

    final String writerUrl =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0).getEndpoint();
    final String url = DriverHelper.getWrapperDriverProtocol()
        + writerUrl + "/"
        + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
        + DriverHelper.getDriverRequiredParameters();

    try (final Connection conn = DriverManager.getConnection(url, props)) {
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(true);
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyTrue_allReadersDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), getProxiedProps())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      // Kill all reader instances
      final List<String> instanceIDs =
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().stream()
              .map(TestInstanceInfo::getInstanceId).collect(Collectors.toList());
      for (int i = 1; i < instanceIDs.size(); i++) {
        ProxyHelper.disableConnectivity(instanceIDs.get(i));
      }

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      String currentConnectionId = assertDoesNotThrow(() -> auroraUtil.queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);

      assertDoesNotThrow(() -> conn.setReadOnly(false));
      currentConnectionId = assertDoesNotThrow(() -> auroraUtil.queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);

      // Bring up one reader
      ProxyHelper.enableConnectivity(instanceIDs.get(instanceIDs.size() - 1));
      assertDoesNotThrow(() -> conn.setReadOnly(true));
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnly_closedConnection() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), getProxiedProps())) {
      conn.close();

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), exception.getSQLState());
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyFalse_allInstancesDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), getProxiedProps())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      // Kill all instances
      ProxyHelper.disableAllConnectivity();

      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
  public void test_executeWithOldConnection() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

      final String writerId = auroraUtil.queryInstanceId(conn);

      final Statement oldStmt = conn.createStatement();
      final ResultSet oldRs = oldStmt.executeQuery("SELECT 1");
      conn.setReadOnly(true); // Connection is switched internally
      conn.setAutoCommit(false);

      assertThrows(SQLException.class, () -> oldStmt.execute("SELECT 1"));
      assertThrows(SQLException.class, () -> oldRs.getInt(1));

      final String readerId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerId, readerId);

      assertDoesNotThrow(oldStmt::close);
      assertDoesNotThrow(oldRs::close);

      final String sameReaderId = auroraUtil.queryInstanceId(conn);
      assertEquals(readerId, sameReaderId);
    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED, TestEnvironmentFeatures.FAILOVER_SUPPORTED})
  public void test_failoverToNewWriter_setReadOnlyTrueFalse()
      throws SQLException, InterruptedException {
    try (final Connection conn =
             DriverManager.getConnection(getProxiedUrl(), getProxiedPropsWithFailover())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      // Kill all reader instances
      final int numOfInstances =
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().size();
      for (int i = 1; i < numOfInstances; i++) {
        ProxyHelper.disableConnectivity(
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(i)
                .getInstanceId());
      }

      // Force internal reader connection to the writer instance
      conn.setReadOnly(true);
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
      conn.setReadOnly(false);

      ProxyHelper.enableAllConnectivity();

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Failure occurs on Connection invocation
      auroraUtil.assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to the new writer after failover happens.
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(currentConnectionId, writerConnectionId);

      conn.setReadOnly(true);
      currentConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);

    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED, TestEnvironmentFeatures.FAILOVER_SUPPORTED})
  public void test_failoverToNewReader_setReadOnlyFalseTrue() throws SQLException {
    try (final Connection conn =
             DriverManager.getConnection(getProxiedUrl(), getProxiedPropsWithFailover())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.info("writerConnectionId: " + writerConnectionId);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      String otherReaderId = "";
      final List<TestInstanceInfo> instances = TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances();

      for (int i = 1; i < instances.size(); i++) {
        if (!instances.get(i).getInstanceId().equals(readerConnectionId)) {
          otherReaderId = instances.get(i).getInstanceId();
          break;
        }
      }
      if (otherReaderId.equals("")) {
        fail("could not acquire new reader ID");
      }

      // Kill all instances except one other reader
      for (final TestInstanceInfo instance : instances) {
        final String instanceId = instance.getInstanceId();
        if (otherReaderId.equals(instanceId)) {
          continue;
        }
        ProxyHelper.disableConnectivity(instanceId);
      }

      auroraUtil.assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());
      assertFalse(conn.isClosed());
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(otherReaderId, currentConnectionId);
      assertNotEquals(readerConnectionId, currentConnectionId);

      ProxyHelper.enableAllConnectivity();

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(true);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(otherReaderId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED, TestEnvironmentFeatures.FAILOVER_SUPPORTED})
  public void test_failoverReaderToWriter_setReadOnlyTrueFalse() throws SQLException {
    try (final Connection conn =
             DriverManager.getConnection(getProxiedUrl(), getProxiedPropsWithFailover())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final List<TestInstanceInfo> instances = TestEnvironment.getCurrent().getInfo().getDatabaseInfo()
          .getInstances();

      // Kill all instances except the writer
      for (final TestInstanceInfo instance : instances) {
        final String instanceId = instance.getInstanceId();
        if (writerConnectionId.equals(instanceId)) {
          continue;
        }
        ProxyHelper.disableConnectivity(instanceId);
      }

      auroraUtil.assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());
      assertFalse(conn.isClosed());
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      ProxyHelper.enableAllConnectivity();

      conn.setReadOnly(true);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }
}
