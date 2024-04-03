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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.aurora.TestPluginServiceImpl;
import integration.container.condition.DisableOnTestDriver;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HikariPoolConfigurator;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;

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
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@MakeSureFirstInstanceWriter
@Order(12)
public class ReadWriteSplittingTests {

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingTests.class.getName());

  protected ExecutorService executor;

  @BeforeEach
  public void setUpEach() {
    this.executor = Executors.newFixedThreadPool(1, r -> {
      final Thread thread = new Thread(r);
      thread.setDaemon(true);
      return thread;
    });
  }

  @AfterEach
  public void tearDownEach() {
    this.executor.shutdownNow();
  }


  protected static Properties getProxiedPropsWithFailover() {
    final Properties props = getPropsWithFailover();
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
        "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    return props;
  }

  protected static Properties getProxiedProps() {
    final Properties props = getProps();
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
        "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    return props;
  }

  protected static Properties getDefaultPropsNoPlugins() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(
        PropertyDefinition.SOCKET_TIMEOUT.name, String.valueOf(TimeUnit.SECONDS.toMillis(3)));
    props.setProperty(
        PropertyDefinition.CONNECT_TIMEOUT.name, String.valueOf(TimeUnit.SECONDS.toMillis(10)));
    return props;
  }

  protected static Properties getProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    return props;
  }

  protected static Properties getPropsWithFailover() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "failover,efm2,readWriteSplitting");
    return props;
  }

  @TestTemplate
  public void test_connectToWriter_switchSetReadOnly() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl();

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
  public void test_connectToReader_setReadOnlyTrueFalse() throws SQLException {
    final String url = getWrapperReaderInstanceUrl();

    LOGGER.finest("Connecting to url " + url);
    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("readerConnectionId: " + readerConnectionId);

      conn.setReadOnly(true);
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("writerConnectionId: " + writerConnectionId);
      assertNotEquals(readerConnectionId, writerConnectionId);
    }
  }

  // Assumes the writer is stored as the first instance and all other instances are readers.
  protected String getWrapperReaderInstanceUrl() {
    TestInstanceInfo readerInstance =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(1);
    return ConnectionStringHelper.getWrapperUrl(readerInstance);
  }

  @TestTemplate
  public void test_connectToReaderCluster_setReadOnlyTrueFalse() throws SQLException {
    final String url = ConnectionStringHelper.getWrapperReaderClusterUrl();
    LOGGER.finest("Connecting to url " + url);
    try (final Connection conn = DriverManager.getConnection(url, getProps())) {
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("readerConnectionId: " + readerConnectionId);

      conn.setReadOnly(true);
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("writerConnectionId: " + writerConnectionId);
      assertNotEquals(readerConnectionId, writerConnectionId);
    }
  }

  @TestTemplate
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), getProps())) {

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
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), getProps())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      conn.setAutoCommit(false);
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
  @EnableOnDatabaseEngine({DatabaseEngine.MYSQL})
  public void test_setReadOnlyTrueInTransaction() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), getProps())) {

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
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyTrue_allReadersDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getProxyWrapperUrl(), getProxiedProps())) {

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

      ProxyHelper.enableAllConnectivity();
      assertDoesNotThrow(() -> conn.setReadOnly(true));
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  public void test_setReadOnly_closedConnection() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getProxyWrapperUrl(), getProxiedProps())) {
      conn.close();

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), exception.getSQLState());
    }
  }

  /**
   * PG driver and MariaDB driver have a check for internal readOnly flag and don't communicate to a DB server
   * if there are no changes. Thus, network exception is not raised.
   */
  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  @DisableOnTestDriver({TestDriver.PG, TestDriver.MARIADB}) // see comments above
  public void test_setReadOnlyFalse_allInstancesDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getProxyWrapperUrl(), getProxiedProps())) {

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
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyFalse_whenAllInstancesDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperReaderClusterUrl(), getProxiedProps())) {

      // Kill all instances
      ProxyHelper.disableAllConnectivity();

      // setReadOnly(false) triggers switching reader connection to a new writer connection.
      // Since connectivity to all instances are down, it's expected to get a network-bound exception
      // while opening a new connection to a writer node.
      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
    }
  }

  @TestTemplate
  public void test_executeWithOldConnection() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), getProps())) {

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
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED, TestEnvironmentFeatures.FAILOVER_SUPPORTED})
  public void test_writerFailover_setReadOnlyTrueFalse() throws SQLException {
    try (final Connection conn =
             DriverManager.getConnection(ConnectionStringHelper.getProxyWrapperUrl(), getProxiedPropsWithFailover())) {

      final String originalWriterId = auroraUtil.queryInstanceId(conn);

      // Kill all reader instances
      List<TestInstanceInfo> instances = TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances();
      for (int i = 1; i < instances.size(); i++) {
        ProxyHelper.disableConnectivity(instances.get(i).getInstanceId());
      }

      // Force internal reader connection to the writer instance
      conn.setReadOnly(true);
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(originalWriterId, currentConnectionId);
      conn.setReadOnly(false);

      ProxyHelper.enableAllConnectivity();

      auroraUtil.crashInstance(executor, originalWriterId);

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

      // Assert that we are connected to the writer after failover happens.
      String newWriterId = auroraUtil.queryInstanceId(conn);
      assertTrue(auroraUtil.isDBInstanceWriter(newWriterId));

      conn.setReadOnly(true);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(newWriterId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(newWriterId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  public void test_failoverToNewReader_setReadOnlyFalseTrue() throws SQLException {

    final Properties props = getProxiedPropsWithFailover();
    props.put(FailoverConnectionPlugin.FAILOVER_MODE.name, "reader-or-writer");

    try (final Connection conn =
             DriverManager.getConnection(ConnectionStringHelper.getProxyWrapperUrl(), props)) {

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

      if (otherReaderId.isEmpty()) {
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

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);
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
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  public void test_failoverReaderToWriter_setReadOnlyTrueFalse()
      throws SQLException {

    try (final Connection conn =
             DriverManager.getConnection(ConnectionStringHelper.getProxyWrapperUrl(), getProxiedPropsWithFailover())) {

      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("writerConnectionId=" + writerConnectionId);

      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("readerConnectionId=" + readerConnectionId);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final List<TestInstanceInfo> instances = TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo()
          .getInstances();

      // Kill all instances except the writer
      for (final TestInstanceInfo instance : instances) {
        final String instanceId = instance.getInstanceId();
        if (writerConnectionId.equals(instanceId)) {
          continue;
        }
        ProxyHelper.disableConnectivity(instanceId);
      }

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);
      assertFalse(conn.isClosed());
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("currentConnectionId=" + currentConnectionId);
      assertEquals(writerConnectionId, currentConnectionId);

      ProxyHelper.enableAllConnectivity();

      // During failover above some of the readers have been tried to connect to and failed since they were not
      // available. We should expect that some of the readers in topology are marked as UNAVAILABLE.
      // The following code reset node availability and make them AVAILABLE.
      // That is important for further steps.
      TestPluginServiceImpl.clearHostAvailabilityCache();
      conn.unwrap(PluginService.class).forceRefreshHostList();

      conn.setReadOnly(true);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("currentConnectionId=" + currentConnectionId);
      assertNotEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.finest("currentConnectionId=" + currentConnectionId);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  public void test_pooledConnection_reuseCachedConnection() throws SQLException {
    Properties props = getProps();

    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(1));
    Driver.setCustomConnectionProvider(provider);

    final Connection conn1;
    final Connection conn2;
    final Connection unwrappedConn1;
    final Connection unwrappedConn2;

    try {
      conn1 = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
      unwrappedConn1 = conn1.unwrap(Connection.class);
      conn1.close();

      conn2 = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
      unwrappedConn2 = conn2.unwrap(Connection.class);
      conn2.close();

      // The ConnectionWrapper objects should be different, but the underlying connection should
      // have been cached and thus should be the same across conn1 and conn2.
      assertNotSame(conn1, conn2);
      assertSame(unwrappedConn1, unwrappedConn2);
    } finally {
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  protected HikariPoolConfigurator getHikariConfig(int maxPoolSize) {
    return (hostSpec, props) -> {
      final HikariConfig config = new HikariConfig();
      config.setMaximumPoolSize(maxPoolSize);
      config.setInitializationFailTimeout(75000);
      return config;
    };
  }

  /**
   * After failover, RDS MultiAz Cluster let old writer dead/unavailable for significant time (up to 10-15 min)
   * that makes this test practically impossible to execute.
   * So test should be executed for Aurora clusters only.
   */
  @TestTemplate
  @EnableOnTestFeature({ TestEnvironmentFeatures.FAILOVER_SUPPORTED, TestEnvironmentFeatures.HIKARI })
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA) // RDS MultiAz Cluster doesn't work for this case
  public void test_pooledConnectionFailover() throws SQLException, InterruptedException {
    Properties props = getPropsWithFailover();

    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(1));
    provider.releaseResources(); // make sure there's no pool's left after prior test
    Driver.setCustomConnectionProvider(provider);

    final String initialWriterId;
    final String nextWriterId;
    final Connection initialWriterConn1;
    final Connection initialWriterConn2;
    final Connection newWriterConn;

    try {
      try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props)) {
        initialWriterConn1 = conn.unwrap(Connection.class);
        initialWriterId = auroraUtil.queryInstanceId(conn);
        LOGGER.finest("initialWriterId: " + initialWriterId);
        provider.logConnections();

        auroraUtil.failoverClusterAndWaitUntilWriterChanged();

        assertThrows(FailoverSuccessSQLException.class, () -> auroraUtil.queryInstanceId(conn));
        nextWriterId = auroraUtil.queryInstanceId(conn);
        LOGGER.finest("nextWriterId: " + nextWriterId);
        assertNotEquals(initialWriterId, nextWriterId);
        newWriterConn = conn.unwrap(Connection.class);
        assertNotSame(initialWriterConn1, newWriterConn);
        provider.logConnections();
      }

      try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props)) {
        // This should be a new connection to the initial writer instance (now a reader).
        final String oldWriterConnectionId = auroraUtil.queryInstanceId(conn);
        LOGGER.finest("oldWriterConnectionId: " + oldWriterConnectionId);
        assertEquals(initialWriterId, oldWriterConnectionId);
        initialWriterConn2 = conn.unwrap(Connection.class);
        // The initial connection should have been evicted from the pool when failover occurred, so
        // this should be a new connection even though it is connected to the same instance.
        assertNotSame(initialWriterConn1, initialWriterConn2);
        provider.logConnections();
      }
    } finally {
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  @TestTemplate
  // This test is not run on multi-AZ because multi-AZ clusters take too long to stabilize. Toxiproxy is not a valid
  // alternative for this test because the proxied cluster and writer endpoint URLs resolve to different IP addresses.
  // This causes AuroraStaleDnsHelper to replace the cluster URL with the writer URL which causes this test to fail
  // because it results in a non-empty database pool.
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnTestFeature({ TestEnvironmentFeatures.FAILOVER_SUPPORTED, TestEnvironmentFeatures.HIKARI })
  public void test_pooledConnectionFailoverWithClusterURL() throws SQLException, InterruptedException {
    Properties props = getPropsWithFailover();

    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(1));
    Driver.setCustomConnectionProvider(provider);

    final Connection initialWriterConn;
    final Connection newWriterConn;

    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperClusterEndpointUrl(),
        props)) {
      initialWriterConn = conn.unwrap(Connection.class);
      // The internal connection pool should not be used if the connection is established via a cluster URL.
      assertEquals(0, provider.getHostCount(), "Internal connection pool should be empty.");
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();
      assertThrows(FailoverSuccessSQLException.class, () -> auroraUtil.queryInstanceId(conn));
      final String nextWriterId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, nextWriterId);
      assertEquals(0, provider.getHostCount(), "Internal connection pool should be empty.");
      newWriterConn = conn.unwrap(Connection.class);
      assertNotSame(initialWriterConn, newWriterConn);
    } finally {
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  @TestTemplate
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED, TestEnvironmentFeatures.HIKARI})
  public void test_pooledConnection_failoverFailed() throws SQLException {
    Properties props = getProxiedPropsWithFailover();
    FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.set(props, "1000");
    props.setProperty("monitoring-" + PropertyDefinition.SOCKET_TIMEOUT.name, "3000");

    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(1));
    Driver.setCustomConnectionProvider(provider);

    final Connection initialWriterConn1;
    final Connection initialWriterConn2;

    try {
      final String initialWriterId;
      try (Connection conn = DriverManager.getConnection(ConnectionStringHelper.getProxyWrapperUrl(), props)) {
        initialWriterConn1 = conn.unwrap(Connection.class);
        initialWriterId = auroraUtil.queryInstanceId(conn);
        ProxyHelper.disableAllConnectivity();
        assertThrows(FailoverFailedSQLException.class, () -> auroraUtil.queryInstanceId(conn));
      }

      ProxyHelper.enableAllConnectivity();
      try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getProxyWrapperUrl(), props)) {
        final String writerConnectionId = auroraUtil.queryInstanceId(conn);
        assertEquals(initialWriterId, writerConnectionId);
        initialWriterConn2 = conn.unwrap(Connection.class);
        // The initial connection should have been evicted from the pool when failover occurred, so
        // this should be a new connection even though it is connected to the same instance.
        assertNotSame(initialWriterConn1, initialWriterConn2);
      }
    } finally {
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
  public void test_pooledConnection_failoverInTransaction() throws SQLException {
    Properties props = getProxiedPropsWithFailover();

    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(1));
    provider.releaseResources(); // make sure there's no pool's left after prior test
    Driver.setCustomConnectionProvider(provider);

    final String initialWriterId;
    final String nextWriterId;
    final Connection initialWriterConn1;
    final Connection initialWriterConn2;
    final Connection newWriterConn;

    try {
      try (Connection conn = DriverManager.getConnection(ConnectionStringHelper.getProxyWrapperUrl(), props)) {
        initialWriterConn1 = conn.unwrap(Connection.class);
        conn.setAutoCommit(false);
        initialWriterId = auroraUtil.queryInstanceId(conn);
        LOGGER.finest("initialWriterId: " + initialWriterId + ", " + initialWriterConn1);
        auroraUtil.crashInstance(executor, initialWriterId);
        assertThrows(TransactionStateUnknownSQLException.class,
            () -> auroraUtil.queryInstanceId(conn));
        conn.setAutoCommit(true);
        nextWriterId = auroraUtil.queryInstanceId(conn);
        newWriterConn = conn.unwrap(Connection.class);
        LOGGER.finest("nextWriterId: " + nextWriterId + ", " + newWriterConn);
        assertNotSame(initialWriterConn1, newWriterConn);
      }

      // Make sure all instances up after failover.
      // Old writer in RDS MultiAz clusters may be unavailable for quite long time.
      auroraUtil.makeSureInstancesUp(TimeUnit.MINUTES.toSeconds(15));

      // It makes sense to run the following step when initial writer is up.
      try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props)) {
        // This should be a new connection to the initial writer instance (now a reader).
        final String writerConnectionId = auroraUtil.queryInstanceId(conn);
        assertEquals(initialWriterId, writerConnectionId);
        initialWriterConn2 = conn.unwrap(Connection.class);
        // The initial connection should have been evicted from the pool when failover occurred, so
        // this should be a new connection even though it is connected to the same instance.
        assertNotSame(initialWriterConn1, initialWriterConn2);
      }
    } finally {
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  @TestTemplate
  public void test_pooledConnection_differentUsers() throws SQLException {
    Properties privilegedUserProps = getProps();

    Properties privilegedUserWithWrongPasswordProps = getProps();
    privilegedUserWithWrongPasswordProps.setProperty(PropertyDefinition.PASSWORD.name, "bogus_password");

    Properties limitedUserProps = getProps();
    String limitedUserName = "limited_user";
    String limitedUserPassword = "limited_user";
    String limitedUserNewDb = "limited_user_db";
    limitedUserProps.setProperty(PropertyDefinition.USER.name, limitedUserName);
    limitedUserProps.setProperty(PropertyDefinition.PASSWORD.name, limitedUserPassword);

    Properties wrongUserRightPasswordProps = getProps();
    wrongUserRightPasswordProps.setProperty(PropertyDefinition.USER.name, "bogus_user");

    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(1));
    Driver.setCustomConnectionProvider(provider);

    try {
      try (Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(),
          privilegedUserProps); Statement stmt = conn.createStatement()) {
        stmt.execute("DROP USER IF EXISTS " + limitedUserName);
        auroraUtil.createUser(conn, limitedUserName, limitedUserPassword);
        TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();
        DatabaseEngine engine = info.getRequest().getDatabaseEngine();
        if (DatabaseEngine.MYSQL.equals(engine)) {
          String db = info.getDatabaseInfo().getDefaultDbName();
          if (!StringUtils.isNullOrEmpty(db)) {
            // MySQL needs this extra command to allow the limited user access to the database
            stmt.execute("GRANT ALL PRIVILEGES ON " + db + ".* to " + limitedUserName);
          }
        }
      }

      try (final Connection conn = DriverManager.getConnection(
          ConnectionStringHelper.getWrapperUrl(), limitedUserProps);
           Statement stmt = conn.createStatement()) {
        assertThrows(SQLException.class,
            () -> stmt.execute("CREATE DATABASE " + limitedUserNewDb));
      }

      assertThrows(
          HikariPool.PoolInitializationException.class, () -> {
            try (final Connection ignored = DriverManager.getConnection(
                ConnectionStringHelper.getWrapperUrl(), wrongUserRightPasswordProps)) {
              // Do nothing (close connection automatically)
            }
          });
    } finally {
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();

      try (Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(),
          privilegedUserProps);
           Statement stmt = conn.createStatement()) {
        stmt.execute("DROP DATABASE IF EXISTS " + limitedUserNewDb);
        stmt.execute("DROP USER IF EXISTS " + limitedUserName);
      }
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 5)
  public void test_pooledConnection_leastConnectionsStrategy() throws SQLException {
    final Properties props = getProps();
    ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.set(props, "leastConnections");

    final List<TestInstanceInfo> instances =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances();
    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(instances.size()));
    Driver.setCustomConnectionProvider(provider);

    final List<Connection> connections = new ArrayList<>();
    final List<String> connectedReaderIDs = new ArrayList<>();
    try {
      // Assume one writer and [size - 1] readers
      for (int i = 0; i < instances.size() - 1; i++) {
        final Connection conn =
            DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
        connections.add(conn);
        conn.setReadOnly(true);
        final String readerId = auroraUtil.queryInstanceId(conn);

        assertFalse(connectedReaderIDs.contains(readerId));
        connectedReaderIDs.add(readerId);
      }
    } finally {
      for (Connection connection : connections) {
        connection.close();
      }
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  /**
   * Tests custom pool mapping together with internal connection pools and the leastConnections
   * host selection strategy. This test overloads one reader with connections and then verifies
   * that new connections are sent to the other readers until their connection count equals that of
   * the overloaded reader.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 5)
  public void test_pooledConnection_leastConnectionsWithPoolMapping() throws SQLException {
    final Properties defaultProps = getProps();
    ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.set(defaultProps, "leastConnections");

    final List<Properties> propSets = new ArrayList<>();
    final int numOverloadedReaderConnections = 3;
    for (int i = 0; i < numOverloadedReaderConnections; i++) {
      final Properties props = PropertyUtils.copyProperties(defaultProps);
      props.setProperty("arbitraryProp", "value" + i);
      propSets.add(props);
    }

    final List<TestInstanceInfo> instances =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances();
    // We will be testing all instances excluding the writer and overloaded reader. Each instance
    // should be tested numOverloadedReaderConnections times to increase the pool connection count
    // until it equals the connection count of the overloaded reader.
    final int numTestConnections = (instances.size() - 2) * numOverloadedReaderConnections;
    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(
            getHikariConfig(numTestConnections),
            // Create a new pool for each instance-arbitraryProp combination
            (hostSpec, connProps) -> hostSpec.getUrl() + connProps.getProperty("arbitraryProp")
        );
    Driver.setCustomConnectionProvider(provider);

    final List<Connection> connections = new ArrayList<>();
    try {
      final TestInstanceInfo overloadedReader = instances.get(1);
      final String overloadedReaderConnString =
          ConnectionStringHelper.getWrapperUrl(overloadedReader);
      for (int i = 0; i < numOverloadedReaderConnections; i++) {
        // This should result in numOverloadedReaderConnections pools to the same reader instance,
        // with each pool consisting of just one connection. The total connection count for the
        // instance should be numOverloadedReaderConnections despite being spread across multiple
        // pools.
        final Connection conn = DriverManager.getConnection(overloadedReaderConnString, propSets.get(i));
        connections.add(conn);
      }

      final String overloadedReaderId = overloadedReader.getInstanceId();
      for (int i = 0; i < numTestConnections; i++) {
        final Connection conn =
            DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), defaultProps);
        connections.add(conn);
        conn.setReadOnly(true);
        final String readerId = auroraUtil.queryInstanceId(conn);
        assertNotEquals(overloadedReaderId, readerId);
      }
    } finally {
      for (Connection connection : connections) {
        connection.close();
      }

      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }
}
