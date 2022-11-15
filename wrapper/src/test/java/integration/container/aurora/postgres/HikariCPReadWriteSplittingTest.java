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

package integration.container.aurora.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import eu.rekawek.toxiproxy.Proxy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.PGProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.util.HikariCPSQLException;
import software.amazon.jdbc.util.SqlState;

public class HikariCPReadWriteSplittingTest extends AuroraPostgresBaseTest {

  private static final Logger logger = Logger.getLogger(HikariCPReadWriteSplittingTest.class.getName());
  private static HikariDataSource dataSource = null;
  private final List<String> clusterTopology = fetchTopology();

  private List<String> fetchTopology() {
    try {
      final List<String> topology = getTopologyEndpoints();
      // topology should contain a writer and at least one reader
      if (topology == null || topology.size() < 2) {
        fail("Topology does not contain the required instances");
      }
      return topology;
    } catch (final SQLException e) {
      fail("Couldn't fetch cluster topology");
    }

    return null;
  }

  @BeforeAll
  static void setup() {
    System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");
  }

  @AfterEach
  public void teardown() {
    dataSource.close();
  }

  private static Stream<Arguments> testParameters() {
    return Stream.of(
        Arguments.of(getTargetProps_allPlugins()),
        Arguments.of(getTargetProps_readWritePlugin())
    );
  }

  private static Properties getTargetProps_readWritePlugin() {
    final Properties props = getDefaultDataSourceProps();
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraHostList,readWriteSplitting");
    return props;
  }

  private static Properties getTargetProps_allPlugins() {
    final Properties props = getDefaultDataSourceProps();
    props.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,efm");
    return props;
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable.
   */
  @ParameterizedTest(name = "test_1_1_hikariCP_lost_connection")
  @MethodSource("testParameters")
  public void test_1_1_hikariCP_lost_connection(final Properties targetDataSourceProps) throws SQLException {
    FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.set(targetDataSourceProps, "1");
    PGProperty.SOCKET_TIMEOUT.set(targetDataSourceProps, "1");
    createDataSource(targetDataSourceProps);
    try (final Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));

      putDownAllInstances(true);

      final SQLException e = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      if (pluginChainIncludesFailoverPlugin(targetDataSourceProps)) {
        assertTrue(e instanceof FailoverFailedSQLException);
      } else {
        assertEquals(SqlState.CONNECTION_FAILURE.getState(), e.getSQLState());
      }
      assertFalse(conn.isValid(5));
    }

    assertThrows(SQLTransientConnectionException.class, () -> dataSource.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance. A connection is then retrieved to check that connections
   * to failed instances are not returned.
   */
  @ParameterizedTest(name = "test_1_2_hikariCP_get_dead_connection")
  @MethodSource("testParameters")
  public void test_1_2_hikariCP_get_dead_connection(final Properties targetDataSourceProps) throws SQLException {
    putDownAllInstances(false);

    final String writer = clusterTopology.get(0);
    final String reader = clusterTopology.get(1);
    final String writerIdentifier = writer.split("\\.")[0];
    final String readerIdentifier = reader.split("\\.")[0];
    logger.fine("Instance to connect to: " + writerIdentifier);
    logger.fine("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);
    createDataSource(targetDataSourceProps);

    // Get a valid connection, then make it fail over to a different instance
    try (final Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      bringUpInstance(readerIdentifier);
      putDownInstance(currentInstance);

      final SQLException e = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      if (pluginChainIncludesFailoverPlugin(targetDataSourceProps)) {
        assertTrue(e instanceof FailoverSuccessSQLException);
      } else {
        assertEquals(SqlState.CONNECTION_FAILURE.getState(), e.getSQLState());
        return;
      }

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      logger.fine("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));

      // Try to get a new connection to the failed instance, which times out
      assertThrows(SQLTransientConnectionException.class, () -> dataSource.getConnection());
    }
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor.
   */
  @ParameterizedTest(name = "test_2_1_hikariCP_efm_failover")
  @MethodSource("testParameters")
  public void test_2_1_hikariCP_efm_failover(final Properties targetDataSourceProps) throws SQLException {
    putDownAllInstances(false);

    final String writer = clusterTopology.get(0);
    final String reader = clusterTopology.get(1);
    final String writerIdentifier = writer.split("\\.")[0];
    final String readerIdentifier = reader.split("\\.")[0];
    logger.fine("Instance to connect to: " + writerIdentifier);
    logger.fine("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);
    createDataSource(targetDataSourceProps);

    // Get a valid connection, then make it fail over to a different instance
    try (final Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      logger.fine("Connected to instance: " + currentInstance);

      bringUpInstance(readerIdentifier);
      putDownInstance(writerIdentifier);

      final SQLException e = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      if (pluginChainIncludesFailoverPlugin(targetDataSourceProps)) {
        assertTrue(e instanceof FailoverSuccessSQLException);
      } else {
        assertEquals(SqlState.CONNECTION_FAILURE.getState(), e.getSQLState());
        return;
      }

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      logger.fine("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
    }
  }

  @ParameterizedTest(name = "test_3_1_readerLoadBalancing_autocommitTrue")
  @MethodSource("testParameters")
  public void test_3_1_readerLoadBalancing_autocommitTrue(final Properties targetDataSourceProps) throws SQLException {
    createDataSourceWithReaderLoadBalancing(targetDataSourceProps);
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(false);
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerId);
      assertTrue(isDBInstanceReader(readerId));

      // Assert switch after each execute
      String nextReaderId;
      for (int i = 0; i < 5; i++) {
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
        assertTrue(isDBInstanceReader(readerId));
        readerId = nextReaderId;
      }

      // Verify behavior for transactions started while autocommit is on (autocommit is implicitly disabled)
      // Connection should not be switched while inside a transaction
      for (int i = 0; i < 5; i++) {
        final Statement stmt = conn.createStatement();
        stmt.execute("  bEgiN ");
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        stmt.execute(" CommIT;");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }
    }
  }


  @ParameterizedTest(name = "test_3_2_readerLoadBalancing_autocommitFalse")
  @MethodSource("testParameters")
  public void test_3_2_readerLoadBalancing_autocommitFalse(final Properties targetDataSourceProps) throws SQLException {
    createDataSourceWithReaderLoadBalancing(targetDataSourceProps);
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(false);
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setAutoCommit(false);
      conn.setReadOnly(true);

      // Connection should not be switched while inside a transaction
      String readerId;
      String nextReaderId;
      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        conn.commit();
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        final Statement stmt = conn.createStatement();
        stmt.execute("commit");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        conn.rollback();
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        final Statement stmt = conn.createStatement();
        stmt.execute(" roLLback ; ");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }
    }
  }

  @ParameterizedTest(name = "test_3_3_readerLoadBalancing_switchAutoCommitInTransaction")
  @MethodSource("testParameters")
  public void test_3_3_readerLoadBalancing_switchAutoCommitInTransaction(final Properties targetDataSourceProps)
      throws SQLException {
    createDataSourceWithReaderLoadBalancing(targetDataSourceProps);
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(false);
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerId;
      String nextReaderId;

      // Start transaction while autocommit is on (autocommit is implicitly disabled)
      // Connection should not be switched while inside a transaction
      Statement stmt = conn.createStatement();
      stmt.execute("  StarT   TRanSACtion  REad onLy  ; ");
      readerId = queryInstanceId(conn);
      nextReaderId = queryInstanceId(conn);
      assertEquals(readerId, nextReaderId);
      conn.setAutoCommit(false); // Switch autocommit value while inside the transaction
      nextReaderId = queryInstanceId(conn);
      assertEquals(readerId, nextReaderId);
      conn.commit();

      assertFalse(conn.getAutoCommit());
      nextReaderId = queryInstanceId(conn);
      assertNotEquals(readerId, nextReaderId); // Connection should have switched after committing

      readerId = nextReaderId;
      nextReaderId = queryInstanceId(conn);
      // Since autocommit is now off, we should be in a transaction; connection should not be switching
      assertEquals(readerId, nextReaderId);
      assertThrows(SQLException.class, () -> conn.setReadOnly(false));

      conn.setAutoCommit(true); // Switch autocommit value while inside the transaction
      stmt = conn.createStatement();
      stmt.execute("commit");

      assertTrue(conn.getAutoCommit());
      readerId = queryInstanceId(conn);

      // Autocommit is now on; connection should switch after each execute
      for (int i = 0; i < 5; i++) {
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
        readerId = nextReaderId;
      }
    }
  }

  private void putDownInstance(final String targetInstance) {
    final Proxy toPutDown = proxyMap.get(targetInstance);
    disableInstanceConnection(toPutDown);
    logger.fine("Took down " + targetInstance);
  }

  private void putDownAllInstances(final Boolean putDownClusters) {
    logger.fine("Putting down all instances");
    proxyMap.forEach((instance, proxy) -> {
      if (putDownClusters || (proxy != proxyCluster && proxy != proxyReadOnlyCluster)) {
        disableInstanceConnection(proxy);
      }
    });
  }

  private void disableInstanceConnection(final Proxy proxy) {
    try {
      containerHelper.disableConnectivity(proxy);
    } catch (final IOException e) {
      fail("Couldn't disable proxy connectivity");
    }
  }

  private void bringUpInstance(final String targetInstance) {
    final Proxy toBringUp = proxyMap.get(targetInstance);
    containerHelper.enableConnectivity(toBringUp);
    logger.fine("Brought up " + targetInstance);
  }

  private static HikariConfig getDefaultConfig() {
    final HikariConfig config = new HikariConfig();
    config.setUsername(AURORA_POSTGRES_USERNAME);
    config.setPassword(AURORA_POSTGRES_PASSWORD);
    config.setMaximumPoolSize(3);
    config.setReadOnly(true);
    config.setExceptionOverrideClassName(HikariCPSQLException.class.getName());
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(1000);

    config.setDataSourceClassName(AwsWrapperDataSource.class.getName());
    config.addDataSourceProperty("targetDataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
    config.addDataSourceProperty("jdbcProtocol", "jdbc:postgresql://");
    config.addDataSourceProperty("portPropertyName", "portNumber");
    config.addDataSourceProperty("serverPropertyName", "serverName");
    config.addDataSourceProperty("databasePropertyName", "databaseName");

    return config;
  }

  private static Properties getDefaultDataSourceProps() {
    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("portNumber", String.valueOf(POSTGRES_PROXY_PORT));
    targetDataSourceProps.setProperty("databaseName", AURORA_POSTGRES_DB);
    targetDataSourceProps.setProperty("socketTimeout", "5");
    targetDataSourceProps.setProperty("connectTimeout", "5");
    targetDataSourceProps.setProperty("monitoring-connectTimeout", "3");
    targetDataSourceProps.setProperty("monitoring-socketTimeout", "3");
    targetDataSourceProps.setProperty(
        AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.name,
        PROXIED_CLUSTER_TEMPLATE);
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "3000");
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "1500");

    return targetDataSourceProps;
  }

  private void createDataSource(final Properties targetDataSourceProps) {
    targetDataSourceProps.setProperty("serverName", clusterTopology.get(0) + PROXIED_DOMAIN_NAME_SUFFIX);

    final HikariConfig config = getDefaultConfig();
    config.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    dataSource = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = dataSource.getHikariPoolMXBean();

    logger.fine("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    logger.fine("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    logger.fine("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  private void createDataSourceWithReaderLoadBalancing(final Properties targetDataSourceProps) {
    targetDataSourceProps.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    createDataSource(targetDataSourceProps);
  }

  private boolean pluginChainIncludesFailoverPlugin(final Properties targetDataSourceProps) {
    final String plugins = targetDataSourceProps.getProperty(PropertyDefinition.PLUGINS.name);
    return plugins.contains("failover");
  }
}
