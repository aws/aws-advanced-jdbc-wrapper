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

package integration.container.aurora.mysql.mysqldriver;

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
import integration.container.aurora.mysql.AuroraMysqlBaseTest;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.util.HikariCPSQLException;
import software.amazon.jdbc.util.SqlState;

@Disabled
public class HikariCPReadWriteSplittingTest extends AuroraMysqlBaseTest {

  private static final Logger logger = Logger.getLogger(
      integration.container.aurora.mysql.mariadbdriver.HikariCPReadWriteSplittingTest.class.getName());
  private static HikariDataSource dataSource = null;
  private final List<String> clusterTopology = fetchTopology();

  private List<String> fetchTopology() {
    try {
      List<String> topology = getTopologyEndpoints();
      // topology should contain a writer and at least one reader
      if (topology == null || topology.size() < 2) {
        fail("Topology does not contain the required instances");
      }
      return topology;
    } catch (SQLException e) {
      fail("Couldn't fetch cluster topology");
    }

    return null;
  }

  private static Stream<Arguments> testParameters() {
    return Stream.of(
        Arguments.of("readWriteSplitting,failover,efm"),
        Arguments.of("failover,efm")
    );
  }

  @BeforeAll
  static void setup() {
    System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");
  }

  @AfterEach
  public void teardown() {
    dataSource.close();
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable.
   */
  @ParameterizedTest(name = "test_1_1_hikariCP_lost_connection")
  @MethodSource("testParameters")
  public void test_1_1_hikariCP_lost_connection(String plugins) throws SQLException {
    createDefaultDataSource(plugins);
    try (Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));

      putDownAllInstances(true);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      if (plugins.contains("failover")) {
        assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
      } else {
        assertEquals(SqlState.COMMUNICATION_ERROR.getState(), exception.getSQLState());
      }
      assertFalse(conn.isValid(5));
    }

    assertThrows(SQLTransientConnectionException.class, () -> dataSource.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance. A connection is then retrieved to check that connections
   * to failed instances are not returned
   */
  @ParameterizedTest(name = "test_1_2_hikariCP_get_dead_connection")
  @MethodSource("testParameters")
  public void test_1_2_hikariCP_get_dead_connection(String plugins) throws SQLException {
    putDownAllInstances(false);

    String writer = clusterTopology.get(0);
    String reader = clusterTopology.get(1);
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    logger.fine("Instance to connect to: " + writerIdentifier);
    logger.fine("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);
    createDefaultDataSource(plugins);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      bringUpInstance(readerIdentifier);
      putDownInstance(currentInstance);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      if (plugins.contains("failover")) {
        assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());
      } else {
        assertEquals(SqlState.COMMUNICATION_ERROR.getState(), exception.getSQLState());
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
  public void test_2_1_hikariCP_efm_failover(String plugins) throws SQLException {
    putDownAllInstances(false);

    String writer = clusterTopology.get(0);
    String reader = clusterTopology.get(1);
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    logger.fine("Instance to connect to: " + writerIdentifier);
    logger.fine("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);
    createDefaultDataSource(plugins);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      logger.fine("Connected to instance: " + currentInstance);

      bringUpInstance(readerIdentifier);
      putDownInstance(writerIdentifier);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      if (plugins.contains("failover")) {
        assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());
      } else {
        assertEquals(SqlState.COMMUNICATION_ERROR.getState(), exception.getSQLState());
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
  public void test_3_1_readerLoadBalancing_autocommitTrue(String plugins) throws SQLException {
    createDataSourceWithReaderLoadBalancing(plugins);
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(false);
      String writerConnectionId = queryInstanceId(conn);
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
      Statement stmt = conn.createStatement();
      for (int i = 0; i < 5; i++) {
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
  public void test_3_2_readerLoadBalancing_autocommitFalse(String plugins) throws SQLException {
    createDataSourceWithReaderLoadBalancing(plugins);
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(false);
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setAutoCommit(false);
      conn.setReadOnly(true);

      // Connection should not be switched while inside a transaction
      String readerId;
      String nextReaderId;
      Statement stmt = conn.createStatement();
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
        stmt.execute(" roLLback ; ");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }
    }
  }

  @ParameterizedTest(name = "test_3_3_readerLoadBalancing_switchAutoCommitInTransaction")
  @MethodSource("testParameters")
  public void test_3_3_readerLoadBalancing_switchAutoCommitInTransaction(String plugins) throws SQLException {
    createDataSourceWithReaderLoadBalancing(plugins);
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(false);
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      Statement stmt = conn.createStatement();
      String readerId;
      String nextReaderId;

      // Start transaction while autocommit is on (autocommit is implicitly disabled)
      // Connection should not be switched while inside a transaction
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

  private void putDownInstance(String targetInstance) {
    Proxy toPutDown = proxyMap.get(targetInstance);
    disableInstanceConnection(toPutDown);
    logger.fine("Took down " + targetInstance);
  }

  private void putDownAllInstances(Boolean putDownClusters) {
    logger.fine("Putting down all instances");
    proxyMap.forEach((instance, proxy) -> {
      if (putDownClusters || (proxy != proxyCluster && proxy != proxyReadOnlyCluster)) {
        disableInstanceConnection(proxy);
      }
    });
  }

  private void disableInstanceConnection(Proxy proxy) {
    try {
      containerHelper.disableConnectivity(proxy);
    } catch (IOException e) {
      fail("Couldn't disable proxy connectivity");
    }
  }

  private void bringUpInstance(String targetInstance) {
    Proxy toBringUp = proxyMap.get(targetInstance);
    containerHelper.enableConnectivity(toBringUp);
    logger.fine("Brought up " + targetInstance);
  }

  private HikariConfig getDefaultConfig(Properties customTestProps) {
    final HikariConfig config = new HikariConfig();

    config.setUsername(AURORA_MYSQL_USERNAME);
    config.setPassword(AURORA_MYSQL_PASSWORD);
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

    Properties targetDataSourceProps = new Properties(customTestProps);
    targetDataSourceProps.setProperty("serverName", clusterTopology.get(0) + PROXIED_DOMAIN_NAME_SUFFIX);
    targetDataSourceProps.setProperty("portNumber", String.valueOf(MYSQL_PROXY_PORT));
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "failover,efm");
    targetDataSourceProps.setProperty("socketTimeout", "5");
    targetDataSourceProps.setProperty("connectTimeout", "5");
    targetDataSourceProps.setProperty("monitoring-connectTimeout", "3");
    targetDataSourceProps.setProperty("monitoring-socketTimeout", "3");
    targetDataSourceProps.setProperty(FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.name, "5000");
    targetDataSourceProps.setProperty(FailoverConnectionPlugin.FAILOVER_READER_CONNECT_TIMEOUT_MS.name, "3000");
    targetDataSourceProps.setProperty(
        AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.name,
        PROXIED_CLUSTER_TEMPLATE);
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "3000");
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "1500");
    config.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    return config;
  }

  private void createDataSource(HikariConfig config) {
    dataSource = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = dataSource.getHikariPoolMXBean();

    logger.fine("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    logger.fine("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    logger.fine("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  private void createDefaultDataSource(String plugins) {
    Properties customTestProps = new Properties();
    customTestProps.setProperty(PropertyDefinition.PLUGINS.name, plugins);
    createDataSource(getDefaultConfig(customTestProps));
  }

  private void createDataSourceWithReaderLoadBalancing(String plugins) {
    Properties customTestProps = new Properties();
    customTestProps.setProperty(PropertyDefinition.PLUGINS.name, plugins);
    customTestProps.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    HikariConfig config = getDefaultConfig(customTestProps);
    createDataSource(config);
  }
}
