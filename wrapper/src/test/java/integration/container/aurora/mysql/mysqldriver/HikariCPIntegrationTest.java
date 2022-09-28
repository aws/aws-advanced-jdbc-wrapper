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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import eu.rekawek.toxiproxy.Proxy;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.util.HikariCPSQLException;
import software.amazon.jdbc.util.SqlState;

public class HikariCPIntegrationTest extends MysqlAuroraMysqlBaseTest {
  private static final Logger logger = Logger.getLogger(HikariCPIntegrationTest.class.getName());
  private static final String URL_SUFFIX = PROXIED_DOMAIN_NAME_SUFFIX + ":" + MYSQL_PROXY_PORT;
  private static HikariDataSource data_source = null;
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

  @BeforeAll
  static void setup() {
    System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");
  }

  @AfterEach
  public void teardown() {
    data_source.close();
  }

  @BeforeEach
  public void setUpTest() {
    String writerEndpoint = clusterTopology.get(0);

    String jdbcUrl = DB_CONN_STR_PREFIX + writerEndpoint + URL_SUFFIX;
    logger.fine("Writer endpoint: " + jdbcUrl);

    final HikariConfig config = new HikariConfig();

    config.setJdbcUrl(jdbcUrl);
    config.setUsername(AURORA_MYSQL_USERNAME);
    config.setPassword(AURORA_MYSQL_PASSWORD);
    config.setMaximumPoolSize(3);
    config.setReadOnly(true);
    config.setExceptionOverrideClassName(HikariCPSQLException.class.getName());
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(1000);
    config.addDataSourceProperty(PropertyDefinition.PLUGINS.name, "failover");
    config.addDataSourceProperty(FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.name, "5000");
    config.addDataSourceProperty(FailoverConnectionPlugin.FAILOVER_READER_CONNECT_TIMEOUT_MS.name, "1000");
    config.addDataSourceProperty(AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.name, PROXIED_CLUSTER_TEMPLATE);
    config.addDataSourceProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "3000");
    config.addDataSourceProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "1500");

    data_source = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = data_source.getHikariPoolMXBean();

    logger.fine("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    logger.fine("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    logger.fine("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable.
   */
  @Disabled
  @Test
  public void test_1_1_hikariCP_lost_connection() throws SQLException {
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));

      putDownAllInstances(true);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
      assertFalse(conn.isValid(5));
    }

    assertThrows(SQLTransientConnectionException.class, () -> data_source.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance. A connection is then retrieved to check that connections
   * to failed instances are not returned.
   */
  @Test
  public void test_1_2_hikariCP_get_dead_connection() throws SQLException {
    putDownAllInstances(false);

    String writer = clusterTopology.get(0);
    String reader = clusterTopology.get(1);
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    logger.fine("Instance to connect to: " + writerIdentifier);
    logger.fine("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      bringUpInstance(readerIdentifier);
      putDownInstance(currentInstance);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      logger.fine("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));

      // Try to get a new connection to the failed instance, which times out
      assertThrows(SQLTransientConnectionException.class, () -> data_source.getConnection());
    }
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor.
   */
  @Disabled
  @Test
  public void test_2_1_hikariCP_efm_failover() throws SQLException {
    putDownAllInstances(false);

    String writer = clusterTopology.get(0);
    String reader = clusterTopology.get(1);
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    logger.fine("Instance to connect to: " + writerIdentifier);
    logger.fine("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      logger.fine("Connected to instance: " + currentInstance);

      bringUpInstance(readerIdentifier);
      putDownInstance(writerIdentifier);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      logger.fine("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
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
}
