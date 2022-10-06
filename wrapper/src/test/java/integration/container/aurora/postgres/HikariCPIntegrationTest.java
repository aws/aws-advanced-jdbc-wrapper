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

import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.util.HikariCPSQLException;

public class HikariCPIntegrationTest extends AuroraPostgresBaseTest {

  private static final Logger logger = Logger.getLogger(HikariCPIntegrationTest.class.getName());
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

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", clusterTopology.get(0) + PROXIED_DOMAIN_NAME_SUFFIX);
    targetDataSourceProps.setProperty("portNumber", String.valueOf(POSTGRES_PROXY_PORT));
    targetDataSourceProps.setProperty("databaseName", AURORA_POSTGRES_DB);
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "failover,efm");
    targetDataSourceProps.setProperty("socketTimeout", "5");
    targetDataSourceProps.setProperty("connectTimeout", "5");
    targetDataSourceProps.setProperty("monitoring-connectTimeout", "3");
    targetDataSourceProps.setProperty("monitoring-socketTimeout", "3");
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "failover,efm");
    targetDataSourceProps.setProperty(
        AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.name,
        PROXIED_CLUSTER_TEMPLATE);
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "3000");
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "1500");
    config.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    data_source = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = data_source.getHikariPoolMXBean();

    logger.fine("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    logger.fine("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    logger.fine("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable.
   */
  @Test
  public void test_1_1_hikariCP_lost_connection() throws SQLException {
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));

      putDownAllInstances(true);

      assertThrows(FailoverFailedSQLException.class, () -> queryInstanceId(conn));
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

      assertThrows(FailoverSuccessSQLException.class, () -> queryInstanceId(conn));

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

      assertThrows(FailoverSuccessSQLException.class, () -> queryInstanceId(conn));

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
