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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;

public class AuroraPostgresStaleDnsTest extends AuroraPostgresBaseTest {

  private static final Logger LOGGER = Logger.getLogger(AuroraPostgresStaleDnsTest.class.getName());

  @Disabled
  @Test
  public void test_StaleDnsDataMitigated()
      throws SQLException, InterruptedException, UnknownHostException {

    final String initialWriterId = instanceIDs[0];
    LOGGER.finest("Initial writer node: " + initialWriterId);
    LOGGER.finest("Initial writer address: " + InetAddress.getByName(initialWriterId + DB_CONN_STR_SUFFIX));
    LOGGER.finest("Initial cluster endpoint DNS: " + InetAddress.getByName(POSTGRES_CLUSTER_URL));

    AuroraHostListProvider.logCache();

    // Failover is supported for the following connection.
    try (final Connection conn = connectToInstance(POSTGRES_CLUSTER_URL, AURORA_POSTGRES_PORT, initDefaultProps())) {

      final String currentConnectionId = queryInstanceId(conn);
      LOGGER.finest("Initial writer node connected with cluster endpoint: " + currentConnectionId);
      //assertTrue(isDBInstanceWriter(currentConnectionId));
    }

    AuroraHostListProvider.logCache();

    LOGGER.finest("Connecting to " + initialWriterId + DB_CONN_STR_SUFFIX);

    // Failover is supported for the following connection.
    try (final Connection conn =
        connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_POSTGRES_PORT, initDefaultProps())) {

      failoverCluster(); // initiate a failover

      Instant endTime = Instant.now().plusSeconds(300); // 5 min
      while (Instant.now().isBefore(endTime)) {
        try {
          LOGGER.finest("Current node: " + queryInstanceId(conn));
          TimeUnit.SECONDS.sleep(1);
        } catch (FailoverSuccessSQLException ex) {
          LOGGER.finest("Failover is processed.");
          break;
        } catch (SQLException ex) {
          // Failover has failed. Test failed.
          throw ex;
        }
      }
      final String newWriterId = queryInstanceId(conn);
      LOGGER.finest("Current node after failover: " + newWriterId);
      LOGGER.finest("Writer node address after failover: " + InetAddress.getByName(newWriterId + DB_CONN_STR_SUFFIX));
    }

    AuroraHostListProvider.logCache();

    // No failover support for the following connection.
    Properties props = initDefaultProps();
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraHostList,auroraStaleDns");

    LOGGER.finest("Cluster endpoint DNS after failover: " + InetAddress.getByName(POSTGRES_CLUSTER_URL));
    for (String instanceId : instanceIDs) {
      LOGGER.finest(instanceId + " resolves to " + InetAddress.getByName(instanceId + DB_CONN_STR_SUFFIX));
    }

    Instant endTime = Instant.now().plusSeconds(300); // 5 min
    while (Instant.now().isBefore(endTime)) {
      try {
        try (final Connection conn2 = connectToInstance(POSTGRES_CLUSTER_URL, AURORA_POSTGRES_PORT, props)) {

          String currentConnectionId = queryInstanceId(conn2);
          LOGGER.finest("Writer node connected with cluster endpoint after failover: " + currentConnectionId);

          TimeUnit.SECONDS.sleep(40); // longer than topology cache expiration time

          currentConnectionId = queryInstanceId(conn2);
          LOGGER.finest("currentConnectionId: " + currentConnectionId);

          AuroraHostListProvider.logCache();

          // Refresh local variables
          List<String> latestTopology = getTopologyIds();
          instanceIDs = new String[latestTopology.size()];
          latestTopology.toArray(instanceIDs);
          clusterSize = instanceIDs.length;
          makeSureInstancesUp(instanceIDs);

          assertTrue(isDBInstanceWriter(currentConnectionId));

          break;
        }
      } catch (SQLException ex) {
        LOGGER.finest("Error connecting to DB: " + ex.getMessage());
        TimeUnit.SECONDS.sleep(3);
      }
    }
  }
}
