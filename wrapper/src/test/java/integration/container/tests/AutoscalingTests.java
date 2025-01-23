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
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HikariPoolConfigurator;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY)
@EnableOnDatabaseEngineDeployment({DatabaseEngineDeployment.AURORA})
@EnableOnNumOfInstances(min = 5)
@MakeSureFirstInstanceWriter
@Order(17)
public class AutoscalingTests {
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  protected static Properties getDefaultPropsNoPlugins() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");

    return props;
  }

  protected static Properties getProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    return props;
  }

  protected static Properties getPropsWithFailover() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover,efm2");
    return props;
  }

  protected HikariPoolConfigurator getHikariConfig(int maxPoolSize) {
    return (hostSpec, props) -> {
      final HikariConfig config = new HikariConfig();
      config.setMaximumPoolSize(maxPoolSize);
      config.setInitializationFailTimeout(75000);
      config.setKeepaliveTime(30000);
      return config;
    };
  }

  // Call setReadOnly on a ConnectionWrapper with an internal readerConnection to the deleted
  // reader. The driver should close the deleted instance pool and connect to a different reader.
  @TestTemplate
  public void test_pooledConnectionAutoScaling_setReadOnlyOnOldConnection()
      throws SQLException, InterruptedException {
    final Properties props = getProps();
    final long topologyRefreshRateMs = 5000;
    ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.set(props, "leastConnections");
    AuroraHostListProvider.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.set(props,
        Long.toString(topologyRefreshRateMs));

    final TestEnvironmentInfo testInfo = TestEnvironment.getCurrent().getInfo();
    final List<TestInstanceInfo> instances = testInfo.getDatabaseInfo().getInstances();
    final int originalClusterSize = instances.size();
    final long poolExpirationNanos = TimeUnit.MINUTES.toNanos(3);
    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(
            getHikariConfig(instances.size()),
            null,
            poolExpirationNanos,
            TimeUnit.MINUTES.toNanos(10));
    Driver.setCustomConnectionProvider(provider);

    final List<Connection> connections = new ArrayList<>();
    try {
      for (int i = 1; i < instances.size(); i++) {
        final String connString = ConnectionStringHelper.getWrapperUrl(instances.get(i));
        final Connection conn = DriverManager.getConnection(connString, props);
        connections.add(conn);
      }

      final Connection newInstanceConn;
      final String instanceClass =
          auroraUtil.getDbInstanceClass(TestEnvironment.getCurrent().getInfo().getRequest());
      final TestInstanceInfo newInstance =
          auroraUtil.createInstance(instanceClass, "auto-scaling-instance");
      instances.add(newInstance);
      try {
        newInstanceConn =
            DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
        connections.add(newInstanceConn);
        Thread.sleep(topologyRefreshRateMs);
        newInstanceConn.setReadOnly(true);
        final String readerId = auroraUtil.queryInstanceId(newInstanceConn);

        assertEquals(newInstance.getInstanceId(), readerId);
        // Verify that there is a pool for the new instance
        assertTrue(provider.getHosts().stream()
            .anyMatch((url) -> url.equals(newInstance.getUrl())));
        newInstanceConn.setReadOnly(false);
      } finally {
        auroraUtil.deleteInstance(newInstance);
        instances.remove(newInstance);
      }

      final long deletionCheckTimeout = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
      while (System.nanoTime() < deletionCheckTimeout
          && auroraUtil.getAuroraInstanceIds().size() != originalClusterSize) {
        TimeUnit.SECONDS.sleep(5);
      }

      if (auroraUtil.getAuroraInstanceIds().size() != originalClusterSize) {
        fail("The deleted instance is still in the cluster topology");
      }

      newInstanceConn.setReadOnly(true);
      // Connection pool cache should have hit the cleanup threshold and removed the pool for the
      // deleted instance.
      String instanceId = auroraUtil.queryInstanceId(newInstanceConn);
      assertNotEquals(instances.get(0).getInstanceId(), instanceId);
      assertNotEquals(newInstance.getInstanceId(), instanceId);
      assertFalse(provider.getHosts().stream()
          .anyMatch((url) -> url.equals(newInstance.getUrl())));
      assertEquals(instances.size(), provider.getHosts().size());
    } finally {
      for (Connection connection : connections) {
        connection.close();
      }
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  // Attempt to use a connection to an instance that is now deleted, failover plugin is loaded
  @TestTemplate
  public void test_pooledConnectionAutoScaling_failoverFromDeletedReader()
      throws SQLException, InterruptedException {
    final Properties props = getPropsWithFailover();
    final long topologyRefreshRateMs = 5000;
    ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.set(props, "leastConnections");
    AuroraHostListProvider.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.set(props,
        Long.toString(topologyRefreshRateMs));

    final TestEnvironmentInfo testInfo = TestEnvironment.getCurrent().getInfo();
    final List<TestInstanceInfo> instances = testInfo.getDatabaseInfo().getInstances();
    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(instances.size() * 5));
    Driver.setCustomConnectionProvider(provider);

    final List<Connection> connections = new ArrayList<>();
    try {
      for (int i = 1; i < instances.size(); i++) {
        final String connString = ConnectionStringHelper.getWrapperUrl(instances.get(i));
        // Create 2 connections per instance.
        connections.add(DriverManager.getConnection(connString, props));
        connections.add(DriverManager.getConnection(connString, props));
      }

      final Connection newInstanceConn;
      final String instanceClass =
          auroraUtil.getDbInstanceClass(TestEnvironment.getCurrent().getInfo().getRequest());
      final TestInstanceInfo newInstance =
          auroraUtil.createInstance(instanceClass, "auto-scaling-instance");
      instances.add(newInstance);
      try {
        newInstanceConn =
            DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(newInstance), props);
        connections.add(newInstanceConn);
        newInstanceConn.setReadOnly(true);
        final String readerId = auroraUtil.queryInstanceId(newInstanceConn);
        assertEquals(newInstance.getInstanceId(), readerId);
        // Verify that there is a pool for the new instance
        assertTrue(provider.getHosts().stream()
            .anyMatch((url) -> url.equals(newInstance.getUrl())));
      } finally {
        auroraUtil.deleteInstance(newInstance);
        instances.remove(newInstance);
      }

      auroraUtil.assertFirstQueryThrows(newInstanceConn, FailoverSuccessSQLException.class);
      String newReaderId = auroraUtil.queryInstanceId(newInstanceConn);
      assertNotEquals(newInstance.getInstanceId(), newReaderId);
    } finally {
      for (Connection connection : connections) {
        connection.close();
      }
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }
}
