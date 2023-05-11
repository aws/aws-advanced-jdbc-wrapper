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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestEnvironmentInfo;
import integration.refactored.TestInstanceInfo;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.EnableOnDatabaseEngineDeployment;
import integration.refactored.container.condition.EnableOnNumOfInstances;
import integration.refactored.container.condition.EnableOnTestFeature;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.ConnectionProviderManager;
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
public class AutoscalingTests {
  protected static final AuroraTestUtility auroraUtil =
      new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());
  private static final Logger LOGGER = Logger.getLogger(AutoscalingTests.class.getName());

  protected static Properties getProxiedProps() {
    final Properties props = getProps();
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
        "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo()
            .getInstanceEndpointSuffix());
    return props;
  }

  protected static Properties getDefaultPropsNoPlugins() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setSocketTimeout(props, 10, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(props, 10, TimeUnit.SECONDS);
    return props;
  }

  protected static Properties getProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "auroraHostList,readWriteSplitting");
    return props;
  }

  protected static Properties getPropsWithFailover() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover,efm");
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

  // Old connection detects deleted instance when setReadOnly(true) is called
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
    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(instances.size()));
    ConnectionProviderManager.setConnectionProvider(provider);

    final List<Connection> connections = new ArrayList<>();
    try {
      for (int i = 1; i < instances.size(); i++) {
        final String connString = ConnectionStringHelper.getWrapperUrl(instances.get(i));
        final Connection conn = DriverManager.getConnection(connString, props);
        connections.add(conn);
      }

      final Connection newInstanceConn;
      final TestInstanceInfo newInstance =
          auroraUtil.createInstance("auto-scaling-instance");
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
      }

      // Should detect that the reader was deleted and remove the associated pool
      newInstanceConn.setReadOnly(true);
      assertFalse(provider.getHosts().stream()
          .anyMatch((url) -> url.equals(newInstance.getUrl())));
    } finally {
      for (Connection connection : connections) {
        connection.close();
      }
      ConnectionProviderManager.releaseResources();
      ConnectionProviderManager.resetProvider();
    }
  }

  // User tries to directly connect to the deleted instance URL
  // TODO: Hikari throws an NPE when closing the connection to the instance that is now deleted
  @TestTemplate
  public void test_pooledConnectionAutoScaling_newConnectionToDeletedReader()
      throws SQLException, InterruptedException {
    final Properties props = getProps();
    final long topologyRefreshRateMs = 5000;
    ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.set(props, "leastConnections");
    AuroraHostListProvider.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.set(props,
        Long.toString(topologyRefreshRateMs));

    final TestEnvironmentInfo testInfo = TestEnvironment.getCurrent().getInfo();
    final List<TestInstanceInfo> instances = testInfo.getDatabaseInfo().getInstances();
    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(instances.size() * 5));
    ConnectionProviderManager.setConnectionProvider(provider);

    final List<Connection> connections = new ArrayList<>();
    try {
      for (int i = 1; i < instances.size(); i++) {
        final String connString = ConnectionStringHelper.getWrapperUrl(instances.get(i));
        final Connection conn1 = DriverManager.getConnection(connString, props);
        connections.add(conn1);
        final Connection conn2 = DriverManager.getConnection(connString, props);
        connections.add(conn2);
      }

      final Connection newInstanceConn;
      final TestInstanceInfo newInstance =
          auroraUtil.createInstance("auto-scaling-instance");
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
      } finally {
        auroraUtil.deleteInstance(newInstance);
      }

      Thread.sleep(topologyRefreshRateMs);
      assertThrows(SQLException.class, () ->
          DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(newInstance), props));

      // Verify that the pool for the deleted instance is now removed
      assertFalse(provider.getHosts().stream()
          .anyMatch((url) -> url.equals(newInstance.getUrl())));
    } finally {
      for (Connection connection : connections) {
        // Hikari throws NPE when closing the connection to the deleted instance
        connection.close();
      }
      ConnectionProviderManager.releaseResources();
      ConnectionProviderManager.resetProvider();
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
    ConnectionProviderManager.setConnectionProvider(provider);

    final List<Connection> connections = new ArrayList<>();
    try {
      for (int i = 1; i < instances.size(); i++) {
        final String connString = ConnectionStringHelper.getWrapperUrl(instances.get(i));
        final Connection conn1 = DriverManager.getConnection(connString, props);
        connections.add(conn1);
        final Connection conn2 = DriverManager.getConnection(connString, props);
        connections.add(conn2);
      }

      final Connection newInstanceConn;
      final TestInstanceInfo newInstance =
          auroraUtil.createInstance("auto-scaling-instance");
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
      }

      auroraUtil.assertFirstQueryThrows(newInstanceConn, FailoverSuccessSQLException.class);
      String newReaderId = auroraUtil.queryInstanceId(newInstanceConn);
      assertNotEquals(newInstance.getInstanceId(), newReaderId);
    } finally {
      for (Connection connection : connections) {
        connection.close();
      }
      ConnectionProviderManager.releaseResources();
      ConnectionProviderManager.resetProvider();
    }
  }
}
