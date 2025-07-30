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

import static integration.util.AuroraTestUtility.executeWithTimeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.jdbc.PropertyDefinition.PLUGINS;
import static software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.TestProxyDatabaseInfo;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.AcceptsUrlFunc;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HikariPoolConfigurator;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.util.HikariCPSQLException;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.HIKARI)
@EnableOnDatabaseEngineDeployment({DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@MakeSureFirstInstanceWriter
@Order(8)
public class HikariTests {

  private static final Logger LOGGER = Logger.getLogger(HikariTests.class.getName());
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  @TestTemplate
  public void testOpenConnectionWithUrl() throws SQLException {
    try (final HikariDataSource dataSource = new HikariDataSource()) {
      final String url = ConnectionStringHelper.getWrapperUrl();
      dataSource.setJdbcUrl(url);
      dataSource.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
      dataSource.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
      dataSource.addDataSourceProperty(PLUGINS.name, "");

      final Connection conn = dataSource.getConnection();

      assertInstanceOf(HikariProxyConnection.class, conn);
      final HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

      assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
      final ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
      assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

      assertTrue(conn.isValid(10));
      conn.close();
    }
  }

  @TestTemplate
  public void testOpenConnectionWithDataSourceClassName() throws SQLException {

    try (final HikariDataSource dataSource = new HikariDataSource()) {

      dataSource.setDataSourceClassName(AwsWrapperDataSource.class.getName());

      // Configure the connection pool:
      dataSource.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
      dataSource.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

      // Configure AwsWrapperDataSource:
      dataSource.addDataSourceProperty("jdbcProtocol", DriverHelper.getDriverProtocol());
      dataSource.addDataSourceProperty("serverName",
          TestEnvironment.getCurrent()
              .getInfo()
              .getDatabaseInfo()
              .getInstances()
              .get(0)
              .getHost());
      dataSource.addDataSourceProperty(PropertyDefinition.DATABASE.name,
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      // Specify the driver-specific DataSource for AwsWrapperDataSource:
      dataSource.addDataSourceProperty("targetDataSourceClassName",
          DriverHelper.getDataSourceClassname());

      // Configuring driver-specific DataSource:
      final Properties targetDataSourceProps = new Properties();
      targetDataSourceProps.setProperty(PLUGINS.name, "");

      if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
          && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
          == DatabaseEngine.MYSQL) {
        // Connecting to Mysql database with MariaDb driver requires a configuration parameter
        // "permitMysqlScheme"
        targetDataSourceProps.setProperty("permitMysqlScheme", "1");
      }

      dataSource.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

      final Connection conn = dataSource.getConnection();

      assertInstanceOf(HikariProxyConnection.class, conn);
      final HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

      assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
      final ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
      assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

      assertTrue(conn.isValid(10));
      conn.close();
    }
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable.
   */
  @TestTemplate
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  @EnableOnNumOfInstances(min = 3)
  public void testFailoverLostConnection() throws SQLException {
    final Properties customProps = new Properties();
    PLUGINS.set(customProps, "failover");
    FAILOVER_TIMEOUT_MS.set(customProps, Integer.toString(1));
    PropertyDefinition.SOCKET_TIMEOUT.set(customProps, String.valueOf(TimeUnit.SECONDS.toMillis(1)));

    try (final HikariDataSource dataSource = createHikariDataSource(customProps)) {

      try (Connection conn = dataSource.getConnection()) {
        assertTrue(conn.isValid(5));

        ProxyHelper.disableAllConnectivity();

        assertThrows(FailoverFailedSQLException.class, () ->
            // It's expected that the following statement executes in matter of seconds
            // (since we rely on small socket timeout and small failover timeout). However,
            // if it takes more time, we'd like to exit by timeout rather than waiting indefinitely.
            executeWithTimeout(
                () -> auroraUtil.queryInstanceId(conn),
                TimeUnit.MINUTES.toMillis(1)
            )
        );
        assertFalse(conn.isValid(5));
      }

      assertThrows(SQLTransientConnectionException.class, dataSource::getConnection);

    } finally {
      ProxyHelper.enableAllConnectivity();
    }
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes
   * unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor.
   */
  @TestTemplate
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  @EnableOnNumOfInstances(min = 3)
  public void testEFMFailover() throws SQLException {
    ProxyHelper.disableAllConnectivity();

    final List<TestInstanceInfo> instances = TestEnvironment.getCurrent()
        .getInfo()
        .getProxyDatabaseInfo()
        .getInstances();

    final String writerIdentifier = instances.get(0).getInstanceId();
    final String readerIdentifier = instances.get(1).getInstanceId();
    LOGGER.fine("Instance to connect to: " + writerIdentifier);
    LOGGER.fine("Instance to fail over to: " + readerIdentifier);

    ProxyHelper.enableConnectivity(writerIdentifier);
    try (final HikariDataSource dataSource = createHikariDataSource(null)) {

      // Get a valid connection, then make it fail over to a different instance
      try (Connection conn = dataSource.getConnection()) {
        assertTrue(conn.isValid(5));
        String currentConnectionId = auroraUtil.queryInstanceId(conn);
        assertTrue(currentConnectionId.equalsIgnoreCase(writerIdentifier));
        LOGGER.fine("Connected to instance: " + currentConnectionId);

        ProxyHelper.enableConnectivity(readerIdentifier);
        ProxyHelper.disableConnectivity(writerIdentifier);

        assertThrows(FailoverSuccessSQLException.class, () -> auroraUtil.queryInstanceId(conn));

        // Check the connection is valid after connecting to a different instance
        assertTrue(conn.isValid(5));
        currentConnectionId = auroraUtil.queryInstanceId(conn);
        LOGGER.fine("Connected to instance: " + currentConnectionId);
        assertTrue(currentConnectionId.equalsIgnoreCase(readerIdentifier));

        // Try to get a new connection to the failed instance, which times out
        assertThrows(SQLTransientConnectionException.class, dataSource::getConnection);
      }
    } finally {
      ProxyHelper.enableAllConnectivity();
    }
  }

  /**
   * After successfully opening and returning a connection to the Hikari pool, writer failover is triggered when
   * getConnection is called.
   */
  @TestTemplate
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  @EnableOnNumOfInstances(min = 2)
  public void testInternalPools_driverWriterFailoverOnGetConnectionInvocation()
      throws SQLException, InterruptedException {
    final TestProxyDatabaseInfo proxyInfo = TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo();
    final List<TestInstanceInfo> instances = proxyInfo.getInstances();
    final TestInstanceInfo reader = instances.get(1);
    final String readerId = reader.getInstanceId();

    setupInternalConnectionPools(getInstanceUrlSubstring(reader.getHost()));
    try {
      final Properties targetDataSourceProps = new Properties();
      targetDataSourceProps.setProperty(FailoverConnectionPlugin.FAILOVER_MODE.name, "strict-writer");
      final AwsWrapperDataSource ds = createWrapperDataSource(reader, proxyInfo, targetDataSourceProps);

      // Open connection and then return it to the pool
      Connection conn = ds.getConnection();
      assertEquals(readerId, auroraUtil.queryInstanceId(conn));
      conn.close();

      ProxyHelper.disableConnectivity(reader.getInstanceId());
      // Hikari's 'com.zaxxer.hikari.aliveBypassWindowMs' property is set to 500ms by default. We need to wait this long
      // to trigger Hikari's validation attempts when HikariDatasource#getConnection is called. These attempts will fail
      // and Hikari will throw an exception, which should trigger failover.
      TimeUnit.MILLISECONDS.sleep(500);

      // Driver will fail over internally and return a connection to another node.
      conn = ds.getConnection();
      // Assert that we connected to a different node.
      assertNotEquals(readerId, auroraUtil.queryInstanceId(conn));
    } finally {
      ConnectionProviderManager.releaseResources();
    }
  }

  /**
   * After successfully opening and returning connections to the Hikari pool, reader failover is triggered when
   * getConnection is called.
   */
  @TestTemplate
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  @EnableOnNumOfInstances(min = 2)
  public void testInternalPools_driverReaderFailoverOnGetConnectionInvocation()
      throws SQLException, InterruptedException {
    final TestProxyDatabaseInfo proxyInfo = TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo();
    final List<TestInstanceInfo> instances = proxyInfo.getInstances();
    final TestInstanceInfo writer = instances.get(0);
    final String writerId = writer.getInstanceId();

    setupInternalConnectionPools(getInstanceUrlSubstring(writer.getHost()));
    try {
      final Properties targetDataSourceProps = new Properties();
      targetDataSourceProps.setProperty(FailoverConnectionPlugin.FAILOVER_MODE.name, "strict-reader");
      final AwsWrapperDataSource ds = createWrapperDataSource(writer, proxyInfo, targetDataSourceProps);

      // Open some connections.
      List<Connection> connections = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Connection conn = ds.getConnection();
        connections.add(conn);
      }

      // Return the opened connections to the pool.
      for (Connection conn : connections) {
        conn.close();
      }

      ProxyHelper.disableConnectivity(writer.getInstanceId());
      // Hikari's 'com.zaxxer.hikari.aliveBypassWindowMs' property is set to 500ms by default. We need to wait this long
      // to trigger Hikari's validation attempts when HikariDatasource#getConnection is called. These attempts will fail
      // and Hikari will throw an exception, which should trigger failover.
      TimeUnit.MILLISECONDS.sleep(500);

      // Driver will fail over internally and return a connection to another node.
      try (Connection conn = ds.getConnection()) {
        // Assert that we connected to a different node.
        assertNotEquals(writerId, auroraUtil.queryInstanceId(conn));
      }
    } finally {
      ConnectionProviderManager.releaseResources();
    }
  }

  /**
   * After successfully opening and returning a connection to the Hikari pool, writer failover is triggered when
   * getConnection is called. Since the cluster only has one instance and the instance stays down, failover fails.
   */
  @TestTemplate
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  @EnableOnNumOfInstances(max = 1)
  public void testInternalPools_driverWriterFailoverOnGetConnectionInvocation_singleInstance()
      throws SQLException, InterruptedException {
    final TestProxyDatabaseInfo proxyInfo = TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo();
    final List<TestInstanceInfo> instances = proxyInfo.getInstances();
    final TestInstanceInfo writer = instances.get(0);
    final String writerId = writer.getInstanceId();

    setupInternalConnectionPools(getInstanceUrlSubstring(writer.getHost()));
    try {
      final Properties targetDataSourceProps = new Properties();
      targetDataSourceProps.setProperty(FailoverConnectionPlugin.FAILOVER_MODE.name, "strict-writer");
      targetDataSourceProps.setProperty(FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.name, "5000");
      final AwsWrapperDataSource ds = createWrapperDataSource(writer, proxyInfo, targetDataSourceProps);

      // Open connection and then return it to the pool
      Connection conn = ds.getConnection();
      assertEquals(writerId, auroraUtil.queryInstanceId(conn));
      conn.close();

      ProxyHelper.disableAllConnectivity();
      // Hikari's 'com.zaxxer.hikari.aliveBypassWindowMs' property is set to 500ms by default. We need to wait this long
      // to trigger Hikari's validation attempts when HikariDatasource#getConnection is called. These attempts will fail
      // and Hikari will throw an exception, which should trigger failover.
      TimeUnit.MILLISECONDS.sleep(500);

      // Driver will attempt to fail over internally, but the node is still down, so it fails.
      assertThrows(FailoverFailedSQLException.class, ds::getConnection);
    } finally {
      ConnectionProviderManager.releaseResources();
    }
  }

  /**
   * Given an instance URL, extracts the substring of the URL that is common to all instance URLs. For example, given
   * "instance-1.ABC.cluster-XYZ.us-west-2.rds.amazonaws.com.proxied", returns
   * ".ABC.cluster-XYZ.us-west-2.rds.amazonaws.com.proxied"
   */
  private String getInstanceUrlSubstring(String instanceUrl) {
    int substringStart = instanceUrl.indexOf(".");
    return instanceUrl.substring(substringStart);
  }

  private AwsWrapperDataSource createWrapperDataSource(TestInstanceInfo instanceInfo,
      TestProxyDatabaseInfo proxyInfo, Properties targetDataSourceProps) {
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "auroraConnectionTracker,failover,efm");
    targetDataSourceProps.setProperty(FailoverConnectionPlugin.ENABLE_CONNECT_FAILOVER.name, "true");
    targetDataSourceProps.setProperty(RdsHostListProvider.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.name,
        String.valueOf(TimeUnit.MINUTES.toMillis(5)));
    targetDataSourceProps.setProperty(RdsHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.name,
        "?." + proxyInfo.getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    targetDataSourceProps.setProperty(RdsHostListProvider.CLUSTER_ID.name, "HikariTestsCluster");

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.MYSQL) {
      targetDataSourceProps.setProperty("permitMysqlScheme", "1");
    }

    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceProperties(targetDataSourceProps);
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());
    ds.setServerName(instanceInfo.getHost());
    ds.setServerPort(String.valueOf(instanceInfo.getPort()));
    ds.setDatabase(proxyInfo.getDefaultDbName());
    ds.setUser(proxyInfo.getUsername());
    ds.setPassword(proxyInfo.getPassword());

    return ds;
  }

  private void setupInternalConnectionPools(String instanceUrlSubstring) {
    HikariPoolConfigurator hikariConfigurator = (hostSpec, props) -> {
      HikariConfig config = new HikariConfig();
      config.setMaximumPoolSize(30);
      config.setMinimumIdle(2);
      config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
      config.setInitializationFailTimeout(-1);
      config.setConnectionTimeout(1500);
      config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
      config.setValidationTimeout(1000);
      config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
      config.setReadOnly(true);
      config.setAutoCommit(true);
      return config;
    };

    AcceptsUrlFunc acceptsUrlFunc = (hostSpec, props) -> hostSpec.getHost().contains(instanceUrlSubstring);

    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(
            hikariConfigurator,
            null,
            acceptsUrlFunc,
            TimeUnit.MINUTES.toNanos(30),
            TimeUnit.MINUTES.toNanos(10));
    Driver.setCustomConnectionProvider(provider);
  }

  private HikariDataSource createHikariDataSource(final Properties customProps) {
    final HikariConfig config = getConfig(customProps);
    final HikariDataSource dataSource = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = dataSource.getHikariPoolMXBean();

    LOGGER.fine("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    LOGGER.fine("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    LOGGER.fine("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
    return dataSource;
  }

  private HikariConfig getConfig(final Properties customProps) {
    final HikariConfig config = new HikariConfig();
    final TestProxyDatabaseInfo proxyDatabaseInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo();
    config.setUsername(proxyDatabaseInfo.getUsername());
    config.setPassword(proxyDatabaseInfo.getPassword());

    /*
     It should be 1 max connection. Otherwise, HikariCP will start adding more connections in
     background. Taking into account that tests intensively enable/disable connectivity to
     configured host (provider with "serverName" property), these attempts may fail.
     That makes test logs less readable but causes no functional failures to the test itself.
    */
    config.setMaximumPoolSize(1);

    config.setExceptionOverrideClassName(HikariCPSQLException.class.getName());
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(1000);

    config.setDataSourceClassName(AwsWrapperDataSource.class.getName());
    config.addDataSourceProperty("targetDataSourceClassName",
        DriverHelper.getDataSourceClassname());
    config.addDataSourceProperty("jdbcProtocol", DriverHelper.getDriverProtocol());
    config.addDataSourceProperty("serverName",
        TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstances()
            .get(0)
            .getHost());
    config.addDataSourceProperty("database",
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
    config.addDataSourceProperty("serverPort",
        Integer.toString(TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo()
            .getClusterEndpointPort()));

    final Properties targetDataSourceProps = ConnectionStringHelper.getDefaultProperties();

    targetDataSourceProps.setProperty(PLUGINS.name, "failover,efm2");
    targetDataSourceProps.setProperty(
        "clusterInstanceHostPattern",
        "?."
            + TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());

    targetDataSourceProps.setProperty(
        FailoverConnectionPlugin.FAILOVER_MODE.name, "reader-or-writer");
    targetDataSourceProps.setProperty(
        HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "2000");
    targetDataSourceProps.setProperty(
        HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "1000");
    targetDataSourceProps.setProperty(
        HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "1");

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.MYSQL) {
      // Connecting to Mysql database with MariaDb driver requires a configuration parameter
      // "permitMysqlScheme"
      targetDataSourceProps.setProperty("permitMysqlScheme", "1");
    }

    targetDataSourceProps.setProperty("monitoring-" + PropertyDefinition.CONNECT_TIMEOUT.name, "3000");
    targetDataSourceProps.setProperty("monitoring-" + PropertyDefinition.SOCKET_TIMEOUT.name, "3000");
    targetDataSourceProps.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, "10000");
    targetDataSourceProps.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, "10000");

    if (customProps != null) {
      final Enumeration<?> propertyNames = customProps.propertyNames();
      while (propertyNames.hasMoreElements()) {
        final String propertyName = propertyNames.nextElement().toString();
        if (!StringUtils.isNullOrEmpty(propertyName)) {
          final String propertyValue = customProps.getProperty(propertyName);
          targetDataSourceProps.setProperty(propertyName, propertyValue);
        }
      }
    }

    config.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);
    return config;
  }
}
