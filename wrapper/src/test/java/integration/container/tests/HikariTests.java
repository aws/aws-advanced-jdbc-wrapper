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

import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.util.HikariCPSQLException;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.HIKARI)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
@MakeSureFirstInstanceWriter
@Order(8)
@Disabled
public class HikariTests {

  private static final Logger LOGGER = Logger.getLogger(HikariTests.class.getName());

  @TestTemplate
  public void testOpenConnectionWithUrl() throws SQLException {
    final HikariDataSource dataSource = new HikariDataSource();
    final String url = ConnectionStringHelper.getWrapperUrl();
    dataSource.setJdbcUrl(url);
    dataSource.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    dataSource.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    dataSource.addDataSourceProperty(PLUGINS.name, "");

    final Connection conn = dataSource.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    final HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    final ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @TestTemplate
  public void testOpenConnectionWithDataSourceClassName() throws SQLException {

    final HikariDataSource dataSource = new HikariDataSource();
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

    assertTrue(conn instanceof HikariProxyConnection);
    final HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    final ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable.
   */
  @TestTemplate
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnTestFeature({TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED})
  @EnableOnNumOfInstances(min = 3)
  public void testFailoverLostConnection() throws SQLException {
    final Properties customProps = new Properties();
    PLUGINS.set(customProps, "failover");
    FAILOVER_TIMEOUT_MS.set(customProps, Integer.toString(1));
    DriverHelper.setSocketTimeout(customProps, 1, TimeUnit.SECONDS);

    final HikariDataSource dataSource = createDataSource(customProps);

    try (Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));

      ProxyHelper.disableAllConnectivity();

      assertThrows(FailoverFailedSQLException.class, () -> AuroraFailoverTest.auroraUtil.queryInstanceId(conn));
      assertFalse(conn.isValid(5));
    }

    assertThrows(SQLTransientConnectionException.class, dataSource::getConnection);
    ProxyHelper.enableAllConnectivity();
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes
   * unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor.
   */
  @TestTemplate
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
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
    final HikariDataSource dataSource = createDataSource(null);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = dataSource.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentConnectionId = AuroraFailoverTest.auroraUtil.queryInstanceId(conn);
      assertTrue(currentConnectionId.equalsIgnoreCase(writerIdentifier));
      LOGGER.fine("Connected to instance: " + currentConnectionId);

      ProxyHelper.enableConnectivity(readerIdentifier);
      ProxyHelper.disableConnectivity(writerIdentifier);

      assertThrows(FailoverSuccessSQLException.class, () -> AuroraFailoverTest.auroraUtil.queryInstanceId(conn));

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentConnectionId = AuroraFailoverTest.auroraUtil.queryInstanceId(conn);
      LOGGER.fine("Connected to instance: " + currentConnectionId);
      assertTrue(currentConnectionId.equalsIgnoreCase(readerIdentifier));

      // Try to get a new connection to the failed instance, which times out
      assertThrows(SQLTransientConnectionException.class, () -> dataSource.getConnection());
    }

    ProxyHelper.enableAllConnectivity();
    dataSource.close();
  }

  private HikariDataSource createDataSource(final Properties customProps) {
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

    targetDataSourceProps.setProperty(PLUGINS.name, "failover,efm");
    targetDataSourceProps.setProperty(
        "clusterInstanceHostPattern",
        "?."
            + TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstanceEndpointSuffix());

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

    DriverHelper.setMonitoringConnectTimeout(targetDataSourceProps, 3, TimeUnit.SECONDS);
    DriverHelper.setMonitoringSocketTimeout(targetDataSourceProps, 3, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(targetDataSourceProps, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(targetDataSourceProps, 10, TimeUnit.SECONDS);

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
