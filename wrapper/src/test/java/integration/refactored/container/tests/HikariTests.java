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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestInstanceInfo;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.MakeSureFirstInstanceWriterExtension;
import integration.refactored.container.ProxyHelper;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestFeature;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
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
@ExtendWith(MakeSureFirstInstanceWriterExtension.class)
@DisableOnTestFeature(TestEnvironmentFeatures.PERFORMANCE)
public class HikariTests {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingTests.class.getName());

  @TestTemplate
  public void testOpenConnectionWithUrl() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(ConnectionStringHelper.getWrapperUrl());
    ds.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @TestTemplate
  public void testOpenConnectionWithDataSourceClassName() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());

    // Configure the connection pool:
    ds.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    // Configure AwsWrapperDataSource:
    ds.addDataSourceProperty(
        "jdbcProtocol", DriverHelper.getDriverProtocol());
    ds.addDataSourceProperty("databasePropertyName", "databaseName");
    ds.addDataSourceProperty("portPropertyName", "port");
    ds.addDataSourceProperty("serverPropertyName", "serverName");

    // Specify the driver-specific data source for AwsWrapperDataSource:
    ds.addDataSourceProperty("targetDataSourceClassName", DriverHelper.getDataSourceClassname());

    // Configuring MariadbDataSource:
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "serverName",
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpoint());
    targetDataSourceProps.setProperty(
        "databaseName",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    targetDataSourceProps.setProperty("url", ConnectionStringHelper.getUrl());
    ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable.
   */
  @TestTemplate
  public void test_1_1_hikariCP_lost_connection() throws SQLException {
    Properties customProps = new Properties();
    customProps.setProperty(FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.name, "2");
    createDataSource(customProps);
    HikariDataSource ds = new HikariDataSource();
    try (Connection conn = ds.getConnection()) {
      assertTrue(conn.isValid(5));

      ProxyHelper.disableAllConnectivity();

      assertThrows(FailoverFailedSQLException.class, () -> queryInstanceId(conn));
      assertFalse(conn.isValid(5));
    }

    assertThrows(SQLTransientConnectionException.class, () -> ds.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance. A connection is then retrieved to check that connections
   * to failed instances are not returned.
   */
  @TestTemplate
  public void test_1_2_hikariCP_get_dead_connection() throws SQLException {
    ProxyHelper.disableAllConnectivity();

    HikariDataSource ds = new HikariDataSource();
    List<TestInstanceInfo> instances = TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances();

    String writerIdentifier = instances.get(0).getEndpoint();
    String readerIdentifier = instances.get(1).getEndpoint();
    LOGGER.fine("Instance to connect to: " + writerIdentifier);
    LOGGER.fine("Instance to fail over to: " + readerIdentifier);

    ProxyHelper.enableConnectivity(writerIdentifier);
    createDataSource(null);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = ds.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      ProxyHelper.enableConnectivity(readerIdentifier);
      ProxyHelper.disableConnectivity(currentInstance);
      assertThrows(FailoverSuccessSQLException.class, () -> queryInstanceId(conn));

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      LOGGER.fine("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));

      // Try to get a new connection to the failed instance, which times out
      assertThrows(SQLTransientConnectionException.class, () -> ds.getConnection());
    }
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor.
   */
  @TestTemplate
  public void test_2_1_hikariCP_efm_failover() throws SQLException {
    ProxyHelper.disableAllConnectivity();

    HikariDataSource ds = new HikariDataSource();
    List<TestInstanceInfo> instances = TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances();

    String writerIdentifier = instances.get(0).getEndpoint();
    String readerIdentifier = instances.get(1).getEndpoint();
    LOGGER.fine("Instance to connect to: " + writerIdentifier);
    LOGGER.fine("Instance to fail over to: " + readerIdentifier);

    ProxyHelper.enableConnectivity(writerIdentifier);
    createDataSource(null);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = ds.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      LOGGER.fine("Connected to instance: " + currentInstance);

      ProxyHelper.enableConnectivity(readerIdentifier);
      ProxyHelper.disableConnectivity(writerIdentifier);

      assertThrows(FailoverSuccessSQLException.class, () -> queryInstanceId(conn));

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      LOGGER.fine("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
    }
  }

  private void createDataSource(final Properties customProps) {
    final HikariConfig config = getConfig(customProps);
    HikariDataSource dataSource = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = dataSource.getHikariPoolMXBean();

    LOGGER.fine("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    LOGGER.fine("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    LOGGER.fine("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  private HikariConfig getConfig(Properties customProps) {
    final HikariConfig config = new HikariConfig();
    config.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    config.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
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

    targetDataSourceProps.setProperty(
        "serverName",
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpoint());
    targetDataSourceProps.setProperty(
        "databaseName",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    targetDataSourceProps.setProperty("portNumber",
        Integer.toString(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort()));
    targetDataSourceProps.setProperty("socketTimeout", "3");
    targetDataSourceProps.setProperty("connectTimeout", "3");
    targetDataSourceProps.setProperty("monitoring-connectTimeout", "1");
    targetDataSourceProps.setProperty("monitoring-socketTimeout", "1");
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "failover,efm");
    targetDataSourceProps.setProperty("url", ConnectionStringHelper.getUrl());
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "2000");
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "1000");
    targetDataSourceProps.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "1");

    if (customProps != null) {
      final Enumeration<?> propertyNames = customProps.propertyNames();
      while (propertyNames.hasMoreElements()) {
        String propertyName = propertyNames.nextElement().toString();
        if (!StringUtils.isNullOrEmpty(propertyName)) {
          final String propertyValue = customProps.getProperty(propertyName);
          targetDataSourceProps.setProperty(propertyName, propertyValue);
        }
      }
    }

    config.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);
    return config;
  }

  protected String queryInstanceId(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(DriverHelper.getHostnameSql());
    rs.next();
    return rs.getString(1);
  }
}
