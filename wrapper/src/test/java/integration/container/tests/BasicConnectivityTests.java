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

import static integration.container.ConnectionStringHelper.getDefaultProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPluginFactory;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPluginFactory;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPluginFactory;
import software.amazon.jdbc.plugin.ConnectTimeConnectionPluginFactory;
import software.amazon.jdbc.plugin.DataCacheConnectionPluginFactory;
import software.amazon.jdbc.plugin.DriverMetaDataConnectionPluginFactory;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPluginFactory;
import software.amazon.jdbc.plugin.LogQueryConnectionPluginFactory;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointPluginFactory;
import software.amazon.jdbc.plugin.dev.DeveloperConnectionPluginFactory;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPluginFactory;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.FederatedAuthPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.OktaAuthPluginFactory;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPluginFactory;
import software.amazon.jdbc.plugin.limitless.LimitlessConnectionPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.staledns.AuroraStaleDnsPluginFactory;
import software.amazon.jdbc.plugin.strategy.fastestresponse.FastestResponseStrategyPluginFactory;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
@Order(4)
public class BasicConnectivityTests {

  private static final Logger LOGGER = Logger.getLogger(BasicConnectivityTests.class.getName());
  private static final List<String> PLUGINS = Arrays.asList(
      "executionTime",
      "logQuery",
      "dataCache",
      "customEndpoint",
      "efm",
      "efm2",
      "failover",
      "failover2",
      "auroraStaleDns",
      "readWriteSplitting",
      "auroraConnectionTracker",
      "driverMetaData",
      "connectTime",
      "dev",
      "fastestResponseStrategy",
      "initialConnection",
      "limitless"
  );

  @TestTemplate
  public void test_DirectConnection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    String url =
        ConnectionStringHelper.getUrlWithPlugins(
            testDriver,
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getHost(),
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getPort(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName(), "");
    LOGGER.finest("Connecting to " + url);

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));

    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));

    conn.close();
  }

  @TestTemplate
  public void test_WrapperConnection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    props.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, "10000");

    String url =
        ConnectionStringHelper.getWrapperUrl(
            testDriver,
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getHost(),
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getPort(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    LOGGER.finest("Connecting to " + url);

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));

    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));

    conn.close();
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_ProxiedDirectConnection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    TestInstanceInfo instanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().get(0);

    String url =
        ConnectionStringHelper.getUrlWithPlugins(
            testDriver,
            instanceInfo.getHost(),
            instanceInfo.getPort(),
            TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName(), "");
    LOGGER.finest("Connecting to " + url);

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));

    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));

    ProxyHelper.disableConnectivity(instanceInfo.getInstanceId());

    assertFalse(conn.isValid(5));

    ProxyHelper.enableConnectivity(instanceInfo.getInstanceId());

    conn.close();
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  @EnableOnDatabaseEngineDeployment({DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER})
  public void testBasicConnectivityTestWithPlugins() throws SQLException {
    final TestInstanceInfo readerInstance = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(1);

    final List<String> urls = Arrays.asList(
        ConnectionStringHelper.getWrapperUrl(),
        ConnectionStringHelper.getWrapperUrl(readerInstance),
        ConnectionStringHelper.getWrapperClusterEndpointUrl(),
        ConnectionStringHelper.getWrapperReaderClusterUrl()
    );

    for (String url : urls) {
      for (String plugin : PLUGINS) {
        final Properties props = getDefaultProperties();
        props.setProperty(PropertyDefinition.PLUGINS.name, plugin);
        LOGGER.finest("Connecting to " + url);

        try (Connection conn = DriverManager.getConnection(url, props);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1");
        ) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }
      }
    }

  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_ProxiedWrapperConnection() throws SQLException {
    LOGGER.info(TestEnvironment.getCurrent().getCurrentDriver().toString());

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");

    TestInstanceInfo instanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().get(0);

    String url =
        ConnectionStringHelper.getWrapperUrl(
            instanceInfo.getHost(),
            instanceInfo.getPort(),
            TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
    LOGGER.finest("Connecting to " + url);

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));

    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));

    ProxyHelper.disableConnectivity(instanceInfo.getInstanceId());

    assertFalse(conn.isValid(5));

    ProxyHelper.enableConnectivity(instanceInfo.getInstanceId());

    conn.close();
  }

  @TestTemplate
  public void testSuccessOpenConnectionNoPort() throws SQLException {
    String url =
        DriverHelper.getWrapperDriverProtocol()
            + TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getHost()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + DriverHelper.getDriverRequiredParameters();

    LOGGER.finest("Connecting to " + url);

    try (Connection conn =
        DriverManager.getConnection(url, ConnectionStringHelper.getDefaultPropertiesWithNoPlugins())) {

      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isValid(10));
    }
  }

  @ParameterizedTest
  @MethodSource("testConnectionParameters")
  public void testFailedConnection(TestDriver testDriver, String url) throws SQLException {
    DriverHelper.unregisterAllDrivers();
    DriverHelper.registerDriver(testDriver);
    TestEnvironment.getCurrent().setCurrentDriver(testDriver);

    LOGGER.finest("Connecting to " + url);

    assertThrows(
        SQLException.class,
        () -> {
          Connection conn =
              DriverManager.getConnection(url, ConnectionStringHelper.getDefaultPropertiesWithNoPlugins());
          conn.close();
        });
  }

  @ParameterizedTest
  @MethodSource("testPropertiesParameters")
  public void testFailedProperties(
      TestDriver testDriver, String url, String username, String password) throws SQLException {
    DriverHelper.unregisterAllDrivers();
    DriverHelper.registerDriver(testDriver);
    TestEnvironment.getCurrent().setCurrentDriver(testDriver);

    if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
        == DatabaseEngine.MARIADB
        && TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
        == DatabaseEngineDeployment.DOCKER
        && StringUtils.isNullOrEmpty(username)) {
      // MariaDb driver uses "root" username if no username is provided. Since MariaDb database in
      // docker container
      // has "root" user by default (the password is the same), the test always passes and makes no
      // sense.
      // Skip this test.
      return;
    }

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    props.setProperty(PropertyDefinition.USER.name, username);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);

    if (testDriver == TestDriver.MARIADB) {
      // If this property is true the driver will still be able to connect, causing the test to fail.
      props.setProperty("allowPublicKeyRetrieval", "false");
    }

    LOGGER.finest("Connecting to " + url);

    assertThrows(
        SQLException.class,
        () -> {
          Connection conn = DriverManager.getConnection(url, props);
          conn.close();
        });
  }

  protected static String buildConnectionString(
      String connStringPrefix,
      String host,
      String port,
      String databaseName,
      String requiredParameters) {
    return connStringPrefix + host + ":" + port + "/" + databaseName + requiredParameters;
  }

  private static Stream<Arguments> testConnectionParameters() {
    final List<Arguments> results = new ArrayList<>();
    for (TestDriver testDriver : TestEnvironment.getCurrent().getAllowedTestDrivers()) {

      // missing connection prefix
      results.add(
          Arguments.of(
              testDriver,
              buildConnectionString(
                  "",
                  TestEnvironment.getCurrent()
                      .getInfo()
                      .getDatabaseInfo()
                      .getInstances()
                      .get(0)
                      .getHost(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getPort()),
                  TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName(),
                  DriverHelper.getDriverRequiredParameters(testDriver))));

      // incorrect database name
      results.add(
          Arguments.of(
              testDriver,
              buildConnectionString(
                  DriverHelper.getWrapperDriverProtocol(testDriver),
                  TestEnvironment.getCurrent()
                      .getInfo()
                      .getDatabaseInfo()
                      .getInstances()
                      .get(0)
                      .getHost(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getPort()),
                  "failedDatabaseNameTest",
                  DriverHelper.getDriverRequiredParameters(testDriver))));
    }
    return results.stream();
  }

  private static Stream<Arguments> testPropertiesParameters() {
    final List<Arguments> results = new ArrayList<>();
    for (TestDriver testDriver : TestEnvironment.getCurrent().getAllowedTestDrivers()) {
      // missing username
      results.add(
          Arguments.of(
              testDriver,
              buildConnectionString(
                  DriverHelper.getWrapperDriverProtocol(testDriver),
                  TestEnvironment.getCurrent()
                      .getInfo()
                      .getDatabaseInfo()
                      .getInstances()
                      .get(0)
                      .getHost(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getPort()),
                  TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName(),
                  DriverHelper.getDriverRequiredParameters(testDriver)),
              "",
              TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));

      // missing password
      results.add(
          Arguments.of(
              testDriver,
              buildConnectionString(
                  DriverHelper.getWrapperDriverProtocol(testDriver),
                  TestEnvironment.getCurrent()
                      .getInfo()
                      .getDatabaseInfo()
                      .getInstances()
                      .get(0)
                      .getHost(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getPort()),
                  TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName(),
                  DriverHelper.getDriverRequiredParameters(testDriver)),
              TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
              ""));
    }
    return results.stream();
  }
}
