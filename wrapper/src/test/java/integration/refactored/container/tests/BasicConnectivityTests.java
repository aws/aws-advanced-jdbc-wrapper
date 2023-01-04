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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.refactored.DatabaseEngine;
import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestInstanceInfo;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.ProxyHelper;
import integration.refactored.container.TestDriver;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@DisableOnTestFeature(TestEnvironmentFeatures.PERFORMANCE)
public class BasicConnectivityTests {

  private static final Logger LOGGER = Logger.getLogger(BasicConnectivityTests.class.getName());

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void test_DirectConnection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    String url =
        ConnectionStringHelper.getUrl(
            testDriver,
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint(),
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort(),
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
  @ExtendWith(TestDriverProvider.class)
  public void test_WrapperConnection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = new Properties();
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    String url =
        ConnectionStringHelper.getWrapperUrl(
            testDriver,
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint(),
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort(),
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
  @ExtendWith(TestDriverProvider.class)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_ProxiedDirectConnection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = new Properties();
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getPassword());
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    TestInstanceInfo instanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().get(0);

    String url =
        ConnectionStringHelper.getUrl(
            testDriver,
            instanceInfo.getEndpoint(),
            instanceInfo.getEndpointPort(),
            TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
    LOGGER.finest("Connecting to " + url);

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));

    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));

    ProxyHelper.disableConnectivity(instanceInfo.getInstanceName());

    assertFalse(conn.isValid(5));

    ProxyHelper.enableConnectivity(instanceInfo.getInstanceName());

    conn.close();
  }

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_ProxiedWrapperConnection() throws SQLException {
    LOGGER.info(TestEnvironment.getCurrent().getCurrentDriver().toString());

    final Properties props = new Properties();
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getPassword());
    DriverHelper.setConnectTimeout(props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 10, TimeUnit.SECONDS);

    TestInstanceInfo instanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().get(0);

    String url =
        ConnectionStringHelper.getWrapperUrl(
            instanceInfo.getEndpoint(),
            instanceInfo.getEndpointPort(),
            TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
    LOGGER.finest("Connecting to " + url);

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));

    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));

    ProxyHelper.disableConnectivity(instanceInfo.getInstanceName());

    assertFalse(conn.isValid(5));

    ProxyHelper.enableConnectivity(instanceInfo.getInstanceName());

    conn.close();
  }

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void testSuccessOpenConnectionNoPort() throws SQLException {
    String url =
        DriverHelper.getWrapperDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + DriverHelper.getDriverRequiredParameters();

    LOGGER.finest("Connecting to " + url);

    try (Connection conn =
        DriverManager.getConnection(url, ConnectionStringHelper.getDefaultProperties())) {

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
              DriverManager.getConnection(url, ConnectionStringHelper.getDefaultProperties());
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

    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, username);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);

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
    ArrayList<Arguments> results = new ArrayList<>();
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
                      .getEndpoint(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getEndpointPort()),
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
                      .getEndpoint(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getEndpointPort()),
                  "failedDatabaseNameTest",
                  DriverHelper.getDriverRequiredParameters(testDriver))));
    }
    return results.stream();
  }

  private static Stream<Arguments> testPropertiesParameters() {
    ArrayList<Arguments> results = new ArrayList<>();
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
                      .getEndpoint(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getEndpointPort()),
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
                      .getEndpoint(),
                  String.valueOf(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getEndpointPort()),
                  TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName(),
                  DriverHelper.getDriverRequiredParameters(testDriver)),
              TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
              ""));
    }
    return results.stream();
  }
}
