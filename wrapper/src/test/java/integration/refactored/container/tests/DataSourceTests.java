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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.refactored.DatabaseEngine;
import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.TestDriver;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestDriver;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnTestDriver;
import integration.util.SimpleJndiContextFactory;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.Logger;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({TestEnvironmentFeatures.PERFORMANCE, TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
public class DataSourceTests {

  private static final Logger LOGGER = Logger.getLogger(DataSourceTests.class.getName());

  @TestTemplate
  public void testConnectionWithDataSourceClassNameAndUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setUrlPropertyName("url");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("url", ConnectionStringHelper.getUrl());
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn =
        ds.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void testConnectionWithUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcUrl(ConnectionStringHelper.getUrl());

    try (final Connection conn =
        ds.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameAndServerName() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
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
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn =
        ds.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameAndCredentialProperties() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
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
    targetDataSourceProps.setProperty(
        "user", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    targetDataSourceProps.setProperty(
        "password", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  // @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameMissingProtocol() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
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
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  public void testConnectionWithDataSourceClassNameMissingServer() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "databaseName",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  // Database is optional for Mysql database.
  // See next test: testConnectionWithDataSourceClassNameMissingDatabase_MysqlDriver
  @DisableOnTestDriver({TestDriver.MARIADB, TestDriver.MYSQL})
  public void testConnectionWithDataSourceClassNameMissingDatabase() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "serverName",
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpoint());
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  @EnableOnTestDriver(TestDriver.MYSQL)
  public void testConnectionWithDataSourceClassNameMissingDatabase_MysqlDriver()
      throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "serverName",
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpoint());
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    Connection conn =
        ds.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    conn.close();
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameMissingUser() {

    if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
            == DatabaseEngineDeployment.DOCKER
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
            == DatabaseEngine.MYSQL
        && TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MYSQL) {
      // Mysql database in docker container uses "root" user (with the same password)
      // if not user provided and the test always passes. The test makes no sense.
      return;
    }

    if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
            == DatabaseEngineDeployment.DOCKER
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
            == DatabaseEngine.MARIADB
        && TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MYSQL) {
      // MariaDb database in docker container uses "root" user (with the same password)
      // if not user provided and the test always passes. The test makes no sense.
      return;
    }

    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
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
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                "", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameMissingPassword() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
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
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(), ""));
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  // @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameMissingPropertyNames() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
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
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameUsingUrl() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);
    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + DriverHelper.getDriverRequiredParameters());

    try (final Connection conn =
        ds.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void testConnectionWithDataSourceClassNameUsingUrlWithCredentials() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final String driverRequiredParameters = DriverHelper.getDriverRequiredParameters();
    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + ":"
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + driverRequiredParameters
            + (StringUtils.isNullOrEmpty(driverRequiredParameters) ? "?" : "&")
            + "user="
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername()
            + "&password="
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()
            + "&wrapperPlugins=\"\"");

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void testConnectionWithDataSourceClassNameUsingUrlWithPort() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + ":"
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + DriverHelper.getDriverRequiredParameters()
            + (DriverHelper.getDriverRequiredParameters().startsWith("?") ? "&" : "?")
            + "wrapperPlugins=\"\"");

    try (final Connection conn =
        ds.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void testConnectionWithDataSourceClassNameUsingUrlMissingPropertyNames() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + DriverHelper.getDriverRequiredParameters()
            + (DriverHelper.getDriverRequiredParameters().startsWith("?") ? "&" : "?")
            + "wrapperPlugins=\"\"");

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  @DisableOnTestDriver({
    TestDriver.MYSQL,
    TestDriver.MARIADB
  }) // Database is optional for Mysql and MariaDb databases
  public void testConnectionWithDataSourceClassNameUsingUrlMissingDatabase() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);
    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + "/"
            + DriverHelper.getDriverRequiredParameters()
            + (DriverHelper.getDriverRequiredParameters().startsWith("?") ? "&" : "?")
            + "wrapperPlugins=\"\"");

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  public void testConnectionWithUrlWithCredentials() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    final String driverRequiredParameters = DriverHelper.getDriverRequiredParameters();
    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + ":"
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + driverRequiredParameters
            + (StringUtils.isNullOrEmpty(driverRequiredParameters) ? "?" : "&")
            + "user="
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername()
            + "&password="
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()
            + "&wrapperPlugins=\"\"");

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void testConnectionWithUrlMissingPort() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + DriverHelper.getDriverRequiredParameters()
            + (DriverHelper.getDriverRequiredParameters().startsWith("?") ? "&" : "?")
            + "wrapperPlugins=\"\"");

    try (final Connection conn =
        ds.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  @DisableOnTestDriver({
    TestDriver.MYSQL,
    TestDriver.MARIADB
  }) // Database is optional for Mysql and MariaDb databases
  public void testConnectionWithUrlMissingDatabase() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + ":"
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort()
            + "/"
            + DriverHelper.getDriverRequiredParameters()
            + (DriverHelper.getDriverRequiredParameters().startsWith("?") ? "&" : "?")
            + "wrapperPlugins=\"\"");

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  public void testConnectionWithUrlMissingUser() {

    if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
            == DatabaseEngineDeployment.DOCKER
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
            == DatabaseEngine.MYSQL) {
      // Mysql database in docker container uses "root" user (with the same password)
      // if not user provided and the test always passes. The test makes no sense.
      return;
    }

    if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
            == DatabaseEngineDeployment.DOCKER
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
            == DatabaseEngine.MARIADB) {
      // MariaDb database in docker container uses "root" user (with the same password)
      // if not user provided and the test always passes. The test makes no sense.
      return;
    }

    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + ":"
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort()
            + "/?wrapperPlugins=\"\"");

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                "", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword()));
  }

  @TestTemplate
  public void testConnectionWithUrlMissingPassword() {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setUrlPropertyName("url");

    ds.setJdbcUrl(
        DriverHelper.getDriverProtocol()
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint()
            + ":"
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpointPort()
            + "/"
            + DriverHelper.getDriverRequiredParameters()
            + (DriverHelper.getDriverRequiredParameters().startsWith("?") ? "&" : "?")
            + "wrapperPlugins=\"\"");

    assertThrows(
        SQLException.class,
        () ->
            ds.getConnection(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(), ""));
  }

  @TestTemplate
  // MariaDb driver datasource class doesn't support serverName so the test doesn't make sense.
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameAndServerNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = new Properties();
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
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    final Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    final InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    final AwsWrapperDataSource dsFromJndiLookup =
        (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    assertNotNull(dsFromJndiLookup);

    assertNotSame(ds, dsFromJndiLookup);
    final Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();
    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    try (final Connection conn =
        dsFromJndiLookup.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void testConnectionWithDataSourceClassNameAndUrlFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setUrlPropertyName("url");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("url", ConnectionStringHelper.getUrl());
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    final Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    final InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    final AwsWrapperDataSource dsFromJndiLookup =
        (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    assertNotNull(dsFromJndiLookup);

    assertNotSame(ds, dsFromJndiLookup);
    final Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();
    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    try (final Connection conn =
        dsFromJndiLookup.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }
}
