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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import integration.refactored.DatabaseEngine;
import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestInstanceInfo;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.ProxyHelper;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnDatabaseEngine;
import integration.refactored.container.condition.EnableOnDatabaseEngineDeployment;
import integration.refactored.container.condition.EnableOnNumOfInstances;
import integration.refactored.container.condition.EnableOnTestFeature;
import integration.refactored.container.condition.MakeSureFirstInstanceWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.util.SqlState;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnNumOfInstances(min = 2)
@DisableOnTestFeature(TestEnvironmentFeatures.PERFORMANCE)
@MakeSureFirstInstanceWriter
public class ReadWriteSplittingTests {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingTests.class.getName());
  private Properties defaultProps;

  /**
   * Properties indirectly depends on a test driver since some properties are driver dependent (for
   * example, socket timeout). That's why properties needs to be initialized before each test run.
   */
  @BeforeEach
  public void beforeEach() {
    this.defaultProps = getDefaultPropsNoPlugins();
    if (TestEnvironment.isAwsDatabase()) {
      PropertyDefinition.PLUGINS.set(this.defaultProps, "auroraHostList,readWriteSplitting");
    } else {
      PropertyDefinition.PLUGINS.set(this.defaultProps, "readWriteSplitting");
      ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.set(this.defaultProps,
          "true");
    }
  }

  protected static Properties getDefaultPropsNoPlugins() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    return props;
  }

  protected String queryInstanceId(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(DriverHelper.getHostnameSql());
    rs.next();
    return rs.getString(1);
  }

  protected String getUrl() {
    if (TestEnvironment.isAwsDatabase()) {
      return ConnectionStringHelper.getWrapperUrl();
    } else {
      List<TestInstanceInfo> instances =
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances();
      return getCommaDelimitedHostUrl(instances);
    }
  }

  protected String getProxiedUrl() {
    if (TestEnvironment.isAwsDatabase()) {
      return ConnectionStringHelper.getProxyWrapperUrl();
    } else {
      List<TestInstanceInfo> instances =
          TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances();
      return getCommaDelimitedHostUrl(instances);
    }
  }

  private String getCommaDelimitedHostUrl(List<TestInstanceInfo> instances) {
    return DriverHelper.getWrapperDriverProtocol()
        + instances.get(0).getUrl() + ","
        + instances.get(1).getUrl() + "/"
        + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
        + DriverHelper.getDriverRequiredParameters();
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_connectToWriter_setReadOnlyTrueTrueFalseFalseTrue() throws SQLException {
    final String url = getUrl();
    LOGGER.finest("Connecting to url " + url);
    try (final Connection conn = DriverManager.getConnection(url, this.defaultProps)) {

      final String writerConnectionId = queryInstanceId(conn);
      LOGGER.finest("writerConnectionId: " + writerConnectionId);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      LOGGER.finest("readerConnectionId: " + readerConnectionId);
      assertNotEquals(writerConnectionId, readerConnectionId);

      conn.setReadOnly(true);
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getUrl(), this.defaultProps)) {

      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      stmt.execute("START TRANSACTION READ ONLY");
      stmt.executeQuery("SELECT 1");

      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getUrl(), this.defaultProps)) {

      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      conn.setAutoCommit(false);
      stmt.executeQuery(
          // TODO: can we replace it with something less database specific?
          "SELECT COUNT(*) FROM information_schema.tables");

      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine(DatabaseEngine.MYSQL)
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_setReadOnlyFalseInTransaction_setAutocommitZero() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getUrl(), this.defaultProps)) {

      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      stmt.execute("SET autocommit = 0");
      stmt.executeQuery(
          // TODO: can we replace it with something less database specific?
          "SELECT COUNT(*) FROM information_schema.tables");

      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnDatabaseEngine(DatabaseEngine.MYSQL)
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_setReadOnlyTrueInTransaction() throws SQLException {
    String url = getUrl();
    LOGGER.info("Connecting to " + url);
    try (final Connection conn = DriverManager.getConnection(url, this.defaultProps)) {

      final String writerConnectionId = queryInstanceId(conn);
      LOGGER.info("writerConnectionId: " + writerConnectionId);

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
      stmt1.executeUpdate(
          "CREATE TABLE test_readWriteSplitting_readOnlyTrueInTransaction "
              + "(id int not null primary key, text_field varchar(255) not null)");
      stmt1.execute("SET autocommit = 0");

      final Statement stmt2 = conn.createStatement();
      stmt2.executeUpdate(
          "INSERT INTO test_readWriteSplitting_readOnlyTrueInTransaction "
              + "VALUES (1, 'test_field value 1')");

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      final String currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      stmt2.execute("COMMIT");
      final ResultSet rs =
          stmt2.executeQuery(
              "SELECT count(*) from test_readWriteSplitting_readOnlyTrueInTransaction");
      rs.next();
      assertEquals(1, rs.getInt(1));

      conn.setReadOnly(false);
      stmt2.execute("SET autocommit = 1");
      stmt2.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyTrue_allReadersDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), this.defaultProps)) {

      final String writerConnectionId = queryInstanceId(conn);

      // Kill all reader instances
      List<String> instanceIDs =
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().stream()
              .map(TestInstanceInfo::getInstanceId).collect(Collectors.toList());
      for (int i = 1; i < instanceIDs.size(); i++) {
        ProxyHelper.disableConnectivity(instanceIDs.get(i));
      }

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      String currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);

      assertDoesNotThrow(() -> conn.setReadOnly(false));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);

      // Bring up one reader
      ProxyHelper.enableConnectivity(instanceIDs.get(instanceIDs.size() - 1));
      assertDoesNotThrow(() -> conn.setReadOnly(true));
      currentConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnly_closedConnection() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), this.defaultProps)) {
      conn.close();

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), exception.getSQLState());
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyFalse_allInstancesDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), this.defaultProps)) {

      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      // Kill all instances
      ProxyHelper.disableAllConnectivity();

      final SQLException exception =
          assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyTrue_oneHost() throws SQLException {
    // Use static host list with only one host
    Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.set(props, "true");

    String writerUrl =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0).getUrl();
    String url = DriverHelper.getWrapperDriverProtocol()
        + writerUrl + "/"
        + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
        + DriverHelper.getDriverRequiredParameters();

    try (final Connection conn = DriverManager.getConnection(url, props)) {
      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }
}
