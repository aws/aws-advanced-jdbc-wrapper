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
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.refactored.DatabaseEngine;
import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.MakeSureFirstInstanceWriterExtension;
import integration.refactored.container.ProxyHelper;
import integration.refactored.container.TestDriver;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestDriver;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnDatabaseEngine;
import integration.refactored.container.condition.EnableOnDatabaseEngineDeployment;
import integration.refactored.container.condition.EnableOnNumOfInstances;
import integration.refactored.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.util.SqlState;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@ExtendWith(MakeSureFirstInstanceWriterExtension.class)
@EnableOnNumOfInstances(min = 2)
@DisableOnTestFeature(TestEnvironmentFeatures.PERFORMANCE)
public class ReadWriteSplittingTests {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingTests.class.getName());

  private Properties staticHostListProps;
  private Properties auroraHostListProps;

  /**
   * Properties indirectly depends on a test driver since some properties are driver dependent (for
   * example, socket timeout). That's why properties needs to be initialized before each test run.
   */
  @BeforeEach
  public void beforeEach() {
    this.staticHostListProps = getReadWriteProps_staticHostList();
    this.auroraHostListProps = getReadWriteProps_auroraHostList();
  }

  protected static Properties getReadWriteProps_staticHostList() {
    final Properties props = getDefaultProps_noPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.set(props, "true");
    return props;
  }

  protected static Properties getReadWriteProps_auroraHostList() {
    Properties props = getDefaultProps_noPlugins();
    PropertyDefinition.PLUGINS.set(props, "auroraHostList,readWriteSplitting");
    return props;
  }

  protected static Properties getDefaultProps_noPlugins() {
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

  protected String getStaticHostListUrl() {
    return DriverHelper.getWrapperDriverProtocol()
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
        + ","
        + TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(1)
        .getEndpoint()
        + ":"
        + TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(1)
        .getEndpointPort()
        + "/"
        + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
        + DriverHelper.getDriverRequiredParameters();
  }

  protected String getClusterUrl() {
    return ConnectionStringHelper.getWrapperUrl(
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  protected String getProxiedStaticHostListUrl() {
    return DriverHelper.getWrapperDriverProtocol()
        + TestEnvironment.getCurrent()
        .getInfo()
        .getProxyDatabaseInfo()
        .getInstances()
        .get(0)
        .getEndpoint()
        + ":"
        + TestEnvironment.getCurrent()
        .getInfo()
        .getProxyDatabaseInfo()
        .getInstances()
        .get(0)
        .getEndpointPort()
        + ","
        + TestEnvironment.getCurrent()
        .getInfo()
        .getProxyDatabaseInfo()
        .getInstances()
        .get(1)
        .getEndpoint()
        + ":"
        + TestEnvironment.getCurrent()
        .getInfo()
        .getProxyDatabaseInfo()
        .getInstances()
        .get(1)
        .getEndpointPort()
        + "/"
        + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName()
        + DriverHelper.getDriverRequiredParameters();
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_connectToWriter_setReadOnlyTrueTrueFalseFalseTrue() throws SQLException {
    final String url = getStaticHostListUrl();
    LOGGER.finest("Connecting to url " + url);
    try (final Connection conn = DriverManager.getConnection(url, this.staticHostListProps)) {

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
    try (final Connection conn = DriverManager.getConnection(getStaticHostListUrl(), this.staticHostListProps)) {

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
    try (final Connection conn = DriverManager.getConnection(getStaticHostListUrl(), this.staticHostListProps)) {

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
    try (final Connection conn = DriverManager.getConnection(getStaticHostListUrl(), this.staticHostListProps)) {

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
    String url = getStaticHostListUrl();
    LOGGER.info("Connecting to " + url);
    try (final Connection conn = DriverManager.getConnection(url, this.staticHostListProps)) {

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
    try (final Connection conn = DriverManager.getConnection(getProxiedStaticHostListUrl(), this.staticHostListProps)) {

      final String writerConnectionId = queryInstanceId(conn);

      // Kill all reader instances
      int numOfInstances =
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().size();
      for (int i = 1; i < numOfInstances; i++) {
        ProxyHelper.disableConnectivity(
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(i)
                .getInstanceName());
      }

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      String currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);

      assertDoesNotThrow(() -> conn.setReadOnly(false));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @TestTemplate
  @Disabled // TODO: fix me
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnlyTrue_allInstancesDown() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedStaticHostListUrl(), this.staticHostListProps)) {

      ProxyHelper.disableAllConnectivity();

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      // A SQL statement setting the read-only status is sent to server.
      // Since the server is down, a SQLException is thrown.
      assertEquals(SqlState.COMMUNICATION_ERROR.getState(), exception.getSQLState());
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_setReadOnly_closedConnection() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(getProxiedStaticHostListUrl(), this.staticHostListProps)) {
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
    try (final Connection conn = DriverManager.getConnection(getProxiedStaticHostListUrl(), this.staticHostListProps)) {

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
  public void test_readerLoadBalancing_singleReader() throws SQLException {
    Properties props = this.staticHostListProps;
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    props.setProperty(ReadWriteSplittingPlugin.READER_BALANCE_AUTOCOMMIT_STATEMENT_LIMIT.name, "1");
    try (final Connection conn = DriverManager.getConnection(getStaticHostListUrl(), props)) {
      final String writerConnectionId = queryInstanceId(conn);
      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      for (int i = 0; i < 3; i++) {
        String currentConnectionId = queryInstanceId(conn);
        assertEquals(readerConnectionId, currentConnectionId);
      }
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnNumOfInstances(min = 3)
  public void test_readerLoadBalancing_autocommitTrue_defaultSettings() throws SQLException {
    Properties props = this.auroraHostListProps;
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    try (final Connection conn = DriverManager.getConnection(getClusterUrl(), props)) {
      final String writerConnectionId = queryInstanceId(conn);
      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      for (int i = 0; i < 5; i++) {
        String currentConnectionId = queryInstanceId(conn);
        assertNotEquals(writerConnectionId, readerConnectionId);
        // By default, should not load balance while autocommit is on
        assertEquals(readerConnectionId, currentConnectionId);
      }
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnNumOfInstances(min = 3)
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_readerLoadBalancing_autocommitTrue_regex() throws SQLException {
    String regexTrigger = "SELECT 9";
    Properties props = this.auroraHostListProps;
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    props.setProperty(ReadWriteSplittingPlugin.READER_BALANCE_AUTOCOMMIT_STATEMENT_LIMIT.name, "2");
    props.setProperty(ReadWriteSplittingPlugin.READER_BALANCE_AUTOCOMMIT_STATEMENT_REGEX.name, regexTrigger);
    try (final Connection conn = DriverManager.getConnection(getClusterUrl(), props)) {
      final String writerConnectionId = queryInstanceId(conn);
      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      for (int i = 0; i < 10; i++) {
        Statement stmt = conn.createStatement();
        stmt.executeQuery(regexTrigger);
        String currentInstanceId = queryInstanceId(conn);
        assertEquals(readerConnectionId, currentInstanceId);

        stmt.execute(regexTrigger);
        currentInstanceId = queryInstanceId(conn);
        assertNotEquals(readerConnectionId, currentInstanceId);
        readerConnectionId = currentInstanceId;
      }
    }
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnNumOfInstances(min = 3)
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_readerLoadBalancing_switchAutoCommit() throws SQLException {
    Properties props = this.auroraHostListProps;
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    props.setProperty(ReadWriteSplittingPlugin.READER_BALANCE_AUTOCOMMIT_STATEMENT_LIMIT.name, "2");
    try (final Connection conn = DriverManager.getConnection(getClusterUrl(), props)) {
      final String writerConnectionId = queryInstanceId(conn);
      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);
      // After this query, the statement limit will be hit, and the next invocation should switch connections
      String currentInstanceId = queryInstanceId(conn);
      assertEquals(readerConnectionId, currentInstanceId);

      for (int i = 0; i < 5; i++) {
        String nextReaderConnectionId = queryInstanceId(conn);
        assertNotEquals(readerConnectionId, nextReaderConnectionId);
        readerConnectionId = nextReaderConnectionId;
        // After this query, the statement limit will be hit, and the next invocation should switch connections
        currentInstanceId = queryInstanceId(conn);
        assertEquals(readerConnectionId, currentInstanceId);
      }

      // Increase statement count to 1. Will verify later that it gets to reset to 0 when
      // setAutoCommit(false) is called.
      readerConnectionId = queryInstanceId(conn);

      conn.setAutoCommit(false);
      for (int i = 0; i < 5; i++) {
        Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT " + i);
        final ResultSet rs = stmt.getResultSet();
        rs.next();
        assertEquals(i, rs.getInt(1));
        conn.commit();

        String nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerConnectionId, nextReaderId);
        readerConnectionId = nextReaderId;

        stmt = conn.createStatement();
        stmt.executeQuery("SELECT " + i);
        conn.rollback();
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerConnectionId, nextReaderId);
        readerConnectionId = nextReaderId;
      }

      // Verify statement count was reset to 0 when setAutoCommit(false) was called.
      conn.setAutoCommit(true);
      currentInstanceId = queryInstanceId(conn);
      assertEquals(readerConnectionId, currentInstanceId);
      currentInstanceId = queryInstanceId(conn);
      assertEquals(readerConnectionId, currentInstanceId);
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  @DisableOnTestDriver(TestDriver.MARIADB) // TODO: investigate and fix
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void test_transactionResolutionUnknown() throws SQLException {
    Properties props = this.staticHostListProps;
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    try (final Connection conn = DriverManager.getConnection(getProxiedStaticHostListUrl(), props)) {
      final String writerConnectionId = queryInstanceId(conn);
      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      final String readerId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerId);

      final Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");

      ProxyHelper.disableConnectivity(
          TestEnvironment.getCurrent()
              .getInfo()
              .getDatabaseInfo()
              .getInstances()
              .get(1)
              .getInstanceName());

      final SQLException e = assertThrows(SQLException.class, conn::rollback);
      assertTrue(
          SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState().equals(e.getSQLState())
              || SqlState.CONNECTION_FAILURE.getState().equals(e.getSQLState()));

      try (final Connection newConn = DriverManager.getConnection(getProxiedStaticHostListUrl(), props)) {
        newConn.setReadOnly(true);
        final Statement newStmt = newConn.createStatement();
        final ResultSet rs = newStmt.executeQuery("SELECT 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }
  }
}
