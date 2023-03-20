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
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
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
import integration.refactored.container.condition.EnableOnDatabaseEngine;
import integration.refactored.container.condition.EnableOnDatabaseEngineDeployment;
import integration.refactored.container.condition.EnableOnNumOfInstances;
import integration.refactored.container.condition.EnableOnTestFeature;
import integration.refactored.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.MariaDBHikariPooledConnectionProvider;
import software.amazon.jdbc.MysqlHikariPooledConnectionProvider;
import software.amazon.jdbc.PostgresHikariPooledConnectionProvider;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.util.SqlState;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnNumOfInstances(min = 2)
@DisableOnTestFeature({TestEnvironmentFeatures.PERFORMANCE, TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
@MakeSureFirstInstanceWriter
public class ReadWriteSplittingTests {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingTests.class.getName());

  protected static Properties getProps() {
    return TestEnvironment.isAwsDatabase() ? getAuroraProps() : getNonAuroraProps();
  }

  protected static Properties getProxiedProps() {
    if (TestEnvironment.isAwsDatabase()) {
      Properties props = getAuroraProps();
      AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
          "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo()
              .getInstanceEndpointSuffix());
      return props;
    } else {
      // No extra properties needed
      return getNonAuroraProps();
    }
  }

  protected static Properties getDefaultPropsNoPlugins() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    return props;
  }

  protected static Properties getAuroraProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "auroraHostList,readWriteSplitting");
    return props;
  }

  protected static Properties getNonAuroraProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.set(props,
        "true");
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
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
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
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

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
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

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
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

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
    try (final Connection conn = DriverManager.getConnection(getUrl(), getProps())) {

      final String writerConnectionId = queryInstanceId(conn);

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
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), getProxiedProps())) {

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
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), getProxiedProps())) {
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
    try (final Connection conn = DriverManager.getConnection(getProxiedUrl(), getProxiedProps())) {

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

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
  public void test_pooledConnectionFailover() throws SQLException, InterruptedException {
    AuroraTestUtility auroraUtil =
        new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());
    Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover,efm");
    props.setProperty("databasePropertyName", "databaseName");
    props.setProperty("portPropertyName", "portNumber");
    props.setProperty("serverPropertyName", "serverName");

    final HikariPooledConnectionProvider provider = getTestConnectionProvider();

    ConnectionProviderManager.setConnectionProvider(provider);

    String initialWriterId;
    String nextWriterId;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      initialWriterId = queryInstanceId(conn);
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();
      assertThrows(SQLException.class, () -> queryInstanceId(conn));
      nextWriterId = queryInstanceId(conn);
      assertNotEquals(initialWriterId, nextWriterId);
    }

    try (final Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // The initial connection should be invalid and evicted by the connection pool.
      // This should be a new connection to the initial writer ID.
      // If the dead connection was not evicted, this query would result in an exception.
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
    }

    ConnectionProviderManager.releaseResources();
  }

  @TestTemplate
  // Tests use Aurora specific SQL to identify instance name
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
  public void test_pooledConnectionFailoverWithClusterURL() throws SQLException, InterruptedException {
    AuroraTestUtility auroraUtil =
        new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());
    Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover,efm");
    props.setProperty("databasePropertyName", "databaseName");
    props.setProperty("portPropertyName", "portNumber");
    props.setProperty("serverPropertyName", "serverName");

    final HikariPooledConnectionProvider provider = getTestConnectionProvider();

    ConnectionProviderManager.setConnectionProvider(provider);
    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperClusterEndpointUrl(),
        props)) {
      // The internal connection pool should not be used if the connection is established via a cluster URL.
      assertEquals(0, provider.getHostCount(), "Internal connection pool should be empty.");
      final String writerConnectionId = queryInstanceId(conn);
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();
      assertThrows(SQLException.class, () -> queryInstanceId(conn));
      final String nextWriterId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, nextWriterId);
      assertEquals(0, provider.getHostCount(), "Internal connection pool should be empty.");
    }
    ConnectionProviderManager.releaseResources();
  }

  private static HikariConfig getHikariConfig(HostSpec hostSpec, Properties props) {
    HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(1);
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(1000);

    return config;
  }

  private HikariPooledConnectionProvider getTestConnectionProvider() {
    TestDriver driver = TestEnvironment.getCurrent().getCurrentDriver();
    switch (driver) {
      case MYSQL:
        return new MysqlHikariPooledConnectionProvider(ReadWriteSplittingTests::getHikariConfig);
      case MARIADB:
        return new MariaDBHikariPooledConnectionProvider(ReadWriteSplittingTests::getHikariConfig);
      case PG:
        return new PostgresHikariPooledConnectionProvider(ReadWriteSplittingTests::getHikariConfig);
      default:
        fail(
            "The provided test driver does not have an equivalent HikariPooledConnectionProvider "
                + "class: "
                + driver);
        return null;
    }
  }
}
