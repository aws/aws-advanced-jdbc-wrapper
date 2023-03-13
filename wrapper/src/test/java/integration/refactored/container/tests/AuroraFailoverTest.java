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

import com.mysql.cj.conf.PropertyKey;
import integration.refactored.DatabaseEngine;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestInstanceInfo;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.ProxyHelper;
import integration.refactored.container.TestDriver;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnNumOfInstances;
import integration.refactored.container.condition.EnableOnTestDriver;
import integration.refactored.container.condition.EnableOnTestFeature;
import integration.refactored.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.util.SqlState;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@DisableOnTestFeature({TestEnvironmentFeatures.PERFORMANCE, TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
@EnableOnNumOfInstances(min = 2)
@MakeSureFirstInstanceWriter
public class AuroraFailoverTest {

  private static final Logger LOGGER = Logger.getLogger(AuroraFailoverTest.class.getName());

  protected static final AuroraTestUtility auroraUtil =
      new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());
  protected static final int IS_VALID_TIMEOUT = 5;

  protected String currentWriter;
  private static final int IDLE_CONNECTIONS_NUM = 5;

  @BeforeEach
  public void setUpEach() {
    this.currentWriter =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0)
            .getInstanceId();
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Driver failover occurs when executing a method against the connection
   */
  @TestTemplate
  public void test_failFromWriterToNewWriter_failOnConnectionInvocation()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getEndpoint(),
                initialWriterInstanceInfo.getEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Failure occurs on Connection invocation
      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to the new writer after failover happens.
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));
      assertNotEquals(currentConnectionId, initialWriterId);
    }
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Driver failover occurs when executing a method against an object bound to the
   * connection (eg a Statement object created by the connection).
   */
  @TestTemplate
  public void test_failFromWriterToNewWriter_failOnConnectionBoundObjectInvocation()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getEndpoint(),
                initialWriterInstanceInfo.getEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      final Statement stmt = conn.createStatement();

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Failure occurs on Statement invocation
      assertFirstQueryThrows(stmt, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that the driver is connected to the new writer after failover happens.
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));

      assertNotEquals(initialWriterId, currentConnectionId);
    }
  }

  /**
   * Current reader dies, no other reader instance, failover to writer, then writer dies, failover
   * to another available reader instance.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_failFromReaderToWriterToAnyAvailableInstance()
      throws SQLException, InterruptedException {

    // Crashing all readers except the first one
    for (int i = 2;
        i < TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().size();
        i++) {
      ProxyHelper.disableConnectivity(
          TestEnvironment.getCurrent()
              .getInfo()
              .getProxyDatabaseInfo()
              .getInstances()
              .get(i)
              .getInstanceId());
    }

    // Connect to Instance2 which is the only reader that is up.
    final TestInstanceInfo instanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().get(1);
    final String instanceId = instanceInfo.getInstanceId();

    final Properties props = initDefaultProxiedProps();
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                instanceInfo.getEndpoint(),
                instanceInfo.getEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName()),
            props)) {

      // Crash Instance2
      ProxyHelper.disableConnectivity(instanceId);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are currently connected to the writer Instance1.
      final String writerId = this.currentWriter;
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerId, currentConnectionId);
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));

      // Stop crashing reader Instance2 and Instance3
      final String readerAId =
          TestEnvironment.getCurrent()
              .getInfo()
              .getProxyDatabaseInfo()
              .getInstances()
              .get(1)
              .getInstanceId();
      ProxyHelper.enableConnectivity(readerAId);

      final String readerBId =
          TestEnvironment.getCurrent()
              .getInfo()
              .getProxyDatabaseInfo()
              .getInstances()
              .get(2)
              .getInstanceId();
      ProxyHelper.enableConnectivity(readerBId);

      // Crash writer Instance1.
      auroraUtil.failoverClusterToATargetAndWaitUntilWriterChanged(writerId, readerBId);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to one of the available instances.
      currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertTrue(readerAId.equals(currentConnectionId) || readerBId.equals(currentConnectionId));
    }
  }

  /* Failure when within a transaction tests. */

  /** Writer fails within a transaction. Open transaction with setAutoCommit(false) */
  @TestTemplate
  public void test_writerFailWithinTransaction_setAutoCommitFalse()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getEndpoint(),
                initialWriterInstanceInfo.getEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_2");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_2 (id int not null primary key, test3_2_field varchar(255) not null)");
      conn.setAutoCommit(false); // open a new transaction

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (1, 'test field string 1')");

      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (2, 'test field string 2')"));
      assertEquals(
          SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = auroraUtil.getDBClusterWriterInstanceId();
      assertEquals(currentConnectionId, nextClusterWriterId);
      assertNotEquals(initialWriterId, nextClusterWriterId);

      // testStmt2 can NOT be used anymore since it's invalid

      final Statement testStmt3 = conn.createStatement();
      final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_2");
      rs.next();
      // Assert that NO row has been inserted to the table;
      assertEquals(0, rs.getInt(1));

      testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_2");
    }
  }

  /** Writer fails within a transaction. Open transaction with "START TRANSACTION". */
  @TestTemplate
  public void test_writerFailWithinTransaction_startTransaction()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getEndpoint(),
                initialWriterInstanceInfo.getEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_3");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)");
      testStmt1.executeUpdate("START TRANSACTION"); // open a new transaction

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (1, 'test field string 1')");

      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (2, 'test field string 2')"));
      assertEquals(
          SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = auroraUtil.getDBClusterWriterInstanceId();
      assertEquals(currentConnectionId, nextClusterWriterId);
      assertNotEquals(initialWriterId, nextClusterWriterId);

      // testStmt2 can NOT be used anymore since it's invalid

      final Statement testStmt3 = conn.createStatement();
      final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_3");
      rs.next();
      // Assert that NO row has been inserted to the table;
      assertEquals(0, rs.getInt(1));

      testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_3");
    }
  }

  /** Writer fails within NO transaction. */
  @TestTemplate
  public void test_writerFailWithNoTransaction() throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getEndpoint(),
                initialWriterInstanceInfo.getEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_4");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_4 (id int not null primary key, test3_4_field varchar(255) not null)");

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (1, 'test field string 1')");

      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      final SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (2, 'test field string 2')"));
      assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = auroraUtil.getDBClusterWriterInstanceId();
      assertEquals(currentConnectionId, nextClusterWriterId);
      assertNotEquals(initialWriterId, nextClusterWriterId);

      // testStmt2 can NOT be used anymore since it's invalid
      final Statement testStmt3 = conn.createStatement();
      final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_4 WHERE id = 1");
      rs.next();
      // Assert that row with id=1 has been inserted to the table;
      assertEquals(1, rs.getInt(1));

      final ResultSet rs1 = testStmt3.executeQuery("SELECT count(*) from test3_4 WHERE id = 2");
      rs1.next();
      // Assert that row with id=2 has NOT been inserted to the table;
      assertEquals(0, rs1.getInt(1));

      testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_4");
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void testServerFailoverWithIdleConnections() throws SQLException, InterruptedException {
    final List<Connection> idleConnections = new ArrayList<>();
    final String initialWriterId = this.currentWriter;
    final String clusterEndpoint = TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint();
    final int clusterEndpointPort = TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort();

    final Properties props = initDefaultProps();
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraConnectionTracker,failover");
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    for (int i = 0; i < IDLE_CONNECTIONS_NUM; i++) {
      // Keep references to 5 idle connections created using the cluster endpoints.
      idleConnections.add(DriverManager.getConnection(
          ConnectionStringHelper.getWrapperUrl(
              clusterEndpoint,
              clusterEndpointPort,
              TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
          props));
    }

    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(
            clusterEndpoint,
            clusterEndpointPort,
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
        props)) {

      final String instanceId = auroraUtil.queryInstanceId(
          TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
          conn);
      assertEquals(this.currentWriter, instanceId);

      // Ensure that all idle connections are still opened.
      for (Connection idleConnection : idleConnections) {
        assertFalse(idleConnection.isClosed());
      }

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Assert failover has occurred.
      assertThrows(
          FailoverSQLException.class,
          () -> auroraUtil.queryInstanceId(
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              conn));

      // Sleep for a second to allow daemon threads to finish running.
      Thread.sleep(1000);

      // Ensure that all idle connections are closed.
      for (Connection idleConnection : idleConnections) {
        assertTrue(idleConnection.isClosed(), String.format("Idle connection %s is still opened.", idleConnection));
      }
    }
  }

  @TestTemplate
  public void test_DataSourceWriterConnection_BasicFailover()
      throws SQLException, InterruptedException {

    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0);
    TestInstanceInfo nominatedWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(1);
    final String nominatedWriterId = nominatedWriterInstanceInfo.getInstanceId();

    try (final Connection conn =
        createDataSourceConnectionWithFailoverUsingInstanceId(
            initialWriterInstanceInfo.getEndpoint())) {

      // Trigger failover
      auroraUtil.failoverClusterToATargetAndWaitUntilWriterChanged(
          initialWriterInstanceInfo.getInstanceId(), nominatedWriterId);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Execute Query again to get the current connection id;
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);

      // Assert that we are connected to the new writer after failover happens.
      List<String> instanceIDs = auroraUtil.getAuroraInstanceIds();
      assertTrue(instanceIDs.size() > 0);
      final String nextWriterId = instanceIDs.get(0);

      assertEquals(nextWriterId, currentConnectionId);
      assertEquals(nominatedWriterId, currentConnectionId);

      assertTrue(conn.isValid(IS_VALID_TIMEOUT));
    }
  }

  @TestTemplate
  @EnableOnTestDriver(TestDriver.MYSQL)
  public void test_takeOverConnectionProperties() throws SQLException, InterruptedException {
    final String initialWriterId = this.currentWriter;

    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "false");
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    // Establish the topology cache so that we can later assert that testConnection does not inherit
    // properties from
    // establishCacheConnection either before or after failover
    final Connection establishCacheConnection =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props);
    establishCacheConnection.close();

    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      // Verify that connection accepts multi-statement sql
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeQuery("select 1; select 2; select 3;");

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that the connection property is maintained.
      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeQuery("select 1; select 2; select 3;");
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_failoverTimeoutMs() throws SQLException {
    final int maxTimeout = 10000; // 10 seconds

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProxiedProps();
    FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.set(props, String.valueOf(maxTimeout));
    DriverHelper.setConnectTimeout(props, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 3, TimeUnit.SECONDS);

    try (final Connection conn =
            DriverManager.getConnection(
                ConnectionStringHelper.getWrapperUrl(
                    initialWriterInstanceInfo.getEndpoint(),
                    initialWriterInstanceInfo.getEndpointPort(),
                    TestEnvironment.getCurrent()
                        .getInfo()
                        .getProxyDatabaseInfo()
                        .getDefaultDbName()),
                props);
        Statement stmt = conn.createStatement()) {

      ProxyHelper.disableConnectivity(initialWriterId);

      final long invokeStartTimeMs = System.currentTimeMillis();
      final SQLException e = assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"));
      final long invokeEndTimeMs = System.currentTimeMillis();

      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e.getSQLState());

      final long duration = invokeEndTimeMs - invokeStartTimeMs;
      assertTrue(duration < 15000); // Add in 5 seconds to account for time to detect the failure
    }
  }

  // Helper methods below

  protected Properties initDefaultProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "failover");
    return props;
  }

  protected Properties initDefaultProxiedProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "failover");
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(
        props,
        "?."
            + TestEnvironment.getCurrent()
                .getInfo()
                .getProxyDatabaseInfo()
                .getInstanceEndpointSuffix());
    return props;
  }

  // Attempt to run a query after the instance is down.
  // This should initiate the driver failover, first query after a failover
  // should always throw with the expected error message.
  protected void assertFirstQueryThrows(Connection connection, String expectedSQLErrorCode) {
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () ->
                auroraUtil.queryInstanceId(
                    TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
                    connection));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  protected void assertFirstQueryThrows(Statement stmt, String expectedSQLErrorCode) {
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () ->
                auroraUtil.executeInstanceIdQuery(
                    TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), stmt));
    assertEquals(
        expectedSQLErrorCode,
        exception.getSQLState(),
        "Unexpected SQL Exception: " + exception.getMessage());
  }

  protected Connection createDataSourceConnectionWithFailoverUsingInstanceId(
      String instanceEndpoint) throws SQLException {

    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    // Configure the property names for the underlying driver-specific data source:
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setDatabasePropertyName("databaseName");
    ds.setServerPropertyName("serverName");
    ds.setPortPropertyName("port");
    ds.setUrlPropertyName("url");

    // Specify the driver-specific data source:
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    // Configure the driver-specific data source:
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", instanceEndpoint);
    targetDataSourceProps.setProperty(
        "databaseName",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    targetDataSourceProps.setProperty("wrapperPlugins", "failover");

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
            == DatabaseEngine.MYSQL) {
      // Connecting to Mysql database with MariaDb driver requires a configuration parameter
      // "permitMysqlScheme"
      targetDataSourceProps.setProperty("permitMysqlScheme", "1");
    }

    DriverHelper.setConnectTimeout(targetDataSourceProps, 3, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(targetDataSourceProps, 3, TimeUnit.SECONDS);
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    return ds.getConnection(
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }
}
