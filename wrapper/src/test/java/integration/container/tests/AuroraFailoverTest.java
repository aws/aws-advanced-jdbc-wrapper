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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.mysql.cj.conf.PropertyKey;
import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.ContainerEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestDriver;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.TestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;
import software.amazon.jdbc.util.SqlState;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
@MakeSureFirstInstanceWriter
@Order(14)
public class AuroraFailoverTest {

  private static final Logger LOGGER = Logger.getLogger(AuroraFailoverTest.class.getName());

  protected static final TestUtility auroraUtil = TestUtility.getUtility();
  protected static final int IS_VALID_TIMEOUT = 5;

  protected String currentWriter;
  private static final int IDLE_CONNECTIONS_NUM = 5;

  @BeforeEach
  public void setUpEach() {
    this.currentWriter =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0)
            .getInstanceId();
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Driver failover occurs when executing a method against the connection
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_failFromWriterToNewWriter_failOnConnectionInvocation()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getHost(),
                initialWriterInstanceInfo.getPort(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Failure occurs on Connection invocation
      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

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
  @EnableOnNumOfInstances(min = 2)
  public void test_failFromWriterToNewWriter_failOnConnectionBoundObjectInvocation()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getHost(),
                initialWriterInstanceInfo.getPort(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      final Statement stmt = conn.createStatement();

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Failure occurs on Statement invocation
      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

      // Assert that the driver is connected to the new writer after failover happens.
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));

      assertNotEquals(initialWriterId, currentConnectionId);
    }
  }

  /**
   * Current reader dies, no other reader instance, failover to writer.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 2, max = 2)
  public void test_failFromReaderToWriter() throws SQLException {
    // Connect to the only available reader instance
    final TestInstanceInfo instanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().get(1);
    final String instanceId = instanceInfo.getInstanceId();
    final Properties props = initDefaultProxiedProps();

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                instanceInfo.getHost(),
                instanceInfo.getPort(),
                ContainerEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName()),
            props)) {

      // Crash the reader instance
      ProxyHelper.disableConnectivity(instanceId);

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

      // Assert that we are currently connected to the writer instance.
      final String writerId = this.currentWriter;
      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerId, currentConnectionId);
      assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));
    }
  }

  /* Failure when within a transaction tests. */

  /** Writer fails within a transaction. Open transaction with setAutoCommit(false) */
  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_writerFailWithinTransaction_setAutoCommitFalse()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getHost(),
                initialWriterInstanceInfo.getPort(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
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
              TransactionStateUnknownSQLException.class,
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
  @EnableOnNumOfInstances(min = 2)
  public void test_writerFailWithinTransaction_startTransaction()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getHost(),
                initialWriterInstanceInfo.getPort(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
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
              TransactionStateUnknownSQLException.class,
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

      // Assert autocommit is reset to true after failover.
      assertTrue(conn.getAutoCommit());
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void testServerFailoverWithIdleConnections() throws SQLException, InterruptedException {
    final List<Connection> idleConnections = new ArrayList<>();
    final String clusterEndpoint = ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint();
    final int clusterEndpointPort = ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort();

    final Properties props = initDefaultProps();
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraConnectionTracker,failover");

    for (int i = 0; i < IDLE_CONNECTIONS_NUM; i++) {
      // Keep references to 5 idle connections created using the cluster endpoints.
      idleConnections.add(DriverManager.getConnection(
          ConnectionStringHelper.getWrapperUrl(
              clusterEndpoint,
              clusterEndpointPort,
              ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
          props));
    }

    // Connect to a writer instance.
    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(
            clusterEndpoint,
            clusterEndpointPort,
            ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
        props)) {

      final String instanceId = auroraUtil.queryInstanceId(
          ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
          ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
          conn);
      assertEquals(this.currentWriter, instanceId);

      // Ensure that all idle connections are still opened.
      for (Connection idleConnection : idleConnections) {
        assertFalse(idleConnection.isClosed());
      }

      // Crash current writer and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Assert failover has occurred.
      assertThrows(
          FailoverSQLException.class,
          () -> auroraUtil.queryInstanceId(
              ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
              conn));
    }

    // Sleep for 30 seconds to allow daemon threads to finish running.
    Thread.sleep(30000);

    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(
            clusterEndpoint,
            clusterEndpointPort,
            ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
        props)) {

      final String instanceId = auroraUtil.queryInstanceId(
          ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
          ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
          conn);

      if (this.currentWriter.equals(instanceId)) {
        LOGGER.finest("Cluster failed over to the same instance " + instanceId + ".");
        for (Connection idleConnection : idleConnections) {
          assertFalse(idleConnection.isClosed(), String.format("Idle connection %s is closed.", idleConnection));
        }
      } else {
        LOGGER.finest("Cluster failed over to the instance " + instanceId + ".");
        // Ensure that all idle connections are closed.
        for (Connection idleConnection : idleConnections) {
          assertTrue(idleConnection.isClosed(), String.format("Idle connection %s is still opened.", idleConnection));
        }
      }
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_DataSourceWriterConnection_BasicFailover()
      throws SQLException, InterruptedException {

    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0);
    TestInstanceInfo nominatedWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(1);
    final String nominatedWriterId = nominatedWriterInstanceInfo.getInstanceId();

    try (final Connection conn =
        createDataSourceConnectionWithFailoverUsingInstanceId(
            initialWriterInstanceInfo.getHost())) {

      // Trigger failover
      auroraUtil.failoverClusterToATargetAndWaitUntilWriterChanged(
          initialWriterInstanceInfo.getInstanceId(), nominatedWriterId);

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

      // Execute Query again to get the current connection id;
      final String currentConnectionId = auroraUtil.queryInstanceId(conn);
      LOGGER.fine("currentConnectionId: " + currentConnectionId);

      // Assert that we are connected to the new writer after failover happens.
      List<String> instanceIDs = null;
      for (TestInstanceInfo instanceInfo : ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances()) {
        if (instanceInfo == initialWriterInstanceInfo
            && ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
                == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER) {
          // Old writer node for RDS MultiAz clusters (usually) isn't available for a long time after failover.
          // Let's skip this node and fetch topology from another node.
          continue;
        }
        try {
          instanceIDs = auroraUtil.getAuroraInstanceIds(
              ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
              ConnectionStringHelper.getUrl(
                  instanceInfo.getHost(), instanceInfo.getPort(),
                  ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
              ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
              ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
          if (!instanceIDs.isEmpty()) {
            break;
          }
        } catch (SQLException ex) {
          // do nothing
        }
      }

      assertTrue(instanceIDs.size() > 0);
      final String nextWriterId = instanceIDs.get(0);

      LOGGER.fine("currentConnectionObject: " + conn.unwrap(Connection.class));
      LOGGER.fine("initialWriterInstanceInfo endpoint: " + initialWriterInstanceInfo.getHost());
      LOGGER.fine("currentConnectionId: " + currentConnectionId);
      LOGGER.fine("nextWriterId: " + nextWriterId);
      LOGGER.fine("nominatedWriterId: " + nominatedWriterId);

      assertNotEquals(initialWriterInstanceInfo.getInstanceId(), currentConnectionId);
      assertEquals(nextWriterId, currentConnectionId);

      assertTrue(conn.isValid(IS_VALID_TIMEOUT));
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  @EnableOnTestDriver(TestDriver.MYSQL)
  public void test_takeOverConnectionProperties() throws SQLException, InterruptedException {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "false");

    // Establish the topology cache so that we can later assert that testConnection does not inherit
    // properties from establishCacheConnection either before or after failover
    final Connection establishCacheConnection =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props);
    establishCacheConnection.close();

    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
                ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props)) {

      // Verify that connection accepts multi-statement sql
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeQuery("select 1; select 2; select 3;");

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

      // Assert that the connection property is maintained.
      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeQuery("select 1; select 2; select 3;");
    }
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Autocommit is set to false and the keepSessionStateOnFailover property is set to true.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_failFromWriterWhereKeepSessionStateOnFailoverIsTrue()
      throws SQLException, InterruptedException {

    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(
                     initialWriterInstanceInfo.getHost(),
                     initialWriterInstanceInfo.getPort(),
                     ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
                 props)) {
      conn.setAutoCommit(false);

      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_3");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)");
      conn.setAutoCommit(false); // open a new transaction
      conn.commit();

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (1, 'test field string 1')");

      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              TransactionStateUnknownSQLException.class,
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
      conn.commit();

      // Assert autocommit is still false after failover.
      assertFalse(conn.getAutoCommit());
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_writerFailover_writerReelected() throws SQLException, InterruptedException {
    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProxiedProps();
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "2000");

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(
                     initialWriterInstanceInfo.getHost(),
                     initialWriterInstanceInfo.getPort(),
                     ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
                 props)) {

      ExecutorService executor = Executors.newFixedThreadPool(1, r -> {
        final Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      });

      try {
        // Failover usually changes the writer instance, but we want to test re-election of the same writer, so we will
        // simulate this by temporarily disabling connectivity to the writer.
        executor.submit(() -> {
          try {
            ProxyHelper.disableConnectivity(initialWriterId);
            TimeUnit.SECONDS.sleep(5);
            ProxyHelper.enableConnectivity(initialWriterId);
          } catch (InterruptedException e) {
            fail("The disable connectivity thread was unexpectedly interrupted.");
          }
        });

        // Leave some time for the other thread to start up
        TimeUnit.MILLISECONDS.sleep(500);

        // Failure occurs on Connection invocation
        auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

        // Assert that we are connected to the new writer after failover happens.
        final String currentConnectionId = auroraUtil.queryInstanceId(conn);
        assertTrue(auroraUtil.isDBInstanceWriter(currentConnectionId));
        assertEquals(currentConnectionId, initialWriterId);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_readerFailover_readerOrWriter() throws SQLException, InterruptedException {
    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();
    props.setProperty("failoverMode", "reader-or-writer");

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(
                     initialWriterInstanceInfo.getHost(),
                     initialWriterInstanceInfo.getPort(),
                     ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
                 props)) {

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Failure occurs on Connection invocation
      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_readerFailover_strictReader() throws SQLException, InterruptedException {
    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProps();
    props.setProperty("failoverMode", "strict-reader");

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(
                     initialWriterInstanceInfo.getHost(),
                     initialWriterInstanceInfo.getPort(),
                     ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
                 props)) {

      // Crash Instance1 and nominate a new writer
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Failure occurs on Connection invocation
      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertFalse(auroraUtil.isDBInstanceWriter(currentConnectionId));
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_readerFailover_writerReelected() throws SQLException, InterruptedException {
    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        ContainerEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProxiedProps();
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "2000");
    props.setProperty("failoverMode", "reader-or-writer");

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(
                     initialWriterInstanceInfo.getHost(),
                     initialWriterInstanceInfo.getPort(),
                     ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
                 props)) {

      ExecutorService executor = Executors.newFixedThreadPool(1, r -> {
        final Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      });

      try {
        // Failover usually changes the writer instance, but we want to test re-election of the same writer, so we will
        // simulate this by temporarily disabling connectivity to the writer.
        executor.submit(() -> {
          try {
            ProxyHelper.disableConnectivity(initialWriterId);
            TimeUnit.SECONDS.sleep(5);
            ProxyHelper.enableConnectivity(initialWriterId);
          } catch (InterruptedException e) {
            fail("The disable connectivity thread was unexpectedly interrupted.");
          }
        });

        // Leave some time for the other thread to start up
        TimeUnit.MILLISECONDS.sleep(500);

        // Failure occurs on Connection invocation
        auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  // Helper methods below

  protected String getFailoverPlugin() {
    return "failover";
  }

  protected Properties initDefaultProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, this.getFailoverPlugin());
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");
    return props;
  }

  protected Properties initDefaultProxiedProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, this.getFailoverPlugin());
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(
        props,
        "?." + ContainerEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointSuffix()
          + ":" + ContainerEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    return props;
  }

  protected Connection createDataSourceConnectionWithFailoverUsingInstanceId(
      String instanceEndpoint) throws SQLException {

    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    // Configure the property names for the underlying driver-specific data source:
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerName(instanceEndpoint);
    ds.setDatabase(ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    // Specify the driver-specific data source:
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    // Configure the driver-specific data source:
    Properties targetDataSourceProps = ConnectionStringHelper.getDefaultProperties();
    targetDataSourceProps.setProperty("wrapperPlugins", this.getFailoverPlugin());

    if (ContainerEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()
            == DatabaseEngine.MYSQL) {
      // Connecting to Mysql database with MariaDb driver requires a configuration parameter
      // "permitMysqlScheme"
      targetDataSourceProps.setProperty("permitMysqlScheme", "1");
    }

    PropertyDefinition.CONNECT_TIMEOUT.set(targetDataSourceProps, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(targetDataSourceProps, "10000");

    ds.setTargetDataSourceProperties(targetDataSourceProps);

    return ds.getConnection(
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        ContainerEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }
}
