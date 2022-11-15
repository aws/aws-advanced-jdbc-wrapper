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

package integration.container.aurora.mysql.mysqldriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.conf.PropertyKey;
import eu.rekawek.toxiproxy.Proxy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.util.SqlState;

public class AuroraMysqlFailoverTest extends MysqlAuroraMysqlBaseTest {
  /* Writer connection failover tests. */

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new writer. Driver failover
   * occurs when executing a method against the connection
   */
  @Test
  public void test_failFromWriterToNewWriter_failOnConnectionInvocation()
      throws SQLException, InterruptedException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn =
             connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT,
                 initDefaultProps())) {
      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // Failure occurs on Connection invocation
      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to the new writer after failover happens.
      final String currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      assertNotEquals(currentConnectionId, initialWriterId);
    }
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new writer. Driver failover
   * occurs when executing a method against an object bound to the connection (eg a Statement object created by the
   * connection).
   */
  @Test
  public void test_failFromWriterToNewWriter_failOnConnectionBoundObjectInvocation()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT,
        initDefaultProps())) {
      final Statement stmt = conn.createStatement();

      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // Failure occurs on Statement invocation
      assertFirstQueryThrows(stmt, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that the driver is connected to the new writer after failover happens.
      final String currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      assertNotEquals(initialWriterId, currentConnectionId);
    }
  }

  /**
   * Current reader dies, no other reader instance, failover to writer, then writer dies, failover to another available
   * reader instance.
   */
  @Test
  public void test_failFromReaderToWriterToAnyAvailableInstance()
      throws SQLException, IOException, InterruptedException {

    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    // Crashing all readers except the first one
    for (int i = 2; i < clusterSize; i++) {
      final Proxy instanceProxy = proxyMap.get(instanceIDs[i]);
      containerHelper.disableConnectivity(instanceProxy);
    }

    // Connect to Instance2 which is the only reader that is up.
    final String instanceId = instanceIDs[1];

    try (final Connection conn = connectToInstance(instanceId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT)) {
      // Crash Instance2
      Proxy instanceProxy = proxyMap.get(instanceId);
      containerHelper.disableConnectivity(instanceProxy);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are currently connected to the writer Instance1.
      final String writerId = instanceIDs[0];
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(writerId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      // Stop crashing reader Instance2 and Instance3
      final String readerAId = instanceIDs[1];
      instanceProxy = proxyMap.get(readerAId);
      containerHelper.enableConnectivity(instanceProxy);

      final String readerBId = instanceIDs[2];
      instanceProxy = proxyMap.get(readerBId);
      containerHelper.enableConnectivity(instanceProxy);

      // Crash writer Instance1.
      failoverClusterToATargetAndWaitUntilWriterChanged(writerId, readerBId);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to one of the available instances.
      currentConnectionId = queryInstanceId(conn);
      assertTrue(readerAId.equals(currentConnectionId) || readerBId.equals(currentConnectionId));
    }
  }

  /* Failure when within a transaction tests. */

  /**
   * Writer fails within a transaction. Open transaction with setAutoCommit(false)
   */
  @Test
  public void test_writerFailWithinTransaction_setAutoCommitFalse()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT,
        initDefaultProps())) {
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_2");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_2 (id int not null primary key, test3_2_field varchar(255) not null)");
      conn.setAutoCommit(false); // open a new transaction

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (1, 'test field string 1')");

      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () -> testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (2, 'test field string 2')"));
      assertEquals(SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = getDBClusterWriterInstanceId();
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

  /**
   * Writer fails within a transaction. Open transaction with "START TRANSACTION".
   */
  @Test
  public void test_writerFailWithinTransaction_startTransaction()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT,
        initDefaultProps())) {
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_3");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)");
      testStmt1.executeUpdate("START TRANSACTION"); // open a new transaction

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (1, 'test field string 1')");

      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () -> testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (2, 'test field string 2')"));
      assertEquals(SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = getDBClusterWriterInstanceId();
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

  /**
   * Writer fails within NO transaction.
   */
  @Test
  public void test_writerFailWithNoTransaction() throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT,
        initDefaultProps())) {
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_4");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_4 (id int not null primary key, test3_4_field varchar(255) not null)");

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (1, 'test field string 1')");

      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      final SQLException exception =
          assertThrows(
              SQLException.class,
              () -> testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (2, 'test field string 2')"));
      assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = getDBClusterWriterInstanceId();
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

  @Test
  public void test_DataSourceWriterConnection_BasicFailover() throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];
    final String nominatedWriterId = instanceIDs[1];

    try (final Connection conn = createDataSourceConnectionWithFailoverUsingInstanceId(initialWriterId)) {
      // Crash writer Instance1 and nominate Instance2 as the new writer
      failoverClusterToATargetAndWaitUntilWriterChanged(initialWriterId, nominatedWriterId);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Execute Query again to get the current connection id;
      final String currentConnectionId = queryInstanceId(conn);

      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextWriterId = getDBClusterWriterInstanceId();
      assertEquals(nextWriterId, currentConnectionId);
      assertEquals(nominatedWriterId, currentConnectionId);

      assertTrue(conn.isValid(IS_VALID_TIMEOUT));
    }
  }

  @Disabled
  @Test
  public void test_takeOverConnectionProperties() throws SQLException, InterruptedException {
    final String initialWriterId = instanceIDs[0];

    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "false");

    // Establish the topology cache so that we can later assert that testConnection does not inherit properties from
    // establishCacheConnection either before or after failover
    final String url = DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL + "/" + AURORA_MYSQL_DB;
    final Connection establishCacheConnection = DriverManager.getConnection(
        url,
        props);
    establishCacheConnection.close();

    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      // Verify that connection accepts multi-statement sql
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeQuery("select @@aurora_server_id; select 1; select 2;");

      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that the connection property is maintained.
      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeQuery("select @@aurora_server_id; select 1; select 2;");
    }
  }

  @Test
  public void test_failoverTimeoutMs() throws SQLException, IOException {
    final int maxTimeout = 10000; // 10 seconds
    final String initialWriterId = instanceIDs[0];
    final Properties props = initDefaultProps();
    FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.set(props, String.valueOf(maxTimeout));

    try (Connection conn = connectToInstance(
        initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props);
         Statement stmt = conn.createStatement()) {
      final Proxy instanceProxy = proxyMap.get(initialWriterId);
      containerHelper.disableConnectivity(instanceProxy);
      final long invokeStartTimeMs = System.currentTimeMillis();
      final SQLException e = assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"));
      final long invokeEndTimeMs = System.currentTimeMillis();
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e.getSQLState());
      final long duration = invokeEndTimeMs - invokeStartTimeMs;
      assertTrue(duration < 15000); // Add in 5 seconds to account for time to detect the failure
    }
  }
}
