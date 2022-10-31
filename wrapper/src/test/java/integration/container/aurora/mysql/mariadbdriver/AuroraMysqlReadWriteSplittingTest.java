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

package integration.container.aurora.mysql.mariadbdriver;

import static com.mysql.cj.conf.PropertyKey.socketTimeout;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import eu.rekawek.toxiproxy.Proxy;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingSQLException;
import software.amazon.jdbc.util.SqlState;

@Disabled
public class AuroraMysqlReadWriteSplittingTest extends MariadbAuroraMysqlBaseTest {

  private static Stream<Arguments> testParameters() {
    return Stream.of(
        Arguments.of(getProps_allPlugins()),
        Arguments.of(getProps_readWritePlugin())
    );
  }

  private static Stream<Arguments> proxiedTestParameters() {
    return Stream.of(
        Arguments.of(getProxiedProps_allPlugins()),
        Arguments.of(getProxiedProps_readWritePlugin())
    );
  }

  @ParameterizedTest(name = "test_connectToWriter_setReadOnlyTrueFalseTrue")
  @MethodSource("testParameters")
  public void test_connectToWriter_setReadOnlyTrueFalseTrue(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);

      conn.setReadOnly(true);
      final String nextReaderConnectionId = queryInstanceId(conn);
      assertEquals(readerConnectionId, nextReaderConnectionId);
    }
  }

  @ParameterizedTest(name = "test_connectToReader_setReadOnlyTrueFalse")
  @MethodSource("testParameters")
  public void test_connectToReader_setReadOnlyTrueFalse(final Properties props) throws SQLException {
    final String initialReaderId = instanceIDs[1];

    try (final Connection conn = connectToInstance(initialReaderId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT, props)) {
      String readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(true);
      readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(instanceIDs[0], writerConnectionId);
      assertNotEquals(initialReaderId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));
    }
  }

  @ParameterizedTest(name = "test_connectToReaderCluster_setReadOnlyTrueFalse")
  @MethodSource("testParameters")
  public void test_connectToReaderCluster_setReadOnlyTrueFalse(final Properties props) throws SQLException {
    try (final Connection conn = connectToInstance(MYSQL_RO_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      final String initialReaderId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(initialReaderId));

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(instanceIDs[0], writerConnectionId);
      assertNotEquals(initialReaderId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));
    }
  }

  @ParameterizedTest(name = "test_connectToReaderIP_setReadOnlyTrueFalse")
  @MethodSource("testParameters")
  public void test_connectToReaderIP_setReadOnlyTrueFalse(final Properties props)
      throws SQLException, UnknownHostException {
    final String instanceHostPattern = "?" + DB_CONN_STR_SUFFIX;
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props, instanceHostPattern);
    final String hostIp = hostToIP(MYSQL_RO_CLUSTER_URL);
    try (final Connection conn = connectToInstance(hostIp, AURORA_MYSQL_PORT, props)) {
      final String initialReaderId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(initialReaderId));

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(instanceIDs[0], writerConnectionId);
      assertNotEquals(initialReaderId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyFalseInReadOnlyTransaction")
  @MethodSource("testParameters")
  public void test_setReadOnlyFalseInReadOnlyTransaction(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTransaction");
      stmt1.executeUpdate(
          "CREATE TABLE test_readWriteSplitting_readOnlyTransaction "
              + "(id int not null primary key, text_field varchar(255) not null)");
      stmt1.executeUpdate("INSERT INTO test_readWriteSplitting_readOnlyTransaction VALUES (1, 'test_field value 1')");

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(readerConnectionId));

      final Statement stmt2 = conn.createStatement();
      stmt2.execute("START TRANSACTION READ ONLY");
      stmt2.executeQuery("SELECT count(*) from test_readWriteSplitting_readOnlyTransaction");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());

      stmt2.execute("COMMIT");

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt3 = conn.createStatement();
      stmt3.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTransaction");
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyFalseInTransaction_setAutocommitFalse")
  @MethodSource("testParameters")
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyFalse_autocommitFalse");
      stmt1.executeUpdate(
          "CREATE TABLE test_readWriteSplitting_readOnlyFalse_autocommitFalse "
              + "(id int not null primary key, text_field varchar(255) not null)");
      stmt1.executeUpdate(
          "INSERT INTO test_readWriteSplitting_readOnlyFalse_autocommitFalse VALUES (1, 'test_field value 1')");

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(readerConnectionId));

      final Statement stmt2 = conn.createStatement();
      conn.setAutoCommit(false);
      stmt2.executeQuery("SELECT count(*) from test_readWriteSplitting_readOnlyFalse_autocommitFalse");

      final ReadWriteSplittingSQLException exception =
          assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());

      stmt2.execute("COMMIT");

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setAutoCommit(true);
      final Statement stmt3 = conn.createStatement();
      stmt3.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyFalse_autocommitFalse");
    }
  }

  @Disabled("Failing on graalvm at the first assertEquals")
  @ParameterizedTest(name = "test_setReadOnlyFalseInTransaction_setAutocommitZero")
  @MethodSource("testParameters")
  public void test_setReadOnlyFalseInTransaction_setAutocommitZero(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyFalse_autocommitZero");
      stmt1.executeUpdate(
          "CREATE TABLE test_readWriteSplitting_readOnlyFalse_autocommitZero "
              + "(id int not null primary key, text_field varchar(255) not null)");
      stmt1.executeUpdate(
          "INSERT INTO test_readWriteSplitting_readOnlyFalse_autocommitZero VALUES (1, 'test_field value 1')");

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(readerConnectionId));

      final Statement stmt2 = conn.createStatement();
      stmt2.execute("SET autocommit = 0");
      stmt2.executeQuery("SELECT count(*) from test_readWriteSplitting_readOnlyFalse_autocommitZero");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());

      stmt2.execute("COMMIT");

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt3 = conn.createStatement();
      stmt3.execute("SET autocommit = 1");
      stmt3.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyFalse_autocommitZero");
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyTrueInTransaction")
  @MethodSource("testParameters")
  public void test_setReadOnlyTrueInTransaction(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
      stmt1.executeUpdate(
          "CREATE TABLE test_readWriteSplitting_readOnlyTrueInTransaction "
              + "(id int not null primary key, text_field varchar(255) not null)");
      conn.setAutoCommit(false);

      final Statement stmt2 = conn.createStatement();
      stmt2.executeUpdate(
          "INSERT INTO test_readWriteSplitting_readOnlyTrueInTransaction VALUES (1, 'test_field value 1')");

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      stmt2.execute("COMMIT");
      final ResultSet rs = stmt2.executeQuery("SELECT count(*) from test_readWriteSplitting_readOnlyTrueInTransaction");
      rs.next();
      assertEquals(1, rs.getInt(1));

      conn.setReadOnly(false);
      conn.setAutoCommit(true);
      stmt2.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_autocommitTrue")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_autocommitTrue(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.set(props, "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerId);
      assertTrue(isDBInstanceReader(readerId));

      // Assert switch after each execute
      String nextReaderId;
      for (int i = 0; i < 5; i++) {
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
        assertTrue(isDBInstanceReader(readerId));
        readerId = nextReaderId;
      }

      // Verify behavior for transactions started while autocommit is on (autocommit is implicitly disabled)
      // Connection should not be switched while inside a transaction
      for (int i = 0; i < 5; i++) {
        final Statement stmt = conn.createStatement();
        stmt.execute("  bEgiN ");
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        stmt.execute(" CommIT;");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_autocommitFalse")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_autocommitFalse(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.set(props, "true");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT, props)) {
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setAutoCommit(false);
      conn.setReadOnly(true);

      // Connection should not be switched while inside a transaction
      String readerId;
      String nextReaderId;

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        conn.commit();
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        final Statement stmt = conn.createStatement();
        stmt.execute("commit");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        conn.rollback();
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        final Statement stmt = conn.createStatement();
        stmt.execute(" roLLback ; ");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }
    }
  }

  @Disabled("Failing on graalvm at the first assertEquals")
  @ParameterizedTest(name = "test_readerLoadBalancing_switchAutoCommitInTransaction")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_switchAutoCommitInTransaction(final Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.set(props, "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerId;
      String nextReaderId;

      // Start transaction while autocommit is on (autocommit is implicitly disabled)
      // Connection should not be switched while inside a transaction
      Statement stmt = conn.createStatement();
      stmt.execute("  StarT   TRanSACtion  REad onLy  ; ");
      readerId = queryInstanceId(conn);
      nextReaderId = queryInstanceId(conn);
      assertEquals(readerId, nextReaderId);
      conn.setAutoCommit(false); // Switch autocommit value while inside the transaction
      nextReaderId = queryInstanceId(conn);
      assertEquals(readerId, nextReaderId);
      conn.commit();

      assertFalse(conn.getAutoCommit());
      nextReaderId = queryInstanceId(conn);
      assertNotEquals(readerId, nextReaderId); // Connection should have switched after committing

      readerId = nextReaderId;
      nextReaderId = queryInstanceId(conn);
      // Since autocommit is now off, we should be in a transaction; connection should not be switching
      assertEquals(readerId, nextReaderId);
      final ReadWriteSplittingSQLException e =
          assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), e.getSQLState());

      conn.setAutoCommit(true); // Switch autocommit value while inside the transaction
      stmt = conn.createStatement();
      stmt.execute("commit");

      assertTrue(conn.getAutoCommit());
      readerId = queryInstanceId(conn);

      // Autocommit is now on; connection should switch after each execute
      for (int i = 0; i < 5; i++) {
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
        readerId = nextReaderId;
      }
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_remainingStateTransitions")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_remainingStateTransitions(final Properties props) throws SQLException {
    // Main functionality has been tested in the other tests
    // This test executes state transitions not covered by other tests to verify no unexpected errors are thrown
    final String initialWriterId = instanceIDs[0];

    ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.set(props, "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT, props)) {
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      conn.setReadOnly(false);
      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      conn.setAutoCommit(true);
      Statement stmt = conn.createStatement();
      stmt.execute("commit");
      stmt = conn.createStatement();
      stmt.execute("commit");
      stmt = conn.createStatement();
      stmt.execute("begin");
      stmt.execute("commit");
      conn.setAutoCommit(false);
      conn.commit();
      conn.commit();
      stmt = conn.createStatement();
      stmt.execute("begin");
      stmt.execute("SELECT 1");
      conn.commit();
      conn.setReadOnly(false);
      conn.setReadOnly(true);
      conn.setReadOnly(false);
      conn.setAutoCommit(true);
      conn.setReadOnly(false);
      stmt = conn.createStatement();
      stmt.execute("commit");
      conn.setReadOnly(false);
      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      conn.commit();
      conn.setAutoCommit(true);
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_lostConnectivity")
  @MethodSource("proxiedTestParameters")
  public void test_readerLoadBalancing_lostConnectivity(final Properties props) throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.set(props, "true");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      conn.setReadOnly(true);
      final Statement stmt1 = conn.createStatement();
      // autocommit on transaction (autocommit implicitly disabled)
      stmt1.execute("BEGIN");
      final String readerId = queryInstanceId(conn);

      final Proxy proxyInstance = proxyMap.get(readerId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", readerId));
      }

      final SQLException e = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      containerHelper.enableConnectivity(proxyInstance);

      if (pluginChainIncludesFailoverPlugin(props)) {
        assertEquals(SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), e.getSQLState());
      } else {
        assertEquals(SqlState.COMMUNICATION_ERROR.getState(), e.getSQLState());
      }

      if (pluginChainIncludesFailoverPlugin(props)) {
        final Statement stmt2 = conn.createStatement();
        stmt2.execute("SELECT 1");
        final ResultSet rs = stmt2.getResultSet();
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }

    // autocommit off transaction
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      final String readerId = queryInstanceId(conn);
      final Statement stmt1 = conn.createStatement();

      final Proxy proxyInstance = proxyMap.get(readerId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", readerId));
      }

      final SQLException e = assertThrows(SQLException.class, () -> stmt1.execute("SELECT 1"));
      containerHelper.enableConnectivity(proxyInstance);

      if (pluginChainIncludesFailoverPlugin(props)) {
        assertEquals(SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), e.getSQLState());
      } else {
        assertEquals(SqlState.COMMUNICATION_ERROR.getState(), e.getSQLState());
        return;
      }

      final Statement stmt2 = conn.createStatement();
      stmt2.execute("SELECT 1");
      final ResultSet rs = stmt2.getResultSet();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyTrue_allReadersDown")
  @MethodSource("proxiedTestParameters")
  public void test_setReadOnlyTrue_allReadersDown(final Properties props) throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    props.setProperty(socketTimeout.getKeyName(), "2000");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      // Kill all reader instances
      for (int i = 1; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertTrue(isDBInstanceWriter(currentConnectionId));

      assertDoesNotThrow(() -> conn.setReadOnly(false));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertTrue(isDBInstanceWriter(currentConnectionId));
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyTrue_allInstancesDown")
  @MethodSource("proxiedTestParameters")
  public void test_setReadOnlyTrue_allInstancesDown(final Properties props) throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    // Ensure a topology query is sent to the database when conn.setReadOnly is called
    AuroraHostListProvider.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.set(props, "1");
    props.setProperty(socketTimeout.getKeyName(), "1000");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      final String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      // Kill all instances
      for (int i = 0; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      final ReadWriteSplittingSQLException e =
          assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(true));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e.getSQLState());
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyFalse_allInstancesDown")
  @MethodSource("proxiedTestParameters")
  public void test_setReadOnlyFalse_allInstancesDown(final Properties props) throws SQLException, IOException {
    final String initialReaderId = instanceIDs[1];

    props.setProperty(socketTimeout.getKeyName(), "1000");
    try (final Connection conn = connectToInstance(initialReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      final String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));

      // Kill all instances
      for (int i = 0; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      final ReadWriteSplittingSQLException exception =
          assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
    }
  }

  @Test
  public void test_failoverToNewWriter_setReadOnlyTrueFalse() throws SQLException, InterruptedException, IOException {
    final String initialWriterId = instanceIDs[0];

    final Properties props = getProxiedProps_allPlugins();
    props.setProperty(socketTimeout.getKeyName(), "2000");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      // Kill all reader instances
      for (int i = 1; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      // Force internal reader connection to the writer instance
      conn.setReadOnly(true);
      String currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      conn.setReadOnly(false);

      enableAllProxies();

      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // Failure occurs on Connection invocation
      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to the new writer after failover happens.
      currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      assertNotEquals(currentConnectionId, initialWriterId);

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(currentConnectionId));

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
    }
  }

  @Test
  public void test_failoverToNewReader_setReadOnlyFalseTrue() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    final Properties props = getProxiedProps_allPlugins();
    props.setProperty(socketTimeout.getKeyName(), "2000");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      String otherReaderId = "";
      for (int i = 1; i < instanceIDs.length; i++) {
        if (!instanceIDs[i].equals(readerConnectionId)) {
          otherReaderId = instanceIDs[i];
          break;
        }
      }
      if (otherReaderId.equals("")) {
        fail("could not acquire new reader ID");
      }

      // Kill all instances except one other reader
      for (int i = 0; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        if (otherReaderId.equals(instanceId)) {
          continue;
        }

        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());
      assertFalse(conn.isClosed());
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(otherReaderId, currentConnectionId);
      assertNotEquals(readerConnectionId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));

      enableAllProxies();

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(otherReaderId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));
    }
  }

  @Test
  public void test_failoverReaderToWriter_setReadOnlyTrueFalse() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    final Properties props = getProxiedProps_allPlugins();
    props.setProperty(socketTimeout.getKeyName(), "2000");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      // Kill all instances except the writer
      for (int i = 1; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];

        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());
      assertFalse(conn.isClosed());
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      enableAllProxies();

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertNotEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));
    }
  }

  @Test
  public void test_multiHostUrl_topologyOverridesHostList() throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = DriverManager.getConnection(
        DB_CONN_STR_PREFIX + initialWriterId + DB_CONN_STR_SUFFIX + ",non-existent-host", getProps_allPlugins())) {
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertNotEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));
    }
  }

  @Test
  public void test_transactionResolutionUnknown_readWriteSplittingPluginOnly() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    final Properties props = getProxiedProps_readWritePlugin();
    ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.set(props, "true");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT, props)) {
      final String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      final String readerId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerId);
      assertTrue(isDBInstanceReader(readerId));

      final Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");
      final Proxy proxyInstance = proxyMap.get(readerId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", readerId));
      }

      final SQLException e = assertThrows(SQLException.class, conn::rollback);
      assertEquals(SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), e.getSQLState());

      try (final Connection newConn = connectToInstance(
          initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
        newConn.setReadOnly(true);
        final Statement newStmt = newConn.createStatement();
        final ResultSet rs = newStmt.executeQuery("SELECT 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  private static Properties getProps_allPlugins() {
    final Properties props = initDefaultProps();
    addAllTestPlugins(props);
    return props;
  }

  private static Properties getProxiedProps_allPlugins() {
    final Properties props = initDefaultProxiedProps();
    addAllTestPlugins(props);
    return props;
  }

  private static Properties getProps_readWritePlugin() {
    final Properties props = initDefaultProps();
    addReadWritePlugins(props);
    return props;
  }

  private static Properties getProxiedProps_readWritePlugin() {
    final Properties props = initDefaultProxiedProps();
    addReadWritePlugins(props);
    return props;
  }

  private static void addAllTestPlugins(final Properties props) {
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover,efm");
  }

  private static void addReadWritePlugins(final Properties props) {
    PropertyDefinition.PLUGINS.set(props, "auroraHostList,readWriteSplitting");
  }

  private boolean pluginChainIncludesFailoverPlugin(final Properties props) {
    final String plugins = PropertyDefinition.PLUGINS.getString(props);
    if (plugins == null) {
      return false;
    }

    return plugins.contains("failover");
  }

  private void enableAllProxies() {
    proxyMap.forEach((instance, proxy) -> {
      assertNotNull(proxy, "Proxy isn't found for " + instance);
      containerHelper.enableConnectivity(proxy);
    });
  }
}
