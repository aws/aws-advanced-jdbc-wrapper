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

package integration.container.standard.postgres;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import eu.rekawek.toxiproxy.Proxy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.PGProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.util.SqlState;

public class StandardPostgresReadWriteSplittingTest extends StandardPostgresBaseTest {

  private final Properties defaultProps = getProps_readWritePlugin();
  private final Properties propsWithLoadBalance;

  StandardPostgresReadWriteSplittingTest() {
    final Properties props = getProps_readWritePlugin();
    ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.set(props, "true");
    this.propsWithLoadBalance = props;
  }

  private Properties getProps_readWritePlugin() {
    final Properties props = initDefaultProps();
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting");
    ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.set(props, "true");
    PGProperty.SOCKET_TIMEOUT.set(props, "1");
    return props;
  }

  @Test
  public void test_connectToWriter_setReadOnlyTrueTrueFalseFalseTrue() throws SQLException {
    try (final Connection conn = connect(defaultProps)) {
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

  @Tag("failing")
  @Test
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException {
    try (final Connection conn = connect(defaultProps)) {
      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      stmt.execute("START TRANSACTION READ ONLY");
      stmt.executeQuery("SELECT 1");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @Tag("failing")
  @Test
  public void test_setReadOnlyFalseInTransaction() throws SQLException {
    try (final Connection conn = connect(defaultProps)) {
      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      conn.setAutoCommit(false);
      stmt.executeQuery("SELECT COUNT(*) FROM information_schema.tables");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @Tag("failing")
  @Test
  public void test_setReadOnlyTrueInTransaction() throws SQLException {
    try (final Connection conn = connect(defaultProps)) {
      final String writerConnectionId = queryInstanceId(conn);

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
      stmt1.executeUpdate(
          "CREATE TABLE test_readWriteSplitting_readOnlyTrueInTransaction "
              + "(id int not null primary key, text_field varchar(255) not null)");

      conn.setAutoCommit(false);
      final Statement stmt2 = conn.createStatement();
      stmt2.executeUpdate(
          "INSERT INTO test_readWriteSplitting_readOnlyTrueInTransaction VALUES (1, 'test_field value 1')");

      final SQLException e = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), e.getSQLState());
      final String currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      stmt2.execute("COMMIT");
      conn.setAutoCommit(true);
      final ResultSet rs = stmt2.executeQuery("SELECT count(*) from test_readWriteSplitting_readOnlyTrueInTransaction");
      rs.next();
      assertEquals(1, rs.getInt(1));

      conn.setReadOnly(false);
      stmt2.executeUpdate("DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction");
    }
  }

  @Test
  public void test_setReadOnlyTrue_allReadersDown() throws SQLException, IOException {
    try (final Connection conn = connectToProxy(defaultProps)) {
      final String writerConnectionId = queryInstanceId(conn);

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
      String currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);

      assertDoesNotThrow(() -> conn.setReadOnly(false));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @Test
  public void test_setReadOnlyTrue_allInstancesDown() throws SQLException, IOException {
    try (final Connection conn = connectToProxy(defaultProps)) {
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

      // Since the postgres property "readOnlyMode" defaults to transaction, no SQL
      // statements are sent to the server and the call to setReadOnly succeeds.
      assertDoesNotThrow(() -> conn.setReadOnly(true));
    }
  }

  @Test
  public void test_setReadOnlyTrue_allInstancesDown_writerClosed() throws SQLException, IOException {
    try (final Connection conn = connectToProxy(defaultProps)) {
      conn.close();

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

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
    }
  }

  @Test
  public void test_setReadOnlyFalse_allInstancesDown() throws SQLException, IOException {
    try (final Connection conn = connectToProxy(defaultProps)) {
      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      final String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

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

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());
    }
  }

  @Test
  public void test_readerLoadBalancing_autocommitTrue() throws SQLException {
    try (final Connection conn = connect(propsWithLoadBalance)) {
      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      for (int i = 0; i < 10; i++) {
        final Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT " + i);
        readerConnectionId = queryInstanceId(conn);
        assertNotEquals(writerConnectionId, readerConnectionId);

        final ResultSet rs = stmt.getResultSet();
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
    }
  }

  @Tag("failing")
  @Test
  public void test_readerLoadBalancing_autocommitFalse() throws SQLException {
    try (final Connection conn = connect(propsWithLoadBalance)) {
      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      conn.setAutoCommit(false);
      final Statement stmt = conn.createStatement();

      for (int i = 0; i < 5; i++) {
        stmt.executeQuery("SELECT " + i);
        conn.commit();
        readerConnectionId = queryInstanceId(conn);
        assertNotEquals(writerConnectionId, readerConnectionId);

        final ResultSet rs = stmt.getResultSet();
        rs.next();
        assertEquals(i, rs.getInt(1));

        stmt.executeQuery("SELECT " + i);
        conn.rollback();
        readerConnectionId = queryInstanceId(conn);
        assertNotEquals(writerConnectionId, readerConnectionId);
      }
    }
  }

  @Test
  public void test_transactionResolutionUnknown() throws SQLException, IOException {
    try (final Connection conn = connectToProxy(propsWithLoadBalance)) {
      final String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      final String readerId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerId);

      final Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");
      final Proxy proxyInstance = proxyMap.get(instanceIDs[1]);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", readerId));
      }

      final SQLException e = assertThrows(SQLException.class, conn::rollback);
      assertEquals(SqlState.CONNECTION_FAILURE.getState(), e.getSQLState());

      try (final Connection newConn = connectToProxy(propsWithLoadBalance)) {
        newConn.setReadOnly(true);
        final Statement newStmt = newConn.createStatement();
        final ResultSet rs = newStmt.executeQuery("SELECT 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }
  }
}
