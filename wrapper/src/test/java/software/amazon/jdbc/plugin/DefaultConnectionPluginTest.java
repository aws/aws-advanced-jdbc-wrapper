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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionInfo;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.ImportantEventService;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

@SuppressWarnings("unchecked")
class DefaultConnectionPluginTest {

  private DefaultConnectionPlugin plugin;

  @Mock FullServicesContainer servicesContainer;
  @Mock ImportantEventService mockImportantEventService;
  @Mock PluginService pluginService;
  @Mock ConnectionProvider connectionProvider;
  @Mock PluginManagerService pluginManagerService;
  @Mock JdbcCallable<Void, SQLException> mockSqlFunction;
  @Mock JdbcCallable<Connection, SQLException> mockConnectFunction;
  @Mock Connection conn;
  @Mock Connection oldConn;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;
  @Mock ConnectionProviderManager mockConnectionProviderManager;
  @Mock HostSpec mockHostSpec;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock Statement mockStatement;


  private AutoCloseable closeable;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(pluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    // noinspection unchecked
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);
    when(mockConnectionProviderManager.getConnectionProvider(anyString(), any(), any()))
        .thenReturn(connectionProvider);
    when(connectionProvider.connect(anyString(), any(), any(), any(), any()))
        .thenReturn(new ConnectionInfo(conn, false));
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getImportantEventService()).thenReturn(mockImportantEventService);
    // A dialect that reports nothing, so the plugin tracks transaction boundaries itself.
    when(pluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.isInTransaction(any())).thenReturn(null);

    plugin = new DefaultConnectionPlugin(
        servicesContainer, connectionProvider, pluginManagerService, mockConnectionProviderManager);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testExecute_closeCurrentConnection() throws SQLException {
    when(this.pluginService.getCurrentConnection()).thenReturn(conn);
    plugin.execute(Void.class, SQLException.class, conn, "Connection.close", mockSqlFunction, new Object[]{});
    verify(pluginManagerService, times(1)).setInTransaction(false);
  }

  @Test
  void testExecute_registersMethodCall() throws SQLException {
    when(this.pluginService.getCurrentConnection()).thenReturn(conn);
    final Statement statement = mock(Statement.class);

    plugin.execute(
        Void.class, SQLException.class, statement, "Statement.executeQuery", mockSqlFunction, new Object[]{});

    // Every pipelined call is recorded in the bounded method-call history, not in the event queue.
    verify(mockImportantEventService, times(1)).registerMethodCall("Statement.executeQuery");
    verify(mockImportantEventService, never()).registerEvent(anyString());
    verify(mockImportantEventService, never()).registerEvent(any(Supplier.class));
  }

  @Test
  void testExecute_closeOldConnection() throws SQLException {
    when(this.pluginService.getCurrentConnection()).thenReturn(conn);
    plugin.execute(Void.class, SQLException.class, oldConn, "Connection.close", mockSqlFunction, new Object[]{});
    verify(pluginManagerService, never()).setInTransaction(anyBoolean());
  }

  @Test
  void testExecute_statementIsNotAskedForItsConnection() throws SQLException {
    // Staleness of a Statement or ResultSet is decided before the pipeline runs (see
    // WrapperUtils and JdbcMethod.checkBoundedConnection), so this plugin must not ask the target
    // driver which connection the object is bound to. That question returned the physical connection
    // even when the wrapper held a pooled or logical handle for the same session, which made valid
    // objects look stale.
    when(this.pluginService.getCurrentConnection()).thenReturn(conn);
    final Statement statement = mock(Statement.class);

    plugin.execute(
        Void.class, SQLException.class, statement, "Statement.close", mockSqlFunction, new Object[]{});

    verify(statement, never()).getConnection();
    verify(statement, never()).isClosed();
  }

  @Test
  void testExecute_tracksExplicitTransactionStatements_whenDriverReportsNothing() throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(true);

    execute(mockStatement, "Statement.execute", "BEGIN");
    verify(pluginManagerService, times(1)).setInTransaction(true);
    verify(pluginManagerService, never()).setInTransaction(false);

    execute(mockStatement, "Statement.execute", "COMMIT");
    verify(pluginManagerService, times(1)).setInTransaction(false);

    execute(mockStatement, "Statement.executeUpdate", "START TRANSACTION");
    verify(pluginManagerService, times(2)).setInTransaction(true);

    execute(mockStatement, "Statement.execute", "ROLLBACK");
    verify(pluginManagerService, times(2)).setInTransaction(false);
  }

  @Test
  void testExecute_tracksDmlWithAutoCommitOff_whenDriverReportsNothing() throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(false);

    execute(mockStatement, "Statement.executeQuery", "SELECT * FROM t");
    verify(pluginManagerService, times(1)).setInTransaction(true);

    execute(conn, "Connection.commit");
    verify(pluginManagerService, times(1)).setInTransaction(false);

    execute(mockStatement, "Statement.executeUpdate", "INSERT INTO t VALUES (1)");
    verify(pluginManagerService, times(2)).setInTransaction(true);

    execute(conn, "Connection.rollback");
    verify(pluginManagerService, times(2)).setInTransaction(false);
  }

  @Test
  void testExecute_dmlWithAutoCommitOnOpensNoTransaction_whenDriverReportsNothing() throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(true);

    execute(mockStatement, "Statement.executeUpdate", "INSERT INTO t VALUES (1)");
    execute(mockStatement, "Statement.executeQuery", "SELECT * FROM t");

    verify(pluginManagerService, never()).setInTransaction(anyBoolean());
  }

  @Test
  void testExecute_autoCommitFalseToTrueEndsTransaction_whenDriverReportsNothing() throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(false);

    // According to the JDBC spec, switching autocommit from false to true commits.
    execute(conn, "Connection.setAutoCommit", true);
    verify(pluginManagerService, times(1)).setInTransaction(false);

    execute(mockStatement, "Statement.execute", "SET autocommit = 1");
    verify(pluginManagerService, times(2)).setInTransaction(false);
    verify(conn, times(1)).setAutoCommit(true);
  }

  @Test
  void testExecute_recordsDriverStateInsteadOfAnalyzingSql_whenDriverReportsState() throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    // What the SQL says is ignored: a BEGIN while the driver reports no transaction records false,
    // and a plain SELECT while the driver reports a transaction records true.
    when(mockTargetDriverDialect.isInTransaction(conn)).thenReturn(false);
    when(conn.getAutoCommit()).thenReturn(false);

    execute(mockStatement, "Statement.execute", "BEGIN");
    verify(pluginManagerService, times(1)).setInTransaction(false);
    verify(pluginManagerService, never()).setInTransaction(true);

    when(mockTargetDriverDialect.isInTransaction(conn)).thenReturn(true);
    execute(mockStatement, "Statement.executeQuery", "SELECT 1");
    verify(pluginManagerService, times(1)).setInTransaction(true);

    execute(conn, "Connection.commit");
    verify(pluginManagerService, times(2)).setInTransaction(true);

    // The plugin never asks the connection about autocommit to work out boundaries itself.
    verify(conn, never()).getAutoCommit();
  }

  @Test
  void testExecute_setAutoCommitStatementStillSyncsDriver_whenDriverReportsState() throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(mockTargetDriverDialect.isInTransaction(conn)).thenReturn(true);

    execute(mockStatement, "Statement.execute", "SET autocommit = 0");

    // The driver's own view of autocommit is still kept in step with the SQL.
    verify(conn, times(1)).setAutoCommit(false);
  }

  @Test
  void testExecute_recordedDriverStateSurvivesConnectionBreak() throws SQLException {
    // The driver reports a transaction after the INSERT. The next call breaks the connection: the
    // call throws, the driver stops reporting, and nothing is recorded, so the state the service
    // falls back to is the one from the last completed call, in a transaction.
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(mockTargetDriverDialect.isInTransaction(conn)).thenReturn(true);

    execute(mockStatement, "Statement.executeUpdate", "INSERT INTO t VALUES (1)");
    verify(pluginManagerService, times(1)).setInTransaction(true);

    when(mockTargetDriverDialect.isInTransaction(conn)).thenReturn(null);
    when(mockSqlFunction.call()).thenThrow(new SQLException("broken", "08006"));
    assertThrows(SQLException.class,
        () -> execute(mockStatement, "Statement.executeUpdate", "INSERT INTO t VALUES (2)"));

    verify(pluginManagerService, never()).setInTransaction(false);
  }

  @Test
  void testExecute_closedConnectionReportsNothing_fallsBackToTracking() throws SQLException {
    // A driver that reports state for an open connection reports nothing once it is closed; the
    // plugin then tracks again, which only matters for the call that closes it.
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(mockTargetDriverDialect.isInTransaction(conn)).thenReturn(true, true, (Boolean) null);

    execute(mockStatement, "Statement.execute", "BEGIN");
    verify(pluginManagerService, times(1)).setInTransaction(true);

    execute(conn, "Connection.close");
    verify(pluginManagerService, times(1)).setInTransaction(false);
  }

  private void execute(final Object methodInvokeOn, final String methodName, final Object... args)
      throws SQLException {
    plugin.execute(Void.class, SQLException.class, methodInvokeOn, methodName, mockSqlFunction, args);
  }

  @Test
  void testConnect() throws SQLException {
    plugin.connect("anyProtocol", mockHostSpec, new Properties(), true, mockConnectFunction);
    verify(connectionProvider, atLeastOnce()).connect(anyString(), any(), any(), any(), any());
    verify(mockConnectionProviderManager, atLeastOnce()).initConnection(any(), anyString(), any(), any());
  }

  @Test
  void testGetHostSpecByStrategy_nullRole_delegatesToProvider() throws SQLException {
    // A null role means "any role is acceptable" (e.g. reader-or-writer failover, or
    // load balancing that includes the writer). It must be forwarded to the selector.
    final List<HostSpec> hosts = Collections.singletonList(mockHostSpec);
    when(mockConnectionProviderManager.getHostSpecByStrategy(any(), isNull(), eq("random"), any()))
        .thenReturn(mockHostSpec);

    final HostSpec result = plugin.getHostSpecByStrategy(hosts, null, "random");

    assertEquals(mockHostSpec, result);
    verify(mockConnectionProviderManager).getHostSpecByStrategy(any(), isNull(), eq("random"), any());
  }

  @Test
  void testGetHostSpecByStrategy_unknownRole_returnsNullWithoutDelegating() throws SQLException {
    final List<HostSpec> hosts = Collections.singletonList(mockHostSpec);

    final HostSpec result = plugin.getHostSpecByStrategy(hosts, HostRole.UNKNOWN, "random");

    assertNull(result);
    verify(mockConnectionProviderManager, never()).getHostSpecByStrategy(any(), any(), anyString(), any());
  }

  @Test
  void testAcceptsStrategy_nullRole_delegatesToProvider() {
    when(mockConnectionProviderManager.acceptsStrategy(isNull(), eq("random"))).thenReturn(true);

    assertTrue(plugin.acceptsStrategy(null, "random"));
    verify(mockConnectionProviderManager).acceptsStrategy(isNull(), eq("random"));
  }

  @Test
  void testAcceptsStrategy_unknownRole_returnsFalseWithoutDelegating() {
    assertFalse(plugin.acceptsStrategy(HostRole.UNKNOWN, "random"));
    verify(mockConnectionProviderManager, never()).acceptsStrategy(any(), anyString());
  }

}
