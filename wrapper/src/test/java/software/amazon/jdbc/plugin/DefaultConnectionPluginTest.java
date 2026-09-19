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
import static org.junit.jupiter.api.Assertions.assertSame;
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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionInfo;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect.AuthorizationStateImpact;
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
  @Mock SessionStateService mockSessionStateService;


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
    when(pluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(pluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.isAuthorizationStateTrackingEnabled())
        .thenReturn(true);

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
  void testExecute_doesNotInspectAuthorizationStateForResultSetAccess() throws SQLException {
    when(this.pluginService.getCurrentConnection()).thenReturn(conn);
    final ResultSet resultSet = mock(ResultSet.class);

    plugin.execute(
        Void.class,
        SQLException.class,
        resultSet,
        "ResultSet.getString",
        mockSqlFunction,
        new Object[] {1});

    verify(mockTargetDriverDialect, never()).supportsAuthorizationSessionState();
    verify(mockSessionStateService, never()).refreshAuthorizationState();
    verify(mockSessionStateService, never()).markAuthorizationStateUnknown();
    verify(mockSessionStateService, never()).markAuthorizationStateUntracked();
  }

  @Test
  void testExecute_refreshesAuthorizationStateAfterSetRole() throws SQLException {
    final Statement statement = mock(Statement.class);
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(true);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockTargetDriverDialect.getAuthorizationStateImpact("SET ROLE tenant_a"))
        .thenReturn(AuthorizationStateImpact.TRACKED);

    plugin.execute(
        Void.class,
        SQLException.class,
        statement,
        "Statement.execute",
        mockSqlFunction,
        new Object[] {"SET ROLE tenant_a"});

    verify(mockSessionStateService).refreshAuthorizationState();
    verify(mockSessionStateService, never()).markAuthorizationStateUnknown();
  }

  @Test
  void testExecute_getsSqlFromPreparedStatement() throws SQLException {
    final PreparedStatement preparedStatement = mock(PreparedStatement.class);
    final String sql = "SET ROLE tenant_a";
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(true);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockTargetDriverDialect.getSQLQueryString(preparedStatement)).thenReturn(sql);
    when(mockTargetDriverDialect.getAuthorizationStateImpact(sql))
        .thenReturn(AuthorizationStateImpact.TRACKED);

    plugin.execute(
        Void.class,
        SQLException.class,
        preparedStatement,
        "PreparedStatement.execute",
        mockSqlFunction,
        new Object[] {});

    verify(mockTargetDriverDialect).getSQLQueryString(preparedStatement);
    verify(mockTargetDriverDialect).getAuthorizationStateImpact(sql);
    verify(mockSessionStateService).refreshAuthorizationState();
    verify(mockSessionStateService, never()).markAuthorizationStateUnknown();
  }

  @Test
  void testExecute_invalidatesAuthorizationStateInsideTransaction() throws SQLException {
    final Statement statement = mock(Statement.class);
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(false);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockTargetDriverDialect.getAuthorizationStateImpact(
        "SET LOCAL search_path TO tenant_a, public"))
        .thenReturn(AuthorizationStateImpact.TRACKED);

    plugin.execute(
        Void.class,
        SQLException.class,
        statement,
        "Statement.execute",
        mockSqlFunction,
        new Object[] {"SET LOCAL search_path TO tenant_a, public"});

    verify(mockSessionStateService).markAuthorizationStateUnknown();
    verify(mockSessionStateService, never()).refreshAuthorizationState();
  }

  @Test
  void testExecute_invalidatesAuthorizationStateAfterCommit() throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);

    plugin.execute(
        Void.class,
        SQLException.class,
        conn,
        "Connection.commit",
        mockSqlFunction,
        new Object[] {});

    verify(mockSessionStateService).markAuthorizationStateUnknown();
    verify(mockSessionStateService, never()).refreshAuthorizationState();
  }

  @Test
  void testExecute_refreshesAuthorizationStateAfterCommitWhenAutoCommitIsEnabled()
      throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(true);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);

    plugin.execute(
        Void.class,
        SQLException.class,
        conn,
        "Connection.commit",
        mockSqlFunction,
        new Object[] {});

    verify(mockSessionStateService).refreshAuthorizationState();
    verify(mockSessionStateService, never()).markAuthorizationStateUnknown();
  }

  @ParameterizedTest
  @ValueSource(strings = {"Connection.setCatalog", "Connection.setSchema"})
  void testExecute_refreshesAuthorizationStateAfterDatabaseContextSetter(
      final String methodName) throws SQLException {
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(true);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);

    plugin.execute(
        Void.class,
        SQLException.class,
        conn,
        methodName,
        mockSqlFunction,
        new Object[] {"tenant_a"});

    verify(mockSqlFunction).call();
    verify(mockSessionStateService).refreshAuthorizationState();
  }

  @Test
  void testExecute_doesNotTrackAuthorizationStateWhenTrackingIsDisabled() throws SQLException {
    final Statement statement = mock(Statement.class);
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(conn.getAutoCommit()).thenReturn(true);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockSessionStateService.isAuthorizationStateTrackingEnabled()).thenReturn(false);

    plugin.execute(
        Void.class,
        SQLException.class,
        statement,
        "Statement.execute",
        mockSqlFunction,
        new Object[] {"SET ROLE tenant_a"});

    verify(mockTargetDriverDialect, never()).getAuthorizationStateImpact(anyString());
    verify(mockSessionStateService, never()).refreshAuthorizationState();
    verify(mockSessionStateService, never()).markAuthorizationStateUnknown();
    verify(mockSessionStateService, never()).markAuthorizationStateUntracked();
  }

  @Test
  void testExecute_marksCustomAuthorizationStateUntrackedBeforeInitialization()
      throws SQLException {
    final Statement statement = mock(Statement.class);
    final String sql = "SET app.tenant_id = 'tenant-a'";
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockTargetDriverDialect.getAuthorizationStateImpact(sql))
        .thenReturn(AuthorizationStateImpact.UNTRACKED);

    plugin.execute(
        Void.class,
        SQLException.class,
        statement,
        "Statement.execute",
        mockSqlFunction,
        new Object[] {sql});

    verify(mockSessionStateService).markAuthorizationStateUntracked();
    verify(mockSessionStateService, never()).getAuthorizationState();
    verify(mockSessionStateService, never()).refreshAuthorizationState();
  }

  @Test
  void testExecute_marksAuthorizationStateUntrackedBeforeFailedBatch() throws SQLException {
    final Statement statement = mock(Statement.class);
    final BatchUpdateException expectedException = new BatchUpdateException();
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockSqlFunction.call()).thenThrow(expectedException);

    final BatchUpdateException actualException = assertThrows(
        BatchUpdateException.class,
        () -> plugin.execute(
            Void.class,
            SQLException.class,
            statement,
            JdbcMethod.STATEMENT_EXECUTEBATCH.methodName,
            mockSqlFunction,
            new Object[] {}));

    assertSame(expectedException, actualException);
    verify(mockSessionStateService).markAuthorizationStateUntracked();
    verify(mockTargetDriverDialect, never()).getAuthorizationStateImpact(anyString());
    verify(mockSessionStateService, never()).refreshAuthorizationState();
    verify(mockSessionStateService, never()).markAuthorizationStateUnknown();
  }

  @Test
  void testExecute_marksAuthorizationStateUnknownAfterFailedStateChange() throws SQLException {
    final Statement statement = mock(Statement.class);
    final String sql = "USE tenant_b; SELECT * FROM missing_table";
    final SQLException expectedException = new SQLException();
    when(pluginService.getCurrentConnection()).thenReturn(conn);
    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockTargetDriverDialect.getAuthorizationStateImpact(sql))
        .thenReturn(AuthorizationStateImpact.TRACKED);
    when(mockSqlFunction.call()).thenThrow(expectedException);

    final SQLException actualException = assertThrows(
        SQLException.class,
        () -> plugin.execute(
            Void.class,
            SQLException.class,
            statement,
            JdbcMethod.STATEMENT_EXECUTE.methodName,
            mockSqlFunction,
            new Object[] {sql}));

    assertSame(expectedException, actualException);
    verify(mockSessionStateService).markAuthorizationStateUnknown();
    verify(mockSessionStateService, never()).markAuthorizationStateUntracked();
    verify(mockSessionStateService, never()).refreshAuthorizationState();
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
