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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.states.SessionStateServiceImpl;
import software.amazon.jdbc.util.SqlState;

/**
 * Functional tests for the unified read/write splitting plugin, exercised through the concrete
 * {@link ReadWriteSplittingPlugin} (topology-based) and {@link SimpleReadWriteSplittingPlugin}
 * (endpoint-based) subclasses that build real helper assemblies. These reproduce the behavioral
 * coverage of the removed per-plugin unit tests against the unified {@code execute}/{@code connect}
 * path.
 */
public class UnifiedReadWriteSplittingPluginTest {

  private static final String TEST_PROTOCOL = "jdbc:postgresql:";
  private static final int TEST_PORT = 5432;
  private static final String SET_READ_ONLY = JdbcMethod.CONNECTION_SETREADONLY.methodName;
  private static final String CLEAR_WARNINGS = JdbcMethod.CONNECTION_CLEARWARNINGS.methodName;
  private static final String RW_TRANSFER_SESSION_STATE_ON_SWITCH =
      UnifiedReadWriteSplittingPlugin.RW_TRANSFER_SESSION_STATE_ON_SWITCH.name;

  private final Properties defaultProps = new Properties();

  private final HostSpec writerHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-0").port(TEST_PORT).role(HostRole.WRITER).build();
  private final HostSpec readerHostSpec1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-1").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpec2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-2").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpec3 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-3").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec instanceUrlHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("my-instance-name.XYZ.us-east-2.rds.amazonaws.com").port(TEST_PORT).role(HostRole.WRITER).build();

  private final List<HostSpec> defaultHosts = Arrays.asList(
      writerHostSpec, readerHostSpec1, readerHostSpec2, readerHostSpec3);
  private final List<HostSpec> singleReaderTopology = Arrays.asList(writerHostSpec, readerHostSpec1);

  private AutoCloseable closeable;

  @Mock private JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock private JdbcCallable<Void, SQLException> mockVoidFunc;
  @Mock private JdbcCallable<ResultSet, SQLException> mockSqlFunc;
  @Mock private PluginService mockPluginService;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private Connection mockWriterConn;
  @Mock private Connection mockReaderConn1;
  @Mock private Connection mockReaderConn2;
  @Mock private Connection mockClosedWriterConn;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private SessionStateService mockSessionStateService;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    mockDefaultBehavior();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    defaultProps.clear();
  }

  void mockDefaultBehavior() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(this.mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(this.mockPluginService.getAllHosts()).thenReturn(defaultHosts);
    when(this.mockPluginService.getHosts()).thenReturn(defaultHosts);
    when(this.mockPluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq("random")))
        .thenReturn(readerHostSpec1);
    when(this.mockPluginService.acceptsStrategy(any(), eq("random"))).thenReturn(true);
    when(this.mockPluginService.getInitialConnectionHostSpec()).thenReturn(writerHostSpec);
    when(this.mockPluginService.getHostRole(mockWriterConn)).thenReturn(HostRole.WRITER);
    when(this.mockPluginService.getHostRole(mockReaderConn1)).thenReturn(HostRole.READER);
    when(this.mockPluginService.connect(eq(writerHostSpec), any(Properties.class), any()))
        .thenReturn(mockWriterConn);
    when(this.mockPluginService.connect(eq(readerHostSpec1), any(Properties.class), any()))
        .thenReturn(mockReaderConn1);
    when(this.mockPluginService.connect(eq(readerHostSpec2), any(Properties.class), any()))
        .thenReturn(mockReaderConn2);
    when(this.mockConnectFunc.call()).thenReturn(mockWriterConn);
    when(this.mockSqlFunc.call()).thenReturn(mockResultSet);
    when(mockClosedWriterConn.isClosed()).thenReturn(true);
  }

  private ReadWriteSplittingPlugin newTopologyPlugin() {
    return new ReadWriteSplittingPlugin(this.mockPluginService, this.defaultProps);
  }

  private void invokeSetReadOnly(
      final ReadWriteSplittingPlugin plugin, final Object invokeOn, final boolean readOnly)
      throws SQLException {
    plugin.execute(
        Void.class, SQLException.class, invokeOn, SET_READ_ONLY, this.mockVoidFunc,
        new Object[] {readOnly});
  }

  private EnumSet<NodeChangeOptions> noChanges() {
    return EnumSet.noneOf(NodeChangeOptions.class);
  }

  @Test
  public void testSetReadOnlyTrue_switchesToReader() throws SQLException {
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    invokeSetReadOnly(plugin, mockWriterConn, true);

    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn1), not(eq(writerHostSpec)));
    assertEquals(mockReaderConn1, plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnlyFalse_switchesToWriter() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    invokeSetReadOnly(plugin, mockReaderConn1, false);

    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockWriterConn), eq(writerHostSpec));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnlyTrue_alreadyOnReader_noOp() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    invokeSetReadOnly(plugin, mockReaderConn1, true);

    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnlyFalse_alreadyOnWriter_noOp() throws SQLException {
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    invokeSetReadOnly(plugin, mockWriterConn, false);

    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnlyFalse_inTransaction_throws() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);
    when(this.mockPluginService.isInTransaction()).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    final SQLException e = assertThrows(SQLException.class,
        () -> invokeSetReadOnly(plugin, mockReaderConn1, false));
    assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), e.getSQLState());
    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  public void testSetReadOnly_onClosedConnection_throws() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockClosedWriterConn);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    final SQLException e = assertThrows(SQLException.class,
        () -> invokeSetReadOnly(plugin, mockClosedWriterConn, true));
    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), e.getSQLState());
    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnlyTrue_singleHost_staysOnWriter() throws SQLException {
    when(this.mockPluginService.getHosts()).thenReturn(Collections.singletonList(writerHostSpec));

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    invokeSetReadOnly(plugin, mockWriterConn, true);

    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnlyTrue_readerConnectionFails_fallsBackToWriter() throws SQLException {
    when(this.mockPluginService.getHosts()).thenReturn(singleReaderTopology);
    when(this.mockPluginService.connect(eq(readerHostSpec1), any(Properties.class), any()))
        .thenThrow(SQLException.class);
    when(this.mockPluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq("random")))
        .thenAnswer(invocation -> {
          final List<HostSpec> candidates = invocation.getArgument(0);
          return candidates.contains(readerHostSpec1) ? readerHostSpec1 : null;
        });

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    invokeSetReadOnly(plugin, mockWriterConn, true);

    // The failing reader is tried exactly once and the current writer connection is kept.
    verify(this.mockPluginService, times(1)).connect(eq(readerHostSpec1), any(Properties.class), any());
    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnlyTrue_readerLoginFailure_rethrows() throws SQLException {
    when(this.mockPluginService.getHosts()).thenReturn(singleReaderTopology);
    when(this.mockPluginService.connect(eq(readerHostSpec1), any(Properties.class), any()))
        .thenThrow(SQLException.class);
    when(this.mockPluginService.isLoginException(any(Throwable.class), any())).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    invokeSetReadOnly(plugin, mockWriterConn, true);

    // A login failure is a showstopper for the retry loop: only a single connection attempt is
    // made (no other readers are tried), and the usable current writer connection is kept as a
    // fallback rather than propagating the failure.
    verify(this.mockPluginService, times(1)).connect(eq(readerHostSpec1), any(Properties.class), any());
    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testNotifyConnectionChanged_bindsWriter_returnsNoOpinion() {
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    final OldConnectionSuggestedAction action = plugin.notifyConnectionChanged(noChanges());

    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(OldConnectionSuggestedAction.NO_OPINION, action);
  }

  @Test
  public void testExecuteClearWarnings_clearsCachedConnections() throws SQLException {
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    // Bind the writer while the current connection is the writer.
    plugin.notifyConnectionChanged(noChanges());

    // Bind the reader while the current connection is a reader.
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);
    plugin.notifyConnectionChanged(noChanges());

    plugin.execute(
        Void.class, SQLException.class, mockReaderConn1, CLEAR_WARNINGS, this.mockVoidFunc,
        new Object[] {});

    verify(mockWriterConn, times(1)).clearWarnings();
    verify(mockReaderConn1, times(1)).clearWarnings();
  }

  @Test
  public void testExecuteClearWarnings_onClosedConnections_notCalled() throws SQLException {
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockReaderConn1.isClosed()).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();
    plugin.notifyConnectionChanged(noChanges());
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);
    plugin.notifyConnectionChanged(noChanges());

    plugin.execute(
        Void.class, SQLException.class, mockReaderConn1, CLEAR_WARNINGS, this.mockVoidFunc,
        new Object[] {});

    verify(mockWriterConn, never()).clearWarnings();
    verify(mockReaderConn1, never()).clearWarnings();
  }

  @Test
  public void testExecuteClearWarnings_onNullConnections_doesNotThrow() {
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    assertDoesNotThrow(() -> plugin.execute(
        Void.class, SQLException.class, mockWriterConn, CLEAR_WARNINGS, this.mockVoidFunc,
        new Object[] {}));
  }

  @Test
  public void testConnect_nonInitialConnection_passesThrough() throws SQLException {
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    final Connection connection =
        plugin.connect(TEST_PROTOCOL, writerHostSpec, defaultProps, false, this.mockConnectFunc);

    assertEquals(mockWriterConn, connection);
    verify(this.mockConnectFunc).call();
    verify(this.mockHostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  public void testConnect_initialConnection_verifiesAndUpdatesRole() throws SQLException {
    when(this.mockHostListProviderService.isStaticHostListProvider()).thenReturn(false);
    when(this.mockConnectFunc.call()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getHostRole(mockReaderConn1)).thenReturn(HostRole.READER);
    // The topology reports the initial host as a writer, but the opened connection is a reader.
    when(this.mockPluginService.getInitialConnectionHostSpec()).thenReturn(instanceUrlHostSpec);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();
    plugin.initHostProvider(
        TEST_PROTOCOL, "url", defaultProps, this.mockHostListProviderService, () -> null);

    final Connection connection = plugin.connect(
        TEST_PROTOCOL, instanceUrlHostSpec, defaultProps, true, this.mockConnectFunc);

    assertEquals(mockReaderConn1, connection);
    verify(this.mockHostListProviderService, times(1))
        .setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  public void testConnect_initialConnection_unknownRole_throws() throws SQLException {
    when(this.mockHostListProviderService.isStaticHostListProvider()).thenReturn(false);
    when(this.mockConnectFunc.call()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getHostRole(mockReaderConn1)).thenReturn(null);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();
    plugin.initHostProvider(
        TEST_PROTOCOL, "url", defaultProps, this.mockHostListProviderService, () -> null);

    assertThrows(SQLException.class, () -> plugin.connect(
        TEST_PROTOCOL, instanceUrlHostSpec, defaultProps, true, this.mockConnectFunc));
    verify(this.mockHostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  // ---- Simple (endpoint-based) read/write splitting ----

  @Test
  public void testSimplePlugin_missingWriteEndpoint_throwsAtConstruction() {
    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.endpoint");
    assertThrows(RuntimeException.class,
        () -> new SimpleReadWriteSplittingPlugin(this.mockPluginService, props));
  }

  @Test
  public void testSimplePlugin_missingReadEndpoint_throwsAtConstruction() {
    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.endpoint");
    assertThrows(RuntimeException.class,
        () -> new SimpleReadWriteSplittingPlugin(this.mockPluginService, props));
  }

  @Test
  public void testSimplePlugin_setReadOnlyTrue_switchesToReadEndpoint() throws SQLException {
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(TEST_PORT).role(HostRole.WRITER).build();
    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(TEST_PORT).role(HostRole.READER).build();

    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.endpoint");
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.endpoint");
    props.setProperty(SimpleReadWriteSplittingPlugin.VERIFY_NEW_SRW_CONNECTIONS.name, "false");

    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(writeEndpointHost);
    when(this.mockHostListProviderService.getCurrentHostSpec()).thenReturn(writeEndpointHost);
    when(this.mockHostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
    when(this.mockPluginService.connect(
        argHostWith("reader.endpoint"), any(Properties.class), any())).thenReturn(mockReaderConn1);

    final SimpleReadWriteSplittingPlugin plugin =
        new SimpleReadWriteSplittingPlugin(this.mockPluginService, props);
    plugin.initHostProvider(
        TEST_PROTOCOL, "url", props, this.mockHostListProviderService, () -> null);

    plugin.execute(
        Void.class, SQLException.class, mockWriterConn, SET_READ_ONLY, this.mockVoidFunc,
        new Object[] {true});

    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn1), argHostWith("reader.endpoint"));
    assertEquals(mockReaderConn1, plugin.getReaderConnection());
  }

  /**
   * Backs the mocked plugin service with a real {@link SessionStateServiceImpl} and makes
   * {@code setCurrentConnection} behave like {@code PluginServiceImpl}: it updates the current
   * connection and host, then transfers the session state to the new connection.
   * {@code setCurrentConnectionWithoutSessionStateTransfer} does the same, but calls
   * {@code onSwitchWithoutSessionStateTransfer} instead of transferring the session state.
   */
  private SessionStateService useRealSessionStateTransfer(
      final Connection initialConnection, final HostSpec initialHost) throws SQLException {
    final AtomicReference<Connection> currentConnection = new AtomicReference<>(initialConnection);
    final AtomicReference<HostSpec> currentHost = new AtomicReference<>(initialHost);
    final SessionStateService sessionStateService =
        new SessionStateServiceImpl(this.mockPluginService, this.defaultProps);
    when(this.mockPluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(this.mockPluginService.getCurrentConnection()).thenAnswer(invocation -> currentConnection.get());
    when(this.mockPluginService.getCurrentHostSpec()).thenAnswer(invocation -> currentHost.get());
    doAnswer(switchAnswer(sessionStateService, currentConnection, currentHost, true))
        .when(this.mockPluginService).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    doAnswer(switchAnswer(sessionStateService, currentConnection, currentHost, false))
        .when(this.mockPluginService)
        .setCurrentConnectionWithoutSessionStateTransfer(any(Connection.class), any(HostSpec.class));
    return sessionStateService;
  }

  private static Answer<Void> switchAnswer(
      final SessionStateService sessionStateService,
      final AtomicReference<Connection> currentConnection,
      final AtomicReference<HostSpec> currentHost,
      final boolean transferSessionState) {
    return invocation -> {
      final Connection newConnection = invocation.getArgument(0);
      sessionStateService.begin();
      try {
        currentConnection.set(newConnection);
        currentHost.set(invocation.getArgument(1));
        if (transferSessionState) {
          sessionStateService.applyCurrentSessionState(newConnection);
        } else {
          sessionStateService.onSwitchWithoutSessionStateTransfer(newConnection);
        }
      } finally {
        sessionStateService.complete();
      }
      return null;
    };
  }

  /**
   * Calls {@code setReadOnly} through the plugin. The method body only records the value in the
   * session state, as {@code ConnectionWrapper.setReadOnly} does after its physical call, so the
   * physical {@code setReadOnly} calls verified on the connections are the ones the switch itself
   * sent while transferring the session state.
   */
  private void invokeSetReadOnlyTracked(
      final UnifiedReadWriteSplittingPlugin plugin,
      final SessionStateService sessionStateService,
      final boolean readOnly) throws SQLException {
    plugin.execute(
        Void.class, SQLException.class, this.mockPluginService.getCurrentConnection(), SET_READ_ONLY,
        () -> {
          sessionStateService.setReadOnly(readOnly);
          return null;
        },
        new Object[] {readOnly});
  }

  @Test
  public void testSetReadOnlyTriggeredSwitch_transfersRequestedReadOnlyOnly() throws SQLException {
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    when(mockWriterConn.isReadOnly()).thenReturn(false);
    when(mockReaderConn1.isReadOnly()).thenReturn(false);
    sessionStateService.setupPristineReadOnly();
    sessionStateService.setReadOnly(false);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();
    for (int i = 0; i < 3; i++) {
      invokeSetReadOnlyTracked(plugin, sessionStateService, true);
      assertEquals(mockReaderConn1, this.mockPluginService.getCurrentConnection());
      invokeSetReadOnlyTracked(plugin, sessionStateService, false);
      assertEquals(mockWriterConn, this.mockPluginService.getCurrentConnection());
    }

    // Each switch transfers only the value being set: the reader is never set back to read-write
    // and the writer is never set to read-only before the requested value arrives.
    final InOrder inOrder = inOrder(mockReaderConn1, mockWriterConn);
    for (int i = 0; i < 3; i++) {
      inOrder.verify(mockReaderConn1, times(1)).setReadOnly(true);
      inOrder.verify(mockWriterConn, times(1)).setReadOnly(false);
    }
    verify(mockReaderConn1, never()).setReadOnly(false);
    verify(mockWriterConn, never()).setReadOnly(true);

    // The pristine value is read once per physical connection, not on every visit.
    verify(mockReaderConn1, times(1)).isReadOnly();
    verify(mockWriterConn, times(1)).isReadOnly();
    assertEquals(Optional.of(false), sessionStateService.getReadOnly());
  }

  @Test
  public void testAutoSimplePlugin_setReadOnlyTriggeredSwitch_transfersRequestedReadOnlyOnly()
      throws SQLException {
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(TEST_PORT).role(HostRole.WRITER).build();

    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.endpoint");
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.endpoint");
    props.setProperty(SimpleReadWriteSplittingPlugin.VERIFY_NEW_SRW_CONNECTIONS.name, "false");

    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writeEndpointHost);
    when(this.mockHostListProviderService.getCurrentHostSpec()).thenReturn(writeEndpointHost);
    when(this.mockHostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
    when(this.mockPluginService.connect(
        argHostWith("reader.endpoint"), any(Properties.class), any())).thenReturn(mockReaderConn1);
    when(this.mockPluginService.connect(
        argHostWith("writer.endpoint"), any(Properties.class), any())).thenReturn(mockWriterConn);
    sessionStateService.setReadOnly(false);

    final AutoSimpleReadWriteSplittingPlugin plugin =
        new AutoSimpleReadWriteSplittingPlugin(this.mockPluginService, props);
    plugin.initHostProvider(
        TEST_PROTOCOL, "url", props, this.mockHostListProviderService, () -> null);

    for (int i = 0; i < 2; i++) {
      invokeSetReadOnlyTracked(plugin, sessionStateService, true);
      assertEquals(mockReaderConn1, this.mockPluginService.getCurrentConnection());
      invokeSetReadOnlyTracked(plugin, sessionStateService, false);
      assertEquals(mockWriterConn, this.mockPluginService.getCurrentConnection());
    }

    final InOrder inOrder = inOrder(mockReaderConn1, mockWriterConn);
    for (int i = 0; i < 2; i++) {
      inOrder.verify(mockReaderConn1, times(1)).setReadOnly(true);
      inOrder.verify(mockWriterConn, times(1)).setReadOnly(false);
    }
    verify(mockReaderConn1, never()).setReadOnly(false);
    verify(mockWriterConn, never()).setReadOnly(true);
  }

  @Test
  public void testSetReadOnlyTriggeredSwitch_noSwitch_keepsTrackedValue() throws SQLException {
    when(this.mockPluginService.getHosts()).thenReturn(Collections.singletonList(writerHostSpec));
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    sessionStateService.setReadOnly(false);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();
    // The method body is a no-op mock, so the only change to the tracked value could come from
    // the plugin. With no reader to switch to, it must leave the tracked value as it was.
    invokeSetReadOnly(plugin, mockWriterConn, true);

    verify(this.mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertEquals(Optional.of(false), sessionStateService.getReadOnly());
  }

  @Test
  public void testSetReadOnlyFalse_inTransaction_keepsTrackedValue() throws SQLException {
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockReaderConn1, readerHostSpec1);
    when(this.mockPluginService.isInTransaction()).thenReturn(true);
    sessionStateService.setReadOnly(true);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    final SQLException e = assertThrows(SQLException.class,
        () -> invokeSetReadOnly(plugin, mockReaderConn1, false));
    assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), e.getSQLState());
    assertEquals(Optional.of(true), sessionStateService.getReadOnly());
  }

  @Test
  public void testSetReadOnlyFalse_writerSwitchFails_keepsTrackedValue() throws SQLException {
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockReaderConn1, readerHostSpec1);
    when(this.mockPluginService.connect(eq(writerHostSpec), any(Properties.class), any()))
        .thenThrow(SQLException.class);
    sessionStateService.setReadOnly(true);

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();

    assertThrows(SQLException.class, () -> invokeSetReadOnly(plugin, mockReaderConn1, false));
    assertEquals(Optional.of(true), sessionStateService.getReadOnly());
    verify(this.mockVoidFunc, never()).call();
  }

  // ---- rwTransferSessionStateOnSwitch ----

  /** Creates the topology plugin and binds the current (writer) connection as the cached writer. */
  private ReadWriteSplittingPlugin newTopologyPluginWithCachedWriter() {
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin();
    plugin.notifyConnectionChanged(noChanges());
    return plugin;
  }

  @Test
  public void testRwTransferFalse_cachedSwitches_skipSessionStateTransfer() throws SQLException {
    this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();

    invokeSetReadOnlyTracked(plugin, sessionStateService, true);  // opens the reader
    invokeSetReadOnlyTracked(plugin, sessionStateService, false); // cached writer
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);  // cached reader

    assertEquals(mockReaderConn1, this.mockPluginService.getCurrentConnection());
    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn1), any(HostSpec.class));
    verify(this.mockPluginService, never())
        .setCurrentConnection(eq(mockWriterConn), any(HostSpec.class));
    verify(this.mockPluginService, times(1))
        .setCurrentConnectionWithoutSessionStateTransfer(mockWriterConn, writerHostSpec);
    verify(this.mockPluginService, times(1))
        .setCurrentConnectionWithoutSessionStateTransfer(eq(mockReaderConn1), any(HostSpec.class));
  }

  @Test
  public void testRwTransferFalse_autoSimplePlugin_cachedSwitches_skipSessionStateTransfer()
      throws SQLException {
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(TEST_PORT).role(HostRole.WRITER).build();

    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.endpoint");
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.endpoint");
    props.setProperty(SimpleReadWriteSplittingPlugin.VERIFY_NEW_SRW_CONNECTIONS.name, "false");
    props.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");

    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writeEndpointHost);
    when(this.mockHostListProviderService.getCurrentHostSpec()).thenReturn(writeEndpointHost);
    when(this.mockHostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
    when(this.mockPluginService.connect(
        argHostWith("reader.endpoint"), any(Properties.class), any())).thenReturn(mockReaderConn1);
    when(this.mockPluginService.connect(
        argHostWith("writer.endpoint"), any(Properties.class), any())).thenReturn(mockWriterConn);

    final AutoSimpleReadWriteSplittingPlugin plugin =
        new AutoSimpleReadWriteSplittingPlugin(this.mockPluginService, props);
    plugin.initHostProvider(
        TEST_PROTOCOL, "url", props, this.mockHostListProviderService, () -> null);
    plugin.notifyConnectionChanged(noChanges());

    for (int i = 0; i < 2; i++) {
      invokeSetReadOnlyTracked(plugin, sessionStateService, true);
      assertEquals(mockReaderConn1, this.mockPluginService.getCurrentConnection());
      invokeSetReadOnlyTracked(plugin, sessionStateService, false);
      assertEquals(mockWriterConn, this.mockPluginService.getCurrentConnection());
    }

    // Only the first switch to the reader opens a connection and transfers the session state.
    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn1), any(HostSpec.class));
    verify(this.mockPluginService, never())
        .setCurrentConnection(eq(mockWriterConn), any(HostSpec.class));
    verify(this.mockPluginService, times(1))
        .setCurrentConnectionWithoutSessionStateTransfer(eq(mockReaderConn1), any(HostSpec.class));
    verify(this.mockPluginService, times(2))
        .setCurrentConnectionWithoutSessionStateTransfer(eq(mockWriterConn), any(HostSpec.class));
  }

  @Test
  public void testRwTransferFalse_realSessionState_cachedConnectionsNotTouched() throws SQLException {
    this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    when(mockWriterConn.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_REPEATABLE_READ);
    when(mockReaderConn1.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_REPEATABLE_READ);
    when(mockWriterConn.getAutoCommit()).thenReturn(true);
    when(mockReaderConn1.getAutoCommit()).thenReturn(true);
    // What ConnectionWrapper records when the application sets these on the writer.
    sessionStateService.setupPristineTransactionIsolation();
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    sessionStateService.setupPristineAutoCommit();
    sessionStateService.setAutoCommit(true);
    sessionStateService.setupPristineReadOnly(false);
    sessionStateService.setReadOnly(false);

    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();
    for (int i = 0; i < 3; i++) {
      invokeSetReadOnlyTracked(plugin, sessionStateService, true);
      assertEquals(mockReaderConn1, this.mockPluginService.getCurrentConnection());
      invokeSetReadOnlyTracked(plugin, sessionStateService, false);
      assertEquals(mockWriterConn, this.mockPluginService.getCurrentConnection());
    }

    // The reader received the session state once, when the first switch opened it.
    verify(mockReaderConn1, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    verify(mockReaderConn1, times(1)).setAutoCommit(true);
    verify(mockReaderConn1, times(1)).setReadOnly(true);
    verify(mockReaderConn1, times(1)).getTransactionIsolation();
    verify(mockReaderConn1, times(1)).getAutoCommit();
    verify(mockReaderConn1, times(1)).isReadOnly();
    // The writer is never changed or queried by a switch: the only reads are the ones made above,
    // before any switch.
    verify(mockWriterConn, never()).setTransactionIsolation(anyInt());
    verify(mockWriterConn, never()).setAutoCommit(anyBoolean());
    verify(mockWriterConn, never()).setReadOnly(anyBoolean());
    verify(mockWriterConn, times(1)).getTransactionIsolation();
    verify(mockWriterConn, times(1)).getAutoCommit();
    verify(mockWriterConn, never()).isReadOnly();
    // The readOnly value tracked is the one the application set last (fix B still holds).
    assertEquals(Optional.of(false), sessionStateService.getReadOnly());
  }

  @Test
  public void testRwTransferFalse_firstOpenOfReader_transfersFullState() throws SQLException {
    this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    sessionStateService.setAutoCommit(false);
    sessionStateService.setCatalog("app_db");

    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);

    assertEquals(mockReaderConn1, this.mockPluginService.getCurrentConnection());
    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn1), any(HostSpec.class));
    verify(this.mockPluginService, never())
        .setCurrentConnectionWithoutSessionStateTransfer(any(Connection.class), any(HostSpec.class));
    verify(mockReaderConn1, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    verify(mockReaderConn1, times(1)).setAutoCommit(false);
    verify(mockReaderConn1, times(1)).setCatalog("app_db");
  }

  @Test
  public void testRwTransferFalse_closedCachedReader_replacementGetsFullState() throws SQLException {
    this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");
    when(this.mockPluginService.connect(eq(readerHostSpec1), any(Properties.class), any()))
        .thenReturn(mockReaderConn1, mockReaderConn2);
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);
    invokeSetReadOnlyTracked(plugin, sessionStateService, false);
    when(mockReaderConn1.isClosed()).thenReturn(true);
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);

    assertEquals(mockReaderConn2, this.mockPluginService.getCurrentConnection());
    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn2), any(HostSpec.class));
    verify(this.mockPluginService, never())
        .setCurrentConnectionWithoutSessionStateTransfer(eq(mockReaderConn2), any(HostSpec.class));
    verify(mockReaderConn2, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  public void testRwTransferFalse_closedCachedWriter_replacementGetsFullState() throws SQLException {
    this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");
    final Connection newWriterConn = mock(Connection.class);
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(this.mockPluginService.connect(eq(writerHostSpec), any(Properties.class), any()))
        .thenReturn(newWriterConn);
    invokeSetReadOnlyTracked(plugin, sessionStateService, false);

    assertEquals(newWriterConn, this.mockPluginService.getCurrentConnection());
    verify(this.mockPluginService, times(1)).setCurrentConnection(newWriterConn, writerHostSpec);
    verify(this.mockPluginService, never())
        .setCurrentConnectionWithoutSessionStateTransfer(eq(newWriterConn), any(HostSpec.class));
    verify(newWriterConn, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "true"})
  public void testRwTransferDefaultAndTrue_cachedSwitches_transferSessionState(final String value)
      throws SQLException {
    if (!value.isEmpty()) {
      this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, value);
    }
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();
    for (int i = 0; i < 2; i++) {
      invokeSetReadOnlyTracked(plugin, sessionStateService, true);
      invokeSetReadOnlyTracked(plugin, sessionStateService, false);
    }

    verify(this.mockPluginService, times(2))
        .setCurrentConnection(eq(mockReaderConn1), any(HostSpec.class));
    verify(this.mockPluginService, times(2)).setCurrentConnection(mockWriterConn, writerHostSpec);
    verify(this.mockPluginService, never())
        .setCurrentConnectionWithoutSessionStateTransfer(any(Connection.class), any(HostSpec.class));
    verify(mockReaderConn1, times(2)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    verify(mockWriterConn, times(2)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  public void testRwTransferFalse_failedCachedReaderSwitch_fallbackOpensReaderWithFullTransfer()
      throws SQLException {
    this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");
    when(this.mockPluginService.connect(eq(readerHostSpec1), any(Properties.class), any()))
        .thenReturn(mockReaderConn1, mockReaderConn2);
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    doThrow(new SQLException("cached reader switch failed")).when(this.mockPluginService)
        .setCurrentConnectionWithoutSessionStateTransfer(eq(mockReaderConn1), any(HostSpec.class));
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);
    invokeSetReadOnlyTracked(plugin, sessionStateService, false);
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);

    // The failed switch to the cached reader closes it; the new reader gets the full session state.
    assertEquals(mockReaderConn2, this.mockPluginService.getCurrentConnection());
    verify(mockReaderConn1, times(1)).close();
    verify(this.mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn2), any(HostSpec.class));
    verify(mockReaderConn2, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  public void testRwTransferFalse_afterSkippedSwitch_nonSplittingSwitchTransfersTrackedState()
      throws SQLException {
    this.defaultProps.setProperty(RW_TRANSFER_SESSION_STATE_ON_SWITCH, "false");
    final Connection failoverConn = mock(Connection.class);
    final SessionStateService sessionStateService =
        useRealSessionStateTransfer(mockWriterConn, writerHostSpec);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    final ReadWriteSplittingPlugin plugin = newTopologyPluginWithCachedWriter();
    invokeSetReadOnlyTracked(plugin, sessionStateService, true);
    invokeSetReadOnlyTracked(plugin, sessionStateService, false);
    verify(this.mockPluginService, times(1))
        .setCurrentConnectionWithoutSessionStateTransfer(mockWriterConn, writerHostSpec);

    // Tracking continues while switches skip the transfer.
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    sessionStateService.setCatalog("c2");
    sessionStateService.setAutoCommit(false);
    assertEquals(Optional.of(Connection.TRANSACTION_SERIALIZABLE), sessionStateService.getTransactionIsolation());
    assertEquals(Optional.of("c2"), sessionStateService.getCatalog());
    assertEquals(Optional.of(false), sessionStateService.getAutoCommit());

    // A switch outside read/write splitting (as failover makes) still transfers the tracked state.
    this.mockPluginService.setCurrentConnection(failoverConn, readerHostSpec2);

    verify(failoverConn, times(1)).setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    verify(failoverConn, times(1)).setCatalog("c2");
    verify(failoverConn, times(1)).setAutoCommit(false);
    verify(failoverConn, times(1)).setReadOnly(false);
  }

  private static HostSpec argHostWith(final String host) {
    return org.mockito.ArgumentMatchers.argThat(
        spec -> spec != null && host.equalsIgnoreCase(spec.getHost()));
  }
}
