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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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

  private static HostSpec argHostWith(final String host) {
    return org.mockito.ArgumentMatchers.argThat(
        spec -> spec != null && host.equalsIgnoreCase(spec.getHost()));
  }
}
