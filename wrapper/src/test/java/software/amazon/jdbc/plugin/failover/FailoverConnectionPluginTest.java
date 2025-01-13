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

package software.amazon.jdbc.plugin.failover;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

class FailoverConnectionPluginTest {

  private static final Class<Connection> MONITOR_METHOD_INVOKE_ON = Connection.class;
  private static final String MONITOR_METHOD_NAME = "Connection.executeQuery";
  private static final Object[] EMPTY_ARGS = {};
  private final List<HostSpec> defaultHosts = Arrays.asList(
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("writer").port(1234).role(HostRole.WRITER).build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("reader1").port(1234).role(HostRole.READER).build());

  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock HostSpec mockHostSpec;
  @Mock HostListProviderService mockHostListProviderService;
  @Mock AuroraHostListProvider mockHostListProvider;
  @Mock JdbcCallable<Void, SQLException> mockInitHostProviderFunc;
  @Mock ReaderFailoverHandler mockReaderFailoverHandler;
  @Mock WriterFailoverHandler mockWriterFailoverHandler;
  @Mock ReaderFailoverResult mockReaderResult;
  @Mock WriterFailoverResult mockWriterResult;
  @Mock JdbcCallable<ResultSet, SQLException> mockSqlFunction;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;

  private final Properties properties = new Properties();
  private FailoverConnectionPlugin plugin;
  private AutoCloseable closeable;

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockPluginService.getHostListProvider()).thenReturn(mockHostListProvider);
    when(mockHostListProvider.getRdsUrlType()).thenReturn(RdsUrlType.RDS_WRITER_CLUSTER);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(mockHostSpec);
    when(mockPluginService.connect(any(HostSpec.class), eq(properties))).thenReturn(mockConnection);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockPluginService.getHosts()).thenReturn(defaultHosts);
    when(mockPluginService.getAllHosts()).thenReturn(defaultHosts);
    when(mockReaderFailoverHandler.failover(any(), any())).thenReturn(mockReaderResult);
    when(mockWriterFailoverHandler.failover(any())).thenReturn(mockWriterResult);
    when(mockWriterResult.isConnected()).thenReturn(true);
    when(mockWriterResult.getTopology()).thenReturn(defaultHosts);
    when(mockReaderResult.isConnected()).thenReturn(true);

    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    // noinspection unchecked
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);

    properties.clear();
  }

  @Test
  void test_notifyNodeListChanged_withFailoverDisabled() {
    properties.setProperty(FailoverConnectionPlugin.ENABLE_CLUSTER_AWARE_FAILOVER.name, "false");
    final Map<String, EnumSet<NodeChangeOptions>> changes = new HashMap<>();

    initializePlugin();
    plugin.notifyNodeListChanged(changes);

    verify(mockPluginService, never()).getCurrentHostSpec();
    verify(mockHostSpec, never()).getAliases();
  }

  @Test
  void test_notifyNodeListChanged_withValidConnectionNotInTopology() {
    final Map<String, EnumSet<NodeChangeOptions>> changes = new HashMap<>();
    changes.put("cluster-host/", EnumSet.of(NodeChangeOptions.NODE_DELETED));
    changes.put("instance/", EnumSet.of(NodeChangeOptions.NODE_ADDED));

    initializePlugin();
    plugin.notifyNodeListChanged(changes);

    when(mockHostSpec.getUrl()).thenReturn("cluster-url/");
    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Collections.singletonList("instance")));

    verify(mockPluginService).getCurrentHostSpec();
    verify(mockHostSpec, never()).getAliases();
  }

  @Test
  void test_updateTopology() throws SQLException {
    initializePlugin();

    // Test updateTopology with failover disabled
    plugin.setRdsUrlType(RdsUrlType.RDS_PROXY);
    plugin.updateTopology(false);
    verify(mockPluginService, never()).forceRefreshHostList();
    verify(mockPluginService, never()).refreshHostList();

    // Test updateTopology with no connection
    when(mockPluginService.getCurrentHostSpec()).thenReturn(null);
    plugin.updateTopology(false);
    verify(mockPluginService, never()).forceRefreshHostList();
    verify(mockPluginService, never()).refreshHostList();

    // Test updateTopology with closed connection
    when(mockConnection.isClosed()).thenReturn(true);
    plugin.updateTopology(false);
    verify(mockPluginService, never()).forceRefreshHostList();
    verify(mockPluginService, never()).refreshHostList();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void test_updateTopology_withForceUpdate(final boolean forceUpdate) throws SQLException {

    when(mockPluginService.getAllHosts()).thenReturn(Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host").build()));
    when(mockPluginService.getHosts()).thenReturn(Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host").build()));
    when(mockConnection.isClosed()).thenReturn(false);
    initializePlugin();
    plugin.setRdsUrlType(RdsUrlType.RDS_INSTANCE);

    plugin.updateTopology(forceUpdate);
    if (forceUpdate) {
      verify(mockPluginService, atLeastOnce()).forceRefreshHostList();
    } else {
      verify(mockPluginService, atLeastOnce()).refreshHostList();
    }
  }

  @Test
  void test_failover_failoverWriter() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(true);

    initializePlugin();
    final FailoverConnectionPlugin spyPlugin = spy(plugin);
    doThrow(FailoverSuccessSQLException.class).when(spyPlugin).failoverWriter();
    spyPlugin.failoverMode = FailoverMode.STRICT_WRITER;

    assertThrows(FailoverSuccessSQLException.class, () -> spyPlugin.failover(mockHostSpec));
    verify(spyPlugin).failoverWriter();
  }

  @Test
  void test_failover_failoverReader() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(false);

    initializePlugin();
    final FailoverConnectionPlugin spyPlugin = spy(plugin);
    doThrow(FailoverSuccessSQLException.class).when(spyPlugin).failoverReader(eq(mockHostSpec));
    spyPlugin.failoverMode = FailoverMode.READER_OR_WRITER;

    assertThrows(FailoverSuccessSQLException.class, () -> spyPlugin.failover(mockHostSpec));
    verify(spyPlugin).failoverReader(eq(mockHostSpec));
  }

  @Test
  void test_failoverReader_withValidFailedHostSpec_successFailover() throws SQLException {
    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockHostSpec.getRawAvailability()).thenReturn(HostAvailability.AVAILABLE);
    when(mockReaderResult.isConnected()).thenReturn(true);
    when(mockReaderResult.getConnection()).thenReturn(mockConnection);
    when(mockReaderResult.getHost()).thenReturn(defaultHosts.get(1));

    initializePlugin();
    plugin.initHostProvider(
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    final FailoverConnectionPlugin spyPlugin = spy(plugin);
    doNothing().when(spyPlugin).updateTopology(true);

    assertThrows(FailoverSuccessSQLException.class, () -> spyPlugin.failoverReader(mockHostSpec));

    verify(mockReaderFailoverHandler).failover(eq(defaultHosts), eq(mockHostSpec));
    verify(mockPluginService).setCurrentConnection(eq(mockConnection), eq(defaultHosts.get(1)));
  }

  @Test
  void test_failoverReader_withNoFailedHostSpec_withException() throws SQLException {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA")
        .build();
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockHostSpec.getAvailability()).thenReturn(HostAvailability.AVAILABLE);
    when(mockPluginService.getAllHosts()).thenReturn(hosts);
    when(mockPluginService.getHosts()).thenReturn(hosts);
    when(mockReaderResult.getException()).thenReturn(new SQLException());
    when(mockReaderResult.getHost()).thenReturn(hostSpec);

    initializePlugin();
    plugin.initHostProvider(
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    assertThrows(SQLException.class, () -> plugin.failoverReader(null));
    verify(mockReaderFailoverHandler).failover(eq(hosts), eq(null));
  }

  @Test
  void test_failoverWriter_failedFailover_throwsException() throws SQLException {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA")
        .build();
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockPluginService.getAllHosts()).thenReturn(hosts);
    when(mockPluginService.getHosts()).thenReturn(hosts);
    when(mockWriterResult.getException()).thenReturn(new SQLException());

    initializePlugin();
    plugin.initHostProvider(
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    assertThrows(SQLException.class, () -> plugin.failoverWriter());
    verify(mockWriterFailoverHandler).failover(eq(hosts));
  }

  @Test
  void test_failoverWriter_failedFailover_withNoResult() throws SQLException {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA")
        .build();
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockPluginService.getAllHosts()).thenReturn(hosts);
    when(mockPluginService.getHosts()).thenReturn(hosts);
    when(mockWriterResult.isConnected()).thenReturn(false);

    initializePlugin();
    plugin.initHostProvider(
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    final SQLException exception = assertThrows(SQLException.class, () -> plugin.failoverWriter());
    assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());

    verify(mockWriterFailoverHandler).failover(eq(hosts));
    verify(mockWriterResult, never()).getNewConnection();
    verify(mockWriterResult, never()).getTopology();
  }

  @Test
  void test_failoverWriter_successFailover() throws SQLException {
    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));

    initializePlugin();
    plugin.initHostProvider(
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    final SQLException exception = assertThrows(FailoverSuccessSQLException.class, () -> plugin.failoverWriter());
    assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());

    verify(mockWriterFailoverHandler).failover(eq(defaultHosts));
  }

  @Test
  void test_invalidCurrentConnection_withNoConnection() {
    when(mockPluginService.getCurrentConnection()).thenReturn(null);
    initializePlugin();
    plugin.invalidateCurrentConnection();

    verify(mockPluginService, never()).getCurrentHostSpec();
  }

  @Test
  void test_invalidateCurrentConnection_inTransaction() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockHostSpec.getHost()).thenReturn("host");
    when(mockHostSpec.getPort()).thenReturn(123);
    when(mockHostSpec.getRole()).thenReturn(HostRole.READER);

    initializePlugin();
    plugin.invalidateCurrentConnection();
    verify(mockConnection).rollback();

    // Assert SQL exceptions thrown during rollback do not get propagated.
    doThrow(new SQLException()).when(mockConnection).rollback();
    assertDoesNotThrow(() -> plugin.invalidateCurrentConnection());
  }

  @Test
  void test_invalidateCurrentConnection_notInTransaction() {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockHostSpec.getHost()).thenReturn("host");
    when(mockHostSpec.getPort()).thenReturn(123);
    when(mockHostSpec.getRole()).thenReturn(HostRole.READER);

    initializePlugin();
    plugin.invalidateCurrentConnection();

    verify(mockPluginService).isInTransaction();
  }

  @Test
  void test_invalidateCurrentConnection_withOpenConnection() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.isClosed()).thenReturn(false);
    when(mockHostSpec.getHost()).thenReturn("host");
    when(mockHostSpec.getPort()).thenReturn(123);
    when(mockHostSpec.getRole()).thenReturn(HostRole.READER);

    initializePlugin();
    plugin.invalidateCurrentConnection();

    doThrow(new SQLException()).when(mockConnection).close();
    assertDoesNotThrow(() -> plugin.invalidateCurrentConnection());

    verify(mockConnection, times(2)).isClosed();
    verify(mockConnection, times(2)).close();
  }

  @Test
  void test_execute_withFailoverDisabled() throws SQLException {
    properties.setProperty(FailoverConnectionPlugin.ENABLE_CLUSTER_AWARE_FAILOVER.name, "false");
    initializePlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        mockSqlFunction,
        EMPTY_ARGS);

    verify(mockSqlFunction).call();
    verify(mockHostListProvider, never()).getRdsUrlType();
  }

  @Test
  void test_execute_withDirectExecute() throws SQLException {
    initializePlugin();
    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        "close",
        mockSqlFunction,
        EMPTY_ARGS);
    verify(mockSqlFunction).call();
    verify(mockHostListProvider, never()).getRdsUrlType();
  }

  private void initializePlugin() {
    plugin = new FailoverConnectionPlugin(mockPluginService, properties);
    plugin.setWriterFailoverHandler(mockWriterFailoverHandler);
    plugin.setReaderFailoverHandler(mockReaderFailoverHandler);
  }
}
