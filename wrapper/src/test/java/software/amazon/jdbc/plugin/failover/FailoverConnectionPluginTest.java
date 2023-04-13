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
import java.util.ArrayList;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostAvailability;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.DynamicHostListProvider;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.SqlState;

class FailoverConnectionPluginTest {

  private static final Class<Connection> MONITOR_METHOD_INVOKE_ON = Connection.class;
  private static final String MONITOR_METHOD_NAME = "Connection.executeQuery";
  private static final Object[] EMPTY_ARGS = {};

  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock HostSpec mockHostSpec;
  @Mock HostListProviderService mockHostListProviderService;
  @Mock AuroraHostListProvider mockHostListProvider;
  @Mock JdbcCallable<Void, SQLException> mockInitHostProviderFunc;
  @Mock ClusterAwareReaderFailoverHandler mockReaderFailoverHandler;
  @Mock ClusterAwareWriterFailoverHandler mockWriterFailoverHandler;
  @Mock ReaderFailoverResult mockReaderResult;
  @Mock WriterFailoverResult mockWriterResult;
  @Captor ArgumentCaptor<HostSpec> hostSpecArgumentCaptor;
  @Mock JdbcCallable<ResultSet, SQLException> mockSqlFunction;

  private Properties properties = new Properties();
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
    when(mockReaderFailoverHandler.failover(any(), any())).thenReturn(mockReaderResult);
    when(mockWriterFailoverHandler.failover(any())).thenReturn(mockWriterResult);

    properties.clear();
  }

  @Test
  void test_initHostProvider_withFailoverDisabled() throws SQLException {
    properties.setProperty(FailoverConnectionPlugin.ENABLE_CLUSTER_AWARE_FAILOVER.name, "false");
    initializePlugin();

    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    verify(mockHostListProviderService, never()).isStaticHostListProvider();
  }

  @Test
  void test_initHostProvider_withStaticHostListProvider() throws SQLException {
    when(mockHostListProviderService.isStaticHostListProvider()).thenReturn(true);

    initializePlugin();

    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    verify(mockHostListProviderService).isStaticHostListProvider();
    verify(mockHostListProviderService).setHostListProvider(eq(mockHostListProvider));
  }

  @Test
  void test_initHostProvider_withDynamicHostListProvider() throws SQLException {
    when(mockHostListProviderService.isStaticHostListProvider()).thenReturn(false);
    when(mockPluginService.getHostListProvider()).thenReturn(new FooHostListProvider());

    initializePlugin();

    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    verify(mockHostListProviderService, atLeastOnce()).isStaticHostListProvider();
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
    when(mockHostListProvider.getRdsUrlType()).thenReturn(RdsUrlType.RDS_PROXY);
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

    when(mockPluginService.getHosts()).thenReturn(Collections.singletonList(new HostSpec("host")));
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
  void test_syncSessionState_withNullConnections() throws SQLException {
    initializePlugin();

    plugin.transferSessionState(null, mockConnection, false);
    verify(mockConnection, never()).getAutoCommit();

    plugin.transferSessionState(mockConnection, null, false);
    verify(mockConnection, never()).getAutoCommit();
  }

  @Test
  void test_syncSessionState() throws SQLException {
    final Connection target = mockConnection;
    final Connection source = mockConnection;

    when(target.getAutoCommit()).thenReturn(false);
    when(target.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_NONE);

    initializePlugin();

    plugin.transferSessionState(mockConnection, mockConnection, false);
    verify(target).setReadOnly(eq(false));
    verify(target).getAutoCommit();
    verify(target).getTransactionIsolation();
    verify(source).setAutoCommit(eq(false));
    verify(source).setTransactionIsolation(eq(Connection.TRANSACTION_NONE));
  }

  @Test
  void test_failover_failoverReader() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(true);

    initializePlugin();
    final FailoverConnectionPlugin spyPlugin = spy(plugin);
    doNothing().when(spyPlugin).failoverWriter();
    when(spyPlugin.shouldPerformWriterFailover()).thenReturn(true);

    SQLException exception = assertThrows(SQLException.class, () -> spyPlugin.failover(mockHostSpec));
    assertEquals(SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), exception.getSQLState());
    verify(spyPlugin).failoverWriter();
  }

  @Test
  void test_failover_failoverWriter() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(false);

    initializePlugin();
    final FailoverConnectionPlugin spyPlugin = spy(plugin);
    doNothing().when(spyPlugin).failoverReader(eq(mockHostSpec));
    when(spyPlugin.shouldPerformWriterFailover()).thenReturn(false);

    SQLException exception = assertThrows(SQLException.class, () -> spyPlugin.failover(mockHostSpec));
    assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());
    verify(spyPlugin).failoverReader(eq(mockHostSpec));
  }

  @Test
  void test_failoverReader_withValidFailedHostSpec_successFailover() throws SQLException {
    final HostSpec hostSpec = new HostSpec("hostA");
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockHostSpec.getAvailability()).thenReturn(HostAvailability.AVAILABLE);
    when(mockPluginService.getHosts()).thenReturn(hosts);
    when(mockReaderResult.isConnected()).thenReturn(true);
    when(mockReaderResult.getConnection()).thenReturn(mockConnection);
    when(mockReaderResult.getHost()).thenReturn(hostSpec);

    initializePlugin();
    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    final FailoverConnectionPlugin spyPlugin = spy(plugin);
    doNothing().when(spyPlugin).updateTopology(true);

    spyPlugin.failoverReader(mockHostSpec);

    verify(mockReaderFailoverHandler).failover(eq(hosts), eq(mockHostSpec));
    verify(mockPluginService).setCurrentConnection(eq(mockConnection), eq(hostSpec));
  }

  @Test
  void test_failoverReader_withVNoFailedHostSpec_withException() throws SQLException {
    final HostSpec hostSpec = new HostSpec("hostA");
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockHostSpec.getAvailability()).thenReturn(HostAvailability.AVAILABLE);
    when(mockPluginService.getHosts()).thenReturn(hosts);
    when(mockReaderResult.getException()).thenReturn(new SQLException());
    when(mockReaderResult.getHost()).thenReturn(hostSpec);

    initializePlugin();
    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    assertThrows(SQLException.class, () -> plugin.failoverReader(null));
    verify(mockReaderFailoverHandler).failover(eq(hosts), eq(null));
  }

  @Test
  void test_failoverWriter_failedFailover_throwsException() throws SQLException {
    final HostSpec hostSpec = new HostSpec("hostA");
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockPluginService.getHosts()).thenReturn(hosts);
    when(mockWriterResult.getException()).thenReturn(new SQLException());

    initializePlugin();
    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    assertThrows(SQLException.class, () -> plugin.failoverWriter());
    verify(mockWriterFailoverHandler).failover(eq(hosts));
  }

  @Test
  void test_failoverWriter_failedFailover_withNoResult() throws SQLException {
    final HostSpec hostSpec = new HostSpec("hostA");
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockPluginService.getHosts()).thenReturn(hosts);
    when(mockWriterResult.isConnected()).thenReturn(false);

    initializePlugin();
    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
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
    final HostSpec hostSpec = new HostSpec("hostA");
    final List<HostSpec> hosts = Collections.singletonList(hostSpec);

    when(mockHostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("alias1", "alias2")));
    when(mockPluginService.getHosts()).thenReturn(hosts);

    initializePlugin();
    plugin.initHostProvider(
        "initialUrl",
        mockHostListProviderService,
        mockInitHostProviderFunc,
        () -> mockHostListProvider,
        () -> mockReaderFailoverHandler,
        () -> mockWriterFailoverHandler);

    final SQLException exception = assertThrows(SQLException.class, () -> plugin.failoverWriter());
    assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), exception.getSQLState());

    verify(mockWriterFailoverHandler).failover(eq(hosts));
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

    final HostSpec expectedHostSpec = new HostSpec("host", 123, HostRole.READER, HostAvailability.NOT_AVAILABLE);

    initializePlugin();
    plugin.invalidateCurrentConnection();
    verify(mockConnection).rollback();

    // Assert SQL exceptions thrown during rollback do not get propagated.
    doThrow(new SQLException()).when(mockConnection).rollback();
    assertDoesNotThrow(() -> plugin.invalidateCurrentConnection());

    verify(mockPluginService, times(2)).setCurrentConnection(eq(mockConnection), hostSpecArgumentCaptor.capture());
    assertEquals(expectedHostSpec, hostSpecArgumentCaptor.getValue());
  }

  @Test
  void test_invalidateCurrentConnection_notInTransaction() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockHostSpec.getHost()).thenReturn("host");
    when(mockHostSpec.getPort()).thenReturn(123);
    when(mockHostSpec.getRole()).thenReturn(HostRole.READER);
    final HostSpec expectedHostSpec = new HostSpec("host", 123, HostRole.READER, HostAvailability.NOT_AVAILABLE);

    initializePlugin();
    plugin.invalidateCurrentConnection();

    verify(mockPluginService).isInTransaction();
    verify(mockPluginService).setCurrentConnection(eq(mockConnection), hostSpecArgumentCaptor.capture());
    assertEquals(expectedHostSpec, hostSpecArgumentCaptor.getValue());
  }

  @Test
  void test_invalidateCurrentConnection_withOpenConnection() throws SQLException {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.isClosed()).thenReturn(false);
    when(mockHostSpec.getHost()).thenReturn("host");
    when(mockHostSpec.getPort()).thenReturn(123);
    when(mockHostSpec.getRole()).thenReturn(HostRole.READER);
    final HostSpec expectedHostSpec = new HostSpec("host", 123, HostRole.READER, HostAvailability.NOT_AVAILABLE);

    initializePlugin();
    plugin.invalidateCurrentConnection();

    doThrow(new SQLException()).when(mockConnection).close();
    assertDoesNotThrow(() -> plugin.invalidateCurrentConnection());

    verify(mockConnection, times(2)).isClosed();
    verify(mockConnection, times(2)).close();
    verify(mockPluginService, times(2)).setCurrentConnection(eq(mockConnection), hostSpecArgumentCaptor.capture());
    assertEquals(expectedHostSpec, hostSpecArgumentCaptor.getValue());
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
  }

  private static class FooHostListProvider implements HostListProvider, DynamicHostListProvider {

    @Override
    public List<HostSpec> refresh() {
      return new ArrayList<>();
    }

    @Override
    public List<HostSpec> refresh(Connection connection) {
      return new ArrayList<>();
    }

    @Override
    public List<HostSpec> forceRefresh() {
      return new ArrayList<>();
    }

    @Override
    public List<HostSpec> forceRefresh(Connection connection) {
      return new ArrayList<>();
    }

    @Override
    public HostRole getHostRole(Connection conn) {
      return HostRole.WRITER;
    }
  }
}
