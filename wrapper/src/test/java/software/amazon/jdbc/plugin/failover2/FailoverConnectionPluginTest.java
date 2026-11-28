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

package software.amazon.jdbc.plugin.failover2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverMode;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.ImportantEventService;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class FailoverConnectionPluginTest {

  private static final Object[] EMPTY_ARGS = {};
  private static final String ROLLBACK_METHOD_NAME = "Connection.rollback";
  private static final String UNSUPPORTED_STRATEGY = "roundRobbin";

  private static final HostSpec WRITER = host("writer", HostRole.WRITER);
  private static final HostSpec READER_1 = host("reader-1", HostRole.READER);
  private static final HostSpec READER_2 = host("reader-2", HostRole.READER);

  @Mock FullServicesContainer mockContainer;
  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock JdbcCallable<Void, SQLException> mockSqlFunction;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock TelemetryCounter mockReaderFailoverFailedCounter;
  @Mock ImportantEventService mockImportantEventService;

  private final Properties properties = new Properties();
  private AutoCloseable closeable;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());
    when(mockConnection.isClosed()).thenReturn(true);

    properties.clear();
  }

  @AfterEach
  void cleanUp() throws Exception {
    Thread.interrupted();
    closeable.close();
  }

  @Test
  void test_execute_closedConnection_interruptedThread_skipFailover() throws SQLException {
    properties.setProperty(FailoverConnectionPlugin.SKIP_FAILOVER_ON_INTERRUPTED_THREAD.name, "true");
    final FailoverConnectionPlugin spyPlugin = spy(new FailoverConnectionPlugin(mockContainer, properties));

    Thread.currentThread().interrupt();
    final SQLException exception = assertThrows(SQLException.class, () -> spyPlugin.execute(
        Void.class,
        SQLException.class,
        mockConnection,
        ROLLBACK_METHOD_NAME,
        mockSqlFunction,
        EMPTY_ARGS));

    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), exception.getSQLState());
    assertTrue(Thread.currentThread().isInterrupted());
    verify(spyPlugin, never()).failover();
    verify(mockSqlFunction, never()).call();
  }

  @Test
  void test_execute_closedConnection_interruptedThread_skipFailoverDisabled() throws SQLException {
    final FailoverConnectionPlugin spyPlugin = spy(new FailoverConnectionPlugin(mockContainer, properties));
    doThrow(new FailoverSuccessSQLException()).when(spyPlugin).failover();

    Thread.currentThread().interrupt();
    assertThrows(FailoverSuccessSQLException.class, () -> spyPlugin.execute(
        Void.class,
        SQLException.class,
        mockConnection,
        ROLLBACK_METHOD_NAME,
        mockSqlFunction,
        EMPTY_ARGS));

    verify(spyPlugin).failover();
  }

  @Test
  void test_execute_closedConnection_notInterrupted_startsFailover() throws SQLException {
    properties.setProperty(FailoverConnectionPlugin.SKIP_FAILOVER_ON_INTERRUPTED_THREAD.name, "true");
    final FailoverConnectionPlugin spyPlugin = spy(new FailoverConnectionPlugin(mockContainer, properties));
    doThrow(new FailoverSuccessSQLException()).when(spyPlugin).failover();

    assertThrows(FailoverSuccessSQLException.class, () -> spyPlugin.execute(
        Void.class,
        SQLException.class,
        mockConnection,
        ROLLBACK_METHOD_NAME,
        mockSqlFunction,
        EMPTY_ARGS));

    verify(spyPlugin).failover();
  }

  @ParameterizedTest
  @EnumSource(value = FailoverMode.class, names = {"READER_OR_WRITER", "STRICT_READER"})
  void test_failoverReader_unsupportedStrategy_failsWithoutSelectingAHost(final FailoverMode mode)
      throws SQLException {
    final FailoverConnectionPlugin plugin = initReaderFailover(mode, UNSUPPORTED_STRATEGY);
    when(mockPluginService.acceptsStrategy(HostRole.READER, UNSUPPORTED_STRATEGY)).thenReturn(false);

    final FailoverFailedSQLException exception =
        assertThrows(FailoverFailedSQLException.class, plugin::failoverReader);

    assertTrue(exception.getMessage().contains(UNSUPPORTED_STRATEGY), exception.getMessage());
    verify(mockPluginService, never()).getHostSpecByStrategy(anyList(), any(), anyString());
    verify(mockPluginService, never()).connect(any(HostSpec.class), any(Properties.class), any());
    verify(mockReaderFailoverFailedCounter).inc();
  }

  @Test
  void test_failoverReader_supportedStrategyWithNoAvailableReader_retriesWithAllReaders() throws SQLException {
    final String strategy = "leastConnections";
    final FailoverConnectionPlugin plugin = initReaderFailover(FailoverMode.STRICT_READER, strategy);
    when(mockPluginService.acceptsStrategy(HostRole.READER, strategy)).thenReturn(true);
    // A pooled strategy with no eligible host can surface as SQLException(UnsupportedOperationException): the
    // provider manager falls through to the default provider. That must not be taken for an unsupported strategy.
    when(mockPluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq(strategy)))
        .thenThrow(new SQLException(new UnsupportedOperationException("not supported by the default provider")))
        .thenReturn(READER_1);
    final Connection readerConnection = mock(Connection.class);
    when(mockPluginService.connect(eq(READER_1), any(Properties.class), any())).thenReturn(readerConnection);
    when(mockPluginService.getHostRole(readerConnection)).thenReturn(HostRole.READER);

    assertThrows(FailoverSuccessSQLException.class, plugin::failoverReader);

    verify(mockPluginService).setCurrentConnection(eq(readerConnection), any(HostSpec.class));
  }

  private FailoverConnectionPlugin initReaderFailover(final FailoverMode mode, final String strategy)
      throws SQLException {
    properties.setProperty(FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS.name, "1000");
    properties.setProperty(FailoverConnectionPlugin.FAILOVER_READER_HOST_SELECTOR_STRATEGY.name, strategy);
    when(mockTelemetryFactory.createCounter("readerFailover.completed.failed.count"))
        .thenReturn(mockReaderFailoverFailedCounter);
    when(mockContainer.getImportantEventService()).thenReturn(mockImportantEventService);
    when(mockPluginService.forceRefreshHostList(anyBoolean(), anyLong())).thenReturn(true);
    when(mockPluginService.getHosts()).thenReturn(Arrays.asList(WRITER, READER_1, READER_2));
    when(mockPluginService.getHostSpecBuilder())
        .thenAnswer(invocation -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final FailoverConnectionPlugin plugin = new FailoverConnectionPlugin(mockContainer, properties);
    plugin.failoverMode = mode;
    return plugin;
  }

  private static HostSpec host(final String name, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(name).role(role).build();
  }
}
