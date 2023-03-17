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

package software.amazon.jdbc.plugin.efm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostAvailability;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;

class HostMonitoringConnectionPluginTest {

  static final Class<Connection> MONITOR_METHOD_INVOKE_ON = Connection.class;
  static final String MONITOR_METHOD_NAME = "Statement.executeQuery";
  static final String NO_MONITOR_METHOD_NAME = "Connection.abort";
  static final int FAILURE_DETECTION_TIME = 10;
  static final int FAILURE_DETECTION_INTERVAL = 100;
  static final int FAILURE_DETECTION_COUNT = 5;
  private static final Object[] EMPTY_ARGS = {};
  @Mock PluginService pluginService;
  @Mock Connection connection;
  @Mock Statement statement;
  @Mock ResultSet resultSet;
  @Captor ArgumentCaptor<String> stringArgumentCaptor;
  Properties properties = new Properties();
  @Mock HostSpec hostSpec;
  @Mock Supplier<MonitorService> supplier;
  @Mock MonitorConnectionContext context;
  @Mock MonitorService monitorService;
  @Mock JdbcCallable<ResultSet, SQLException> sqlFunction;
  private HostMonitoringConnectionPlugin plugin;
  private AutoCloseable closeable;

  /**
   * Generate different sets of method arguments where one argument is null to ensure {@link
   * software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin#HostMonitoringConnectionPlugin(PluginService,
   * Properties)} can handle null arguments correctly.
   *
   * @return different sets of arguments.
   */
  private static Stream<Arguments> generateNullArguments() {
    final PluginService pluginService = mock(PluginService.class);
    final Properties properties = new Properties();

    return Stream.of(
        Arguments.of(null, null),
        Arguments.of(pluginService, null),
        Arguments.of(null, properties));
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    initDefaultMockReturns();
    properties.clear();
  }

  void initDefaultMockReturns() throws Exception {
    when(supplier.get()).thenReturn(monitorService);
    when(monitorService.startMonitoring(
            any(Connection.class),
            anySet(),
            any(HostSpec.class),
            any(Properties.class),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(context);

    when(pluginService.getCurrentConnection()).thenReturn(connection);
    when(pluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(hostSpec.getHost()).thenReturn("host");
    when(hostSpec.getHost()).thenReturn("port");
    when(hostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("host:port")));
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(any())).thenReturn(resultSet);

    properties.put("failureDetectionEnabled", Boolean.TRUE.toString());
    properties.put("failureDetectionTime", String.valueOf(FAILURE_DETECTION_TIME));
    properties.put("failureDetectionInterval", String.valueOf(FAILURE_DETECTION_INTERVAL));
    properties.put("failureDetectionCount", String.valueOf(FAILURE_DETECTION_COUNT));
  }

  private void initializePlugin() {
    plugin = new HostMonitoringConnectionPlugin(pluginService, properties, supplier);
  }

  @ParameterizedTest
  @MethodSource("generateNullArguments")
  void test_initWithNullArguments(
      final PluginService pluginService, final Properties properties) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HostMonitoringConnectionPlugin(pluginService, properties));
  }

  @Test
  void test_executeWithMonitoringDisabled() throws Exception {
    properties.put("failureDetectionEnabled", Boolean.FALSE.toString());

    initializePlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    verify(supplier, never()).get();
    verify(monitorService, never())
        .startMonitoring(any(), any(), any(), any(), anyInt(), anyInt(), anyInt());
    verify(monitorService, never()).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  @Test
  void test_executeWithNoNeedToMonitor() throws Exception {

    initializePlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        NO_MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    verify(supplier, atMostOnce()).get();
    verify(monitorService, never())
        .startMonitoring(any(), any(), any(), any(), anyInt(), anyInt(), anyInt());
    verify(monitorService, never()).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  @Test
  void test_executeMonitoringEnabled() throws Exception {

    initializePlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    verify(supplier, times(1)).get();
    verify(monitorService, times(1))
        .startMonitoring(any(), any(), any(), any(), anyInt(), anyInt(), anyInt());
    verify(monitorService, times(1)).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  /**
   * Tests exception being thrown in the finally block when checking connection status in the execute method.
   */
  @Test
  void test_executeCleanUp_whenCheckingConnectionStatus_throwsException() throws SQLException {
    initializePlugin();

    final SQLException expectedException = new SQLException("exception thrown during isClosed");
    when(context.isNodeUnhealthy()).thenReturn(true);
    doThrow(expectedException).when(connection).isClosed();
    final SQLException actualException = assertThrows(SQLException.class, () -> plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS));

    assertEquals(expectedException, actualException);
  }

  /**
   * Tests exception being thrown in the finally block
   * when an open connection object is detected for an unavailable node in the execute method.
   */
  @Test
  void test_executeCleanUp_whenAbortConnection_throwsException() throws SQLException {
    initializePlugin();

    final String errorMessage = Messages.get(
        "HostMonitoringConnectionPlugin.unavailableNode",
        new Object[] {"alias"});

    when(hostSpec.asAlias()).thenReturn("alias");
    when(connection.isClosed()).thenReturn(false);
    when(context.isNodeUnhealthy()).thenReturn(true);
    final SQLException actualException = assertThrows(SQLException.class, () -> plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS));

    assertEquals(errorMessage, actualException.getMessage());
    verify(pluginService).setAvailability(any(), eq(HostAvailability.NOT_AVAILABLE));
    verify(connection).close();
  }

  @ParameterizedTest
  @MethodSource("getHostPortSQLParameters")
  void test_connect_withNoAdditionalHostAlias(final String protocol, final String expectedSql) throws SQLException {
    initializePlugin();

    when(hostSpec.asAlias()).thenReturn("hostSpec alias");

    plugin.connect(protocol, hostSpec, properties, true, () -> connection);
    verify(hostSpec).addAlias("hostSpec alias");
    verify(statement).executeQuery(eq(expectedSql));
  }

  @ParameterizedTest
  @MethodSource("getHostPortSQLParameters")
  void test_connect_withHostAliases(final String protocol, final String expectedSql) throws SQLException {
    initializePlugin();

    when(hostSpec.asAlias()).thenReturn("hostSpec alias");

    // ResultSet contains one row.
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(eq(1))).thenReturn("second alias");

    plugin.connect(protocol, hostSpec, properties, true, () -> connection);
    verify(hostSpec, times(2)).addAlias(stringArgumentCaptor.capture());
    final List<String> captures = stringArgumentCaptor.getAllValues();
    assertEquals(2, captures.size());
    assertEquals("hostSpec alias", captures.get(0));
    assertEquals("second alias", captures.get(1));
    verify(statement).executeQuery(eq(expectedSql));
  }

  @Test
  void test_connect_exceptionRaisedDuringGenerateHostAliases() throws SQLException {
    initializePlugin();

    doThrow(new SQLException()).when(connection).createStatement();

    // Ensure SQLException raised in `generateHostAliases` are ignored.
    final Connection conn = plugin.connect("protocol", hostSpec, properties, true, () -> connection);
    assertNotNull(conn);
  }

  @Test
  void test_connect_unsupportedDriverProtocol() {
    initializePlugin();
    assertThrows(
        UnsupportedOperationException.class,
        () -> plugin.connect("badProtocol", hostSpec, properties, true, () -> connection));
  }

  @ParameterizedTest
  @MethodSource("nodeChangeOptions")
  void test_notifyConnectionChanged_nodeWentDown(final NodeChangeOptions option) throws SQLException {
    initializePlugin();
    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    final Set<String> aliases = new HashSet<>(Arrays.asList("alias1", "alias2"));
    when(hostSpec.getAliases()).thenReturn(aliases);
    assertEquals(OldConnectionSuggestedAction.NO_OPINION, plugin.notifyConnectionChanged(EnumSet.of(option)));

    // NodeKeys should be empty at first
    verify(monitorService, never()).stopMonitoringForAllConnections(any());

    assertEquals(OldConnectionSuggestedAction.NO_OPINION, plugin.notifyConnectionChanged(EnumSet.of(option)));
    // NodeKeys should contain {"alias1", "alias2"}
    verify(monitorService).stopMonitoringForAllConnections(aliases);
  }

  @Test
  void test_releaseResources() throws SQLException {
    initializePlugin();

    // Test releaseResources when the monitor service has not been initialized.
    plugin.releaseResources();
    verify(monitorService, never()).releaseResources();

    // Test releaseResources when the monitor service has been initialized.
    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);
    plugin.releaseResources();
    verify(monitorService).releaseResources();
  }

  static Stream<Arguments> getHostPortSQLParameters() {
    final String MYSQL_RETRIEVE_HOST_PORT_SQL = "SELECT CONCAT(@@hostname, ':', @@port)";
    final String PG_RETRIEVE_HOST_PORT_SQL = "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())";

    return Stream.of(
        Arguments.of("jdbc:mysql:", MYSQL_RETRIEVE_HOST_PORT_SQL),
        Arguments.of("jdbc:mysql:someUrl", MYSQL_RETRIEVE_HOST_PORT_SQL),
        Arguments.of("jdbc:postgresql:", PG_RETRIEVE_HOST_PORT_SQL),
        Arguments.of("jdbc:postgresql:someUrl", PG_RETRIEVE_HOST_PORT_SQL)
    );
  }

  static Stream<Arguments> nodeChangeOptions() {
    return Stream.of(
        Arguments.of(NodeChangeOptions.WENT_DOWN),
        Arguments.of(NodeChangeOptions.NODE_DELETED)
    );
  }
}
