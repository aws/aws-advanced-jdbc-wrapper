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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.GlobalAuroraMysqlDialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class AuroraConnectionTrackerPluginTest {

  public static final Properties EMPTY_PROPERTIES = new Properties();
  @Mock Connection mockConnection;
  @Mock Statement mockStatement;
  @Mock ResultSet mockResultSet;
  @Mock PluginService mockPluginService;
  @Mock Dialect mockTopologyAwareDialect;
  @Mock RdsUtils mockRdsUtils;
  @Mock OpenedConnectionTracker mockTracker;
  @Mock JdbcCallable<Connection, SQLException> mockConnectionFunction;
  @Mock JdbcCallable<ResultSet, SQLException> mockSqlFunction;
  @Mock JdbcCallable<Void, SQLException> mockCloseOrAbortFunction;
  @Mock TargetDriverDialect mockTargetDriverDialect;

  private static final Object[] SQL_ARGS = {"sql"};

  private AutoCloseable closeable;


  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockConnectionFunction.call()).thenReturn(mockConnection);
    when(mockSqlFunction.call()).thenReturn(mockResultSet);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(any(String.class))).thenReturn(mockResultSet);
    when(mockRdsUtils.getRdsInstanceHostPattern(any(String.class))).thenReturn("?");
    when(mockRdsUtils.identifyRdsType(any())).thenReturn(RdsUrlType.RDS_INSTANCE);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getDialect()).thenReturn(mockTopologyAwareDialect);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());
  }

  @AfterEach
  void tearDown() throws Exception {
    AuroraConnectionTrackerPlugin.clearHandledPrimaryRegionChanges();
    closeable.close();
  }

  @ParameterizedTest
  @MethodSource("trackNewConnectionsParameters")
  public void testTrackNewInstanceConnections(
      final String protocol,
      final boolean isInitialConnection) throws SQLException {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance1")
        .build();
    when(mockPluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(mockRdsUtils.isRdsInstance("instance1")).thenReturn(true);

    final AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        EMPTY_PROPERTIES,
        mockRdsUtils,
        mockTracker);

    final Connection actualConnection = plugin.connect(
        protocol,
        hostSpec,
        EMPTY_PROPERTIES,
        isInitialConnection,
        mockConnectionFunction);

    assertEquals(mockConnection, actualConnection);
    verify(mockTracker).populateOpenedConnectionQueue(eq(hostSpec), eq(mockConnection));
  }

  @Test
  public void testInvalidateOpenedConnectionsWhenWriterHostNotChange() throws SQLException {
    final FailoverSQLException expectedException = new FailoverSQLException("reason", "sqlstate");
    final HostSpec originalHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host")
        .role(HostRole.WRITER)
        .build();
    final HostSpec newHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("new-host")
        .role(HostRole.WRITER)
        .build();

    // Host list changes during simulated failover
    when(mockPluginService.getAllHosts()).thenReturn(Collections.singletonList(originalHost));
    doThrow(expectedException).when(mockSqlFunction).call();

    final AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        EMPTY_PROPERTIES,
        mockRdsUtils,
        mockTracker);

    final SQLException exception = assertThrows(FailoverSQLException.class, () -> plugin.execute(
        ResultSet.class,
        SQLException.class,
        Statement.class,
        "Statement.executeQuery",
        mockSqlFunction,
        SQL_ARGS
    ));

    assertEquals(expectedException, exception);
    verify(mockTracker, never()).removeConnectionTracking(eq(originalHost), eq(mockConnection));
    verify(mockTracker, never()).invalidateAllConnections(originalHost);
  }

  @Test
  public void testInvalidateOpenedConnectionsWhenWriterHostChanged() throws SQLException {
    final FailoverSQLException expectedException = new FailoverSQLException("reason", "sqlstate");
    final HostSpec originalHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host")
        .build();
    final HostSpec failoverTargetHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host2")
        .build();
    when(mockPluginService.getAllHosts())
        .thenReturn(Collections.singletonList(originalHost))
        .thenReturn(Collections.singletonList(failoverTargetHost));
    when(mockSqlFunction.call())
        .thenReturn(mockResultSet)
        .thenThrow(expectedException);

    final AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        EMPTY_PROPERTIES,
        mockRdsUtils,
        mockTracker);

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        Statement.class,
        "Statement.executeQuery",
        mockSqlFunction,
        SQL_ARGS
    );

    final SQLException exception = assertThrows(FailoverSQLException.class, () -> plugin.execute(
        ResultSet.class,
        SQLException.class,
        Statement.class,
        "Statement.executeQuery",
        mockSqlFunction,
        SQL_ARGS
    ));
    assertEquals(expectedException, exception);
    verify(mockTracker, never()).removeConnectionTracking(eq(originalHost), eq(mockConnection));
    verify(mockTracker).invalidateAllConnections(originalHost);
  }

  private static final String OHIO_WRITER_ENDPOINT =
      "global-ohio-mysql.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com";
  private static final HostSpec OHIO_1 = gdbHost(
      "global-ohio-mysql-instance-1.c12pgqavxipt.us-east-2.rds.amazonaws.com", HostRole.WRITER);
  private static final HostSpec OHIO_2 = gdbHost(
      "global-ohio-mysql-instance-2.c12pgqavxipt.us-east-2.rds.amazonaws.com", HostRole.READER);
  private static final HostSpec OREGON_1 = gdbHost(
      "global-oregon-mysql-instance-1.cxmnwnbkcxsy.us-west-2.rds.amazonaws.com", HostRole.WRITER);

  private static HostSpec gdbHost(final String host, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(host).port(3306).role(role).build();
  }

  /** Real region parsing, mocked everything else the plugin needs from RdsUtils. */
  private void useGlobalDatabase(final AtomicReference<List<HostSpec>> topology) {
    final RdsUtils real = new RdsUtils();
    when(mockRdsUtils.getRdsRegion(any())).thenAnswer(i -> real.getRdsRegion(i.getArgument(0)));
    when(mockRdsUtils.isSameRegion(any(), any()))
        .thenAnswer(i -> real.isSameRegion(i.getArgument(0), i.getArgument(1)));
    when(mockRdsUtils.isRdsInstance(any())).thenAnswer(i -> real.isRdsInstance(i.getArgument(0)));
    when(mockRdsUtils.identifyRdsType(any())).thenAnswer(i -> real.identifyRdsType(i.getArgument(0)));
    when(mockPluginService.getDialect()).thenReturn(mock(GlobalAuroraMysqlDialect.class));
    when(mockPluginService.getAllHosts()).thenAnswer(i -> topology.get());
  }

  private void executeThenFailover(final AuroraConnectionTrackerPlugin plugin,
      final AtomicReference<List<HostSpec>> topology, final List<HostSpec> topologyAfterFailover)
      throws SQLException {
    final FailoverSQLException failover = new FailoverSQLException("reason", "08S02");
    // doReturn/doThrow: a when(call()) stub would invoke the previous stub, which may throw.
    doReturn(mockResultSet).when(mockSqlFunction).call();
    plugin.execute(ResultSet.class, SQLException.class, Statement.class, "Statement.executeQuery",
        mockSqlFunction, SQL_ARGS);

    topology.set(topologyAfterFailover);
    doThrow(failover).when(mockSqlFunction).call();
    assertThrows(FailoverSQLException.class, () -> plugin.execute(ResultSet.class, SQLException.class,
        Statement.class, "Statement.executeQuery", mockSqlFunction, SQL_ARGS));
  }

  @Test
  public void testPrimaryRegionChangeInvalidatesOtherInstancesOfNewPrimaryRegion() throws SQLException {
    final AtomicReference<List<HostSpec>> topology = new AtomicReference<>(Arrays.asList(OREGON_1, OHIO_2));
    useGlobalDatabase(topology);
    final AuroraConnectionTrackerPlugin plugin =
        new AuroraConnectionTrackerPlugin(mockPluginService, EMPTY_PROPERTIES, mockRdsUtils, mockTracker);

    executeThenFailover(plugin, topology, Arrays.asList(OHIO_1, OHIO_2));

    verify(mockTracker).invalidateAllConnections(OREGON_1);
    verify(mockTracker).invalidateConnectionsInRegion("us-east-2", OHIO_1.getHostAndPort());
  }

  @Test
  public void testPrimaryRegionChangeIsHandledOnce() throws SQLException {
    final AtomicReference<List<HostSpec>> topology = new AtomicReference<>(Arrays.asList(OREGON_1, OHIO_2));
    useGlobalDatabase(topology);
    final AuroraConnectionTrackerPlugin first =
        new AuroraConnectionTrackerPlugin(mockPluginService, EMPTY_PROPERTIES, mockRdsUtils, mockTracker);
    final AuroraConnectionTrackerPlugin second =
        new AuroraConnectionTrackerPlugin(mockPluginService, EMPTY_PROPERTIES, mockRdsUtils, mockTracker);

    executeThenFailover(first, topology, Arrays.asList(OHIO_1, OHIO_2));
    topology.set(Arrays.asList(OREGON_1, OHIO_2));
    executeThenFailover(second, topology, Arrays.asList(OHIO_1, OHIO_2));

    verify(mockTracker, times(1)).invalidateConnectionsInRegion("us-east-2", OHIO_1.getHostAndPort());
  }

  @Test
  public void testWriterChangeWithinRegionDoesNotInvalidateRegion() throws SQLException {
    final AtomicReference<List<HostSpec>> topology = new AtomicReference<>(Arrays.asList(OHIO_1));
    useGlobalDatabase(topology);
    final AuroraConnectionTrackerPlugin plugin =
        new AuroraConnectionTrackerPlugin(mockPluginService, EMPTY_PROPERTIES, mockRdsUtils, mockTracker);

    executeThenFailover(plugin, topology,
        Arrays.asList(gdbHost(OHIO_2.getHost(), HostRole.WRITER)));

    verify(mockTracker).invalidateAllConnections(OHIO_1);
    verify(mockTracker, never()).invalidateConnectionsInRegion(any(), any());
  }

  @Test
  public void testPrimaryRegionChangeIgnoredWhenDisabled() throws SQLException {
    final AtomicReference<List<HostSpec>> topology = new AtomicReference<>(Arrays.asList(OREGON_1, OHIO_2));
    useGlobalDatabase(topology);
    final Properties props = new Properties();
    props.setProperty(AuroraConnectionTrackerPlugin.INVALIDATE_ON_PRIMARY_REGION_CHANGE.name, "false");
    final AuroraConnectionTrackerPlugin plugin =
        new AuroraConnectionTrackerPlugin(mockPluginService, props, mockRdsUtils, mockTracker);

    executeThenFailover(plugin, topology, Arrays.asList(OHIO_1, OHIO_2));

    verify(mockTracker).invalidateAllConnections(OREGON_1);
    verify(mockTracker, never()).invalidateConnectionsInRegion(any(), any());
  }

  @Test
  public void testPrimaryRegionChangeIgnoredForNonGlobalDialect() throws SQLException {
    final AtomicReference<List<HostSpec>> topology = new AtomicReference<>(Arrays.asList(OREGON_1, OHIO_2));
    useGlobalDatabase(topology);
    when(mockPluginService.getDialect()).thenReturn(mockTopologyAwareDialect);
    final AuroraConnectionTrackerPlugin plugin =
        new AuroraConnectionTrackerPlugin(mockPluginService, EMPTY_PROPERTIES, mockRdsUtils, mockTracker);

    executeThenFailover(plugin, topology, Arrays.asList(OHIO_1, OHIO_2));

    verify(mockTracker, never()).invalidateConnectionsInRegion(any(), any());
  }

  @Test
  public void testPrimaryRegionChangeAbortsTrackedReaderConnections() throws Exception {
    // End to end with the real tracker: a connection opened to OHIO_2 while Ohio was secondary is aborted when
    // the writer moves to Ohio; a connection to the new writer is kept.
    final AtomicReference<List<HostSpec>> topology = new AtomicReference<>(Arrays.asList(OREGON_1, OHIO_2));
    useGlobalDatabase(topology);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mock(TelemetryFactory.class));
    final OpenedConnectionTracker realTracker = new OpenedConnectionTracker(mockPluginService);
    final AuroraConnectionTrackerPlugin plugin =
        new AuroraConnectionTrackerPlugin(mockPluginService, EMPTY_PROPERTIES, mockRdsUtils, realTracker);

    final Connection readerConnection = mock(Connection.class);
    final Connection writerConnection = mock(Connection.class);
    try {
      when(mockConnectionFunction.call()).thenReturn(readerConnection);
      plugin.connect("mysql", OHIO_2, EMPTY_PROPERTIES, false, mockConnectionFunction);
      when(mockConnectionFunction.call()).thenReturn(writerConnection);
      plugin.connect("mysql", OHIO_1, EMPTY_PROPERTIES, false, mockConnectionFunction);

      executeThenFailover(plugin, topology, Arrays.asList(OHIO_1, OHIO_2));

      verify(readerConnection, timeout(2000)).abort(any());
      verify(writerConnection, after(300).never()).abort(any());
    } finally {
      OpenedConnectionTracker.clearCache();
    }
  }

  @Test
  public void testConnectionThroughWriterEndpointIsTrackedAsIs() throws SQLException {
    // Whether the connection reached the right instance is not the tracker's concern: it tracks what it got.
    final AtomicReference<List<HostSpec>> topology = new AtomicReference<>(Arrays.asList(OHIO_1, OHIO_2));
    useGlobalDatabase(topology);
    final HostSpec endpoint = gdbHost(OHIO_WRITER_ENDPOINT, HostRole.WRITER);
    when(mockPluginService.identifyConnection(mockConnection, endpoint)).thenReturn(OHIO_2);
    final AuroraConnectionTrackerPlugin plugin =
        new AuroraConnectionTrackerPlugin(mockPluginService, EMPTY_PROPERTIES, mockRdsUtils, mockTracker);

    assertEquals(mockConnection,
        plugin.connect("mysql", endpoint, EMPTY_PROPERTIES, true, mockConnectionFunction));
    verify(mockConnection, never()).close();
    verify(mockTracker).populateOpenedConnectionQueue(OHIO_2, mockConnection);
  }

  @ParameterizedTest
  @MethodSource("testInvalidateConnectionsOnCloseOrAbortArgs")
  public void testInvalidateConnectionsOnCloseOrAbort(final String method) throws SQLException {
    final HostSpec originalHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host")
        .build();
    when(mockPluginService.getCurrentHostSpec()).thenReturn(originalHost);

    final AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        EMPTY_PROPERTIES,
        mockRdsUtils,
        mockTracker);

    plugin.execute(
        Void.class,
        SQLException.class,
        Connection.class,
        method,
        mockCloseOrAbortFunction,
        SQL_ARGS
    );

    verify(mockTracker).removeConnectionTracking(eq(originalHost), eq(mockConnection));
  }

  static Stream<Arguments> testInvalidateConnectionsOnCloseOrAbortArgs() {
    return Stream.of(
        Arguments.of(JdbcMethod.CONNECTION_ABORT.methodName),
        Arguments.of(JdbcMethod.CONNECTION_CLOSE.methodName)
    );
  }

  private static Stream<Arguments> trackNewConnectionsParameters() {
    return Stream.of(
        Arguments.of("postgresql", true),
        Arguments.of("postgresql", false),
        Arguments.of("otherProtocol", true),
        Arguments.of("otherProtocol", false)
    );
  }
}
