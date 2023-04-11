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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.util.RdsUtils;

public class AuroraConnectionTrackerPluginTest {

  public static final Properties EMPTY_PROPERTIES = new Properties();
  @Mock Connection mockConnection;
  @Mock Statement mockStatement;
  @Mock ResultSet mockResultSet;
  @Mock PluginService mockPluginService;
  @Mock RdsUtils mockRdsUtils;
  @Mock OpenedConnectionTracker mockTracker;
  @Mock JdbcCallable<Connection, SQLException> mockConnectionFunction;
  @Mock JdbcCallable<ResultSet, SQLException> mockSqlFunction;
  @Mock JdbcCallable<Void, SQLException> mockCloseOrAbortFunction;
  private static final Object[] EMPTY_ARGS = {};
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
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @ParameterizedTest
  @MethodSource("trackNewConnectionsParameters")
  public void testTrackNewInstanceConnections(
      final String protocol,
      final boolean isInitialConnection) throws SQLException {
    final HostSpec hostSpec = new HostSpec("instance1");
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
    final Set<String> aliases = hostSpec.getAliases();
    assertEquals(0, aliases.size());
  }

  @ParameterizedTest
  @MethodSource("trackNewConnectionsParameters")
  public void testTrackNewClusterConnections(
      final String protocol,
      final boolean isInitialConnection) throws SQLException {
    final HostSpec hostSpec = new HostSpec("writerCluster");
    when(mockPluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(mockRdsUtils.isRdsInstance("writerCluster")).thenReturn(false);
    when(mockResultSet.next()).thenReturn(true, false); // ResultSet should only have 1 row.
    when(mockResultSet.getString(any(String.class))).thenReturn("writerInstance");

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
    final Set<String> aliases = hostSpec.getAliases();
    assertEquals(1, aliases.size());
    assertEquals("writerInstance", aliases.toArray()[0]);
  }

  @ParameterizedTest
  @MethodSource("trackNonRdsInstanceUrlParameters")
  public void testTrackNewConnections_nonRdsInstanceUrl(
      final String endpoint,
      final boolean isInitialConnection,
      final String expected) throws SQLException {
    final Properties properties = new Properties();
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(properties, "?.pattern");
    final HostSpec hostSpec = new HostSpec(endpoint);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(mockRdsUtils.isRdsInstance(endpoint)).thenReturn(false);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(any())).thenReturn(endpoint);

    final AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        properties,
        mockRdsUtils,
        mockTracker);

    final Connection actualConnection = plugin.connect(
        "protocol",
        hostSpec,
        properties,
        isInitialConnection,
        mockConnectionFunction);

    assertEquals(mockConnection, actualConnection);
    verify(mockTracker).populateOpenedConnectionQueue(eq(hostSpec), eq(mockConnection));
    final Set<String> aliases = hostSpec.getAliases();
    assertEquals(1, aliases.size());
    assertEquals(expected, aliases.toArray()[0]);
  }

  @ParameterizedTest
  @MethodSource("trackNonRdsInstanceUrlWithoutClusterHostInstancePatternParameters")
  public void testTrackNewConnections_nonRdsInstanceUrl_withoutClusterInstanceHostPattern(
      final String endpoint,
      final boolean isInitialConnection,
      final String expected) throws SQLException {
    final HostSpec hostSpec = new HostSpec(endpoint);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(mockRdsUtils.isRdsInstance(endpoint)).thenReturn(false);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(any())).thenReturn("instance-1");

    final AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        EMPTY_PROPERTIES,
        mockRdsUtils,
        mockTracker);

    final Connection actualConnection = plugin.connect(
        "protocol",
        hostSpec,
        EMPTY_PROPERTIES,
        isInitialConnection,
        mockConnectionFunction);

    assertEquals(mockConnection, actualConnection);
    verify(mockTracker).populateOpenedConnectionQueue(eq(hostSpec), eq(mockConnection));
    final Set<String> aliases = hostSpec.getAliases();
    assertEquals(1, aliases.size());
    assertEquals(expected, aliases.toArray()[0]);
  }

  @Test
  public void testInvalidateOpenedConnections() throws SQLException {
    final FailoverSQLException expectedException = new FailoverSQLException("reason", "sqlstate");
    final HostSpec originalHost = new HostSpec("host");
    when(mockPluginService.getCurrentHostSpec()).thenReturn(originalHost);
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
    verify(mockTracker, never()).invalidateCurrentConnection(eq(originalHost), eq(mockConnection));
    verify(mockTracker).invalidateAllConnections(originalHost);
  }

  @ParameterizedTest
  @ValueSource(strings = {AuroraConnectionTrackerPlugin.METHOD_ABORT, AuroraConnectionTrackerPlugin.METHOD_CLOSE})
  public void testInvalidateConnectionsOnCloseOrAbort(final String method) throws SQLException {
    final HostSpec originalHost = new HostSpec("host");
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

    verify(mockTracker).invalidateCurrentConnection(eq(originalHost), eq(mockConnection));
  }

  private static Stream<Arguments> trackNewConnectionsParameters() {
    return Stream.of(
        Arguments.of("postgresql", true),
        Arguments.of("postgresql", false),
        Arguments.of("otherProtocol", true),
        Arguments.of("otherProtocol", false)
    );
  }

  private static Stream<Arguments> trackNonRdsInstanceUrlParameters() {
    return Stream.of(
        Arguments.of("custom.domain", true, "custom.domain.pattern"),
        Arguments.of("instanceName", false, "instanceName.pattern"),
        Arguments.of("8.8.8.8", true, "8.8.8.8.pattern")
    );
  }

  private static Stream<Arguments> trackNonRdsInstanceUrlWithoutClusterHostInstancePatternParameters() {
    return Stream.of(
        Arguments.of("custom.domain", true, "instance-1"),
        Arguments.of("instanceName", false, "instance-1"),
        Arguments.of("8.8.8.8", true, "instance-1")
    );
  }
}
