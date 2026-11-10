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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;

class ConnectionLifetimeConnectionPluginTest {

  private static final String NETWORK_METHOD = JdbcMethod.STATEMENT_EXECUTE.methodName;
  private static final String LOCAL_METHOD = JdbcMethod.CONNECTION_GETAUTOCOMMIT.methodName;

  private final FullServicesContainer servicesContainer = mock(FullServicesContainer.class);
  private final PluginService pluginService = mock(PluginService.class);
  private final HostListProviderService hostListProviderService = mock(HostListProviderService.class);
  private final TargetDriverDialect targetDriverDialect = mock(TargetDriverDialect.class);
  private final MutableNanoClock clock = new MutableNanoClock();
  private final AtomicReference<Connection> currentConnection = new AtomicReference<>();
  private final AtomicBoolean inTransaction = new AtomicBoolean();
  private final AtomicBoolean xaTransactionActive = new AtomicBoolean();
  private final HostSpec originalHost = host("original.example.com");
  private final HostSpec movedHost = host("moved.example.com");

  @BeforeEach
  void setUp() throws SQLException {
    when(this.servicesContainer.getPluginService()).thenReturn(this.pluginService);
    when(this.pluginService.getTargetDriverDialect()).thenReturn(this.targetDriverDialect);
    when(this.targetDriverDialect.getNetworkBoundMethodNames(any()))
        .thenReturn(
            new HashSet<>(
                Arrays.asList(
                    NETWORK_METHOD,
                    JdbcMethod.CONNECTION_COMMIT.methodName,
                    JdbcMethod.CONNECTION_ROLLBACK.methodName)));
    when(this.pluginService.getCurrentConnection()).thenAnswer(invocation -> this.currentConnection.get());
    when(this.pluginService.isInTransaction()).thenAnswer(invocation -> this.inTransaction.get());
    when(this.pluginService.isXaTransactionActive()).thenAnswer(invocation -> this.xaTransactionActive.get());
    doAnswer(
            invocation -> {
              this.currentConnection.set(invocation.getArgument(0));
              return EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED);
            })
        .when(this.pluginService)
        .setCurrentConnection(any(Connection.class), any(HostSpec.class), any(ConnectionPlugin.class));
  }

  @Test
  void missingLifetimeFailsFast() {
    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new ConnectionLifetimeConnectionPlugin(
            this.servicesContainer, new Properties(), this.clock));

    assertEquals(
        Messages.get(
            "ConnectionLifetimeConnectionPlugin.invalidConnectionLifetime",
            new Object[] {null}),
        exception.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "not-a-number", "0", "-1"})
  void invalidLifetimeFailsFast(final String value) {
    final Properties properties = new Properties();
    properties.setProperty(ConnectionLifetimeConnectionPlugin.CONNECTION_LIFETIME_MS.name, value);

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new ConnectionLifetimeConnectionPlugin(
            this.servicesContainer, properties, this.clock));

    assertEquals(
        Messages.get(
            "ConnectionLifetimeConnectionPlugin.invalidConnectionLifetime",
            new Object[] {value}),
        exception.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "replace"})
  void invalidActionFailsFast(final String action) {
    final Properties properties = properties(100);
    properties.setProperty(
        ConnectionLifetimeConnectionPlugin.CONNECTION_LIFETIME_ACTION.name, action);

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new ConnectionLifetimeConnectionPlugin(
            this.servicesContainer, properties, this.clock));

    assertEquals(
        Messages.get(
            "ConnectionLifetimeConnectionPlugin.invalidConnectionLifetimeAction",
            new Object[] {action}),
        exception.getMessage());
  }

  @Test
  void subscribesOnlyToNetworkAndRequiredLifecycleMethods() {
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);

    final Set<String> expected = new HashSet<>();
    expected.add(NETWORK_METHOD);
    expected.add(JdbcMethod.CONNECTION_COMMIT.methodName);
    expected.add(JdbcMethod.CONNECTION_ROLLBACK.methodName);
    expected.add(JdbcMethod.CONNECT.methodName);
    expected.add(JdbcMethod.INITHOSTPROVIDER.methodName);
    expected.add(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName);
    assertEquals(expected, plugin.getSubscribedMethods());
    assertFalse(plugin.getSubscribedMethods().contains(JdbcMethod.FORCECONNECT.methodName));
    assertFalse(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECTION_CLOSE.methodName));
    assertFalse(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECTION_ABORT.methodName));
    assertFalse(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECTION_ISCLOSED.methodName));
    assertFalse(plugin.getSubscribedMethods().contains("Connection.unwrap"));
    assertFalse(plugin.getSubscribedMethods().contains(LOCAL_METHOD));
  }

  @Test
  void callBeforeExpirationRunsAndExactBoundaryDoesNotRun() throws SQLException {
    final Connection connection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, connection, 0);
    final JdbcCallable<Object, SQLException> beforeBoundary = mockCallable();
    final JdbcCallable<Object, SQLException> atBoundary = mockCallable();

    this.clock.setMillis(99);
    execute(plugin, NETWORK_METHOD, beforeBoundary);
    verify(beforeBoundary).call();

    this.clock.setMillis(100);
    final SQLException exception = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, atBoundary));

    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), exception.getSQLState());
    verifyNoInteractions(atBoundary);
  }

  @Test
  void closeActionClosesBeforeThrowAndRemainsTerminal() throws SQLException {
    final Connection connection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "ClOsE");
    initializeConnection(plugin, connection, 0);
    this.clock.setMillis(100);
    final JdbcCallable<Object, SQLException> pendingCall = mockCallable();

    final SQLException firstException = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, pendingCall));
    final SQLException secondException = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, pendingCall));

    assertInstanceOf(ConnectionLifetimeSQLException.class, firstException);
    assertEquals(
        Messages.get("ConnectionLifetimeConnectionPlugin.connectionClosed"),
        firstException.getMessage());
    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), firstException.getSQLState());
    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), secondException.getSQLState());
    verify(connection, times(1)).close();
    verify(this.targetDriverDialect, never()).abortConnection(any(), any());
    verifyNoInteractions(pendingCall);
  }

  @Test
  void closeActionRetainsCloseFailureAsCause() throws SQLException {
    final Connection connection = mock(Connection.class);
    final SQLException closeFailure = new SQLException("close failed");
    doThrow(closeFailure).when(connection).close();
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, connection, 0);
    this.clock.setMillis(100);

    final SQLException exception = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));

    assertInstanceOf(ConnectionLifetimeSQLException.class, exception);
    assertSame(closeFailure, exception.getCause());
  }

  @Test
  void terminalStateDoesNotBlockUnsubscribedLocalMethod() throws SQLException {
    final Connection connection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, connection, 0);
    this.clock.setMillis(100);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    final JdbcCallable<Object, SQLException> localCall = mockCallable();

    execute(plugin, LOCAL_METHOD, localCall);

    verify(localCall).call();
  }

  @Test
  void connectTimestampIsPreservedThroughInitialNotification() throws SQLException {
    final Connection connection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, connection, 0);
    this.clock.setMillis(50);
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION));
    this.clock.setMillis(100);

    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
  }

  @Test
  void hostOnlyNotificationDoesNotResetTimestamp() throws SQLException {
    final Connection connection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, connection, 0);
    this.clock.setMillis(90);
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.HOSTNAME));
    this.clock.setMillis(100);

    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
  }

  @Test
  void connectionObjectNotificationUsesPhysicalConnectionCreationTimestamp() throws SQLException {
    final Connection firstConnection = mock(Connection.class);
    final Connection secondConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, firstConnection, 0);
    this.clock.setMillis(50);
    plugin.connect("jdbc:test:", this.movedHost, new Properties(), false, () -> secondConnection);
    this.clock.setMillis(80);
    this.currentConnection.set(secondConnection);
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
    this.clock.setMillis(150);

    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
  }

  @Test
  void switchingBackToCachedConnectionRetainsItsOriginalCreationTimestamp() throws SQLException {
    final Connection firstConnection = mock(Connection.class);
    final Connection secondConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, firstConnection, 0);
    this.clock.setMillis(50);
    plugin.connect("jdbc:test:", this.movedHost, new Properties(), false, () -> secondConnection);
    this.currentConnection.set(secondConnection);
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
    this.clock.setMillis(99);
    this.currentConnection.set(firstConnection);
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
    this.clock.setMillis(100);

    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
  }

  @Test
  void defaultReconnectUsesCapturedOriginalHostAndSuppressesPendingCall() throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final Connection replacementConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.getCurrentHostSpec()).thenReturn(this.movedHost);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(replacementConnection);
    this.clock.setMillis(100);
    final JdbcCallable<Object, SQLException> pendingCall = mockCallable();

    final SQLException exception = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, pendingCall));

    assertInstanceOf(ConnectionLifetimeSuccessSQLException.class, exception);
    assertEquals(SqlState.COMMUNICATION_LINK_CHANGED.getState(), exception.getSQLState());
    assertSame(replacementConnection, this.currentConnection.get());
    verify(this.pluginService).connect(eq(this.originalHost), any(), same(plugin));
    verify(this.pluginService)
        .setCurrentConnection(eq(replacementConnection), eq(this.originalHost), same(plugin));
    verifyNoInteractions(pendingCall);
  }

  @Test
  void explicitReconnectIsCaseInsensitiveAndResetsTimerAfterSuccessfulSwitch()
      throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final Connection firstReplacement = mock(Connection.class);
    final Connection secondReplacement = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "ReCoNnEcT");
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(firstReplacement, secondReplacement);
    this.clock.setMillis(100);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    final JdbcCallable<Object, SQLException> beforeSecondBoundary = mockCallable();

    this.clock.setMillis(199);
    execute(plugin, NETWORK_METHOD, beforeSecondBoundary);
    verify(beforeSecondBoundary).call();

    this.clock.setMillis(200);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    verify(this.pluginService, times(2)).connect(eq(this.originalHost), any(), same(plugin));
  }

  @Test
  void repeatedReconnectsDiscardClosedConnectionTimestamps() throws Exception {
    final Connection initialConnection = mock(Connection.class);
    final Connection firstReplacement = mock(Connection.class);
    final Connection secondReplacement = mock(Connection.class);
    final Connection thirdReplacement = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, initialConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(firstReplacement, secondReplacement, thirdReplacement);
    when(initialConnection.isClosed()).thenReturn(true);
    when(firstReplacement.isClosed()).thenReturn(true);
    when(secondReplacement.isClosed()).thenReturn(true);

    this.clock.setMillis(100);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    this.clock.setMillis(200);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    this.clock.setMillis(300);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));

    assertEquals(1, trackedConnectionCount(plugin));
    verify(initialConnection).isClosed();
    verify(firstReplacement).isClosed();
    verify(secondReplacement).isClosed();
  }

  @Test
  void reconnectDiscardsTimestampWhenSupersededConnectionCannotReportState() throws Exception {
    final Connection initialConnection = mock(Connection.class);
    final Connection replacementConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, initialConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(replacementConnection);
    when(initialConnection.isClosed()).thenThrow(new SQLException("state unavailable"));

    this.clock.setMillis(100);
    final SQLException exception = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));

    assertInstanceOf(ConnectionLifetimeSuccessSQLException.class, exception);
    assertEquals(1, trackedConnectionCount(plugin));
  }

  @Test
  void reconnectRetainsTimestampOfPreservedOpenConnection() throws Exception {
    final Connection initialConnection = mock(Connection.class);
    final Connection replacementConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, initialConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(replacementConnection);
    when(initialConnection.isClosed()).thenReturn(false);

    this.clock.setMillis(100);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));

    assertEquals(2, trackedConnectionCount(plugin));
  }

  @Test
  void reconnectFailureIsWrappedWithCauseAndDoesNotResetTimer() throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final Connection replacementConnection = mock(Connection.class);
    final SQLException connectFailure = new SQLException("connect failed");
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenThrow(connectFailure)
        .thenReturn(replacementConnection);
    this.clock.setMillis(100);

    final SQLException failure = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    this.clock.setMillis(101);
    final SQLException success = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));

    assertInstanceOf(ConnectionLifetimeFailedSQLException.class, failure);
    assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), failure.getSQLState());
    assertSame(connectFailure, failure.getCause());
    assertInstanceOf(ConnectionLifetimeSuccessSQLException.class, success);
    verify(this.pluginService, times(2)).connect(eq(this.originalHost), any(), same(plugin));
  }

  @Test
  void installationFailureClosesUnattachedReplacementAndPreservesOldTimer() throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final Connection failedReplacement = mock(Connection.class);
    final Connection successfulReplacement = mock(Connection.class);
    final SQLException installationFailure = new SQLException("install failed");
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(failedReplacement, successfulReplacement);
    doThrow(installationFailure)
        .doAnswer(
            invocation -> {
              this.currentConnection.set(invocation.getArgument(0));
              return EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED);
            })
        .when(this.pluginService)
        .setCurrentConnection(any(Connection.class), eq(this.originalHost), same(plugin));
    this.clock.setMillis(100);

    final SQLException failure = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    this.clock.setMillis(101);
    final SQLException success = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));

    assertInstanceOf(ConnectionLifetimeFailedSQLException.class, failure);
    assertSame(installationFailure, failure.getCause());
    assertInstanceOf(ConnectionLifetimeSuccessSQLException.class, success);
    verify(failedReplacement).close();
    verify(successfulReplacement, never()).close();
    verify(this.pluginService, times(2)).connect(eq(this.originalHost), any(), same(plugin));
  }

  @Test
  void partialInstallationFailureClosesBothConnectionsAndMakesPluginTerminal()
      throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final Connection replacementConnection = mock(Connection.class);
    final SQLException installationFailure = new SQLException("install failed after publication");
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(replacementConnection);
    doAnswer(
            invocation -> {
              this.currentConnection.set(invocation.getArgument(0));
              throw installationFailure;
            })
        .when(this.pluginService)
        .setCurrentConnection(any(Connection.class), eq(this.originalHost), same(plugin));
    this.clock.setMillis(100);

    final SQLException failure = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    final JdbcCallable<Object, SQLException> laterCall = mockCallable();
    final SQLException terminalFailure = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, laterCall));

    assertInstanceOf(ConnectionLifetimeFailedSQLException.class, failure);
    assertSame(installationFailure, failure.getCause());
    assertInstanceOf(ConnectionLifetimeSQLException.class, terminalFailure);
    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), terminalFailure.getSQLState());
    verify(replacementConnection).close();
    verify(oldConnection).close();
    verify(this.pluginService, times(1)).connect(eq(this.originalHost), any(), same(plugin));
    verifyNoInteractions(laterCall);
  }

  @Test
  void nullReplacementIsReportedAsReconnectFailure() throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin))).thenReturn(null);
    this.clock.setMillis(100);

    final SQLException exception = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));

    assertInstanceOf(ConnectionLifetimeFailedSQLException.class, exception);
    assertInstanceOf(SQLException.class, exception.getCause());
  }

  @Test
  void localTransactionDefersExpiredReconnectThroughCommitAndRollback() throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final Connection replacementConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(replacementConnection);
    this.clock.setMillis(100);
    this.inTransaction.set(true);
    final JdbcCallable<Object, SQLException> commit = mockCallable();
    final JdbcCallable<Object, SQLException> rollback = mockCallable();

    execute(plugin, JdbcMethod.CONNECTION_COMMIT.methodName, commit);
    execute(plugin, JdbcMethod.CONNECTION_ROLLBACK.methodName, rollback);
    verify(commit).call();
    verify(rollback).call();
    verify(this.pluginService, never()).connect(any(), any(), any());

    this.inTransaction.set(false);
    final JdbcCallable<Object, SQLException> pendingCall = mockCallable();
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, pendingCall));
    verify(this.pluginService).connect(eq(this.originalHost), any(), same(plugin));
    verifyNoInteractions(pendingCall);
  }

  @Test
  void localTransactionDefersExpiredCloseThroughCommit() throws SQLException {
    final Connection connection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, "close");
    initializeConnection(plugin, connection, 0);
    this.clock.setMillis(100);
    this.inTransaction.set(true);
    final JdbcCallable<Object, SQLException> duringTransaction = mockCallable();
    final JdbcCallable<Object, SQLException> commit = mockCallable();

    execute(plugin, NETWORK_METHOD, duringTransaction);
    execute(plugin, JdbcMethod.CONNECTION_COMMIT.methodName, commit);
    verify(duringTransaction).call();
    verify(commit).call();
    verify(connection, never()).close();

    this.inTransaction.set(false);
    final JdbcCallable<Object, SQLException> pendingCall = mockCallable();
    final SQLException exception = assertThrows(
        SQLException.class, () -> execute(plugin, NETWORK_METHOD, pendingCall));
    assertInstanceOf(ConnectionLifetimeSQLException.class, exception);
    verify(connection).close();
    verifyNoInteractions(pendingCall);
  }

  @Test
  void xaTransactionDefersExpiredReconnectUntilXaStateClears() throws SQLException {
    final Connection oldConnection = mock(Connection.class);
    final Connection replacementConnection = mock(Connection.class);
    final ConnectionLifetimeConnectionPlugin plugin = newPlugin(100, null);
    initializeOriginalHost(plugin);
    initializeConnection(plugin, oldConnection, 0);
    when(this.pluginService.connect(eq(this.originalHost), any(), same(plugin)))
        .thenReturn(replacementConnection);
    this.clock.setMillis(100);
    this.xaTransactionActive.set(true);
    final JdbcCallable<Object, SQLException> duringXa = mockCallable();

    execute(plugin, NETWORK_METHOD, duringXa);
    verify(duringXa).call();
    verify(this.pluginService, never()).connect(any(), any(), any());

    this.xaTransactionActive.set(false);
    assertThrows(SQLException.class, () -> execute(plugin, NETWORK_METHOD, mockCallable()));
    verify(this.pluginService).connect(eq(this.originalHost), any(), same(plugin));
  }

  private ConnectionLifetimeConnectionPlugin newPlugin(
      final long lifetimeMs, final String action) {
    final Properties properties = properties(lifetimeMs);
    if (action != null) {
      properties.setProperty(
          ConnectionLifetimeConnectionPlugin.CONNECTION_LIFETIME_ACTION.name, action);
    }
    return new ConnectionLifetimeConnectionPlugin(
        this.servicesContainer, properties, this.clock);
  }

  private void initializeOriginalHost(final ConnectionLifetimeConnectionPlugin plugin)
      throws SQLException {
    final AtomicReference<HostSpec> hostDuringInitialization = new AtomicReference<>(this.movedHost);
    when(this.pluginService.getInitialConnectionHostSpec())
        .thenAnswer(invocation -> hostDuringInitialization.get());
    plugin.initHostProvider(
        "jdbc:test:",
        "jdbc:test://original.example.com",
        new Properties(),
        this.hostListProviderService,
        () -> {
          hostDuringInitialization.set(this.originalHost);
          return null;
        });
    hostDuringInitialization.set(this.movedHost);
  }

  private void initializeConnection(
      final ConnectionLifetimeConnectionPlugin plugin,
      final Connection connection,
      final long creationTimeMs)
      throws SQLException {
    this.clock.setMillis(creationTimeMs);
    plugin.connect(
        "jdbc:test:", this.originalHost, new Properties(), true, () -> connection);
    this.currentConnection.set(connection);
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION));
  }

  private Object execute(
      final ConnectionLifetimeConnectionPlugin plugin,
      final String methodName,
      final JdbcCallable<Object, SQLException> callable)
      throws SQLException {
    return plugin.execute(
        Object.class,
        SQLException.class,
        this.currentConnection.get(),
        methodName,
        callable,
        null);
  }

  @SuppressWarnings("unchecked")
  private JdbcCallable<Object, SQLException> mockCallable() throws SQLException {
    final JdbcCallable<Object, SQLException> callable = mock(JdbcCallable.class);
    when(callable.call()).thenReturn(new Object());
    return callable;
  }

  @SuppressWarnings("unchecked")
  private static int trackedConnectionCount(final ConnectionLifetimeConnectionPlugin plugin)
      throws ReflectiveOperationException {
    final Field field = ConnectionLifetimeConnectionPlugin.class.getDeclaredField(
        "connectionCreationTimes");
    field.setAccessible(true);
    return ((Map<Connection, Long>) field.get(plugin)).size();
  }

  private static Properties properties(final long lifetimeMs) {
    final Properties properties = new Properties();
    properties.setProperty(
        ConnectionLifetimeConnectionPlugin.CONNECTION_LIFETIME_MS.name,
        Long.toString(lifetimeMs));
    return properties;
  }

  private static HostSpec host(final String hostname) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(hostname).build();
  }

  private static class MutableNanoClock implements LongSupplier {

    private long nanoTime;

    @Override
    public long getAsLong() {
      return this.nanoTime;
    }

    void setMillis(final long millis) {
      this.nanoTime = TimeUnit.MILLISECONDS.toNanos(millis);
    }
  }
}
