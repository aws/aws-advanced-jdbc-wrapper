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

package software.amazon.jdbc.states;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public class SessionStateServiceImplTests {

  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock Connection mockNewConnection;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  Properties props = new Properties();
  SessionStateService sessionStateService;
  private AutoCloseable closeable;

  @Captor ArgumentCaptor<Boolean> captorReadOnly;
  @Captor ArgumentCaptor<Boolean> captorAutoCommit;
  @Captor ArgumentCaptor<String> captorCatalog;
  @Captor ArgumentCaptor<String> captorSchema;
  @Captor ArgumentCaptor<Integer> captorHoldability;
  @Captor ArgumentCaptor<Integer> captorNetworkTimeout;
  @Captor ArgumentCaptor<Integer> captorTransactionIsolation;
  @Captor ArgumentCaptor<Map<String, Class<?>>> captorTypeMap;

  @AfterEach
  void afterEach() throws Exception {
    closeable.close();
    sessionStateService = null;
    props.clear();
  }

  @BeforeEach
  void beforeEach() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    sessionStateService = spy(new SessionStateServiceImpl(mockPluginService, props));
  }

  @ParameterizedTest
  @MethodSource("getBoolArguments")
  public void test_ResetConnection_ReadOnly(
      boolean pristineValue, boolean value, boolean shouldReset) throws SQLException {

    when(mockConnection.isReadOnly()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getReadOnly());
    sessionStateService.setupPristineReadOnly();
    sessionStateService.setReadOnly(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getReadOnly());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0)).setReadOnly(captorReadOnly.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorReadOnly.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getBoolArguments")
  public void test_ResetConnection_AutoCommit(
      boolean pristineValue, boolean value, boolean shouldReset) throws SQLException {

    when(mockConnection.getAutoCommit()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getAutoCommit());
    sessionStateService.setupPristineAutoCommit();
    sessionStateService.setAutoCommit(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getAutoCommit());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0)).setAutoCommit(captorAutoCommit.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorAutoCommit.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getStringArguments")
  public void test_ResetConnection_Catalog(
      String pristineValue, String value, boolean shouldReset) throws SQLException {

    when(mockConnection.getCatalog()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getCatalog());
    sessionStateService.setupPristineCatalog();
    sessionStateService.setCatalog(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getCatalog());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0)).setCatalog(captorCatalog.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorCatalog.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getStringArguments")
  public void test_ResetConnection_Schema(
      String pristineValue, String value, boolean shouldReset) throws SQLException {

    when(mockConnection.getSchema()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getSchema());
    sessionStateService.setupPristineSchema();
    sessionStateService.setSchema(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getSchema());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0)).setSchema(captorSchema.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorSchema.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getIntegerArguments")
  public void test_ResetConnection_Holdability(
      int pristineValue, int value, boolean shouldReset) throws SQLException {

    when(mockConnection.getHoldability()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getHoldability());
    sessionStateService.setupPristineHoldability();
    sessionStateService.setHoldability(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getHoldability());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0)).setHoldability(captorHoldability.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorHoldability.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getIntegerArguments")
  public void test_ResetConnection_NetworkTimeout(
      int pristineValue, int value, boolean shouldReset) throws SQLException {

    when(mockConnection.getNetworkTimeout()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getNetworkTimeout());
    sessionStateService.setupPristineNetworkTimeout();
    sessionStateService.setNetworkTimeout(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getNetworkTimeout());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0)).setNetworkTimeout(any(), captorNetworkTimeout.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorNetworkTimeout.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getIntegerArguments")
  public void test_ResetConnection_TransactionIsolation(
      int pristineValue, int value, boolean shouldReset) throws SQLException {

    when(mockConnection.getTransactionIsolation()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getTransactionIsolation());
    sessionStateService.setupPristineTransactionIsolation();
    sessionStateService.setTransactionIsolation(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getTransactionIsolation());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0))
        .setTransactionIsolation(captorTransactionIsolation.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorTransactionIsolation.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getTypeMapArguments")
  public void test_ResetConnection_TypeMap(
      Map<String, Class<?>> pristineValue, Map<String, Class<?>> value, boolean shouldReset) throws SQLException {

    when(mockConnection.getTypeMap()).thenReturn(pristineValue);
    assertEquals(Optional.empty(), sessionStateService.getTypeMap());
    sessionStateService.setupPristineTypeMap();
    sessionStateService.setTypeMap(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getTypeMap());

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(shouldReset ? 1 : 0)).setTypeMap(captorTypeMap.capture());
    if (shouldReset) {
      assertEquals(pristineValue, captorTypeMap.getValue());
    }
  }

  @ParameterizedTest
  @MethodSource("getBoolArguments")
  public void test_TransferToNewConnection_ReadOnly(boolean pristineValue, boolean value) throws SQLException {
    when(mockConnection.isReadOnly()).thenReturn(pristineValue);
    when(mockNewConnection.isReadOnly()).thenReturn(pristineValue);
    sessionStateService.setReadOnly(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getReadOnly());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setReadOnly(captorReadOnly.capture());
    assertEquals(value, captorReadOnly.getValue());
  }

  @ParameterizedTest
  @MethodSource("getBoolArguments")
  public void test_TransferToNewConnection_AutoCommit(boolean pristineValue, boolean value) throws SQLException {
    when(mockConnection.getAutoCommit()).thenReturn(pristineValue);
    when(mockNewConnection.getAutoCommit()).thenReturn(pristineValue);
    sessionStateService.setAutoCommit(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getAutoCommit());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setAutoCommit(captorAutoCommit.capture());
    assertEquals(value, captorAutoCommit.getValue());
  }

  @ParameterizedTest
  @MethodSource("getStringArguments")
  public void test_TransferToNewConnection_Catalog(String pristineValue, String value) throws SQLException {
    when(mockConnection.getCatalog()).thenReturn(pristineValue);
    when(mockNewConnection.getCatalog()).thenReturn(pristineValue);
    sessionStateService.setCatalog(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getCatalog());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setCatalog(captorCatalog.capture());
    assertEquals(value, captorCatalog.getValue());
  }

  @ParameterizedTest
  @MethodSource("getStringArguments")
  public void test_TransferToNewConnection_Schema(String pristineValue, String value) throws SQLException {
    when(mockConnection.getSchema()).thenReturn(pristineValue);
    when(mockNewConnection.getSchema()).thenReturn(pristineValue);
    sessionStateService.setSchema(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getSchema());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setSchema(captorSchema.capture());
    assertEquals(value, captorSchema.getValue());
  }

  @ParameterizedTest
  @MethodSource("getIntegerArguments")
  public void test_TransferToNewConnection_Holdability(int pristineValue, int value) throws SQLException {
    when(mockConnection.getHoldability()).thenReturn(pristineValue);
    when(mockNewConnection.getHoldability()).thenReturn(pristineValue);
    sessionStateService.setHoldability(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getHoldability());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setHoldability(captorHoldability.capture());
    assertEquals(value, captorHoldability.getValue());
  }

  @ParameterizedTest
  @MethodSource("getIntegerArguments")
  public void test_TransferToNewConnection_NetworkTimeout(int pristineValue, int value) throws SQLException {
    when(mockConnection.getNetworkTimeout()).thenReturn(pristineValue);
    when(mockNewConnection.getNetworkTimeout()).thenReturn(pristineValue);
    sessionStateService.setNetworkTimeout(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getNetworkTimeout());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setNetworkTimeout(any(), captorNetworkTimeout.capture());
    assertEquals(value, captorNetworkTimeout.getValue());
  }

  @ParameterizedTest
  @MethodSource("getIntegerArguments")
  public void test_TransferToNewConnection_TransactionIsolation(int pristineValue, int value) throws SQLException {
    when(mockConnection.getTransactionIsolation()).thenReturn(pristineValue);
    when(mockNewConnection.getTransactionIsolation()).thenReturn(pristineValue);
    sessionStateService.setTransactionIsolation(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getTransactionIsolation());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1))
        .setTransactionIsolation(captorTransactionIsolation.capture());
    assertEquals(value, captorTransactionIsolation.getValue());
  }

  @ParameterizedTest
  @MethodSource("getTypeMapArguments")
  public void test_TransferToNewConnection_TypeMap(
      Map<String, Class<?>> pristineValue, Map<String, Class<?>> value) throws SQLException {

    when(mockConnection.getTypeMap()).thenReturn(pristineValue);
    when(mockNewConnection.getTypeMap()).thenReturn(pristineValue);
    sessionStateService.setTypeMap(value);
    assertEquals(Optional.ofNullable(value), sessionStateService.getTypeMap());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setTypeMap(captorTypeMap.capture());
    assertEquals(value, captorTypeMap.getValue());
  }

  /**
   * Makes the mocked plugin service report a mutable current connection, as PluginServiceImpl does.
   */
  private AtomicReference<Connection> trackCurrentConnection(final Connection initialConnection) {
    final AtomicReference<Connection> currentConnection = new AtomicReference<>(initialConnection);
    when(mockPluginService.getCurrentConnection()).thenAnswer(invocation -> currentConnection.get());
    return currentConnection;
  }

  /**
   * Switches to the given connection the way PluginServiceImpl.setCurrentConnection does.
   */
  private void switchTo(final AtomicReference<Connection> currentConnection, final Connection connection)
      throws SQLException {
    sessionStateService.begin();
    try {
      currentConnection.set(connection);
      sessionStateService.applyCurrentSessionState(connection);
    } finally {
      sessionStateService.complete();
    }
  }

  @Test
  void test_TransferToNewConnection_PristineValuesReadOncePerConnection() throws SQLException {
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    when(mockConnection.isReadOnly()).thenReturn(false);
    when(mockNewConnection.isReadOnly()).thenReturn(false);
    when(mockConnection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_REPEATABLE_READ);
    when(mockNewConnection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_REPEATABLE_READ);

    sessionStateService.setupPristineReadOnly();
    sessionStateService.setReadOnly(true);
    sessionStateService.setupPristineTransactionIsolation();
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    switchTo(currentConnection, mockNewConnection);
    switchTo(currentConnection, mockConnection);
    switchTo(currentConnection, mockNewConnection);
    switchTo(currentConnection, mockConnection);

    verify(mockConnection, times(1)).isReadOnly();
    verify(mockNewConnection, times(1)).isReadOnly();
    verify(mockConnection, times(1)).getTransactionIsolation();
    verify(mockNewConnection, times(1)).getTransactionIsolation();
    // The values are still transferred on every switch.
    verify(mockConnection, times(2)).setReadOnly(true);
    verify(mockNewConnection, times(2)).setReadOnly(true);
    verify(mockConnection, times(2)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    verify(mockNewConnection, times(2)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  void test_TransferToNewConnection_PristineValueOfNewConnectionIsRead() throws SQLException {
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    when(mockNewConnection.isReadOnly()).thenReturn(true);
    sessionStateService.setReadOnly(false);

    switchTo(currentConnection, mockNewConnection);

    // The pristine value comes from the connection switched to, not from the previous one.
    verify(mockConnection, never()).isReadOnly();
    verify(mockNewConnection, times(1)).isReadOnly();
    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();
    verify(mockNewConnection, times(1)).setReadOnly(true);
  }

  @Test
  void test_ResetConnection_RestoresOriginalValueOfThatConnection() throws SQLException {
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    // After the wrapper sets readOnly=true on the connection, the driver reports true: a second read
    // would return the applied value, not the original one.
    when(mockConnection.isReadOnly()).thenReturn(false, true);
    when(mockNewConnection.isReadOnly()).thenReturn(false);

    sessionStateService.setupPristineReadOnly();
    sessionStateService.setReadOnly(true);
    switchTo(currentConnection, mockNewConnection);
    switchTo(currentConnection, mockConnection);

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockConnection);
    sessionStateService.complete();

    verify(mockConnection, times(1)).isReadOnly();
    verify(mockConnection, times(1)).setReadOnly(false);
  }

  @Test
  void test_ResetConnection_UsesOriginalValueRecordedForThatConnection() throws SQLException {
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    when(mockConnection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_REPEATABLE_READ);
    when(mockNewConnection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    switchTo(currentConnection, mockNewConnection);
    switchTo(currentConnection, mockConnection);
    // Restoring the connection switched away from uses that connection's own original value.
    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();

    verify(mockNewConnection, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  void test_Reset_ClearsRecordedPristineValues() throws SQLException {
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    when(mockConnection.isReadOnly()).thenReturn(false);
    when(mockNewConnection.isReadOnly()).thenReturn(false);
    sessionStateService.setReadOnly(true);
    switchTo(currentConnection, mockNewConnection);
    switchTo(currentConnection, mockConnection);
    verify(mockConnection, times(1)).isReadOnly();

    sessionStateService.reset();
    sessionStateService.setReadOnly(true);
    switchTo(currentConnection, mockNewConnection);

    verify(mockNewConnection, times(2)).isReadOnly();
  }

  @Test
  void test_TransferToNewConnection_ResetOnCloseDisabled_NoPristineReads() throws SQLException {
    props.setProperty(PropertyDefinition.RESET_SESSION_STATE_ON_CLOSE.name, "false");
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    sessionStateService.setReadOnly(true);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    switchTo(currentConnection, mockNewConnection);
    switchTo(currentConnection, mockConnection);
    switchTo(currentConnection, mockNewConnection);

    verify(mockConnection, never()).isReadOnly();
    verify(mockNewConnection, never()).isReadOnly();
    verify(mockConnection, never()).getTransactionIsolation();
    verify(mockNewConnection, never()).getTransactionIsolation();
    verify(mockNewConnection, times(2)).setReadOnly(true);
  }

  /**
   * Switches to the given connection the way PluginServiceImpl.setCurrentConnectionWithoutSessionStateTransfer
   * does.
   */
  private void switchWithoutTransferTo(
      final AtomicReference<Connection> currentConnection, final Connection connection) throws SQLException {
    sessionStateService.begin();
    try {
      currentConnection.set(connection);
      sessionStateService.onSwitchWithoutSessionStateTransfer(connection);
    } finally {
      sessionStateService.complete();
    }
  }

  private SessionState sessionStateOf(final SessionStateService service) {
    return ((SessionStateServiceImpl) service).sessionState;
  }

  @Test
  void test_SwitchWithoutTransfer_DoesNotTouchConnection() throws SQLException {
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    final AtomicReference<Boolean> customFuncCalled = new AtomicReference<>(false);
    Driver.setTransferSessionStateOnSwitchFunc((sessionState, newConnection) -> {
      customFuncCalled.set(true);
      return false;
    });
    try {
      sessionStateService.setAutoCommit(false);
      sessionStateService.setReadOnly(true);
      sessionStateService.setCatalog("catalog");
      sessionStateService.setSchema("schema");
      sessionStateService.setHoldability(1);
      sessionStateService.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      sessionStateService.setNetworkTimeout(1000);
      sessionStateService.setTypeMap(new HashMap<>());

      switchWithoutTransferTo(currentConnection, mockNewConnection);

      verifyNoInteractions(mockNewConnection);
      assertFalse(customFuncCalled.get());
      // The tracked values are kept.
      assertEquals(Optional.of(false), sessionStateService.getAutoCommit());
      assertEquals(Optional.of(true), sessionStateService.getReadOnly());
      assertEquals(Optional.of("catalog"), sessionStateService.getCatalog());
      assertEquals(Optional.of("schema"), sessionStateService.getSchema());
      assertEquals(Optional.of(1), sessionStateService.getHoldability());
      assertEquals(Optional.of(Connection.TRANSACTION_SERIALIZABLE), sessionStateService.getTransactionIsolation());
      assertEquals(Optional.of(1000), sessionStateService.getNetworkTimeout());
      assertEquals(Optional.of(new HashMap<>()), sessionStateService.getTypeMap());
    } finally {
      Driver.resetTransferSessionStateOnSwitchFunc();
    }
  }

  @Test
  void test_SwitchWithoutTransfer_UsesRecordedPristineValues() throws SQLException {
    final Connection thirdConnection = mock(Connection.class);
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    when(mockNewConnection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
    when(thirdConnection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_UNCOMMITTED);
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    switchTo(currentConnection, mockNewConnection);
    switchTo(currentConnection, thirdConnection);
    switchWithoutTransferTo(currentConnection, mockNewConnection);

    // The pristine value is the one recorded for the connection switched to, and it is not read again.
    assertEquals(Optional.of(Connection.TRANSACTION_READ_COMMITTED),
        sessionStateOf(sessionStateService).transactionIsolation.getPristineValue());
    verify(mockNewConnection, times(1)).getTransactionIsolation();
    verify(mockNewConnection, times(1)).setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockNewConnection);
    sessionStateService.complete();
    verify(mockNewConnection, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    // A connection never visited has no recorded pristine value, so nothing is restored on it.
    switchWithoutTransferTo(currentConnection, mockConnection);
    assertEquals(Optional.empty(),
        sessionStateOf(sessionStateService).transactionIsolation.getPristineValue());
    sessionStateService.begin();
    sessionStateService.applyPristineSessionState(mockConnection);
    sessionStateService.complete();
    verifyNoInteractions(mockConnection);
  }

  @Test
  void test_SwitchWithoutTransfer_ResetsAuthorizationState() throws SQLException {
    final AuthorizationSessionState authorizationState = new AuthorizationSessionState(
        "application_user", "tenant_a", "public", "[\"public\"]");
    when(mockTargetDriverDialect.readAuthorizationSessionState(mockConnection))
        .thenReturn(Optional.of(authorizationState));
    sessionStateService.refreshAuthorizationState();
    assertEquals(Optional.of(authorizationState), sessionStateService.getAuthorizationState());

    sessionStateService.begin();
    sessionStateService.onSwitchWithoutSessionStateTransfer(mockNewConnection);
    sessionStateService.complete();

    assertEquals(Optional.empty(), sessionStateService.getAuthorizationState());
  }

  @Test
  void test_SwitchWithoutTransfer_TrackingContinues() throws SQLException {
    props.setProperty("rwTransferSessionStateOnSwitch", "false");
    final Connection thirdConnection = mock(Connection.class);
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    switchWithoutTransferTo(currentConnection, mockNewConnection);

    sessionStateService.setAutoCommit(false);
    sessionStateService.setCatalog("c2");
    sessionStateService.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    switchWithoutTransferTo(currentConnection, mockConnection);

    assertEquals(Optional.of(false), sessionStateService.getAutoCommit());
    assertEquals(Optional.of("c2"), sessionStateService.getCatalog());
    assertEquals(Optional.of(Connection.TRANSACTION_SERIALIZABLE), sessionStateService.getTransactionIsolation());

    // A later switch that transfers the session state applies the tracked values.
    switchTo(currentConnection, thirdConnection);
    verify(thirdConnection, times(1)).setAutoCommit(false);
    verify(thirdConnection, times(1)).setCatalog("c2");
    verify(thirdConnection, times(1)).setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
  }

  @Test
  void test_SwitchWithoutTransfer_GlobalTransferDisabled_NoOp() throws SQLException {
    props.setProperty(PropertyDefinition.TRANSFER_SESSION_STATE_ON_SWITCH.name, "false");
    final AtomicReference<Connection> currentConnection = trackCurrentConnection(mockConnection);
    sessionStateService.setupPristineReadOnly(true);

    switchWithoutTransferTo(currentConnection, mockNewConnection);

    verifyNoInteractions(mockNewConnection);
    assertEquals(Optional.of(true), sessionStateOf(sessionStateService).readOnly.getPristineValue());
  }

  @Test
  void test_SessionStateServiceDefault_SwitchWithoutTransfer_Delegates() throws SQLException {
    final SessionStateService service = mock(SessionStateService.class, CALLS_REAL_METHODS);
    doNothing().when(service).applyCurrentSessionState(any());

    service.onSwitchWithoutSessionStateTransfer(mockNewConnection);

    verify(service, times(1)).applyCurrentSessionState(mockNewConnection);
  }

  @Test
  void test_AuthorizationStateTrackingCanBeEnabled() {
    assertFalse(sessionStateService.isAuthorizationStateTrackingEnabled());

    sessionStateService.enableAuthorizationStateTracking();

    assertTrue(sessionStateService.isAuthorizationStateTrackingEnabled());
  }

  @Test
  void test_AuthorizationStateIsReadAndInvalidatedOnConnectionSwitch() throws SQLException {
    final AuthorizationSessionState authorizationState = new AuthorizationSessionState(
        "application_user",
        "tenant_a",
        "\"tenant_a\", public",
        "[\"pg_catalog\",\"tenant_a\",\"public\"]");
    when(mockTargetDriverDialect.readAuthorizationSessionState(mockConnection))
        .thenReturn(Optional.of(authorizationState));

    sessionStateService.refreshAuthorizationState();

    assertEquals(Optional.of(authorizationState), sessionStateService.getAuthorizationState());

    sessionStateService.begin();
    sessionStateService.applyCurrentSessionState(mockNewConnection);
    sessionStateService.complete();

    assertEquals(Optional.empty(), sessionStateService.getAuthorizationState());
    verify(mockTargetDriverDialect).readAuthorizationSessionState(mockConnection);
  }

  @Test
  void test_UntrackedAuthorizationStateRemainsUnsafeUntilReset() {
    assertFalse(sessionStateService.hasUntrackedAuthorizationState());

    sessionStateService.markAuthorizationStateUntracked();

    assertTrue(sessionStateService.hasUntrackedAuthorizationState());
    assertEquals(Optional.empty(), sessionStateService.getAuthorizationState());

    sessionStateService.reset();

    assertFalse(sessionStateService.hasUntrackedAuthorizationState());
  }

  static Stream<Arguments> getBoolArguments() {
    return Stream.of(
        Arguments.of(false, false, false),
        Arguments.of(true, false, true),
        Arguments.of(false, true, true),
        Arguments.of(true, true, false)
    );
  }

  static Stream<Arguments> getStringArguments() {
    return Stream.of(
        Arguments.of("a", "a", false),
        Arguments.of("b", "a", true),
        Arguments.of("a", "b", true),
        Arguments.of("b", "b", false)
    );
  }

  static Stream<Arguments> getIntegerArguments() {
    return Stream.of(
        Arguments.of(1, 1, false),
        Arguments.of(2, 1, true),
        Arguments.of(1, 2, true),
        Arguments.of(2, 2, false));
  }

  static Stream<Arguments> getTypeMapArguments() {
    final Map<String, Class<?>> a1 = new HashMap<>();
    final Map<String, Class<?>> a2 = new HashMap<>();
    final Map<String, Class<?>> b1 = new HashMap<>();
    b1.put("test", Object.class); // actual mapping isn't important here
    final Map<String, Class<?>> b2 = new HashMap<>();
    b2.put("test", Object.class); // actual mapping isn't important here

    return Stream.of(
        Arguments.of(a1, a2, false),
        Arguments.of(b1, a2, true),
        Arguments.of(a1, b2, true),
        Arguments.of(b1, b2, false));
  }
}
