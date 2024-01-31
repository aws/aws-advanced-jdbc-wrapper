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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginService;

public class SessionStateServiceImplTests {

  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock Connection mockNewConnection;
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
