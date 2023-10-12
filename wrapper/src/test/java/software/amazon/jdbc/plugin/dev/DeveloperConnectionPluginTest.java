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

package software.amazon.jdbc.plugin.dev;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.DialectCodes;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class DeveloperConnectionPluginTest {

  @Mock ConnectionProvider mockConnectionProvider;
  @Mock Connection mockConnection;
  @Mock PluginServiceImpl mockService;
  @Mock ConnectionPluginManager mockConnectionPluginManager;
  @Mock ExceptionSimulatorConnectCallback mockConnectCallback;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;

  private AutoCloseable closeable;

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockConnectionProvider.connect(any(), any(), any(), any())).thenReturn(mockConnection);
    when(mockConnectCallback.getExceptionToRaise(any(), any(), any(), anyBoolean())).thenReturn(null);

    when(mockService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockConnectionPluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
  }

  @Test
  public void test_RaiseException() throws SQLException {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);
    try (ConnectionWrapper wrapper = new ConnectionWrapper(
        props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory)) {

      ExceptionSimulator simulator = wrapper.unwrap(ExceptionSimulator.class);
      assertNotNull(simulator);

      assertDoesNotThrow(() -> wrapper.createStatement());

      final RuntimeException runtimeException = new RuntimeException("test");
      simulator.raiseExceptionOnNextCall(runtimeException);
      Throwable thrownException = assertThrows(RuntimeException.class, wrapper::createStatement);
      assertSame(runtimeException, thrownException);

      assertDoesNotThrow(() -> wrapper.createStatement());
    }
  }

  @Test
  public void test_RaiseExceptionForMethodName() throws SQLException {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);
    try (ConnectionWrapper wrapper = new ConnectionWrapper(
        props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory)) {

      ExceptionSimulator simulator = wrapper.unwrap(ExceptionSimulator.class);
      assertNotNull(simulator);

      assertDoesNotThrow(() -> wrapper.createStatement());

      final RuntimeException runtimeException = new RuntimeException("test");
      simulator.raiseExceptionOnNextCall("Connection.createStatement", runtimeException);
      Throwable thrownException = assertThrows(RuntimeException.class, wrapper::createStatement);
      assertSame(runtimeException, thrownException);

      assertDoesNotThrow(() -> wrapper.createStatement());
    }
  }

  @Test
  public void test_RaiseExceptionForAnyMethodName() throws SQLException {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);
    try (ConnectionWrapper wrapper = new ConnectionWrapper(
        props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory)) {

      ExceptionSimulator simulator = wrapper.unwrap(ExceptionSimulator.class);
      assertNotNull(simulator);

      assertDoesNotThrow(() -> wrapper.createStatement());

      final RuntimeException runtimeException = new RuntimeException("test");
      simulator.raiseExceptionOnNextCall("*", runtimeException);
      Throwable thrownException = assertThrows(RuntimeException.class, wrapper::createStatement);
      assertSame(runtimeException, thrownException);

      assertDoesNotThrow(() -> wrapper.createStatement());
    }
  }

  @Test
  public void test_RaiseExceptionForWrongMethodName() throws SQLException {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);
    try (ConnectionWrapper wrapper = new ConnectionWrapper(
        props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory)) {

      ExceptionSimulator simulator = wrapper.unwrap(ExceptionSimulator.class);
      assertNotNull(simulator);

      assertDoesNotThrow(() -> wrapper.createStatement());

      final RuntimeException runtimeException = new RuntimeException("test");
      simulator.raiseExceptionOnNextCall("Connection.isClosed", runtimeException);
      assertDoesNotThrow(() -> wrapper.createStatement());

      Throwable thrownException = assertThrows(RuntimeException.class, wrapper::isClosed);
      assertSame(runtimeException, thrownException);

      assertDoesNotThrow(() -> wrapper.createStatement());
    }
  }

  @Test
  public void test_RaiseExpectedExceptionClass() throws SQLException {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);
    try (ConnectionWrapper wrapper = new ConnectionWrapper(
        props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory)) {

      ExceptionSimulator simulator = wrapper.unwrap(ExceptionSimulator.class);
      assertNotNull(simulator);

      assertDoesNotThrow(() -> wrapper.createStatement());

      final SQLException sqlException = new SQLException("test");
      simulator.raiseExceptionOnNextCall(sqlException);
      Throwable thrownException = assertThrows(SQLException.class, wrapper::createStatement);
      assertSame(sqlException, thrownException);

      assertDoesNotThrow(() -> wrapper.createStatement());
    }
  }

  @Test
  public void test_RaiseUnexpectedExceptionClass() throws SQLException {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);
    try (ConnectionWrapper wrapper = new ConnectionWrapper(
        props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory)) {

      ExceptionSimulator simulator = wrapper.unwrap(ExceptionSimulator.class);
      assertNotNull(simulator);

      assertDoesNotThrow(() -> wrapper.createStatement());

      final Exception exception = new Exception("test");
      simulator.raiseExceptionOnNextCall(exception);
      Throwable thrownException = assertThrows(SQLException.class, wrapper::createStatement);
      assertNotNull(thrownException);
      assertNotSame(exception, thrownException);
      assertTrue(thrownException instanceof SQLException);
      assertNotNull(thrownException.getCause());
      assertSame(thrownException.getCause(), exception);

      assertDoesNotThrow(() -> wrapper.createStatement());
    }
  }

  @Test
  public void test_RaiseExceptionOnConnect() {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);

    final SQLException exception = new SQLException("test");
    ExceptionSimulatorManager.raiseExceptionOnNextConnect(exception);

    Throwable thrownException = assertThrows(
        SQLException.class,
        () -> new ConnectionWrapper(
            props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory));
    assertSame(exception, thrownException);

    assertDoesNotThrow(
        () -> new ConnectionWrapper(
            props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory));
  }

  @Test
  public void test_NoExceptionOnConnectWithCallback() {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);

    ExceptionSimulatorManager.setCallback(mockConnectCallback);

    assertDoesNotThrow(
        () -> new ConnectionWrapper(
            props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory));
  }

  @Test
  public void test_RaiseExceptionOnConnectWithCallback() {

    final Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev");
    props.put(DialectManager.DIALECT.name, DialectCodes.PG);

    final SQLException exception = new SQLException("test");
    when(mockConnectCallback.getExceptionToRaise(any(), any(), any(), anyBoolean()))
        .thenReturn(exception)
        .thenReturn(null);
    ExceptionSimulatorManager.setCallback(mockConnectCallback);

    Throwable thrownException = assertThrows(
        SQLException.class,
        () -> new ConnectionWrapper(
            props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory));
    assertSame(exception, thrownException);

    assertDoesNotThrow(
        () -> new ConnectionWrapper(
            props, "any-protocol://any-host/", mockConnectionProvider, mockTelemetryFactory));
  }
}
