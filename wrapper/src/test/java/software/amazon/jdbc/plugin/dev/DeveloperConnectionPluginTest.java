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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionInfo;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.FullServicesContainerImpl;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

@SuppressWarnings({"resource"})
public class DeveloperConnectionPluginTest {
  @Mock StorageService mockStorageService;
  @Mock MonitorService mockMonitorService;
  @Mock EventPublisher mockEventPublisher;
  @Mock PluginServiceImpl mockPluginService;
  @Mock Connection mockConnection;
  @Mock ConnectionPluginManager mockConnectionPluginManager;
  @Mock ConnectionProvider mockConnectionProvider;
  @Mock HostSpec mockHostSpec;
  @Mock ExceptionSimulatorConnectCallback mockConnectCallback;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;

  @SuppressWarnings("rawtypes")
  @Mock JdbcCallable mockCallable;

  protected Properties props = new Properties();
  protected DeveloperConnectionPlugin plugin = new DeveloperConnectionPlugin();
  protected FullServicesContainer servicesContainer;
  private AutoCloseable closeable;

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    servicesContainer = new FullServicesContainerImpl(
        mockStorageService,
        mockMonitorService,
        mockEventPublisher,
        mockConnectionProvider,
        mockTelemetryFactory,
        mockConnectionPluginManager,
        mockPluginService,
        mockPluginService,
        mockPluginService);

    when(mockConnectionProvider.connect(any(), any(), any(), any(), any()))
        .thenReturn(new ConnectionInfo(mockConnection, false));
    when(mockConnectCallback.getExceptionToRaise(any(), any(), any(), anyBoolean())).thenReturn(null);

    when(mockConnectionPluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
  }

  @Test
  @SuppressWarnings("try")
  public void test_RaiseException() {
    assertDoesNotThrow(this::createStatement);

    final RuntimeException runtimeException = new RuntimeException("test");
    plugin.raiseExceptionOnNextCall(runtimeException);
    Throwable thrownException = assertThrows(RuntimeException.class, this::createStatement);
    assertSame(runtimeException, thrownException);

    assertDoesNotThrow(this::createStatement);
  }

  @SuppressWarnings("unchecked")
  protected void createStatement() throws SQLException {
    plugin.execute(
        Statement.class,
        SQLException.class,
        mockConnection,
        "Connection.createStatement",
        mockCallable,
        new Object[]{});
  }

  @Test
  public void test_RaiseExceptionForMethodName() {
    assertDoesNotThrow(this::createStatement);

    final RuntimeException runtimeException = new RuntimeException("test");
    plugin.raiseExceptionOnNextCall("Connection.createStatement", runtimeException);
    Throwable thrownException = assertThrows(RuntimeException.class, this::createStatement);
    assertSame(runtimeException, thrownException);

    assertDoesNotThrow(this::createStatement);
  }

  @Test
  public void test_RaiseExceptionForAnyMethodName() {
    assertDoesNotThrow(this::createStatement);

    final RuntimeException runtimeException = new RuntimeException("test");
    plugin.raiseExceptionOnNextCall("*", runtimeException);
    Throwable thrownException = assertThrows(RuntimeException.class, this::createStatement);
    assertSame(runtimeException, thrownException);

    assertDoesNotThrow(this::createStatement);
  }

  @Test
  public void test_RaiseExceptionForWrongMethodName() {
    assertDoesNotThrow(this::createStatement);

    final RuntimeException runtimeException = new RuntimeException("test");
    plugin.raiseExceptionOnNextCall("Connection.isClosed", runtimeException);
    assertDoesNotThrow(this::createStatement);

    @SuppressWarnings("unchecked")
    Throwable thrownException = assertThrows(
        RuntimeException.class,
        () -> plugin.execute(
            Boolean.class,
            SQLException.class,
            mockConnection,
            "Connection.isClosed",
            mockCallable,
            new Object[]{}));
    assertSame(runtimeException, thrownException);

    assertDoesNotThrow(this::createStatement);
  }

  @Test
  public void test_RaiseExpectedExceptionClass() {
    assertDoesNotThrow(this::createStatement);

    final SQLException sqlException = new SQLException("test");
    plugin.raiseExceptionOnNextCall(sqlException);
    Throwable thrownException = assertThrows(SQLException.class, this::createStatement);
    assertSame(sqlException, thrownException);

    assertDoesNotThrow(this::createStatement);
  }

  @Test
  public void test_RaiseUnexpectedExceptionClass() {
    assertDoesNotThrow(this::createStatement);

    final Exception exception = new Exception("test");
    plugin.raiseExceptionOnNextCall(exception);
    Throwable thrownException = assertThrows(SQLException.class, this::createStatement);
    assertNotNull(thrownException);
    assertNotSame(exception, thrownException);
    assertInstanceOf(SQLException.class, thrownException);
    assertNotNull(thrownException.getCause());
    assertSame(thrownException.getCause(), exception);

    assertDoesNotThrow(this::createStatement);
  }

  @Test
  public void test_RaiseExceptionOnConnect() {
    final SQLException exception = new SQLException("test");
    ExceptionSimulatorManager.raiseExceptionOnNextConnect(exception);

    Throwable thrownException = assertThrows(SQLException.class, this::connect);
    assertSame(exception, thrownException);

    assertDoesNotThrow(this::connect);
  }

  @SuppressWarnings("unchecked")
  protected Connection connect() throws SQLException {
    return plugin.connect("any-protocol://", mockHostSpec, props, true, mockCallable);
  }

  @Test
  public void test_NoExceptionOnConnectWithCallback() {
    ExceptionSimulatorManager.setCallback(mockConnectCallback);
    assertDoesNotThrow(this::connect);
  }

  @Test
  public void test_RaiseExceptionOnConnectWithCallback() {
    final SQLException exception = new SQLException("test");
    when(mockConnectCallback.getExceptionToRaise(any(), any(), any(), anyBoolean()))
        .thenReturn(exception)
        .thenReturn(null);
    ExceptionSimulatorManager.setCallback(mockConnectCallback);

    Throwable thrownException = assertThrows(SQLException.class, this::connect);
    assertSame(exception, thrownException);

    assertDoesNotThrow(this::connect);
  }
}
