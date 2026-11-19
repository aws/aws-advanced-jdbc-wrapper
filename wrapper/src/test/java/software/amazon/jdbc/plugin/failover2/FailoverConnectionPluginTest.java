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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class FailoverConnectionPluginTest {

  private static final Object[] EMPTY_ARGS = {};
  private static final String ROLLBACK = "Connection.rollback";

  @Mock FullServicesContainer mockContainer;
  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock JdbcCallable<ResultSet, SQLException> mockSqlFunction;

  private final Properties properties = new Properties();
  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    properties.clear();
  }

  @AfterEach
  void cleanUp() throws Exception {
    // Never leak the interrupt flag into other tests.
    Thread.interrupted();
    closeable.close();
  }

  @Test
  void test_execute_closedConnection_interruptedThread_skipFailover() throws SQLException {
    properties.setProperty("skipFailoverOnInterruptedThread", "true");
    when(mockConnection.isClosed()).thenReturn(true);
    final FailoverConnectionPlugin spyPlugin = spy(new FailoverConnectionPlugin(mockContainer, properties));

    Thread.currentThread().interrupt();
    final SQLException exception = assertThrows(SQLException.class, () -> spyPlugin.execute(
        ResultSet.class, SQLException.class, mockConnection, ROLLBACK, mockSqlFunction, EMPTY_ARGS));

    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), exception.getSQLState());
    assertTrue(Thread.currentThread().isInterrupted());
    verify(spyPlugin, never()).failover();
    verify(mockSqlFunction, never()).call();
  }

  @Test
  void test_invalidInvocationOnClosedConnection_interruptedThread_skipFailover() throws SQLException {
    properties.setProperty("skipFailoverOnInterruptedThread", "true");
    final FailoverConnectionPlugin spyPlugin = spy(new FailoverConnectionPlugin(mockContainer, properties));

    Thread.currentThread().interrupt();
    final SQLException exception =
        assertThrows(SQLException.class, spyPlugin::invalidInvocationOnClosedConnection);

    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), exception.getSQLState());
    verify(spyPlugin, never()).failover();
  }

  @Test
  void test_execute_closedConnection_interruptedThread_skipFailoverDisabled() throws SQLException {
    when(mockConnection.isClosed()).thenReturn(true);
    final FailoverConnectionPlugin spyPlugin = spy(new FailoverConnectionPlugin(mockContainer, properties));
    doThrow(new FailoverSuccessSQLException()).when(spyPlugin).failover();

    Thread.currentThread().interrupt();
    assertThrows(FailoverSuccessSQLException.class, () -> spyPlugin.execute(
        ResultSet.class, SQLException.class, mockConnection, ROLLBACK, mockSqlFunction, EMPTY_ARGS));

    verify(spyPlugin).failover();
  }

  @Test
  void test_execute_closedConnection_notInterrupted_skipFailoverEnabled() throws SQLException {
    properties.setProperty("skipFailoverOnInterruptedThread", "true");
    when(mockConnection.isClosed()).thenReturn(true);
    final FailoverConnectionPlugin spyPlugin = spy(new FailoverConnectionPlugin(mockContainer, properties));
    doThrow(new FailoverSuccessSQLException()).when(spyPlugin).failover();

    assertThrows(FailoverSuccessSQLException.class, () -> spyPlugin.execute(
        ResultSet.class, SQLException.class, mockConnection, ROLLBACK, mockSqlFunction, EMPTY_ARGS));

    verify(spyPlugin).failover();
  }
}
