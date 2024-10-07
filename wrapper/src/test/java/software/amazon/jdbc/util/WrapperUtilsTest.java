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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.CallableStatementWrapper;
import software.amazon.jdbc.wrapper.PreparedStatementWrapper;
import software.amazon.jdbc.wrapper.StatementWrapper;

public class WrapperUtilsTest {

  @Mock ConnectionPluginManager pluginManager;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock Object object;
  private AutoCloseable closeable;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void init() {
    final ReentrantLock testLock = new ReentrantLock();
    closeable = MockitoAnnotations.openMocks(this);

    doAnswer(invocation -> {
      boolean lockIsFree = testLock.tryLock();
      if (!lockIsFree) {
        fail("Lock is in use, should not be attempting to fetch it right now");
      }
      Thread.sleep(3000);
      testLock.unlock();
      return 1;
    }).when(pluginManager).execute(
        any(Class.class),
        any(Class.class),
        any(Object.class),
        argThat(methodName -> !AsynchronousMethodsHelper.ASYNCHRONOUS_METHODS.contains(methodName)),
        any(JdbcCallable.class),
        any(Object[].class));

    when(pluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  Integer callCancelExecuteWithPlugins() {
    return callExecuteWithPlugins("Statement.cancel");
  }

  Integer callExecuteWithPlugins() {
    return callExecuteWithPlugins("methodName");
  }

  Integer callExecuteWithPlugins(String methodName) {
    return WrapperUtils.executeWithPlugins(
        Integer.class,
        pluginManager,
        object,
        methodName,
        () -> 1);
  }

  Integer callCancelExecuteWithPluginsWithException() {
    return callExecuteWithPluginsWithException("Statement.cancel");
  }

  Integer callExecuteWithPluginsWithException() {
    return callExecuteWithPluginsWithException("methodName");
  }

  Integer callExecuteWithPluginsWithException(String methodName) {
    try {
      return WrapperUtils.executeWithPlugins(
          Integer.class,
          SQLException.class,
          pluginManager,
          object,
          methodName,
          () -> 1);
    } catch (SQLException e) {
      fail();
    }

    return null;
  }

  @Test
  void testCancelStatementIsNotBlockedExecute() {
    CompletableFuture.allOf(
        CompletableFuture.supplyAsync(this::callExecuteWithPlugins),
        CompletableFuture.supplyAsync(this::callCancelExecuteWithPlugins)
    ).join();
  }

  @Test
  void testCancelStatementIsNotBlockedExecuteWithException() {
    CompletableFuture.allOf(
        CompletableFuture.supplyAsync(this::callExecuteWithPluginsWithException),
        CompletableFuture.supplyAsync(this::callCancelExecuteWithPluginsWithException)
    ).join();
  }

  @Test
  void getConnectionFromSqlObjectChecksStatementNotClosed() throws Exception {
    final Statement mockClosedStatement = mock(Statement.class);
    when(mockClosedStatement.isClosed()).thenReturn(true);
    when(mockClosedStatement.getConnection()).thenThrow(IllegalStateException.class);

    final ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getStatement()).thenReturn(mockClosedStatement);

    final Connection stmtConn = WrapperUtils.getConnectionFromSqlObject(mockClosedStatement);
    assertNull(stmtConn);
    final Connection rsConn = WrapperUtils.getConnectionFromSqlObject(mockClosedStatement);
    assertNull(rsConn);
  }

  @Test
  void getConnectionFromSqlObjectChecksResultSetNotClosed() throws Exception {
    final ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.isClosed()).thenReturn(true);
    when(mockResultSet.getStatement()).thenThrow(IllegalStateException.class);

    final Connection rsConn = WrapperUtils.getConnectionFromSqlObject(mockResultSet);
    assertNull(rsConn);
  }

  @Test
  void testStatementWrapper() throws InstantiationException {
    ConnectionPluginManager mockPluginManager = mock(ConnectionPluginManager.class);

    assertInstanceOf(StatementWrapper.class,
        WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class,
            mock(Statement.class),
            mockPluginManager));

    assertInstanceOf(PreparedStatementWrapper.class,
        WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class,
            mock(PreparedStatement.class),
            mockPluginManager));

    assertInstanceOf(CallableStatementWrapper.class,
        WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class,
            mock(CallableStatement.class),
            mockPluginManager));
  }
}
