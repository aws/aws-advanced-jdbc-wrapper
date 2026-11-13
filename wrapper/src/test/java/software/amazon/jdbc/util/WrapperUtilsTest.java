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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.CallableStatementWrapper;
import software.amazon.jdbc.wrapper.ConnectionWrapper;
import software.amazon.jdbc.wrapper.PreparedStatementWrapper;
import software.amazon.jdbc.wrapper.ResultSetWrapper;
import software.amazon.jdbc.wrapper.StatementWrapper;

@SuppressWarnings("unchecked")
public class WrapperUtilsTest {

  @Mock ConnectionWrapper mockConnectionWrapper;
  @Mock FullServicesContainer mockServicesContainer;
  @Mock ConnectionPluginManager mockPluginManager;
  @Mock PluginService mockPluginService;
  @Mock PluginManagerService mockPluginManagerService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock Object object;
  ResourceLock testLock;
  private AutoCloseable closeable;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void init() {
    testLock = new ResourceLock();
    closeable = MockitoAnnotations.openMocks(this);

    mockExecuteReturnValue(1);

    when(mockPluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockConnectionWrapper.getServicesContainer()).thenReturn(mockServicesContainer);
    when(mockServicesContainer.getConnectionPluginManager()).thenReturn(mockPluginManager);
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getPluginManagerService()).thenReturn(mockPluginManagerService);
  }

  private void mockExecuteReturnValue(Object returnValue) {
    doAnswer(invocation -> {
      try (ResourceLock  lockIsFree = testLock.obtain()) {
        if (lockIsFree == null) {
          fail("Lock is in use, should not be attempting to fetch it right now");
        }
        Thread.sleep(3000);
        return returnValue;
      }
    }).when(mockPluginManager).execute(
        any(Class.class),
        any(Class.class),
        any(Object.class),
        argThat(jdbcMethod -> jdbcMethod.shouldLockConnection),
        any(JdbcCallable.class),
        any(Object[].class));
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  Integer callCancelExecuteWithPlugins() {
    return callExecuteWithPlugins(JdbcMethod.STATEMENT_CANCEL);
  }

  Integer callExecuteWithPlugins() {
    return callExecuteWithPlugins(JdbcMethod.BLOB_LENGTH);
  }

  Integer callExecuteWithPlugins(JdbcMethod jdbcMethod) {
    return WrapperUtils.executeWithPlugins(
        Integer.class,
        mockConnectionWrapper,
        mockPluginManager,
        object,
        jdbcMethod,
        () -> 1);
  }

  Integer callCancelExecuteWithPluginsWithException() {
    return callExecuteWithPluginsWithException(JdbcMethod.STATEMENT_CANCEL);
  }

  Integer callExecuteWithPluginsWithException() {
    return callExecuteWithPluginsWithException(JdbcMethod.BLOB_LENGTH);
  }

  Integer callExecuteWithPluginsWithException(JdbcMethod jdbcMethod) {
    try {
      return WrapperUtils.executeWithPlugins(
          Integer.class,
          SQLException.class,
          mockConnectionWrapper,
          mockPluginManager,
          object,
          jdbcMethod,
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
  void testStatementWrapper() throws InstantiationException {
    ConnectionPluginManager mockPluginManager = mock(ConnectionPluginManager.class);

    assertInstanceOf(StatementWrapper.class,
        WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class,
            mock(Statement.class),
            mockConnectionWrapper,
            mockPluginManager));

    assertInstanceOf(PreparedStatementWrapper.class,
        WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class,
            mock(PreparedStatement.class),
            mockConnectionWrapper,
            mockPluginManager));

    assertInstanceOf(CallableStatementWrapper.class,
        WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class,
            mock(CallableStatement.class),
            mockConnectionWrapper,
            mockPluginManager));
  }

  @Test
  public void testExecuteAgainstOldConnection() throws Exception {
    final Connection mockOldConnection = mock(Connection.class);
    final Connection mockCurrentConnection = mock(Connection.class);
    final Statement mockOldStatement = mock(Statement.class);
    final ResultSet mockOldResultSet = mock(ResultSet.class);

    // The wrappers are created while the old connection is the current one.
    when(mockConnectionWrapper.getCurrentConnection()).thenReturn(mockOldConnection);
    final StatementWrapper oldStatementWrapper =
        new StatementWrapper(mockOldStatement, mockConnectionWrapper, mockPluginManager);
    final ResultSetWrapper oldResultSetWrapper =
        new ResultSetWrapper(mockOldResultSet, mockConnectionWrapper, mockPluginManager);

    // The internal connection is then swapped out (failover or read/write splitting).
    when(mockConnectionWrapper.getCurrentConnection()).thenReturn(mockCurrentConnection);

    mockExecuteReturnValue("result");
    assertThrows(SQLException.class,
        () -> WrapperUtils.executeWithPluginsWithBoundObject(String.class, Exception.class, mockConnectionWrapper,
            mockPluginManager, oldStatementWrapper, mockOldStatement,
            JdbcMethod.CALLABLESTATEMENT_GETCONNECTION, () -> "result"));
    assertThrows(SQLException.class,
        () -> WrapperUtils.executeWithPluginsWithBoundObject(String.class, Exception.class, mockConnectionWrapper,
            mockPluginManager, oldStatementWrapper, mockOldStatement,
            JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS, () -> "result"));
    assertThrows(SQLException.class,
        () -> WrapperUtils.executeWithPluginsWithBoundObject(String.class, Exception.class, mockConnectionWrapper,
            mockPluginManager, oldResultSetWrapper, mockOldResultSet,
            JdbcMethod.RESULTSET_GETSTATEMENT, () -> "result"));

    // Methods that are not declared with checkBoundedConnection stay usable on a stale object, so
    // that an application can still release it.
    mockExecuteReturnValue(null);
    assertDoesNotThrow(
        () -> WrapperUtils.executeWithPlugins(Void.class, SQLException.class, mockConnectionWrapper, mockPluginManager,
            mockOldConnection, JdbcMethod.CONNECTION_CLOSE, () -> null));
    assertDoesNotThrow(
        () -> WrapperUtils.executeWithPlugins(Void.class, SQLException.class, mockConnectionWrapper, mockPluginManager,
            mockOldConnection, JdbcMethod.CONNECTION_ABORT, () -> null));
    assertDoesNotThrow(
        () -> WrapperUtils.executeWithPlugins(Void.class, SQLException.class, mockConnectionWrapper, mockPluginManager,
            mockOldStatement, JdbcMethod.STATEMENT_CLOSE, () -> null));
    assertDoesNotThrow(
        () -> WrapperUtils.executeWithPlugins(Void.class, SQLException.class, mockConnectionWrapper, mockPluginManager,
            mockOldResultSet, JdbcMethod.RESULTSET_CLOSE, () -> null));
  }

  @Test
  public void testExecuteAgainstConnectionRestoredAfterSwitchingBack() throws Exception {
    // Read/write splitting caches the writer connection and switches back to the very same object,
    // so a statement created before setReadOnly(true) is valid again after setReadOnly(false).
    final Connection mockWriterConnection = mock(Connection.class);
    final Connection mockReaderConnection = mock(Connection.class);
    final Statement mockStatement = mock(Statement.class);

    when(mockConnectionWrapper.getCurrentConnection()).thenReturn(mockWriterConnection);
    final StatementWrapper statementWrapper =
        new StatementWrapper(mockStatement, mockConnectionWrapper, mockPluginManager);

    when(mockConnectionWrapper.getCurrentConnection()).thenReturn(mockReaderConnection);
    mockExecuteReturnValue("result");
    assertThrows(SQLException.class,
        () -> WrapperUtils.executeWithPluginsWithBoundObject(String.class, Exception.class, mockConnectionWrapper,
            mockPluginManager, statementWrapper, mockStatement,
            JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS, () -> "result"));

    when(mockConnectionWrapper.getCurrentConnection()).thenReturn(mockWriterConnection);
    assertDoesNotThrow(
        () -> WrapperUtils.executeWithPluginsWithBoundObject(String.class, Exception.class, mockConnectionWrapper,
            mockPluginManager, statementWrapper, mockStatement,
            JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS, () -> "result"));
  }

  @Test
  public void testResultSetBoundToPhysicalConnectionIsNotStale() throws Exception {
    // Regression test for the Array#getResultSet false positive: the target driver builds the
    // array's ResultSet on the physical connection, while the wrapper's current connection is a
    // pooled handle wrapping that same physical connection. It is the same session, so the
    // ResultSet must remain usable.
    final Connection physicalConnection = mock(Connection.class);
    final Connection pooledConnection = mock(Connection.class);
    final Statement arrayStatement = mock(Statement.class);
    final ResultSet arrayResultSet = mock(ResultSet.class);

    when(arrayStatement.getConnection()).thenReturn(physicalConnection);
    when(arrayResultSet.getStatement()).thenReturn(arrayStatement);
    when(mockConnectionWrapper.getCurrentConnection()).thenReturn(pooledConnection);

    final ResultSetWrapper arrayResultSetWrapper =
        new ResultSetWrapper(arrayResultSet, mockConnectionWrapper, mockPluginManager);

    mockExecuteReturnValue("result");
    assertDoesNotThrow(
        () -> WrapperUtils.executeWithPluginsWithBoundObject(String.class, Exception.class, mockConnectionWrapper,
            mockPluginManager, arrayResultSetWrapper, arrayResultSet,
            JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS, () -> "result"));
  }

  // Flags live in the test class, not in the probe class, so reading them never loads or
  // initializes the probe.
  static final AtomicBoolean NOT_A_DATASOURCE_STATIC_INIT_RAN = new AtomicBoolean(false);
  static final AtomicBoolean NOT_A_DATASOURCE_CONSTRUCTED = new AtomicBoolean(false);

  /**
   * Deliberately NOT a {@link DataSource}. createInstance(..., DataSource.class) must reject it
   * before its static initializer or constructor runs.
   */
  public static class NotADataSource {
    static {
      NOT_A_DATASOURCE_STATIC_INIT_RAN.set(true);
    }

    public NotADataSource() {
      NOT_A_DATASOURCE_CONSTRUCTED.set(true);
    }
  }

  @Test
  void testCreateInstanceByNameRejectsWrongTypeBeforeRunningAnyCode() {
    // Build the name as a string: a NotADataSource.class literal would not initialize it either,
    // but the string form mirrors the real call path (targetDataSourceClassName).
    final String className = WrapperUtilsTest.class.getName() + "$NotADataSource";

    final InstantiationException ex = assertThrows(InstantiationException.class,
        () -> WrapperUtils.createInstance(className, DataSource.class));
    assertTrue(ex.getMessage().contains(className));
    assertTrue(ex.getMessage().contains(DataSource.class.getName()));

    assertFalse(NOT_A_DATASOURCE_STATIC_INIT_RAN.get(), "static initializer must not run");
    assertFalse(NOT_A_DATASOURCE_CONSTRUCTED.get(), "constructor must not run");
  }

  @Test
  void testCreateInstanceByClassRejectsWrongType() {
    final InstantiationException ex = assertThrows(InstantiationException.class,
        () -> WrapperUtils.createInstance(String.class, DataSource.class, (Class<?>[]) null));
    assertTrue(ex.getMessage().contains(String.class.getName()));
  }

  @Test
  void testCreateInstanceByNameAcceptsMatchingType() throws Exception {
    final Object instance = WrapperUtils.createInstance(
        java.util.ArrayList.class.getName(), java.util.List.class);
    assertInstanceOf(java.util.ArrayList.class, instance);
  }
}
