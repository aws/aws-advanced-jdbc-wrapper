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

package software.amazon.jdbc.wrapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionBoundObject;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Tests the stale-object detection of the wrapper objects bound to a connection
 * ({@link ConnectionBoundObject}): a wrapper records the internal connection it was created on, and
 * a method declared with {@code checkBoundedConnection} is rejected once that connection has been
 * swapped out.
 */
public class ConnectionBoundObjectTest {

  @Mock private ConnectionWrapper connectionWrapper;
  @Mock private FullServicesContainer servicesContainer;
  @Mock private ConnectionPluginManager pluginManager;
  @Mock private PluginService pluginService;
  @Mock private PluginManagerService pluginManagerService;
  @Mock private TelemetryFactory telemetryFactory;

  /** The connection the wrapper currently reports as the internal (current) connection. */
  private final AtomicReference<Connection> currentConnection = new AtomicReference<>();

  private Connection connectionA;
  private Connection connectionB;
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);

    connectionA = mock(Connection.class);
    connectionB = mock(Connection.class);
    currentConnection.set(connectionA);

    when(connectionWrapper.getCurrentConnection()).thenAnswer(i -> currentConnection.get());
    when(connectionWrapper.getServicesContainer()).thenReturn(servicesContainer);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getPluginManagerService()).thenReturn(pluginManagerService);
    when(servicesContainer.getConnectionPluginManager()).thenReturn(pluginManager);
    when(pluginService.getCallContext()).thenReturn(new PluginCallContext());
    when(pluginManager.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(pluginManager.mustUsePipeline(any())).thenReturn(true);

    // Run the JDBC call the wrapper handed to the pipeline, as the real plugin chain would.
    when(pluginManager.execute(any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> ((JdbcCallable<?, ?>) i.getArgument(4)).call());
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private <T> T wrap(final Class<T> resultClass, final T target) throws InstantiationException {
    return WrapperUtils.wrapWithProxyIfNeeded(resultClass, target, connectionWrapper, pluginManager);
  }

  @Test
  void wrappersRecordTheConnectionTheyWereCreatedOn() throws Exception {
    final Statement statement = wrap(Statement.class, mock(Statement.class));
    final PreparedStatement preparedStatement =
        (PreparedStatement) wrap(Statement.class, mock(PreparedStatement.class));
    final CallableStatement callableStatement =
        (CallableStatement) wrap(Statement.class, mock(CallableStatement.class));
    final ResultSet resultSet = wrap(ResultSet.class, mock(ResultSet.class));
    final DatabaseMetaData metaData = wrap(DatabaseMetaData.class, mock(DatabaseMetaData.class));

    for (final Object wrapper :
        new Object[] {statement, preparedStatement, callableStatement, resultSet, metaData}) {
      assertSame(connectionA, ((ConnectionBoundObject) wrapper).getCreatedOnConnection(),
          wrapper.getClass().getSimpleName() + " must record the connection it was created on");
    }

    // An object created after the switch records the new connection.
    currentConnection.set(connectionB);
    final Statement afterSwitch = wrap(Statement.class, mock(Statement.class));
    assertSame(connectionB, ((ConnectionBoundObject) afterSwitch).getCreatedOnConnection());
  }

  @Test
  void guardedMethodsOnStaleStatementAreRejected() throws Exception {
    final Statement targetStatement = mock(Statement.class);
    final Statement statement = wrap(Statement.class, targetStatement);

    currentConnection.set(connectionB);

    final SQLException e =
        assertThrows(SQLException.class, () -> statement.execute("select 1"));
    assertEquals(
        Messages.get("ConnectionPluginManager.invokedAgainstOldConnection",
            new Object[] {targetStatement}),
        e.getMessage());

    assertThrows(SQLException.class, () -> statement.executeQuery("select 1"));
    assertThrows(SQLException.class, () -> statement.executeUpdate("update t set a = 1"));
    assertThrows(SQLException.class, statement::executeBatch);
    assertThrows(SQLException.class, statement::getConnection);
    assertThrows(SQLException.class, statement::getResultSet);
    assertThrows(SQLException.class, statement::getMoreResults);
  }

  @Test
  void guardedMethodsOnStalePreparedStatementAndResultSetAreRejected() throws Exception {
    final PreparedStatement preparedStatement =
        (PreparedStatement) wrap(Statement.class, mock(PreparedStatement.class));
    final ResultSet resultSet = wrap(ResultSet.class, mock(ResultSet.class));

    currentConnection.set(connectionB);

    assertThrows(SQLException.class, preparedStatement::execute);
    assertThrows(SQLException.class, preparedStatement::executeQuery);
    assertThrows(SQLException.class, preparedStatement::getResultSet);
    assertThrows(SQLException.class, resultSet::getStatement);
  }

  @Test
  void unguardedMethodsOnStaleObjectsStillWork() throws Exception {
    final Statement statement = wrap(Statement.class, mock(Statement.class));
    final ResultSet resultSet = wrap(ResultSet.class, mock(ResultSet.class));
    final CallableStatement callableStatement =
        (CallableStatement) wrap(Statement.class, mock(CallableStatement.class));

    currentConnection.set(connectionB);

    // An application must still be able to release a stale object, and cancel must keep working
    // (Statement.cancel is deliberately declared without connection locking).
    assertDoesNotThrow(resultSet::next);
    assertDoesNotThrow(resultSet::close);
    assertDoesNotThrow(statement::close);
    assertDoesNotThrow(statement::cancel);
    assertDoesNotThrow(callableStatement::cancel);
  }

  @Test
  void guardedMethodsSucceedWhileTheConnectionIsUnchanged() throws Exception {
    final Statement statement = wrap(Statement.class, mock(Statement.class));

    assertDoesNotThrow(() -> {
      statement.execute("select 1");
      statement.executeUpdate("update t set a = 1");
      statement.getMoreResults();
    });
  }

  @Test
  void objectIsValidAgainAfterSwitchingBackToTheSameConnection() throws Exception {
    // Read/write splitting caches the writer connection and switches back to that same object, so a
    // statement created before setReadOnly(true) must work again after setReadOnly(false).
    final Statement statement = wrap(Statement.class, mock(Statement.class));

    currentConnection.set(connectionB);
    assertThrows(SQLException.class, () -> statement.execute("select 1"));

    currentConnection.set(connectionA);
    assertDoesNotThrow(() -> {
      statement.execute("select 1");
    });
  }

  @Test
  void staleObjectIsRejectedDuringXaTransaction() throws Exception {
    // The guard no longer skips while an XA branch is active: it does not ask the target driver for
    // the bound connection, so the XA false positive it worked around cannot happen. A statement
    // created before a swap that predates the branch is genuinely stale.
    final Statement statement = wrap(Statement.class, mock(Statement.class));
    when(pluginService.isXaTransactionActive()).thenReturn(true);

    currentConnection.set(connectionB);

    assertThrows(SQLException.class, () -> statement.execute("select 1"));
  }

  @Test
  void resultSetFromArrayIsUsableWhenTheDriverReportsAnotherConnection() throws Exception {
    // Regression test for issue #1367: with internal connection pools the current connection is a
    // pooled handle, while the target driver builds the array's ResultSet on the physical connection
    // underneath it. It is the same session, so the ResultSet must be usable.
    final Connection pooledConnection = connectionA;
    final Connection physicalConnection = mock(Connection.class);

    final Statement arrayTargetStatement = mock(Statement.class);
    when(arrayTargetStatement.getConnection()).thenReturn(physicalConnection);

    final ResultSet arrayTargetResultSet = mock(ResultSet.class);
    when(arrayTargetResultSet.getStatement()).thenReturn(arrayTargetStatement);
    when(arrayTargetResultSet.next()).thenReturn(true, false);
    when(arrayTargetResultSet.getString(anyInt())).thenReturn("a");

    final Array targetArray = mock(Array.class);
    when(targetArray.getResultSet()).thenReturn(arrayTargetResultSet);

    final Array array = wrap(Array.class, targetArray);
    final ResultSet arrayResultSet = array.getResultSet();

    assertSame(pooledConnection,
        ((ConnectionBoundObject) arrayResultSet).getCreatedOnConnection());
    assertDoesNotThrow(() -> {
      arrayResultSet.next();
      arrayResultSet.getString(2);
      arrayResultSet.getStatement();
    });
  }

  @Test
  void rebindingAStatementClearsStaleness() throws Exception {
    final Statement targetStatement = mock(Statement.class);
    when(targetStatement.getResultSetType()).thenReturn(ResultSet.TYPE_FORWARD_ONLY);
    when(targetStatement.getResultSetConcurrency()).thenReturn(ResultSet.CONCUR_READ_ONLY);
    when(targetStatement.getResultSetHoldability()).thenReturn(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    when(connectionB.createStatement(anyInt(), anyInt(), anyInt())).thenReturn(mock(Statement.class));

    final Statement statement = wrap(Statement.class, targetStatement);
    final StatementWrapper statementWrapper = (StatementWrapper) statement;

    currentConnection.set(connectionB);
    assertThrows(SQLException.class, () -> statement.execute("select 1"));

    // A plugin re-creates the target on the routed connection; the wrapper is no longer stale.
    statementWrapper.rebind(connectionB);

    assertSame(connectionB, statementWrapper.getCreatedOnConnection());
    assertDoesNotThrow(() -> {
      statement.execute("select 1");
    });
  }
}
