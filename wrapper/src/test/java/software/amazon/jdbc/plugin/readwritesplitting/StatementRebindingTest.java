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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.Rebindable;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.states.SessionStateService;

/**
 * Functional tests for the SQL-driven statement-rebinding and bound-statement reuse behavior,
 * exercised through {@link AutoReadWriteSplittingPlugin} (SQL-routed) so a plain
 * {@code Statement.executeQuery(sql)} is observed for rerouting.
 */
public class StatementRebindingTest {

  private static final String EXECUTE_QUERY = JdbcMethod.STATEMENT_EXECUTEQUERY.methodName;

  private AutoCloseable closeable;

  @Mock private PluginService pluginService;
  @Mock private SessionStateService sessionStateService;
  @Mock private Connection writerConn;
  @Mock private Connection readerConn;
  @Mock private Statement boundStatement;
  @Mock private ResultSet resultSet;
  @Mock private JdbcCallable<ResultSet, SQLException> sqlFunc;
  @Mock private Rebindable rebindHandle;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer").port(5432).role(HostRole.WRITER).build();
  private final HostSpec readerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-1").port(5432).role(HostRole.READER).build();

  private final AtomicReference<Connection> currentConn = new AtomicReference<>();
  private final AtomicReference<HostSpec> currentHost = new AtomicReference<>();

  private PluginCallContext callContext;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    currentConn.set(writerConn);
    currentHost.set(writerHost);

    when(pluginService.getCurrentConnection()).thenAnswer(i -> currentConn.get());
    when(pluginService.getCurrentHostSpec()).thenAnswer(i -> currentHost.get());
    doAnswer(i -> {
      currentConn.set(i.getArgument(0));
      currentHost.set(i.getArgument(1));
      return null;
    }).when(pluginService).setCurrentConnection(any(Connection.class), any(HostSpec.class));

    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, readerHost));
    when(pluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq("random")))
        .thenReturn(readerHost);
    when(pluginService.connect(eq(readerHost), any(Properties.class), any())).thenReturn(readerConn);
    when(pluginService.connect(eq(writerHost), any(Properties.class), any())).thenReturn(writerConn);
    when(pluginService.getCallContext()).thenReturn(callContext);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.of(true));
    when(sqlFunc.call()).thenReturn(resultSet);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private AutoReadWriteSplittingPlugin plugin(final Properties props) {
    return new AutoReadWriteSplittingPlugin(pluginService, props);
  }

  private void mockBoundStatementOn(final Connection conn) throws SQLException {
    when(boundStatement.isClosed()).thenReturn(false);
    when(boundStatement.getConnection()).thenReturn(conn);
  }

  private void setSql(final QueryType type) {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, type);
  }

  private void executeQuery(final AutoReadWriteSplittingPlugin plugin) throws SQLException {
    plugin.execute(
        ResultSet.class, SQLException.class, boundStatement, EXECUTE_QUERY, sqlFunc,
        new Object[] {"select 1"});
  }

  @Test
  void selectOnWriterBoundStatement_rebindsToReader() throws SQLException {
    mockBoundStatementOn(writerConn);
    setSql(QueryType.SELECT);
    callContext.setRebindHandle(rebindHandle);

    executeQuery(plugin(new Properties()));

    // Routed to a reader and the bound statement re-created on the reader connection.
    verify(pluginService).setCurrentConnection(eq(readerConn), eq(readerHost));
    verify(rebindHandle).rebind(readerConn);
  }

  @Test
  void updateOnReaderBoundStatement_rebindsToWriter() throws SQLException {
    currentConn.set(readerConn);
    currentHost.set(readerHost);
    mockBoundStatementOn(readerConn);
    setSql(QueryType.UPDATE);
    callContext.setRebindHandle(rebindHandle);

    executeQuery(plugin(new Properties()));

    verify(pluginService).setCurrentConnection(eq(writerConn), eq(writerHost));
    verify(rebindHandle).rebind(writerConn);
  }

  @Test
  void alreadyOnTargetRole_noRebind() throws SQLException {
    currentConn.set(readerConn);
    currentHost.set(readerHost);
    mockBoundStatementOn(readerConn);
    setSql(QueryType.SELECT);
    callContext.setRebindHandle(rebindHandle);

    executeQuery(plugin(new Properties()));

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));
  }

  @Test
  void inTransaction_noRebind() throws SQLException {
    mockBoundStatementOn(writerConn);
    setSql(QueryType.SELECT);
    callContext.setRebindHandle(rebindHandle);
    when(pluginService.isInTransaction()).thenReturn(true);

    executeQuery(plugin(new Properties()));

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));
  }

  @Test
  void rebindDisabled_noRebindOrSwitch() throws SQLException {
    mockBoundStatementOn(writerConn);
    setSql(QueryType.SELECT);
    callContext.setRebindHandle(rebindHandle);
    final Properties props = new Properties();
    props.setProperty(
        UnifiedReadWriteSplittingPlugin.ALLOW_STATEMENT_RECREATION_ON_CONNECTION_SWITCH.name, "false");

    executeQuery(plugin(props));

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));
  }

  @Test
  void noRebindHandle_noSwitch() throws SQLException {
    mockBoundStatementOn(writerConn);
    setSql(QueryType.SELECT);
    // No rebind handle published on the call context.

    executeQuery(plugin(new Properties()));

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));
  }

  @Test
  void rebindFailure_fallsBackWithoutPropagating() throws SQLException {
    mockBoundStatementOn(writerConn);
    setSql(QueryType.SELECT);
    callContext.setRebindHandle(rebindHandle);
    // The rebind attempt fails; the failure must not propagate out of execute().
    org.mockito.Mockito.doThrow(new SQLException("cannot rebind")).when(rebindHandle).rebind(any());

    final ResultSet result = plugin(new Properties()).execute(
        ResultSet.class, SQLException.class, boundStatement, EXECUTE_QUERY, sqlFunc,
        new Object[] {"select 1"});

    assertEquals(resultSet, result);
    verify(rebindHandle).rebind(readerConn);
  }

  @Test
  void reusedBoundStatement_withoutHandle_warnsOnlyOnce() throws SQLException {
    mockBoundStatementOn(writerConn);
    setSql(QueryType.SELECT);
    // No rebind handle → reuse warning path.

    final Logger logger =
        Logger.getLogger(UnifiedReadWriteSplittingPlugin.class.getName());
    final AtomicInteger warnings = new AtomicInteger(0);
    final Handler handler = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel() == Level.WARNING) {
          warnings.incrementAndGet();
        }
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    logger.addHandler(handler);
    try {
      final AutoReadWriteSplittingPlugin plugin = plugin(new Properties());
      executeQuery(plugin);
      executeQuery(plugin);
      executeQuery(plugin);
    } finally {
      logger.removeHandler(handler);
    }

    // First use records the statement (no warning); subsequent reuse warns exactly once.
    assertEquals(1, warnings.get());
  }
}
