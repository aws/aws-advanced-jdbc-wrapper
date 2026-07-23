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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
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
 * Functional tests for query-level load balancing (reader-to-reader rotation), exercised through
 * {@link AutoReadWriteSplittingPlugin} where each read {@code prepareStatement} is a routing point.
 */
public class QueryLevelLoadBalancingTest {

  private static final String PREPARE_STATEMENT = JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName;

  private AutoCloseable closeable;

  @Mock private PluginService pluginService;
  @Mock private SessionStateService sessionStateService;
  @Mock private Connection writerConn;
  @Mock private Connection reader1Conn;
  @Mock private Connection reader2Conn;
  @Mock private Statement preparedStatement;
  @Mock private JdbcCallable<Statement, SQLException> prepareFunc;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer").port(5432).role(HostRole.WRITER).build();
  private final HostSpec reader1Host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-1").port(5432).role(HostRole.READER).build();
  private final HostSpec reader2Host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-2").port(5432).role(HostRole.READER).build();

  private final AtomicReference<Connection> currentConn = new AtomicReference<>();
  private final AtomicReference<HostSpec> currentHost = new AtomicReference<>();

  private PluginCallContext callContext;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    // Start on a reader (an established read-only phase).
    currentConn.set(reader1Conn);
    currentHost.set(reader1Host);

    when(pluginService.getCurrentConnection()).thenAnswer(i -> currentConn.get());
    when(pluginService.getCurrentHostSpec()).thenAnswer(i -> currentHost.get());
    doAnswer(i -> {
      currentConn.set(i.getArgument(0));
      currentHost.set(i.getArgument(1));
      return null;
    }).when(pluginService).setCurrentConnection(any(Connection.class), any(HostSpec.class));

    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1Host, reader2Host));
    when(pluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq("random")))
        .thenReturn(reader2Host);
    when(pluginService.connect(eq(reader2Host), any(Properties.class), any())).thenReturn(reader2Conn);
    when(pluginService.getCallContext()).thenReturn(callContext);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.of(true));
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private void prepare(final Properties props) throws SQLException {
    new AutoReadWriteSplittingPlugin(pluginService, props).execute(
        Statement.class, SQLException.class, currentConn.get(), PREPARE_STATEMENT, prepareFunc,
        new Object[] {"select 1"});
  }

  @Test
  void queryLevelLb_onReader_readRoute_rotatesToNewReader() throws SQLException {
    final Properties props = new Properties();
    props.setProperty(UnifiedReadWriteSplittingPlugin.QUERY_LEVEL_LOAD_BALANCING.name, "true");

    prepare(props);

    // Rotated reader-to-reader to a fresh reader; the previous reader is closed.
    verify(pluginService).setCurrentConnection(eq(reader2Conn), eq(reader2Host));
    verify(reader1Conn).close();
  }

  @Test
  void sticky_onReader_readRoute_doesNotRotate() throws SQLException {
    // Default (sticky) reader: staying on the current reader, no rotation.
    prepare(new Properties());

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(reader1Conn, never()).close();
  }

  @Test
  void queryLevelLb_inTransaction_doesNotRotate() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(true);
    final Properties props = new Properties();
    props.setProperty(UnifiedReadWriteSplittingPlugin.QUERY_LEVEL_LOAD_BALANCING.name, "true");

    prepare(props);

    // A transaction pins the current reader; no rotation mid-transaction.
    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(reader1Conn, never()).close();
  }

  private int countReuseWarnings(final Properties props, final int executeCount) throws SQLException {
    when(preparedStatement.isClosed()).thenReturn(false);
    when(preparedStatement.getConnection()).thenReturn(reader1Conn);

    final Logger logger = Logger.getLogger(UnifiedReadWriteSplittingPlugin.class.getName());
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
      final AutoReadWriteSplittingPlugin plugin = new AutoReadWriteSplittingPlugin(pluginService, props);
      for (int i = 0; i < executeCount; i++) {
        plugin.execute(
            Statement.class, SQLException.class, preparedStatement,
            JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName, prepareFunc, new Object[] {});
      }
    } finally {
      logger.removeHandler(handler);
    }
    return warnings.get();
  }

  @Test
  void reusedPreparedStatement_withQueryLevelLb_warnsOnce() throws SQLException {
    final Properties props = new Properties();
    props.setProperty(UnifiedReadWriteSplittingPlugin.QUERY_LEVEL_LOAD_BALANCING.name, "true");
    // First execute records the statement; the second re-execute warns; a third does not warn again.
    assertEquals(1, countReuseWarnings(props, 3));
  }

  @Test
  void reusedPreparedStatement_withoutQueryLevelLb_noWarning() throws SQLException {
    // Query-level LB disabled: reusing a prepared statement is normal, so no warning.
    assertEquals(0, countReuseWarnings(new Properties(), 3));
  }

  @Test
  void reExecutedRebindablePreparedStatement_rotatesAndRebinds() throws SQLException {
    when(preparedStatement.isClosed()).thenReturn(false);
    when(preparedStatement.getConnection()).thenReturn(reader1Conn);
    final Rebindable rebindHandle = mock(Rebindable.class);
    when(rebindHandle.canRebind()).thenReturn(true);
    callContext.setRebindHandle(rebindHandle);

    final Properties props = new Properties();
    props.setProperty(UnifiedReadWriteSplittingPlugin.QUERY_LEVEL_LOAD_BALANCING.name, "true");
    final AutoReadWriteSplittingPlugin plugin = new AutoReadWriteSplittingPlugin(pluginService, props);

    // First execution stays on the reader chosen at prepare time (no rotation).
    plugin.execute(Statement.class, SQLException.class, preparedStatement,
        JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName, prepareFunc, new Object[] {});
    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));

    // Re-execution rotates reader-to-reader and re-creates the statement on the new reader.
    plugin.execute(Statement.class, SQLException.class, preparedStatement,
        JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName, prepareFunc, new Object[] {});
    verify(pluginService).setCurrentConnection(eq(reader2Conn), eq(reader2Host));
    verify(rebindHandle).rebind(reader2Conn);
  }
}
