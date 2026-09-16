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
  @Mock private Statement plainStatement;
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

  /** Runs {@code action} and returns how many WARNING records the plugin logged while it ran. */
  private int countWarnings(final ThrowingRunnable action) throws SQLException {
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
      action.run();
    } finally {
      logger.removeHandler(handler);
    }
    return warnings.get();
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws SQLException;
  }

  private int countReuseWarnings(final Properties props, final int executeCount) throws SQLException {
    when(preparedStatement.isClosed()).thenReturn(false);
    when(preparedStatement.getConnection()).thenReturn(reader1Conn);

    return countWarnings(() -> {
      final AutoReadWriteSplittingPlugin plugin = new AutoReadWriteSplittingPlugin(pluginService, props);
      for (int i = 0; i < executeCount; i++) {
        plugin.execute(
            Statement.class, SQLException.class, preparedStatement,
            JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName, prepareFunc, new Object[] {});
      }
    });
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

  /**
   * Executes a plain {@code Statement.executeQuery(sql)}. Its SQL is only known at execute time, so
   * it is never a routing point and the role is resolved by the bound-statement path.
   */
  private void executePlainStatement(final Properties props, final int executeCount) throws SQLException {
    final UnifiedReadWriteSplittingPlugin plugin = new AutoReadWriteSplittingPlugin(pluginService, props);
    for (int i = 0; i < executeCount; i++) {
      plugin.execute(
          Statement.class, SQLException.class, plainStatement,
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName, prepareFunc, new Object[] {"select 1"});
    }
  }

  private static Properties queryLevelLbProps() {
    final Properties props = new Properties();
    props.setProperty(UnifiedReadWriteSplittingPlugin.QUERY_LEVEL_LOAD_BALANCING.name, "true");
    return props;
  }

  @Test
  void plainStatement_onReader_withQueryLevelLb_rotatesAndRebinds() throws SQLException {
    final Rebindable rebindHandle = mock(Rebindable.class);
    when(rebindHandle.canRebind()).thenReturn(true);
    callContext.setRebindHandle(rebindHandle);

    executePlainStatement(queryLevelLbProps(), 1);

    // A plain Statement is not a routing point, so without the bound-statement rotation this read
    // would have stayed on reader-1 and never been balanced.
    verify(pluginService).setCurrentConnection(eq(reader2Conn), eq(reader2Host));
    verify(rebindHandle).rebind(reader2Conn);
  }

  @Test
  void plainStatement_onReader_withoutQueryLevelLb_doesNotRotate() throws SQLException {
    final Rebindable rebindHandle = mock(Rebindable.class);
    when(rebindHandle.canRebind()).thenReturn(true);
    callContext.setRebindHandle(rebindHandle);

    executePlainStatement(new Properties(), 1);

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));
  }

  @Test
  void plainStatement_inTransaction_doesNotRotate() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(true);
    final Rebindable rebindHandle = mock(Rebindable.class);
    when(rebindHandle.canRebind()).thenReturn(true);
    callContext.setRebindHandle(rebindHandle);

    executePlainStatement(queryLevelLbProps(), 1);

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));
  }

  /**
   * A rotation that cannot be applied must stay quiet: the statement simply runs where it is, and
   * this path is reached on every read, so warning would flood the log.
   */
  @Test
  void plainStatement_notRebindable_skipsRotationWithoutWarning() throws SQLException {
    final Rebindable rebindHandle = mock(Rebindable.class);
    when(rebindHandle.canRebind()).thenReturn(false);
    callContext.setRebindHandle(rebindHandle);

    assertEquals(0, countWarnings(() -> executePlainStatement(queryLevelLbProps(), 3)));
    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    verify(rebindHandle, never()).rebind(any(Connection.class));
  }

  @Test
  void plainStatement_rebindingDisabled_skipsRotationWithoutWarning() throws SQLException {
    final Rebindable rebindHandle = mock(Rebindable.class);
    callContext.setRebindHandle(rebindHandle);

    final Properties props = queryLevelLbProps();
    props.setProperty(
        UnifiedReadWriteSplittingPlugin.ALLOW_STATEMENT_RECREATION_ON_CONNECTION_SWITCH.name, "false");

    assertEquals(0, countWarnings(() -> executePlainStatement(props, 3)));
    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
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
