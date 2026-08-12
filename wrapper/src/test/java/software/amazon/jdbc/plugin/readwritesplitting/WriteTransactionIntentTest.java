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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
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
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.states.SessionStateService;

/**
 * Functional tests for {@code assumeWriteTransaction}, exercised through
 * {@link AutoReadWriteSplittingPlugin}. The scenario is a read-write transaction whose first
 * statement is a read (Spring's default {@code @Transactional} running {@code findAll()} before
 * {@code save()}): the transaction manager only calls {@code setAutoCommit(false)}, so without this
 * setting the leading read moves the connection to a reader and the transaction physically starts
 * there, where the later write cannot run.
 */
public class WriteTransactionIntentTest {

  private static final String PREPARE_STATEMENT = JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName;

  private AutoCloseable closeable;

  @Mock private PluginService pluginService;
  @Mock private SessionStateService sessionStateService;
  @Mock private Connection writerConn;
  @Mock private Connection readerConn;
  @Mock private JdbcCallable<Statement, SQLException> prepareFunc;

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
    // The pooled connection was handed out while sitting on a reader (a previous read-only phase).
    currentConn.set(readerConn);
    currentHost.set(readerHost);

    when(pluginService.getCurrentConnection()).thenAnswer(i -> currentConn.get());
    when(pluginService.getCurrentHostSpec()).thenAnswer(i -> currentHost.get());
    doAnswer(i -> {
      currentConn.set(i.getArgument(0));
      currentHost.set(i.getArgument(1));
      return null;
    }).when(pluginService).setCurrentConnection(any(Connection.class), any(HostSpec.class));

    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, readerHost));
    when(pluginService.connect(eq(writerHost), any(Properties.class), any())).thenReturn(writerConn);
    when(pluginService.getCallContext()).thenReturn(callContext);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    // A transaction manager opened a transaction without declaring it read-only.
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.of(false));
    when(sessionStateService.getReadOnly()).thenReturn(Optional.empty());
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private void prepareRead(final Properties props) throws SQLException {
    new AutoReadWriteSplittingPlugin(pluginService, props).execute(
        Statement.class, SQLException.class, currentConn.get(), PREPARE_STATEMENT, prepareFunc,
        new Object[] {"select 1"});
  }

  private static Properties enabled() {
    final Properties props = new Properties();
    props.setProperty(UnifiedReadWriteSplittingPlugin.ASSUME_WRITE_TRANSACTION.name, "true");
    return props;
  }

  @Test
  void enabled_readOpeningUndeclaredTransaction_switchesToWriter() throws SQLException {
    prepareRead(enabled());

    verify(pluginService).setCurrentConnection(eq(writerConn), eq(writerHost));
  }

  @Test
  void disabled_readOpeningUndeclaredTransaction_staysOnReader() throws SQLException {
    // Default behavior is unchanged: the read stays on the reader it is already using.
    prepareRead(new Properties());

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  void enabled_readInReadOnlyTransaction_staysOnReader() throws SQLException {
    // @Transactional(readOnly = true): setReadOnly(true) states read intent, which is honored.
    when(sessionStateService.getReadOnly()).thenReturn(Optional.of(true));

    prepareRead(enabled());

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  void enabled_readOutsideTransaction_staysOnReader() throws SQLException {
    // No transaction (autocommit on): read offloading is unaffected.
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.of(true));

    prepareRead(enabled());

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  void enabled_readerHintInTransaction_staysOnReader() throws SQLException {
    // An explicit /*@reader*/ hint outranks the inferred write intent.
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.READER);

    prepareRead(enabled());

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  void enabled_readInOpenTransactionOnReader_isNotMoved() throws SQLException {
    // The transaction is already in progress on the reader, so the gate pins the connection: the
    // assumption only positions the statement that opens a transaction, it never moves an open one.
    when(pluginService.isInTransaction()).thenReturn(true);

    prepareRead(enabled());

    verify(pluginService, never()).setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }
}
