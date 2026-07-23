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

package software.amazon.jdbc.plugin.readwritesplitting.signal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/** Unit tests for the routing-signal helpers. */
public class RoutingSignalsTest {

  private static final String SET_READ_ONLY = JdbcMethod.CONNECTION_SETREADONLY.methodName;
  private static final String PREPARE_STATEMENT = JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName;
  private static final String PREPARE_CALL = JdbcMethod.CONNECTION_PREPARECALL.methodName;
  private static final String STMT_EXECUTE_QUERY = JdbcMethod.STATEMENT_EXECUTEQUERY.methodName;

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;

  private PluginCallContext callContext;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    when(ctx.pluginService()).thenReturn(pluginService);
    when(pluginService.getCallContext()).thenReturn(callContext);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  // ---- ReadOnlyFlagSignal ----

  @Test
  void readOnlyFlag_true_mapsToReader() throws SQLException {
    final ReadOnlyFlagSignal signal = new ReadOnlyFlagSignal();
    assertEquals(TargetRole.READER, signal.resolve(ctx, SET_READ_ONLY, new Object[] {true}));
  }

  @Test
  void readOnlyFlag_false_mapsToWriter() throws SQLException {
    final ReadOnlyFlagSignal signal = new ReadOnlyFlagSignal();
    assertEquals(TargetRole.WRITER, signal.resolve(ctx, SET_READ_ONLY, new Object[] {false}));
  }

  @Test
  void readOnlyFlag_otherMethod_abstains() throws SQLException {
    final ReadOnlyFlagSignal signal = new ReadOnlyFlagSignal();
    assertEquals(TargetRole.NO_DECISION, signal.resolve(ctx, STMT_EXECUTE_QUERY, new Object[] {"sql"}));
  }

  @Test
  void readOnlyFlag_nullOrEmptyArgs_abstains() throws SQLException {
    final ReadOnlyFlagSignal signal = new ReadOnlyFlagSignal();
    assertEquals(TargetRole.NO_DECISION, signal.resolve(ctx, SET_READ_ONLY, null));
    assertEquals(TargetRole.NO_DECISION, signal.resolve(ctx, SET_READ_ONLY, new Object[] {}));
  }

  @Test
  void readOnlyFlag_boundStatement_abstains() throws SQLException {
    // ReadOnlyFlagSignal does not route bound statements by SQL.
    assertEquals(TargetRole.NO_DECISION, new ReadOnlyFlagSignal().resolveForBoundStatement(ctx));
  }

  // ---- SqlRoutingSignal ----

  @Test
  void sql_prepareStatement_selectMapsToReader() throws SQLException {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    assertEquals(TargetRole.READER, new SqlRoutingSignal().resolve(ctx, PREPARE_STATEMENT, null));
  }

  @Test
  void sql_prepareCall_selectForUpdateMapsToWriter() throws SQLException {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    callContext.setAttribute(SqlContextKeys.FOR_UPDATE, Boolean.TRUE);
    assertEquals(TargetRole.WRITER, new SqlRoutingSignal().resolve(ctx, PREPARE_CALL, null));
  }

  @Test
  void sql_dmlMapsToWriter() throws SQLException {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.UPDATE);
    assertEquals(TargetRole.WRITER, new SqlRoutingSignal().resolve(ctx, PREPARE_STATEMENT, null));
  }

  @Test
  void sql_hintOverridesQueryType() throws SQLException {
    // A reader hint wins even for a DML query type.
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.UPDATE);
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.READER);
    assertEquals(TargetRole.READER, new SqlRoutingSignal().resolve(ctx, PREPARE_STATEMENT, null));

    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.WRITER);
    assertEquals(TargetRole.WRITER, new SqlRoutingSignal().resolve(ctx, PREPARE_STATEMENT, null));

    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.KEEP);
    assertEquals(TargetRole.KEEP, new SqlRoutingSignal().resolve(ctx, PREPARE_STATEMENT, null));
  }

  @Test
  void sql_noParseResult_fallsBackToWriter() throws SQLException {
    // Empty call context (no query type, no hint) → safe writer fallback.
    assertEquals(TargetRole.WRITER, new SqlRoutingSignal().resolve(ctx, PREPARE_STATEMENT, null));
  }

  @Test
  void sql_missingCallContext_fallsBackToWriter() throws SQLException {
    when(pluginService.getCallContext()).thenReturn(null);
    assertEquals(TargetRole.WRITER, new SqlRoutingSignal().resolve(ctx, PREPARE_STATEMENT, null));
  }

  @Test
  void sql_plainStatementExecute_abstainsForRole() throws SQLException {
    // A bound plain Statement cannot be rerouted by switching current connection.
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    assertEquals(TargetRole.NO_DECISION, new SqlRoutingSignal().resolve(ctx, STMT_EXECUTE_QUERY, null));
  }

  @Test
  void sql_boundStatement_resolvesFromSql() throws SQLException {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    assertEquals(TargetRole.READER, new SqlRoutingSignal().resolveForBoundStatement(ctx));
  }

  @Test
  void sql_subscribesToPrepareAndPlainExecuteMethods() {
    final java.util.Set<String> subscribed = new SqlRoutingSignal().extraSubscribedMethods();
    assertTrue(subscribed.contains(PREPARE_STATEMENT));
    assertTrue(subscribed.contains(PREPARE_CALL));
    assertTrue(subscribed.contains(STMT_EXECUTE_QUERY));
  }

  // ---- CompositeSignal ----

  @Test
  void composite_primaryWins() throws SQLException {
    // SQL routing (primary) resolves SELECT → reader; setReadOnly fallback would abstain here.
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    final CompositeSignal composite = new CompositeSignal(new SqlRoutingSignal(), new ReadOnlyFlagSignal());
    assertEquals(TargetRole.READER, composite.resolve(ctx, PREPARE_STATEMENT, null));
  }

  @Test
  void composite_fallbackConsultedWhenPrimaryAbstains() throws SQLException {
    // On setReadOnly, SqlRoutingSignal abstains (NO_DECISION) → ReadOnlyFlagSignal decides.
    final CompositeSignal composite = new CompositeSignal(new SqlRoutingSignal(), new ReadOnlyFlagSignal());
    assertEquals(TargetRole.READER, composite.resolve(ctx, SET_READ_ONLY, new Object[] {true}));
    assertEquals(TargetRole.WRITER, composite.resolve(ctx, SET_READ_ONLY, new Object[] {false}));
  }

  @Test
  void composite_boundStatement_primaryWins() throws SQLException {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    final CompositeSignal composite = new CompositeSignal(new SqlRoutingSignal(), new ReadOnlyFlagSignal());
    assertEquals(TargetRole.READER, composite.resolveForBoundStatement(ctx));
  }

  @Test
  void composite_mergesSubscribedMethods() {
    final CompositeSignal composite = new CompositeSignal(new SqlRoutingSignal(), new ReadOnlyFlagSignal());
    assertTrue(composite.extraSubscribedMethods().contains(PREPARE_STATEMENT));
  }
}
