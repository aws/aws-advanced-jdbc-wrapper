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

package software.amazon.jdbc.plugin.sqlparser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.ParameterColumnMapper;
import software.amazon.jdbc.parser.ParameterMappingProblem;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.targetdriverdialect.MysqlConnectorJTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;

public class SqlParserConnectionPluginTest {

  private AutoCloseable closeable;
  private SqlParserConnectionPlugin plugin;
  private PluginCallContext callContext;

  @Mock PluginService mockPluginService;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    when(mockPluginService.getCallContext()).thenReturn(callContext);
    plugin = new SqlParserConnectionPlugin(mockPluginService, new Properties());
  }

  @AfterEach
  void cleanUp() throws Exception {
    callContext.reset();
    closeable.close();
  }

  private void executeWithSql(String sql) throws Exception {
    JdbcCallable<Object, SQLException> callable = () -> null;
    plugin.execute(Object.class, SQLException.class, null,
        "Connection.prepareStatement", callable, new Object[]{sql});
  }

  @Test
  void test_selectQuery_setsQueryTypeSelect() throws Exception {
    executeWithSql("SELECT name FROM users WHERE id = ?");

    assertEquals(QueryType.SELECT, callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
  }

  @Test
  void test_insertQuery_setsQueryTypeInsert() throws Exception {
    executeWithSql("INSERT INTO users (name) VALUES (?)");

    assertEquals(QueryType.INSERT, callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
  }

  @Test
  void test_updateQuery_setsQueryTypeUpdate() throws Exception {
    executeWithSql("UPDATE users SET name = ? WHERE id = ?");

    assertEquals(QueryType.UPDATE, callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
  }

  @Test
  void test_deleteQuery_setsQueryTypeDelete() throws Exception {
    executeWithSql("DELETE FROM users WHERE id = ?");

    assertEquals(QueryType.DELETE, callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
  }

  @Test
  void test_populatesTables() throws Exception {
    executeWithSql("SELECT * FROM users");

    Set<String> tables = callContext.getAttribute(SqlContextKeys.TABLES, Set.class);
    assertTrue(tables.contains("users"));
  }

  @Test
  void test_readerHint_parsed() throws Exception {
    executeWithSql("/*@reader*/ SELECT * FROM users");

    assertEquals(RoutingHint.READER,
        callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class));
  }

  @Test
  void test_writerHint_parsed() throws Exception {
    executeWithSql("/*@writer*/ SELECT * FROM users FOR UPDATE");

    assertEquals(RoutingHint.WRITER,
        callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class));
  }

  @Test
  void test_keepHint_parsed() throws Exception {
    executeWithSql("/*@keep*/ SELECT * FROM users");

    assertEquals(RoutingHint.KEEP,
        callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class));
  }

  @Test
  void test_noHint_routingHintIsNull() throws Exception {
    executeWithSql("SELECT * FROM users");

    assertNull(callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class));
  }

  @Test
  void test_hintIsCaseInsensitive() throws Exception {
    executeWithSql("/*@READER*/ SELECT * FROM users");

    assertEquals(RoutingHint.READER,
        callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class));
  }

  @Test
  void test_hintStrippedFromCleanSql() throws Exception {
    executeWithSql("/*@reader*/ SELECT * FROM users");

    String cleanSql = callContext.getAttribute(SqlContextKeys.CLEAN_SQL, String.class);
    assertFalse(cleanSql.contains("@reader"));
    assertTrue(cleanSql.contains("SELECT"));
  }

  @Test
  void test_noSqlArg_noContext() throws Exception {
    JdbcCallable<Object, SQLException> callable = () -> null;
    plugin.execute(Object.class, SQLException.class, null,
        "Connection.prepareStatement", callable, new Object[]{42});

    assertNull(callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
  }

  @Test
  void test_selectForUpdate_setsForUpdate() throws Exception {
    executeWithSql("SELECT * FROM users WHERE id = 1 FOR UPDATE");

    assertEquals(Boolean.TRUE, callContext.getAttribute(SqlContextKeys.FOR_UPDATE, Boolean.class));
  }

  @Test
  void test_selectForShare_setsForUpdate() throws Exception {
    executeWithSql("SELECT * FROM users FOR SHARE");

    assertEquals(Boolean.TRUE, callContext.getAttribute(SqlContextKeys.FOR_UPDATE, Boolean.class));
  }

  @Test
  void test_selectForNoKeyUpdate_setsForUpdate() throws Exception {
    executeWithSql("SELECT * FROM users FOR NO KEY UPDATE");

    assertEquals(Boolean.TRUE, callContext.getAttribute(SqlContextKeys.FOR_UPDATE, Boolean.class));
  }

  @Test
  void test_selectForKeyShare_setsForUpdate() throws Exception {
    executeWithSql("SELECT * FROM users FOR KEY SHARE");

    assertEquals(Boolean.TRUE, callContext.getAttribute(SqlContextKeys.FOR_UPDATE, Boolean.class));
  }

  @Test
  void test_plainSelect_forUpdateIsFalse() throws Exception {
    executeWithSql("SELECT * FROM users WHERE id = 1");

    assertEquals(Boolean.FALSE, callContext.getAttribute(SqlContextKeys.FOR_UPDATE, Boolean.class));
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, String> paramMapping() {
    return callContext.getAttribute(SqlContextKeys.PARAM_MAPPING, Map.class);
  }

  @SuppressWarnings("unchecked")
  private List<ParameterMappingProblem> problems() {
    return callContext.getAttribute(SqlContextKeys.PARAM_MAPPING_PROBLEMS, List.class);
  }

  @Test
  void test_literalBeforeEncryptedColumn_mapsByPosition() throws Exception {
    executeWithSql("INSERT INTO users (name, ssn) VALUES ('literal-name', ?)");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
    assertTrue(problems().isEmpty());
  }

  @Test
  void test_questionMarkInLiteralWithAnnotation_mapsAnnotatedPlaceholder() throws Exception {
    executeWithSql(
        "UPDATE users SET note = 'really?', ssn = /*@encrypt:users.ssn*/ ? WHERE id = 1");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
    assertTrue(problems().isEmpty());
  }

  @Test
  void test_postgresDialect_backtickQuestionMark_reportsCountMismatch() throws Exception {
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());

    executeWithSql("UPDATE users SET `what?` = 'x', ssn = ? WHERE id = ?");

    assertEquals(1, problems().size());
    assertEquals(ParameterMappingProblem.Type.PLACEHOLDER_COUNT_MISMATCH, problems().get(0).getType());
  }

  @Test
  void test_mysqlDialect_backtickQuestionMark_hasNoProblems() throws Exception {
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new MysqlConnectorJTargetDriverDialect());

    executeWithSql("UPDATE users SET `what?` = 'x', ssn = ? WHERE id = ?");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
    assertTrue(problems().isEmpty());
  }

  @Test
  void test_unparsableWrite_reportsUnparsedStatement() throws Exception {
    executeWithSql("UPDATE users SET flag = (attrs ?? 'k'), ssn = ? WHERE id = ?");

    assertEquals(1, problems().size());
    assertEquals(ParameterMappingProblem.Type.UNPARSED_STATEMENT, problems().get(0).getType());
    assertEquals(QueryType.UPDATE, callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
    assertTrue(callContext.getAttribute(SqlContextKeys.TABLES, Set.class).isEmpty());
  }

  @Test
  void test_annotationConflictingWithMapping_annotationWinsAndWarns() throws Exception {
    final Logger logger = Logger.getLogger(SqlParserConnectionPlugin.class.getName());
    final List<LogRecord> warnings = new ArrayList<>();
    final Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel() == Level.WARNING) {
          warnings.add(record);
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
      executeWithSql("INSERT INTO users (name, ssn) VALUES (/*@encrypt:users.ssn*/ ?, ?)");
    } finally {
      logger.removeHandler(handler);
    }

    final Map<Integer, String> expected = new HashMap<>();
    expected.put(1, "users.ssn");
    expected.put(2, "users.ssn");
    assertEquals(expected, paramMapping());
    assertEquals(1, warnings.size());
    assertFalse(warnings.get(0).getMessage().contains("INSERT"));
  }

  @Test
  void test_schemaQualifiedAnnotation_doesNotWarn() throws Exception {
    final Logger logger = Logger.getLogger(SqlParserConnectionPlugin.class.getName());
    final List<LogRecord> warnings = new ArrayList<>();
    final Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel() == Level.WARNING) {
          warnings.add(record);
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
      executeWithSql("INSERT INTO public.users (name, ssn) VALUES (?, /*@encrypt:public.users.ssn*/ ?)");
    } finally {
      logger.removeHandler(handler);
    }

    // The annotation agrees with the automatic mapping, whose table name the metadata uses.
    assertEquals("users.ssn", paramMapping().get(2));
    assertTrue(warnings.isEmpty());
  }

  @Test
  void test_plainSelect_setsEmptyProblems() throws Exception {
    executeWithSql("SELECT * FROM users WHERE id = 1");

    assertTrue(callContext.hasAttribute(SqlContextKeys.PARAM_MAPPING_PROBLEMS));
    assertTrue(problems().isEmpty());
  }

  @SuppressWarnings("unchecked")
  private List<ParameterColumnMapper.UnboundColumn> unboundColumns() {
    return callContext.getAttribute(SqlContextKeys.UNBOUND_WRITE_COLUMNS, List.class);
  }

  private void execute(String methodName, Object methodInvokeOn, Object... args) throws Exception {
    JdbcCallable<Object, SQLException> callable = () -> null;
    plugin.execute(Object.class, SQLException.class, methodInvokeOn, methodName, callable, args);
  }

  @Test
  void test_literalWrite_setsUnboundColumns() throws Exception {
    executeWithSql("INSERT INTO users (name, ssn) VALUES (?, '123')");

    assertEquals(1, unboundColumns().size());
    assertEquals("ssn", unboundColumns().get(0).getColumn());
    assertEquals("users", unboundColumns().get(0).getTable());
  }

  @Test
  void test_subscribesToAddBatch() {
    Set<String> methods = plugin.getSubscribedMethods();

    assertTrue(methods.contains("Statement.addBatch"));
    assertFalse(methods.contains("PreparedStatement.addBatch"));
    assertFalse(methods.contains("CallableStatement.addBatch"));
  }

  @Test
  void test_statementMethodsWithSql_populateContext() throws Exception {
    for (String methodName : new String[] {
        "Statement.execute", "Statement.executeQuery", "Statement.executeUpdate", "Statement.addBatch"}) {
      callContext.reset();
      execute(methodName, mock(Statement.class), "UPDATE users SET ssn = 'x'");

      assertEquals(QueryType.UPDATE,
          callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class), methodName);
      assertEquals(1, unboundColumns().size(), methodName);
    }
  }

  @Test
  void test_preparedStatementExecuteWithSql_parsesThatSqlNotThePreparedOne() throws Exception {
    PreparedStatement ps = mock(PreparedStatement.class);
    plugin.execute(PreparedStatement.class, SQLException.class, null,
        "Connection.prepareStatement", () -> ps, new Object[] {"SELECT * FROM users WHERE id = ?"});
    callContext.reset();

    execute("PreparedStatement.executeUpdate", ps, "UPDATE users SET ssn = 'x'");

    assertEquals(QueryType.UPDATE, callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
    assertEquals(1, unboundColumns().size());

    callContext.reset();
    execute("PreparedStatement.executeQuery", ps);

    assertEquals(QueryType.SELECT, callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class));
    assertTrue(unboundColumns().isEmpty());
  }

  private List<LogRecord> warningsWhilePreparing(String sql) throws Exception {
    final Logger logger = Logger.getLogger(SqlParserConnectionPlugin.class.getName());
    final List<LogRecord> warnings = new ArrayList<>();
    final Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel() == Level.WARNING) {
          warnings.add(record);
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
      executeWithSql(sql);
    } finally {
      logger.removeHandler(handler);
    }
    return warnings;
  }

  @Test
  void test_multiTableUpdate_tablesListWrittenTablesInStatementOrder() throws Exception {
    executeWithSql("UPDATE other o JOIN users u ON o.id = u.id SET u.ssn = ?");

    Set<String> tables = callContext.getAttribute(SqlContextKeys.TABLES, Set.class);
    assertEquals(Arrays.asList("other", "users"), new ArrayList<>(tables));
  }

  @Test
  void test_bareAnnotation_takesTableOfPositionalMapping() throws Exception {
    final List<LogRecord> warnings = warningsWhilePreparing(
        "UPDATE other o JOIN users u ON o.id = u.id SET u.ssn = /*@encrypt:ssn*/ ? WHERE o.id = ?");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
    assertTrue(warnings.isEmpty());
  }

  @Test
  void test_aliasAnnotation_resolvesToItsTable() throws Exception {
    executeWithSql("SELECT * FROM users u WHERE lower(u.ssn) = /*@encrypt:u.ssn*/ ?");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
  }

  @Test
  void test_aliasAnnotationAgreeingWithMapping_doesNotWarn() throws Exception {
    final List<LogRecord> warnings =
        warningsWhilePreparing("UPDATE users u SET ssn = /*@encrypt:u.ssn*/ ? WHERE u.id = ?");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
    assertTrue(warnings.isEmpty());
  }

  @Test
  void test_resolveAnnotation() {
    final Map<String, String> aliases = new HashMap<>();
    aliases.put("u", "users");
    aliases.put("other", "other");

    assertEquals("users.ssn",
        SqlParserConnectionPlugin.resolveAnnotation("ssn", "users.ssn", aliases, null));
    assertEquals("ssn", SqlParserConnectionPlugin.resolveAnnotation("ssn", "users.name", aliases, null));
    assertEquals("ssn", SqlParserConnectionPlugin.resolveAnnotation("ssn", null, aliases, null));
    assertEquals("users.ssn", SqlParserConnectionPlugin.resolveAnnotation("ssn", null, aliases, "users"));
    assertEquals("users.ssn", SqlParserConnectionPlugin.resolveAnnotation("U.ssn", null, aliases, null));
    assertEquals("other.ssn", SqlParserConnectionPlugin.resolveAnnotation("other.ssn", null, aliases, null));
    assertEquals("public.u.ssn",
        SqlParserConnectionPlugin.resolveAnnotation("public.u.ssn", null, aliases, null));
  }

  @Test
  void test_bareAnnotation_singleTableStatement_takesThatTable() throws Exception {
    executeWithSql("SELECT * FROM users WHERE lower(ssn) = /*@encrypt:ssn*/ ?");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
  }

  @Test
  void test_bareAnnotation_schemaQualifiedSingleTable_takesBareTableName() throws Exception {
    executeWithSql("UPDATE public.users SET note = lower(/*@encrypt:ssn*/ ?) WHERE id = 1");
    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
    for (ParameterMappingProblem problem : problems()) {
      assertFalse(problem.getParameterIndexes().contains(1)
          && problem.getType() == ParameterMappingProblem.Type.UNKNOWN_TARGET_PARAMS, problem.toString());
    }

    executeWithSql("SELECT * FROM public.users WHERE lower(ssn) = /*@encrypt:ssn*/ ?");
    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
  }

  @Test
  void test_bareAnnotation_tablesInDifferentSchemas_stayBare() throws Exception {
    executeWithSql("SELECT * FROM a.users JOIN b.other ON users.id = other.id"
        + " WHERE lower(ssn) = /*@encrypt:ssn*/ ?");
    assertEquals(Collections.singletonMap(1, "ssn"), paramMapping());

    executeWithSql("SELECT * FROM a.users x JOIN b.users y ON x.id = y.id"
        + " WHERE lower(x.ssn) = /*@encrypt:ssn*/ ?");
    assertEquals(Collections.singletonMap(1, "ssn"), paramMapping());
  }

  @Test
  void test_bareAnnotation_tablesDifferingInCase_stayBare() throws Exception {
    executeWithSql("SELECT * FROM \"Users\" x JOIN users y ON x.id = y.id"
        + " WHERE lower(x.ssn) = /*@encrypt:ssn*/ ?");

    assertEquals(Collections.singletonMap(1, "ssn"), paramMapping());
  }

  @Test
  void test_bareAnnotation_sameTableTwice_takesThatTable() throws Exception {
    executeWithSql("SELECT * FROM users x JOIN users y ON x.parent = y.id"
        + " WHERE lower(x.ssn) = /*@encrypt:ssn*/ ?");

    assertEquals(Collections.singletonMap(1, "users.ssn"), paramMapping());
  }

  @Test
  void test_bareAnnotation_subqueryTableCounts_staysBare() throws Exception {
    executeWithSql("SELECT * FROM other WHERE id IN (SELECT id FROM users WHERE lower(ssn) = /*@encrypt:ssn*/ ?)");

    assertEquals(Collections.singletonMap(1, "ssn"), paramMapping());
  }

  @Test
  void test_bareAnnotationWithUncertainTableInWrite_reportsUnmappedParameter() throws Exception {
    executeWithSql("UPDATE other o JOIN users u ON o.id = u.id SET u.ssn = lower(/*@encrypt:ssn*/ ?)");

    assertEquals(Collections.singletonMap(1, "ssn"), paramMapping());
    boolean reported = false;
    for (ParameterMappingProblem problem : problems()) {
      reported |= problem.getType() == ParameterMappingProblem.Type.UNKNOWN_TARGET_PARAMS
          && "users".equals(problem.getTable())
          && problem.getColumns().equals(Collections.singletonList("ssn"));
    }
    assertTrue(reported, problems().toString());
  }

  @Test
  void test_annotationOverridingPositionalColumnInWrite_reportsThatColumn() throws Exception {
    executeWithSql("INSERT INTO users (ssn) SELECT /*@encrypt:o.ssn*/ ? FROM other o");

    assertEquals(Collections.singletonMap(1, "o.ssn"), paramMapping());
    boolean reported = false;
    for (ParameterMappingProblem problem : problems()) {
      reported |= problem.getType() == ParameterMappingProblem.Type.UNKNOWN_TARGET_PARAMS
          && "users".equals(problem.getTable())
          && problem.getColumns().equals(Collections.singletonList("ssn"))
          && problem.getParameterIndexes().equals(Collections.singletonList(1));
    }
    assertTrue(reported, problems().toString());
  }

  @Test
  void test_schemaQualifiedAnnotationWithoutMapping_isKeptAsWritten() throws Exception {
    executeWithSql("SELECT * FROM public.users WHERE lower(ssn) = /*@encrypt:public.users.ssn*/ ?");

    assertEquals(Collections.singletonMap(1, "public.users.ssn"), paramMapping());
  }
}
