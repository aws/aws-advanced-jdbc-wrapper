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
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;

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
}
