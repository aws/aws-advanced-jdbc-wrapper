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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.SqlContextKeys;

public class AutoReadWriteSplittingPluginTest {

  private AutoCloseable closeable;
  private AutoReadWriteSplittingPlugin plugin;
  private PluginCallContext callContext;

  @Mock PluginService mockPluginService;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    when(mockPluginService.getCallContext()).thenReturn(callContext);
    plugin = new AutoReadWriteSplittingPlugin(mockPluginService, new Properties());
  }

  @AfterEach
  void cleanUp() throws Exception {
    callContext.reset();
    closeable.close();
  }

  private void setContextQueryType(String queryType) {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, queryType);
  }

  private void setContextCleanSql(String sql) {
    callContext.setAttribute(SqlContextKeys.CLEAN_SQL, sql);
  }

  private void setContextRoutingHint(String hint) {
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, hint);
  }

  @Test
  void test_subscribedMethods_includesExecuteAndParentMethods() {
    Set<String> methods = plugin.getSubscribedMethods();
    assertTrue(methods.contains("PreparedStatement.executeQuery"));
    assertTrue(methods.contains("PreparedStatement.executeUpdate"));
    assertTrue(methods.contains("PreparedStatement.execute"));
    assertTrue(methods.contains("PreparedStatement.executeBatch"));
    assertTrue(methods.contains("Statement.executeQuery"));
    assertTrue(methods.contains("Statement.executeUpdate"));
    assertTrue(methods.contains("Statement.execute"));
    assertTrue(methods.contains("Statement.executeBatch"));
    assertTrue(methods.contains("Connection.setReadOnly"));
    assertTrue(methods.contains("Connection.setAutoCommit"));
  }

  @Test
  void test_selectQuery_routesToReader() {
    setContextQueryType("SELECT");
    setContextCleanSql("SELECT * FROM users");
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertTrue(plugin.shouldRouteToReader("PreparedStatement.executeQuery"));
  }

  @Test
  void test_insertQuery_routesToWriter() {
    setContextQueryType("INSERT");
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertFalse(plugin.shouldRouteToReader("PreparedStatement.executeUpdate"));
  }

  @Test
  void test_updateQuery_routesToWriter() {
    setContextQueryType("UPDATE");
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertFalse(plugin.shouldRouteToReader("PreparedStatement.executeUpdate"));
  }

  @Test
  void test_deleteQuery_routesToWriter() {
    setContextQueryType("DELETE");
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertFalse(plugin.shouldRouteToReader("PreparedStatement.executeUpdate"));
  }

  @Test
  void test_selectForUpdate_routesToWriter() {
    setContextQueryType("SELECT");
    callContext.setAttribute(SqlContextKeys.FOR_UPDATE, true);
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertFalse(plugin.shouldRouteToReader("PreparedStatement.executeQuery"));
  }

  @Test
  void test_inTransaction_alwaysRoutesToWriter() {
    setContextQueryType("SELECT");
    setContextCleanSql("SELECT * FROM users");
    when(mockPluginService.isInTransaction()).thenReturn(true);

    assertFalse(plugin.shouldRouteToReader("PreparedStatement.executeQuery"));
  }

  @Test
  void test_writerHint_overridesSelectToWriter() {
    setContextQueryType("SELECT");
    setContextCleanSql("SELECT * FROM users");
    setContextRoutingHint("writer");
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertFalse(plugin.shouldRouteToReader("PreparedStatement.executeQuery"));
  }

  @Test
  void test_readerHint_overridesToReader() {
    setContextQueryType("INSERT");
    setContextRoutingHint("reader");
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertTrue(plugin.shouldRouteToReader("PreparedStatement.execute"));
  }

  @Test
  void test_noContext_executeQueryFallsBackToReader() {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    // No context set — falls back to method name heuristic

    assertTrue(plugin.shouldRouteToReader("Statement.executeQuery"));
  }

  @Test
  void test_noContext_executeUpdateFallsBackToWriter() {
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertFalse(plugin.shouldRouteToReader("Statement.executeUpdate"));
  }

  @Test
  void test_noContext_executeFallsBackToWriter() {
    when(mockPluginService.isInTransaction()).thenReturn(false);

    assertFalse(plugin.shouldRouteToReader("Statement.execute"));
  }
}
