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

import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.states.SessionStateService;

public class AutoReadWriteSplittingPluginTest {

  private AutoCloseable closeable;
  private AutoReadWriteSplittingPlugin plugin;
  private PluginCallContext callContext;

  @Mock PluginService mockPluginService;
  @Mock SessionStateService mockSessionStateService;

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    when(mockPluginService.getCallContext()).thenReturn(callContext);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.empty());
    plugin = new AutoReadWriteSplittingPlugin(mockPluginService, new Properties());
  }

  @AfterEach
  void cleanUp() throws Exception {
    callContext.reset();
    closeable.close();
  }

  private void setContextQueryType(QueryType queryType) {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, queryType);
  }

  private void setContextRoutingHint(RoutingHint hint) {
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
    assertTrue(methods.contains("CallableStatement.execute"));
    assertTrue(methods.contains("Connection.setReadOnly"));
    assertTrue(methods.contains("Connection.setAutoCommit"));
  }

  @Test
  void test_selectQuery_routesToReader() {
    setContextQueryType(QueryType.SELECT);

    assertTrue(plugin.shouldRouteToReader());
  }

  @Test
  void test_insertQuery_routesToWriter() {
    setContextQueryType(QueryType.INSERT);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_updateQuery_routesToWriter() {
    setContextQueryType(QueryType.UPDATE);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_deleteQuery_routesToWriter() {
    setContextQueryType(QueryType.DELETE);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_selectForUpdate_routesToWriter() {
    setContextQueryType(QueryType.SELECT);
    callContext.setAttribute(SqlContextKeys.FOR_UPDATE, true);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_writerHint_overridesSelectToWriter() {
    setContextQueryType(QueryType.SELECT);
    setContextRoutingHint(RoutingHint.WRITER);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_readerHint_overridesToReader() {
    setContextQueryType(QueryType.INSERT);
    setContextRoutingHint(RoutingHint.READER);

    assertTrue(plugin.shouldRouteToReader());
  }

  @Test
  void test_noContext_fallsBackToWriter() {
    // No context attributes set — falls back to writer to be safe
    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_shouldKeepCurrentConnection_falseWhenAutoCommitOnAndNoTransaction() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.of(true));

    assertFalse(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_shouldKeepCurrentConnection_falseWhenAutoCommitUnset() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.empty());

    assertFalse(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_shouldKeepCurrentConnection_trueWhenAutoCommitOff() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.of(false));

    assertTrue(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_shouldKeepCurrentConnection_trueWhenInTransaction() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.of(true));

    assertTrue(plugin.shouldKeepCurrentConnection());
  }
}
