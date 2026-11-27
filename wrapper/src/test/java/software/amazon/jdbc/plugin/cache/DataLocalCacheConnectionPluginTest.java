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

package software.amazon.jdbc.plugin.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect.AuthorizationStateImpact;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

@SuppressWarnings("unchecked")
class DataLocalCacheConnectionPluginTest {

  private static final String METHOD = JdbcMethod.STATEMENT_EXECUTEQUERY.methodName;
  private static final AuthorizationSessionState TENANT_A =
      new AuthorizationSessionState("appuser", "tenant_a", "\"$user\", public", "public");
  private static final AuthorizationSessionState TENANT_B =
      new AuthorizationSessionState("appuser", "tenant_b", "\"$user\", public", "public");

  private static final Properties props = new Properties();

  private AutoCloseable closeable;

  @Mock
  PluginService mockPluginService;
  @Mock
  TelemetryFactory mockTelemetryFactory;
  @Mock
  TelemetryCounter mockTelemetryCounter;
  @Mock
  TargetDriverDialect mockDialect;
  @Mock
  SessionStateService mockSessionState;
  @Mock
  HostSpec mockHostSpec;
  @Mock
  Connection mockConnection;
  @Mock
  ResultSet mockResult1;
  @Mock
  ResultSet mockResult2;
  @Mock
  CachedResultSet mockCached1;
  @Mock
  CachedResultSet mockCached2;
  @Mock
  Statement mockStatement;
  @Mock
  ResultSetMetaData mockMetaData;

  @Mock
  JdbcCallable mockCallable;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props.clear();
    props.setProperty(DataLocalCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, "foo.*");
    DataLocalCacheConnectionPlugin.clearCache();

    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);

    // A connection whose authorization state is tracked and currently identifies tenant A.
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockDialect);
    when(mockDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockDialect.getAuthorizationStateImpact(any())).thenReturn(AuthorizationStateImpact.NONE);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionState);
    when(mockSessionState.isAuthorizationStateTrackingEnabled()).thenReturn(true);
    when(mockSessionState.hasUntrackedAuthorizationState()).thenReturn(false);
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.of(TENANT_A));
    when(mockSessionState.getCatalog()).thenReturn(Optional.of("appdb"));
    when(mockSessionState.getSchema()).thenReturn(Optional.of("public"));
    when(mockPluginService.getCurrentHostSpec()).thenReturn(mockHostSpec);
    when(mockHostSpec.getHost()).thenReturn("instance-1.example.com");
    when(mockHostSpec.getPort()).thenReturn(5432);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockConnection.getAutoCommit()).thenReturn(true);
    when(mockConnection.getCatalog()).thenReturn("appdb");

    when(mockResult1.getMetaData()).thenReturn(mockMetaData);
    when(mockResult2.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumnCount()).thenReturn(1);
    when(mockMetaData.getColumnName(1)).thenReturn("fooName");
    when(mockMetaData.getColumnLabel(1)).thenReturn("fooName");

    // Mock result sets contain 1 row.
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult2.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");
    when(mockResult2.getObject(1)).thenReturn("bar2");
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  private ResultSet query(final DataLocalCacheConnectionPlugin plugin, final String sql)
      throws SQLException {
    return plugin.execute(
        ResultSet.class, SQLException.class, mockStatement, METHOD, mockCallable, new String[] {sql});
  }

  private static String firstValue(final ResultSet rs) throws SQLException {
    assertTrue(rs.next());
    return rs.getString(1);
  }

  @Test
  void test_execute_withEmptyCache() throws SQLException {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);

    final ResultSet rs = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement, METHOD,
        () -> mockResult1,
        new String[]{"foo"}
    );

    assertEquals("bar1", firstValue(rs));
  }

  @Test
  void test_execute_withCache() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertEquals("bar1", firstValue(query(plugin, "foo")));
    // Execute the query again; the database would now return a different result.
    assertEquals("bar1", firstValue(query(plugin, "foo")));

    verify(mockCallable).call();
  }

  @Test
  void test_noTriggerConditionPassesThroughWithoutTracking() throws Exception {
    final DataLocalCacheConnectionPlugin plugin =
        new DataLocalCacheConnectionPlugin(mockPluginService, new Properties());
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertSame(mockResult1, query(plugin, "foo"));
    assertSame(mockResult2, query(plugin, "foo"));
    assertFalse(plugin.getSubscribedMethods().contains(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName));
    verify(mockSessionState, never()).getAuthorizationState();
  }

  // Each test below changes one component of the connection identity and expects the second query
  // to reach the database.

  @Test
  void test_differentAuthorizationStateIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertEquals("bar1", firstValue(query(plugin, "foo")));
    // The same login switched to another tenant role, as SET ROLE does on PostgreSQL.
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.of(TENANT_B));

    assertEquals("bar2", firstValue(query(plugin, "foo")));
    verify(mockCallable, times(2)).call();
  }

  @Test
  void test_differentUserPropertyIsAMiss() throws Exception {
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    final Properties userA = new Properties();
    userA.putAll(props);
    userA.setProperty(PropertyDefinition.USER.name, "user_a");
    final Properties userB = new Properties();
    userB.putAll(props);
    userB.setProperty(PropertyDefinition.USER.name, "user_b");

    assertEquals("bar1", firstValue(query(new DataLocalCacheConnectionPlugin(mockPluginService, userA), "foo")));
    assertEquals("bar2", firstValue(query(new DataLocalCacheConnectionPlugin(mockPluginService, userB), "foo")));
  }

  @Test
  void test_differentHostIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertEquals("bar1", firstValue(query(plugin, "foo")));
    // Another cluster with the same database name, user and role.
    when(mockHostSpec.getHost()).thenReturn("other-cluster.example.com");

    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_differentCatalogIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertEquals("bar1", firstValue(query(plugin, "foo")));
    when(mockConnection.getCatalog()).thenReturn("tenant2db");

    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_differentSchemaIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertEquals("bar1", firstValue(query(plugin, "foo")));
    when(mockSessionState.getSchema()).thenReturn(Optional.of("t2"));

    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_unknownAuthorizationStateBypassesCache() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.empty());
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertSame(mockResult1, query(plugin, "foo"));
    assertSame(mockResult2, query(plugin, "foo"));
    assertEquals(0, DataLocalCacheConnectionPlugin.dataCache.size());
  }

  @Test
  void test_untrackedAuthorizationStateBypassesCache() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    // For example SET app.tenant_id = '42' read by a row-level security policy.
    when(mockSessionState.hasUntrackedAuthorizationState()).thenReturn(true);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertSame(mockResult1, query(plugin, "foo"));
    assertSame(mockResult2, query(plugin, "foo"));
    assertEquals(0, DataLocalCacheConnectionPlugin.dataCache.size());
  }

  @Test
  void test_unsupportedTargetDriverBypassesCache() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockDialect.supportsAuthorizationSessionState()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertSame(mockResult1, query(plugin, "foo"));
    assertSame(mockResult2, query(plugin, "foo"));
    assertEquals(0, DataLocalCacheConnectionPlugin.dataCache.size());
  }

  @Test
  void test_sessionDependentQueryBypassesCache() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockDialect.getAuthorizationStateImpact("foo current_setting('app.tenant')"))
        .thenReturn(AuthorizationStateImpact.UNCACHEABLE);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertSame(mockResult1, query(plugin, "foo current_setting('app.tenant')"));
    assertSame(mockResult2, query(plugin, "foo current_setting('app.tenant')"));
  }

  @Test
  void test_untrackedStatementThroughPluginDisablesCaching() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockDialect.getAuthorizationStateImpact("SET app.tenant_id = '42'"))
        .thenReturn(AuthorizationStateImpact.UNTRACKED);

    plugin.execute(
        Boolean.class, SQLException.class, mockStatement, JdbcMethod.STATEMENT_EXECUTE.methodName,
        () -> false, new String[] {"SET app.tenant_id = '42'"});

    verify(mockSessionState).markAuthorizationStateUntracked();
  }

  @Test
  void test_trackedStatementThroughPluginRefreshesState() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockDialect.getAuthorizationStateImpact("SET ROLE tenant_b"))
        .thenReturn(AuthorizationStateImpact.TRACKED);

    plugin.execute(
        Boolean.class, SQLException.class, mockStatement, JdbcMethod.STATEMENT_EXECUTE.methodName,
        () -> false, new String[] {"SET ROLE tenant_b"});

    verify(mockSessionState).refreshAuthorizationState();
  }

  @Test
  void test_connectionChangeDuringQueryIsNotCached() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);

    final ResultSet rs = plugin.execute(
        ResultSet.class, SQLException.class, mockStatement, METHOD,
        () -> {
          // Failover replaces the underlying connection while the query runs.
          plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
          return mockResult1;
        },
        new String[] {"foo"});

    assertSame(mockResult1, rs);
    assertEquals(0, DataLocalCacheConnectionPlugin.dataCache.size());
  }

  @Test
  void test_initialConnectionEnablesAuthorizationTracking() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);

    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION));

    verify(mockSessionState).enableAuthorizationStateTracking();
    verify(mockSessionState).refreshAuthorizationState();
  }

  @Test
  void test_cacheHitsHaveIndependentCursors() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1);

    final ResultSet first = query(plugin, "foo");
    final ResultSet second = query(plugin, "foo");
    final ResultSet third = query(plugin, "foo");

    assertNotSame(second, third);
    assertEquals("bar1", firstValue(second));
    second.close();
    // Reading or closing one result must not move or close the others.
    assertEquals("bar1", firstValue(third));
    assertEquals("bar1", firstValue(first));
  }

  @Test
  void test_cacheKeyDoesNotWriteSessionState() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockSessionState.getCatalog()).thenReturn(Optional.empty());
    when(mockSessionState.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getCatalog()).thenReturn("appdb");
    when(mockCallable.call()).thenReturn(mockResult1);

    query(plugin, "foo");

    // Failover and read/write splitting replay recorded catalog and schema onto the new connection.
    verify(mockSessionState, never()).setCatalog(anyString());
    verify(mockSessionState, never()).setSchema(anyString());
    verify(mockConnection, never()).getSchema();
  }

  @Test
  void test_ignoredSetCatalogDoesNotReplaceRealDatabase() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    // The PostgreSQL driver ignores setCatalog, so the recorded catalog does not name the database.
    when(mockSessionState.getCatalog()).thenReturn(Optional.of("tenant_b"));
    when(mockConnection.getCatalog()).thenReturn("tenant_a");
    assertEquals("bar1", firstValue(query(plugin, "foo")));

    when(mockConnection.getCatalog()).thenReturn("tenant_b");
    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_trackedStatementInsideTransactionMarksStateUnknown() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockDialect.getAuthorizationStateImpact("SET LOCAL ROLE tenant_b"))
        .thenReturn(AuthorizationStateImpact.TRACKED);
    // Auto-commit is on, but the application opened a transaction with BEGIN.
    when(mockPluginService.isInTransaction()).thenReturn(true);

    plugin.execute(
        Boolean.class, SQLException.class, mockStatement, JdbcMethod.STATEMENT_EXECUTE.methodName,
        () -> false, new String[] {"SET LOCAL ROLE tenant_b"});

    // Reading the state here would record a role that the end of the transaction reverts.
    verify(mockSessionState, never()).refreshAuthorizationState();
    verify(mockSessionState).markAuthorizationStateUnknown();
  }

  @Test
  void test_connectionChangeInsideTransactionDoesNotQuery() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockPluginService.isInTransaction()).thenReturn(true);

    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));

    verify(mockSessionState, never()).refreshAuthorizationState();
    verify(mockSessionState).markAuthorizationStateUnknown();
  }

  @Test
  void test_cacheKeyFailureBypassesCache() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockSessionState.getCatalog()).thenReturn(Optional.empty());
    when(mockConnection.getCatalog()).thenThrow(new SQLException("closed"));
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    assertSame(mockResult1, query(plugin, "foo"));
    assertSame(mockResult2, query(plugin, "foo"));
    assertEquals(0, DataLocalCacheConnectionPlugin.dataCache.size());
  }

  @Test
  void test_materializationFailureIsThrown() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockResult1.next()).thenReturn(true).thenThrow(new SQLException("connection reset"));
    when(mockCallable.call()).thenReturn(mockResult1);

    // The rows already read cannot be given back, so a partial result must not be returned.
    assertThrows(SQLException.class, () -> query(plugin, "foo"));
    assertEquals(0, DataLocalCacheConnectionPlugin.dataCache.size());
  }

  @Test
  void test_commitWithAutoCommitOffKeepsKnownState() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockConnection.getAutoCommit()).thenReturn(false);

    plugin.execute(
        Void.class, SQLException.class, mockConnection, JdbcMethod.CONNECTION_COMMIT.methodName,
        () -> null, new Object[0]);

    verify(mockSessionState, never()).markAuthorizationStateUnknown();
    verify(mockSessionState, never()).refreshAuthorizationState();
  }

  @Test
  void test_commitWithAutoCommitOffKeepsUnknownState() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockConnection.getAutoCommit()).thenReturn(false);
    // A SET ROLE earlier in the transaction left the state unknown.
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.empty());

    plugin.execute(
        Void.class, SQLException.class, mockConnection, JdbcMethod.CONNECTION_COMMIT.methodName,
        () -> null, new Object[0]);

    verify(mockSessionState).markAuthorizationStateUnknown();
    verify(mockSessionState, never()).refreshAuthorizationState();
  }

  @Test
  void test_autoCommitOnAfterUnchangedTransactionSkipsRefresh() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockConnection.getAutoCommit()).thenReturn(false, true);

    plugin.execute(
        Void.class, SQLException.class, mockConnection, JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName,
        () -> null, new Object[] {true});

    verify(mockSessionState, never()).refreshAuthorizationState();
  }

  @Test
  void test_autoCommitOnAfterTrackedChangeRefreshesState() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockConnection.getAutoCommit()).thenReturn(false, true);
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.empty());

    plugin.execute(
        Void.class, SQLException.class, mockConnection, JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName,
        () -> null, new Object[] {true});

    verify(mockSessionState).refreshAuthorizationState();
  }

  @Test
  void test_connectionChangeWithAutoCommitOffDoesNotQuery() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockConnection.getAutoCommit()).thenReturn(false);

    // A read/write splitting switch that transferred autoCommit=false. A query here would open a
    // transaction, and a later setReadOnly on the PostgreSQL driver would then fail.
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));

    verify(mockSessionState, never()).refreshAuthorizationState();
    verify(mockSessionState).markAuthorizationStateUnknown();
  }

  @Test
  void test_differentPortIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    assertEquals("bar1", firstValue(query(plugin, "foo")));
    when(mockHostSpec.getPort()).thenReturn(5433);
    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_activeRolesIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.of(
        new AuthorizationSessionState("app@%", "app@%", "", "", "`tenant_a`@`%`", "appdb")));
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    assertEquals("bar1", firstValue(query(plugin, "foo")));
    // MySQL SET ROLE changes CURRENT_ROLE() only.
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.of(
        new AuthorizationSessionState("app@%", "app@%", "", "", "`tenant_b`@`%`", "appdb")));
    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_currentDatabaseIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.of(
        new AuthorizationSessionState("app@%", "app@%", "", "", "", "tenant1db")));
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    assertEquals("bar1", firstValue(query(plugin, "foo")));
    // MySQL USE via SQL; the session-state catalog is not updated.
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.of(
        new AuthorizationSessionState("app@%", "app@%", "", "", "", "tenant2db")));
    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_searchPathIsAMiss() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    assertEquals("bar1", firstValue(query(plugin, "foo")));
    // PostgreSQL SET search_path; the session-state schema is not updated.
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.of(
        new AuthorizationSessionState("appuser", "tenant_a", "t2, public", "t2, public")));
    assertEquals("bar2", firstValue(query(plugin, "foo")));
  }

  @Test
  void test_untrackedExecuteQueryDisablesCaching() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    final String sql = "SELECT set_config('app.tenant_id', '42', false)";
    when(mockDialect.getAuthorizationStateImpact(sql)).thenReturn(AuthorizationStateImpact.UNTRACKED);
    when(mockCallable.call()).thenReturn(mockResult1);
    query(plugin, sql);
    verify(mockSessionState).markAuthorizationStateUntracked();
  }

  @Test
  void test_multiStatementBypassesCache() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    assertSame(mockResult1, query(plugin, "foo; SELECT 1"));
    assertSame(mockResult2, query(plugin, "foo; SELECT 1"));
  }

  @Test
  void test_subscribesToTrackedMethodsWhenEnabled() {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    assertTrue(plugin.getSubscribedMethods().contains(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName));
    assertTrue(plugin.getSubscribedMethods().contains(JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName));
    assertTrue(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECTION_SETCATALOG.methodName));
  }

  @Test
  void test_trackedStatementWithAutoCommitOffMarksKnownStateUnknown() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    when(mockConnection.getAutoCommit()).thenReturn(false);
    when(mockDialect.getAuthorizationStateImpact("SET ROLE tenant_b"))
        .thenReturn(AuthorizationStateImpact.TRACKED);

    plugin.execute(
        Boolean.class, SQLException.class, mockStatement, JdbcMethod.STATEMENT_EXECUTE.methodName,
        () -> false, new String[] {"SET ROLE tenant_b"});

    verify(mockSessionState).markAuthorizationStateUnknown();
  }

  @Test
  void test_sqlCommitWithAutoCommitOnRefreshesState() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);

    // BEGIN; SET LOCAL ROLE ...; COMMIT under JDBC auto-commit: COMMIT ends the transaction-local role.
    plugin.execute(
        Boolean.class, SQLException.class, mockStatement, JdbcMethod.STATEMENT_EXECUTE.methodName,
        () -> false, new String[] {"COMMIT"});

    verify(mockSessionState).refreshAuthorizationState();
  }

  @Test
  void test_commitStatementAfterTransactionRefreshesUnknownState() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);
    // A change inside the transaction left the state unknown; COMMIT has ended the transaction.
    when(mockSessionState.getAuthorizationState()).thenReturn(Optional.empty());
    when(mockPluginService.isInTransaction()).thenReturn(false);

    plugin.execute(
        Boolean.class, SQLException.class, mockStatement, JdbcMethod.STATEMENT_EXECUTE.methodName,
        () -> false, new String[] {"COMMIT"});

    verify(mockSessionState).refreshAuthorizationState();
  }

  @Test
  void test_unsupportedTargetDriverLogsWarning() throws Exception {
    final Logger logger = Logger.getLogger(DataLocalCacheConnectionPlugin.class.getName());
    final List<LogRecord> records = new ArrayList<>();
    final Handler handler = new Handler() {
      @Override
      public void publish(final LogRecord logRecord) {
        records.add(logRecord);
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
      // The warning is logged once per JVM, so the flag is reset for this test.
      final Field flag = DataLocalCacheConnectionPlugin.class.getDeclaredField("unsupportedDriverWarningLogged");
      flag.setAccessible(true);
      ((AtomicBoolean) flag.get(null)).set(false);
      when(mockDialect.supportsAuthorizationSessionState()).thenReturn(false);
      final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);

      plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION));

      assertTrue(records.stream().anyMatch(r -> r.getLevel() == Level.WARNING));
    } finally {
      logger.removeHandler(handler);
    }
  }

  /**
   * Builds a plugin with an explicit time-to-live and size limit. The tests below exercise the cache
   * bounds directly rather than through {@code execute}, because a materialized {@code CachedResultSet}
   * is not needed to observe eviction.
   */
  private DataLocalCacheConnectionPlugin boundedPlugin(final String ttlMs, final String maxSize) {
    final Properties boundedProps = new Properties();
    boundedProps.setProperty(DataLocalCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, ".*");
    boundedProps.setProperty(DataLocalCacheConnectionPlugin.DATA_CACHE_TTL_MS.name, ttlMs);
    boundedProps.setProperty(DataLocalCacheConnectionPlugin.DATA_CACHE_MAX_SIZE.name, maxSize);
    return new DataLocalCacheConnectionPlugin(mockPluginService, boundedProps);
  }

  @Test
  void test_freshEntryIsServed() {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("60000", "10");

    plugin.tryCache("SELECT 1", mockCached1);

    assertSame(mockCached1, plugin.getIfFresh("SELECT 1"));
  }

  /**
   * A negative bound must fail fast rather than be read as "unlimited". {@code 0} is the value that
   * opts out of expiration and of the size limit, so treating a negative value as an opt-out too
   * would silently give a user who wrote {@code -1} the opposite of what they asked for.
   */
  @Test
  void test_negativeTtlIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> boundedPlugin("-1", "10"));
  }

  @Test
  void test_negativeMaxSizeIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> boundedPlugin("60000", "-1"));
  }

  @Test
  void test_zeroBoundsAreAccepted() {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("0", "0");

    plugin.tryCache("SELECT 1", mockCached1);

    assertSame(mockCached1, plugin.getIfFresh("SELECT 1"),
        "Zero remains the documented opt-out for both bounds");
  }

  @Test
  void test_absentEntryIsAMiss() {
    assertNull(boundedPlugin("60000", "10").getIfFresh("SELECT 1"));
  }

  /**
   * Before {@code dataCacheTtlMs} existed, a cached result was served indefinitely no matter what was
   * written to the table in the meantime.
   */
  @Test
  void test_expiredEntryIsAMissAndIsEvicted() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("1", "10");

    plugin.tryCache("SELECT 1", mockCached1);
    assertEquals(1, DataLocalCacheConnectionPlugin.dataCache.size());

    // The wait is the thing under test: the entry must stop being served once its TTL has elapsed.
    Thread.sleep(20);

    assertNull(plugin.getIfFresh("SELECT 1"));
    assertEquals(0, DataLocalCacheConnectionPlugin.dataCache.size(),
        "An expired entry should be removed when it is found to be stale");
  }

  @Test
  void test_ttlZeroKeepsEntryIndefinitely() throws Exception {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("0", "10");

    plugin.tryCache("SELECT 1", mockCached1);
    Thread.sleep(20);

    assertSame(mockCached1, plugin.getIfFresh("SELECT 1"),
        "dataCacheTtlMs=0 opts out of expiration");
  }

  /**
   * Before {@code dataCacheMaxSize} existed, the cache grew without bound in the number of distinct
   * SQL strings the process had ever seen.
   */
  @Test
  void test_sizeLimitStopsCachingNewStatements() {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("60000", "2");

    plugin.tryCache("SELECT 1", mockCached1);
    plugin.tryCache("SELECT 2", mockCached2);
    plugin.tryCache("SELECT 3", mockCached1);

    assertEquals(2, DataLocalCacheConnectionPlugin.dataCache.size(),
        "The cache must not grow past dataCacheMaxSize");
    assertNotNull(plugin.getIfFresh("SELECT 1"));
    assertNotNull(plugin.getIfFresh("SELECT 2"));
    assertNull(plugin.getIfFresh("SELECT 3"), "The statement that did not fit is not cached");
  }

  @Test
  void test_sizeLimitStillRefreshesAnExistingKey() {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("60000", "1");

    plugin.tryCache("SELECT 1", mockCached1);
    plugin.tryCache("SELECT 1", mockCached2);

    assertEquals(1, DataLocalCacheConnectionPlugin.dataCache.size());
    assertSame(mockCached2, plugin.getIfFresh("SELECT 1"),
        "Replacing the value of a key already present does not consume a new slot");
  }

  @Test
  void test_sizeLimitPurgesExpiredEntriesToMakeRoom() throws Exception {
    boundedPlugin("1", "1").tryCache("SELECT 1", mockCached1);
    Thread.sleep(20);

    // The cache is full, but only of an expired entry, so the new statement must displace it.
    final DataLocalCacheConnectionPlugin longLived = boundedPlugin("60000", "1");
    longLived.tryCache("SELECT 2", mockCached2);

    assertEquals(1, DataLocalCacheConnectionPlugin.dataCache.size());
    assertSame(mockCached2, longLived.getIfFresh("SELECT 2"));
    assertNull(longLived.getIfFresh("SELECT 1"));
  }
}
