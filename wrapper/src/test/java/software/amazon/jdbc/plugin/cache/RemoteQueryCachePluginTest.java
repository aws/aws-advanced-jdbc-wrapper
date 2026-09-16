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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

@SuppressWarnings({"unchecked", "deprecation"})
public class RemoteQueryCachePluginTest {
  private Properties props;
  private final String methodName = "Statement.executeQuery";
  private AutoCloseable closeable;

  private RemoteQueryCachePlugin plugin;
  @Mock
  FullServicesContainer mockServicesContainer;
  @Mock
  PluginService mockPluginService;
  @Mock
  TelemetryFactory mockTelemetryFactory;
  @Mock
  MonitorService mockMonitorService;
  @Mock
  TelemetryCounter mockCacheHitCounter;
  @Mock
  TelemetryCounter mockCacheMissCounter;
  @Mock
  TelemetryCounter mockTotalQueryCounter;
  @Mock
  TelemetryCounter mockMalformedHintCounter;
  @Mock
  TelemetryCounter mockCacheBypassCounter;
  @Mock
  TelemetryContext mockTelemetryContext;
  @Mock
  ResultSet mockResult1;
  @Mock
  ResultSet mockResult2;
  @Mock
  Statement mockStatement;
  @Mock
  PreparedStatement mockPreparedStatement;
  @Mock
  ResultSetMetaData mockMetaData;
  @Mock
  Connection mockConnection;
  @Mock
  SessionStateService mockSessionStateService;
  @Mock
  TargetDriverDialect mockTargetDriverDialect;
  @Mock
  DatabaseMetaData mockDbMetadata;
  @Mock
  CacheConnection mockCacheConn;
  @Mock
  JdbcCallable mockCallable;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    props.setProperty("wrapperPlugins", "remoteQueryCache");
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    // Setup mock services container
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockServicesContainer.getMonitorService()).thenReturn(mockMonitorService);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockConnection.getAutoCommit()).thenReturn(true);
    when(mockTelemetryFactory.createCounter("remoteQueryCache.cache.hit")).thenReturn(mockCacheHitCounter);
    when(mockTelemetryFactory.createCounter("remoteQueryCache.cache.miss")).thenReturn(mockCacheMissCounter);
    when(mockTelemetryFactory.createCounter("remoteQueryCache.cache.totalQueries")).thenReturn(mockTotalQueryCounter);
    when(mockTelemetryFactory.createCounter("remoteQueryCache.cache.malformedHints"))
        .thenReturn(mockMalformedHintCounter);
    when(mockTelemetryFactory.createCounter("remoteQueryCache.cache.bypass")).thenReturn(mockCacheBypassCounter);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockResult1.getMetaData()).thenReturn(mockMetaData);
    when(mockResult2.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumnCount()).thenReturn(1);
    when(mockMetaData.getColumnLabel(1)).thenReturn("fooName");
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_getTTLFromQueryHint() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Null and empty query hint content are not cacheable
    assertNull(plugin.getTtlForQuery(null));
    assertNull(plugin.getTtlForQuery(""));
    assertNull(plugin.getTtlForQuery("    "));
    // Valid CACHE_PARAM cases - these are the hint contents after /*+ and before */
    assertEquals(300, plugin.getTtlForQuery("CACHE_PARAM(ttl=300s)"));
    assertEquals(100, plugin.getTtlForQuery("CACHE_PARAM(ttl=100s)"));
    assertEquals(35, plugin.getTtlForQuery("CACHE_PARAM(ttl=35s)"));

    // Case insensitive
    assertEquals(200, plugin.getTtlForQuery("cache_param(ttl=200s)"));
    assertEquals(150, plugin.getTtlForQuery("Cache_Param(ttl=150s)"));
    assertEquals(200, plugin.getTtlForQuery("cache_param(tTl=200s)"));
    assertEquals(150, plugin.getTtlForQuery("Cache_Param(ttl=150S)"));
    assertEquals(200, plugin.getTtlForQuery("cache_param(TTL=200S)"));

    // CACHE_PARAM anywhere in hint content (mixed with other hint directives)
    assertEquals(250, plugin.getTtlForQuery("INDEX(table1 idx1) CACHE_PARAM(ttl=250s)"));
    assertEquals(200, plugin.getTtlForQuery("CACHE_PARAM(ttl=200s) USE_NL(t1 t2)"));
    assertEquals(180, plugin.getTtlForQuery("FIRST_ROWS(10) CACHE_PARAM(ttl=180s) PARALLEL(4)"));
    assertEquals(200, plugin.getTtlForQuery("foo=bar,CACHE_PARAM(ttl=200s),baz=qux"));

    // Maximum TTL enforcement
    assertEquals(15552000, plugin.getTtlForQuery("CACHE_PARAM(ttl=1000000000s)"));

    // Whitespace handling
    assertEquals(400, plugin.getTtlForQuery("CACHE_PARAM( ttl=400s )"));
    assertEquals(500, plugin.getTtlForQuery("CACHE_PARAM(ttl = 500s)"));
    assertEquals(200, plugin.getTtlForQuery("CACHE_PARAM( ttl = 200s , key = test )"));

    // Invalid cases - no CACHE_PARAM in hint content
    assertNull(plugin.getTtlForQuery("INDEX(table1 idx1)"));
    assertNull(plugin.getTtlForQuery("FIRST_ROWS(100)"));
    assertNull(plugin.getTtlForQuery("cachettl=300s")); // old format
    assertNull(plugin.getTtlForQuery("NO_CACHE"));

    // Missing parentheses
    assertNull(plugin.getTtlForQuery("CACHE_PARAM ttl=300s"));
    assertNull(plugin.getTtlForQuery("CACHE_PARAM(ttl=300s"));

    // Multiple parameters (future-proofing)
    assertEquals(300, plugin.getTtlForQuery("CACHE_PARAM(ttl=300s, key=test)"));

    // Large TTL values should work
    assertEquals(999999, plugin.getTtlForQuery("CACHE_PARAM(ttl=999999s)"));
    assertEquals(86400, plugin.getTtlForQuery("CACHE_PARAM(ttl=86400s)")); // 24 hours
  }

  @Test
  void test_getTTLFromQueryHint_MalformedHints() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Test malformed cases
    assertNull(plugin.getTtlForQuery("CACHE_PARAM()"));
    assertNull(plugin.getTtlForQuery("CACHE_PARAM(ttl=abc)"));
    assertNull(plugin.getTtlForQuery("CACHE_PARAM(ttl=300)")); // missing 's'

    assertNull(plugin.getTtlForQuery("CACHE_PARAM(ttl=)"));
    assertNull(plugin.getTtlForQuery("CACHE_PARAM(invalid_format)"));

    // Invalid TTL values (negative and zero) does not count toward malformed hints
    assertNull(plugin.getTtlForQuery("CACHE_PARAM(ttl=0s)"));
    assertNull(plugin.getTtlForQuery("CACHE_PARAM(ttl=-10s)"));
    assertNull(plugin.getTtlForQuery("CACHE_PARAM(ttl=-1s)"));

    // Verify counter was incremented 8 times (5 original + 3 new)
    verify(mockMalformedHintCounter, times(5)).inc();
  }

  @Test
  void test_execute_noCaching() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1);

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"select * from mytable where ID = 2"});

    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1");
    compareResults(mockResult1, rs);
    verify(mockPluginService, never()).isInTransaction();
    verify(mockCallable).call();
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    verify(mockCacheMissCounter, never()).inc();
    // Verify TelemetryContext behavior for no-caching scenario
    verify(mockTelemetryFactory).openTelemetryContext("jdbc-database-query", TelemetryTraceLevel.NESTED);
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryContext).closeContext();
  }

  @Test
  void test_execute_emptyQuery_noCaching() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1);

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{});

    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1");
    compareResults(mockResult1, rs);
    verify(mockPluginService, never()).isInTransaction();
    verify(mockCallable).call();
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    verify(mockCacheMissCounter, never()).inc();
    // Verify TelemetryContext behavior for no-caching scenario
    verify(mockTelemetryFactory).openTelemetryContext("jdbc-database-query", TelemetryTraceLevel.NESTED);
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryContext).closeContext();
  }

  @Test
  void test_execute_emptyPreparedStatement_noCaching() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockTargetDriverDialect.getSQLQueryString(mockPreparedStatement))
        .thenReturn("", (String) null);
    when(mockCallable.call()).thenReturn(mockResult1);

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockPreparedStatement,
        methodName, mockCallable, new String[]{});
    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false, true, true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1", "bar1", "bar1");
    compareResults(mockResult1, rs);

    rs = plugin.execute(ResultSet.class, SQLException.class, mockPreparedStatement,
        methodName, mockCallable, new String[]{});
    // Mock result set containing 1 row
    compareResults(mockResult1, rs);

    verify(mockPluginService, never()).isInTransaction();
    verify(mockPluginService, times(2)).getTargetDriverDialect();
    verify(mockCallable, times(2)).call();
    verify(mockTotalQueryCounter, times(2)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheBypassCounter, times(2)).inc();
    verify(mockCacheMissCounter, never()).inc();
    // Verify TelemetryContext behavior for no-caching scenario
    verify(mockTelemetryFactory, times(2)).openTelemetryContext("jdbc-database-query", TelemetryTraceLevel.NESTED);
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryContext, times(2)).closeContext();
  }

  @Test
  void test_execute_noCachingLongQuery() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1);

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable,
        new String[]{"/* CACHE_PARAM(ttl=20s) */ select * from T " + RandomStringUtils.randomAlphanumeric(16350)});

    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1");
    compareResults(mockResult1, rs);
    verify(mockCallable).call();
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    verify(mockCacheMissCounter, never()).inc();
    // Verify TelemetryContext behavior for no-caching scenario
    verify(mockTelemetryFactory).openTelemetryContext("jdbc-database-query", TelemetryTraceLevel.NESTED);
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryContext).closeContext();
  }

  @Test
  void test_execute_cachingMissAndHit() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    final String expectedCacheKey = cacheKey("mysql", null, "", "select * from A");
    // Query is not cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty()).thenReturn(Optional.of("mysql"));
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getCatalog()).thenReturn("mysql");
    when(mockConnection.getSchema()).thenReturn(null);
    when(mockCacheConn.readFromCache(expectedCacheKey)).thenReturn(null);
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"/*+CACHE_PARAM(ttl=50s)*/ select * from A"});

    // Cached result set contains 1 row
    assertTrue(rs.next());
    assertEquals("bar1", rs.getString("fooName"));
    assertFalse(rs.next());

    rs.beforeFirst();
    byte[] serializedTestResultSet = ((CachedResultSet) rs).serializeIntoByteArray();
    when(mockCacheConn.readFromCache(expectedCacheKey)).thenReturn(serializedTestResultSet);

    ResultSet rs2 = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{" /*+CACHE_PARAM(ttl=50s)*/select * from A"});

    assertTrue(rs2.next());
    assertEquals("bar1", rs2.getString("fooName"));
    assertFalse(rs2.next());
    verify(mockPluginService, times(4)).getCurrentConnection();
    verify(mockPluginService, times(2)).isInTransaction();
    verify(mockCacheConn, times(2)).readFromCache(expectedCacheKey);
    verify(mockPluginService, times(2)).getSessionStateService();
    verify(mockSessionStateService, times(2)).getCatalog();
    verify(mockSessionStateService, times(2)).getSchema();
    verify(mockConnection).getCatalog();
    verify(mockConnection).getSchema();
    verify(mockSessionStateService).setCatalog("mysql");
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq(expectedCacheKey), any(), eq(50));
    verify(mockTotalQueryCounter, times(2)).inc();
    verify(mockCacheMissCounter, times(1)).inc();
    verify(mockCacheHitCounter, times(1)).inc();
    verify(mockCacheBypassCounter, never()).inc();
    // Verify TelemetryContext behavior for cache miss and hit scenario
    // First call: Cache miss + Database call
    verify(mockTelemetryFactory, times(2)).openTelemetryContext(eq("jdbc-cache-lookup"),
        eq(TelemetryTraceLevel.NESTED));
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    // Cache context calls: 1 miss (setSuccess(false)) + 1 hit (setSuccess(true))
    verify(mockTelemetryContext, times(1)).setSuccess(false); // Cache miss
    verify(mockTelemetryContext, times(1)).setSuccess(true);  // Cache hit
    // Context closure: 2 cache contexts + 1 database context = 3 total
    verify(mockTelemetryContext, times(3)).closeContext();
  }

  @Test
  void test_cachingMissAndHit_preparedStatement() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    final String expectedCacheKey = cacheKey("mysql", null, "", "select * from A");
    // Query is a cache miss
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty()).thenReturn(Optional.of("mysql"));
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getCatalog()).thenReturn("mysql");
    when(mockConnection.getSchema()).thenReturn(null);
    when(mockCacheConn.readFromCache(expectedCacheKey)).thenReturn(null);
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");
    when(mockTargetDriverDialect.getSQLQueryString(mockPreparedStatement))
        .thenReturn("/* CACHE_PARAM(ttl=50s) */ select * from A");

    // Now query is a cache hit
    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockPreparedStatement,
        methodName, mockCallable, new String[]{});

    // Cached result set contains 1 row
    assertTrue(rs.next());
    assertEquals("bar1", rs.getString("fooName"));
    assertFalse(rs.next());

    rs.beforeFirst();
    byte[] serializedTestResultSet = ((CachedResultSet) rs).serializeIntoByteArray();
    when(mockCacheConn.readFromCache(expectedCacheKey)).thenReturn(serializedTestResultSet);

    ResultSet rs2 = plugin.execute(ResultSet.class, SQLException.class, mockPreparedStatement,
        methodName, mockCallable, new String[]{});

    assertTrue(rs2.next());
    assertEquals("bar1", rs2.getString("fooName"));
    assertFalse(rs2.next());
    verify(mockPluginService, times(4)).getCurrentConnection();
    verify(mockPluginService, times(2)).isInTransaction();
    verify(mockCacheConn, times(2)).readFromCache(expectedCacheKey);
    verify(mockPluginService, times(2)).getSessionStateService();
    verify(mockSessionStateService, times(2)).getCatalog();
    verify(mockSessionStateService, times(2)).getSchema();
    verify(mockConnection).getCatalog();
    verify(mockConnection).getSchema();
    verify(mockSessionStateService).setCatalog("mysql");
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq(expectedCacheKey), any(), eq(50));
    verify(mockTotalQueryCounter, times(2)).inc();
    verify(mockCacheMissCounter, times(1)).inc();
    verify(mockCacheHitCounter, times(1)).inc();
    verify(mockCacheBypassCounter, never()).inc();
    // Verify TelemetryContext behavior for cache miss and hit scenario
    // First call: Cache miss + Database call
    verify(mockTelemetryFactory, times(2)).openTelemetryContext(eq("jdbc-cache-lookup"),
        eq(TelemetryTraceLevel.NESTED));
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    // Cache context calls: 1 miss (setSuccess(false)) + 1 hit (setSuccess(true))
    verify(mockTelemetryContext, times(1)).setSuccess(false); // Cache miss
    verify(mockTelemetryContext, times(1)).setSuccess(true);  // Cache hit
    // Context closure: 2 cache contexts + 1 database context = 3 total
    verify(mockTelemetryContext, times(3)).closeContext();
  }

  @Test
  void test_transaction_cacheQuery() throws Exception {
    props.setProperty("user", "dbuser");
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty());
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getCatalog()).thenReturn("postgres");
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"/*+ CACHE_PARAM(ttl=300s) */ select * from T"});

    assertSame(mockResult1, rs);
    verify(mockPluginService, never()).getCurrentConnection();
    verify(mockPluginService).isInTransaction();
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockCacheConn, never()).writeToCache(anyString(), any(), anyInt());
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheMissCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    // Verify TelemetryContext behavior for transaction scenario
    // In transaction: No cache lookup attempted, only database call
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_transaction_cacheQuery_multiple_query_params() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty());
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getCatalog()).thenReturn(null);
    when(mockConnection.getSchema()).thenReturn("mysql");
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement, methodName, mockCallable,
        new String[]{"/*+ CACHE_PARAM(ttl=300s, otherParam=abc) */ select * from T"});

    assertSame(mockResult1, rs);
    verify(mockPluginService, never()).getCurrentConnection();
    verify(mockPluginService).isInTransaction();
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockCacheConn, never()).writeToCache(anyString(), any(), anyInt());
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheMissCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    // Verify TelemetryContext behavior for transaction scenario
    // In transaction: No cache lookup attempted, only database call
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_transaction_noCaching() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockCallable.call()).thenReturn(mockResult1);
    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        "Statement.execute", mockCallable, new String[]{"delete from mytable"});

    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1");
    compareResults(mockResult1, rs);
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheMissCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    // Verify TelemetryContext behavior for transaction scenario
    // In transaction: No cache lookup attempted, only database call
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_JdbcCacheBypassCount_malformed_hint() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Setup - not in transaction with malformed cache hint
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1);

    // Query with malformed cache hint - should increment both malformed and bypass counters
    String queryWithMalformedHint = "/*+ CACHE_PARAM(ttl=invalid) */ SELECT * FROM users WHERE id = 123";
    plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{queryWithMalformedHint});
    // Verify malformed counter incremented first
    verify(mockMalformedHintCounter, times(1)).inc();
    // Verify bypass counter incremented (because configuredQueryTtl becomes null)
    verify(mockCacheBypassCounter, times(1)).inc();
    // Verify cache flow counters were NOT called
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheMissCounter, never()).inc();
    // Verify TelemetryContext behavior for transaction scenario
    // In transaction: No cache lookup attempted, only database call
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_JdbcCacheBypassCount_double_bypass_prevention() throws Exception {
    props.setProperty("user", "testuser");
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    // Setup - query that meets MULTIPLE bypass conditions
    when(mockPluginService.isInTransaction()).thenReturn(true); // Bypass condition #1
    when(mockCallable.call()).thenReturn(mockResult1);

    // Mock result set for caching
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("testdata");

    // Query that is BOTH too large AND in transaction - double bypass conditions
    String largeQueryInTransaction = "/*+ CACHE_PARAM(ttl=300s) */ SELECT * FROM table WHERE data = '"
        + RandomStringUtils.randomAlphanumeric(16384) + "'"; // >16KB AND in transaction

    // Execute
    plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{largeQueryInTransaction});

    // Verify bypass counter incremented EXACTLY ONCE (not twice)
    verify(mockCacheBypassCounter, times(1)).inc();

    // Verify cache flow counters were NOT called
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheMissCounter, never()).inc();

    // Verify malformed counter not called (hint is valid, just large query)
    verify(mockMalformedHintCounter, never()).inc();
    // Verify TelemetryContext behavior for transaction scenario
    // In transaction: No cache lookup attempted, only database call
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_execute_multipleCacheHits() throws Exception {
    props.setProperty("user", "user");
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);
    final String expectedCacheKey = cacheKey(null, "public", "user", "select * from A");
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty());
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty()).thenReturn(Optional.of("public"));
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockConnection.getCatalog()).thenReturn(null);
    when(mockCacheConn.readFromCache(expectedCacheKey)).thenReturn(null);
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"/*+CACHE_PARAM(ttl=50s)*/ select * from A"});

    // Cached result set contains 1 row
    assertTrue(rs.next());
    assertEquals("bar1", rs.getString("fooName"));
    assertFalse(rs.next());

    rs.beforeFirst();
    byte[] serializedTestResultSet = ((CachedResultSet) rs).serializeIntoByteArray();
    when(mockCacheConn.readFromCache(expectedCacheKey)).thenReturn(serializedTestResultSet);

    for (int i = 0; i < 10; i++) {
      ResultSet curRs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
          methodName, mockCallable, new String[]{" /*+CACHE_PARAM(ttl=50s)*/select * from A"});

      assertTrue(curRs.next());
      assertEquals("bar1", curRs.getString("fooName"));
      assertFalse(curRs.next());
    }

    verify(mockPluginService, times(22)).getCurrentConnection();
    verify(mockPluginService, times(11)).isInTransaction();
    verify(mockCacheConn, times(11)).readFromCache(expectedCacheKey);
    verify(mockPluginService, times(11)).getSessionStateService();
    verify(mockSessionStateService, times(11)).getCatalog();
    verify(mockSessionStateService, times(11)).getSchema();
    verify(mockConnection).getSchema();
    verify(mockConnection).getCatalog();
    verify(mockSessionStateService).setSchema("public");
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq(expectedCacheKey), any(), eq(50));
    verify(mockTotalQueryCounter, times(11)).inc();
    verify(mockCacheMissCounter, times(1)).inc();
    verify(mockCacheHitCounter, times(10)).inc();
    verify(mockCacheBypassCounter, never()).inc();
    // Verify TelemetryContext behavior for cache miss and hit scenario
    verify(mockTelemetryFactory, times(11)).openTelemetryContext(eq("jdbc-cache-lookup"),
        eq(TelemetryTraceLevel.NESTED));
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"),
        eq(TelemetryTraceLevel.NESTED));
    verify(mockTelemetryContext, times(1)).setSuccess(false); // Cache miss
    verify(mockTelemetryContext, times(10)).setSuccess(true);  // Cache hit
    // Context closure: 2 cache contexts + 1 database context = 3 total
    verify(mockTelemetryContext, times(12)).closeContext();
  }

  @Test
  void test_execute_partitionsCacheByPostgresqlAuthorizationState() throws Exception {
    props.setProperty("user", "application_user");
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);

    final String query = "select * from orders";
    final AuthorizationSessionState tenantAState = new AuthorizationSessionState(
        "application_user",
        "tenant_a",
        "\"tenant_a\", public",
        "[\"pg_catalog\",\"tenant_a\",\"public\"]");
    final AuthorizationSessionState tenantBState = new AuthorizationSessionState(
        "application_user",
        "tenant_b",
        "\"tenant_b\", public",
        "[\"pg_catalog\",\"tenant_b\",\"public\"]");
    final String tenantAKey =
        cacheKey("orders_db", "public", "application_user", tenantAState, query);
    final String tenantBKey =
        cacheKey("orders_db", "public", "application_user", tenantBState, query);

    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockSessionStateService.getAuthorizationState())
        .thenReturn(Optional.of(tenantAState), Optional.of(tenantBState));
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.of("orders_db"));
    when(mockSessionStateService.getSchema()).thenReturn(Optional.of("public"));
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockCacheConn.readFromCache(tenantAKey)).thenReturn(null);
    when(mockCacheConn.readFromCache(tenantBKey)).thenReturn(null);
    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("tenant-a-order");
    when(mockResult2.next()).thenReturn(true, false);
    when(mockResult2.getObject(1)).thenReturn("tenant-b-order");

    final ResultSet tenantAResult = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement,
        methodName,
        mockCallable,
        new String[] {"/*+CACHE_PARAM(ttl=50s)*/ " + query});
    final ResultSet tenantBResult = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement,
        methodName,
        mockCallable,
        new String[] {"/*+CACHE_PARAM(ttl=50s)*/ " + query});

    assertNotEquals(tenantAKey, tenantBKey);
    assertTrue(tenantAResult.next());
    assertEquals("tenant-a-order", tenantAResult.getString("fooName"));
    assertTrue(tenantBResult.next());
    assertEquals("tenant-b-order", tenantBResult.getString("fooName"));
    verify(mockCacheConn).readFromCache(tenantAKey);
    verify(mockCacheConn).readFromCache(tenantBKey);
    verify(mockCacheConn).writeToCache(eq(tenantAKey), any(), eq(50));
    verify(mockCacheConn).writeToCache(eq(tenantBKey), any(), eq(50));
  }

  @Test
  void test_execute_lazilyRefreshesPostgresqlAuthorizationState() throws Exception {
    props.setProperty("user", "application_user");
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);

    final String query = "select * from orders";
    final AuthorizationSessionState authorizationState = new AuthorizationSessionState(
        "application_user",
        "tenant_a",
        "\"tenant_a\", public",
        "[\"pg_catalog\",\"tenant_a\",\"public\"]");
    final String expectedCacheKey =
        cacheKey("orders_db", "public", "application_user", authorizationState, query);

    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getAuthorizationState())
        .thenReturn(Optional.empty(), Optional.of(authorizationState));
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.of("orders_db"));
    when(mockSessionStateService.getSchema()).thenReturn(Optional.of("public"));
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockCacheConn.readFromCache(expectedCacheKey)).thenReturn(null);
    when(mockCallable.call()).thenReturn(mockResult1);
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("tenant-a-order");

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement,
        methodName,
        mockCallable,
        new String[] {"/*+CACHE_PARAM(ttl=50s)*/ " + query});

    verify(mockSessionStateService).refreshAuthorizationState();
    verify(mockCacheConn).readFromCache(expectedCacheKey);
    verify(mockCacheConn).writeToCache(eq(expectedCacheKey), any(), eq(50));
  }

  @Test
  void test_execute_bypassesCacheWhenAuthorizationStateCannotBeRead() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);

    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getAuthorizationState()).thenReturn(Optional.empty());
    doThrow(new SQLException("authorization state unavailable"))
        .when(mockSessionStateService).refreshAuthorizationState();
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockCallable.call()).thenReturn(mockResult1);

    final ResultSet result = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement,
        methodName,
        mockCallable,
        new String[] {"/*+CACHE_PARAM(ttl=50s)*/ select * from orders"});

    assertSame(mockResult1, result);
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCacheConn, never()).writeToCache(anyString(), any(), anyInt());
    verify(mockCacheBypassCounter).inc();
  }

  @Test
  void test_execute_bypassesCacheForUntrackedAuthorizationState() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);

    when(mockTargetDriverDialect.supportsAuthorizationSessionState()).thenReturn(true);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.hasUntrackedAuthorizationState()).thenReturn(true);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockCallable.call()).thenReturn(mockResult1);

    final ResultSet result = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement,
        methodName,
        mockCallable,
        new String[] {"/*+CACHE_PARAM(ttl=50s)*/ select * from orders"});

    assertSame(mockResult1, result);
    verify(mockSessionStateService, never()).getAuthorizationState();
    verify(mockSessionStateService, never()).refreshAuthorizationState();
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCacheConn, never()).writeToCache(anyString(), any(), anyInt());
    verify(mockCacheBypassCounter).inc();
  }

  @Test
  void test_execute_bypassesCacheForAuthorizationStateChangingQuery() throws Exception {
    plugin = new RemoteQueryCachePlugin(mockServicesContainer, props);
    plugin.setCacheConnection(mockCacheConn);

    final String query = "SELECT set_config('search_path', 'tenant_a, public', false)";
    when(mockTargetDriverDialect.mayChangeAuthorizationSessionState(query)).thenReturn(true);
    when(mockCallable.call()).thenReturn(mockResult1);

    final ResultSet result = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement,
        methodName,
        mockCallable,
        new String[] {"/*+CACHE_PARAM(ttl=50s)*/ " + query});

    assertSame(mockResult1, result);
    verify(mockPluginService, never()).getSessionStateService();
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCacheConn, never()).writeToCache(anyString(), any(), anyInt());
    verify(mockCacheBypassCounter).inc();
  }

  void compareResults(final ResultSet expected, final ResultSet actual) throws SQLException {
    int i = 1;
    while (expected.next() && actual.next()) {
      assertEquals(expected.getObject(i), actual.getObject(i));
      i++;
    }
  }

  private static String cacheKey(
      final String catalog,
      final String schema,
      final String user,
      final String query) {
    return cacheKey(catalog, schema, user, null, query);
  }

  private static String cacheKey(
      final String catalog,
      final String schema,
      final String user,
      final AuthorizationSessionState authorizationState,
      final String query) {
    final StringBuilder cacheKey = new StringBuilder();
    appendCacheKeyPart(cacheKey, "remote-query-cache:v2");
    appendCacheKeyPart(cacheKey, catalog);
    appendCacheKeyPart(cacheKey, schema);
    appendCacheKeyPart(cacheKey, user);
    if (authorizationState != null) {
      appendCacheKeyPart(cacheKey, authorizationState.getSessionUser());
      appendCacheKeyPart(cacheKey, authorizationState.getCurrentUser());
      appendCacheKeyPart(cacheKey, authorizationState.getSearchPath());
      appendCacheKeyPart(cacheKey, authorizationState.getResolvedSearchPath());
    }
    appendCacheKeyPart(cacheKey, query);
    return cacheKey.toString();
  }

  private static void appendCacheKeyPart(final StringBuilder cacheKey, final String value) {
    if (value == null) {
      cacheKey.append("-1:");
      return;
    }
    cacheKey.append(value.length()).append(':').append(value);
  }
}
