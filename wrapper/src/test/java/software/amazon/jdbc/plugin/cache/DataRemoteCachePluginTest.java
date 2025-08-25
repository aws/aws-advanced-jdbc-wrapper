package software.amazon.jdbc.plugin.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.*;
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
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class DataRemoteCachePluginTest {
  private Properties props;
  private final String methodName = "Statement.executeQuery";
  private AutoCloseable closeable;

  private DataRemoteCachePlugin plugin;
  @Mock PluginService mockPluginService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryCounter mockCacheHitCounter;
  @Mock TelemetryCounter mockCacheMissCounter;
  @Mock TelemetryCounter mockTotalQueryCounter;
  @Mock TelemetryCounter mockMalformedHintCounter;
  @Mock TelemetryCounter mockCacheBypassCounter;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock ResultSet mockResult1;
  @Mock Statement mockStatement;
  @Mock ResultSetMetaData mockMetaData;
  @Mock Connection mockConnection;
  @Mock SessionStateService mockSessionStateService;
  @Mock DatabaseMetaData mockDbMetadata;
  @Mock CacheConnection mockCacheConn;
  @Mock JdbcCallable mockCallable;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    props.setProperty("wrapperPlugins", "dataRemoteCache");
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter("JdbcCachedQueryCount")).thenReturn(mockCacheHitCounter);
    when(mockTelemetryFactory.createCounter("JdbcCacheMissCount")).thenReturn(mockCacheMissCounter);
    when(mockTelemetryFactory.createCounter("JdbcCacheTotalQueryCount")).thenReturn(mockTotalQueryCounter);
    when(mockTelemetryFactory.createCounter("JdbcCacheMalformedQueryHint")).thenReturn(mockMalformedHintCounter);
    when(mockTelemetryFactory.createCounter("JdbcCacheBypassCount")).thenReturn(mockCacheBypassCounter);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockResult1.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumnCount()).thenReturn(1);
    when(mockMetaData.getColumnLabel(1)).thenReturn("fooName");
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_getTTLFromQueryHint() throws Exception {
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
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
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
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
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
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
    verify(mockPluginService).isInTransaction();
    verify(mockCallable).call();
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    verify(mockCacheMissCounter, never()).inc();
    // Verify TelemetryContext behavior for no-caching scenario
    verify(mockTelemetryFactory).openTelemetryContext("jdbc-database-query", TelemetryTraceLevel.TOP_LEVEL);
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryContext).closeContext();
  }

  @Test
  void test_execute_noCachingLongQuery() throws Exception {
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1);

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"/* CACHE_PARAM(ttl=20s) */ select * from T" + RandomStringUtils.randomAlphanumeric(15990)});

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
    verify(mockTelemetryFactory).openTelemetryContext("jdbc-database-query", TelemetryTraceLevel.TOP_LEVEL);
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryContext).closeContext();
  }

  @Test
  void test_execute_cachingMissAndHit() throws Exception {
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is not cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty()).thenReturn(Optional.of("mysql"));
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getCatalog()).thenReturn("mysql");
    when(mockConnection.getSchema()).thenReturn(null);
    when(mockDbMetadata.getUserName()).thenReturn("user1@1.1.1.1");
    when(mockCacheConn.readFromCache("mysql_null_user1_select * from A")).thenReturn(null);
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
    byte[] serializedTestResultSet = ((CachedResultSet)rs).serializeIntoByteArray();
    when(mockCacheConn.readFromCache("mysql_null_user1_select * from A")).thenReturn(serializedTestResultSet);

    ResultSet rs2 = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{" /*+CACHE_PARAM(ttl=50s)*/select * from A"});

    assertTrue(rs2.next());
    assertEquals("bar1", rs2.getString("fooName"));
    assertFalse(rs2.next());
    verify(mockPluginService, times(3)).getCurrentConnection();
    verify(mockPluginService, times(2)).isInTransaction();
    verify(mockCacheConn, times(2)).readFromCache("mysql_null_user1_select * from A");
    verify(mockPluginService, times(3)).getSessionStateService();
    verify(mockSessionStateService, times(3)).getCatalog();
    verify(mockSessionStateService, times(3)).getSchema();
    verify(mockConnection).getCatalog();
    verify(mockConnection).getSchema();
    verify(mockSessionStateService).setCatalog("mysql");
    verify(mockDbMetadata).getUserName();
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("mysql_null_user1_select * from A"), any(), eq(50));
    verify(mockTotalQueryCounter, times(2)).inc();
    verify(mockCacheMissCounter, times(1)).inc();
    verify(mockCacheHitCounter, times(1)).inc();
    verify(mockCacheBypassCounter, never()).inc();
    // Verify TelemetryContext behavior for cache miss and hit scenario
    // First call: Cache miss + Database call
    verify(mockTelemetryFactory, times(2)).openTelemetryContext(eq("jdbc-cache-lookup"), eq(TelemetryTraceLevel.TOP_LEVEL));
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"), eq(TelemetryTraceLevel.TOP_LEVEL));
    // Cache context calls: 1 miss (setSuccess(false)) + 1 hit (setSuccess(true))
    verify(mockTelemetryContext, times(1)).setSuccess(false); // Cache miss
    verify(mockTelemetryContext, times(1)).setSuccess(true);  // Cache hit
    // Context closure: 2 cache contexts + 1 database context = 3 total
    verify(mockTelemetryContext, times(3)).closeContext();
  }

  @Test
  void test_transaction_cacheQuery() throws Exception {
    props.setProperty("user", "dbuser");
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
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

    // Cached result set contains 1 row
    assertTrue(rs.next());
    assertEquals("bar1", rs.getString("fooName"));
    assertFalse(rs.next());
    verify(mockPluginService).getCurrentConnection();
    verify(mockPluginService).isInTransaction();
    verify(mockPluginService).getSessionStateService();
    verify(mockSessionStateService).getSchema();
    verify(mockSessionStateService).getCatalog();
    verify(mockConnection).getSchema();
    verify(mockConnection).getCatalog();
    verify(mockSessionStateService).setSchema("public");
    verify(mockSessionStateService).setCatalog("postgres");
    verify(mockDbMetadata, never()).getUserName();
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("postgres_public_dbuser_select * from T"), any(), eq(300));
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheMissCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    // Verify TelemetryContext behavior for transaction scenario
    // In transaction: No cache lookup attempted, only database call
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"), eq(TelemetryTraceLevel.TOP_LEVEL));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_transaction_cacheQuery_multiple_query_params() throws Exception {
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
    plugin.setCacheConnection(mockCacheConn);
    // Query is cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty());
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockDbMetadata.getUserName()).thenReturn("dbuser");
    when(mockConnection.getCatalog()).thenReturn(null);
    when(mockConnection.getSchema()).thenReturn("mysql");
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement, methodName, mockCallable, new String[]{"/*+ CACHE_PARAM(ttl=300s, otherParam=abc) */ select * from T"});

    // Cached result set contains 1 row
    assertTrue(rs.next());
    assertEquals("bar1", rs.getString("fooName"));
    assertFalse(rs.next());
    verify(mockPluginService).getCurrentConnection();
    verify(mockPluginService).isInTransaction();
    verify(mockPluginService).getSessionStateService();
    verify(mockSessionStateService).getCatalog();
    verify(mockSessionStateService).getSchema();
    verify(mockConnection).getSchema();
    verify(mockConnection).getCatalog();
    verify(mockSessionStateService).setSchema("mysql");
    verify(mockDbMetadata).getUserName();
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("null_mysql_dbuser_select * from T"), any(), eq(300));
    verify(mockTotalQueryCounter, times(1)).inc();
    verify(mockCacheHitCounter, never()).inc();
    verify(mockCacheMissCounter, never()).inc();
    verify(mockCacheBypassCounter, times(1)).inc();
    // Verify TelemetryContext behavior for transaction scenario
    // In transaction: No cache lookup attempted, only database call
    verify(mockTelemetryFactory, never()).openTelemetryContext(eq("jdbc-cache-lookup"), any());
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"), eq(TelemetryTraceLevel.TOP_LEVEL));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_transaction_noCaching() throws Exception {
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
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
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"), eq(TelemetryTraceLevel.TOP_LEVEL));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_JdbcCacheBypassCount_malformed_hint() throws Exception {
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
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
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"), eq(TelemetryTraceLevel.TOP_LEVEL));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_JdbcCacheBypassCount_double_bypass_prevention() throws Exception {
    props.setProperty("user", "testuser");
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
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
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"), eq(TelemetryTraceLevel.TOP_LEVEL));
    // Context closure: Only 1 database context
    verify(mockTelemetryContext, times(1)).closeContext();
  }

  @Test
  void test_execute_multipleCacheHits() throws Exception {
    props.setProperty("user", "user");
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
    plugin.setCacheConnection(mockCacheConn);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getCatalog()).thenReturn(Optional.empty());
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty()).thenReturn(Optional.of("public"));
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockConnection.getCatalog()).thenReturn(null);
    when(mockCacheConn.readFromCache("null_public_user_select * from A")).thenReturn(null);
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
    byte[] serializedTestResultSet = ((CachedResultSet)rs).serializeIntoByteArray();
    when(mockCacheConn.readFromCache("null_public_user_select * from A")).thenReturn(serializedTestResultSet);

    for (int i = 0; i < 10; i ++) {
      ResultSet cur_rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
          methodName, mockCallable, new String[]{" /*+CACHE_PARAM(ttl=50s)*/select * from A"});

      assertTrue(cur_rs.next());
      assertEquals("bar1", cur_rs.getString("fooName"));
      assertFalse(cur_rs.next());
    }

    verify(mockPluginService, times(12)).getCurrentConnection();
    verify(mockPluginService, times(11)).isInTransaction();
    verify(mockCacheConn, times(11)).readFromCache("null_public_user_select * from A");
    verify(mockPluginService, times(12)).getSessionStateService();
    verify(mockSessionStateService, times(12)).getCatalog();
    verify(mockSessionStateService, times(12)).getSchema();
    verify(mockConnection).getSchema();
    verify(mockConnection).getCatalog();
    verify(mockSessionStateService).setSchema("public");
    verify(mockDbMetadata, never()).getUserName();
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("null_public_user_select * from A"), any(), eq(50));
    verify(mockTotalQueryCounter, times(11)).inc();
    verify(mockCacheMissCounter, times(1)).inc();
    verify(mockCacheHitCounter, times(10)).inc();
    verify(mockCacheBypassCounter, never()).inc();
    // Verify TelemetryContext behavior for cache miss and hit scenario
    verify(mockTelemetryFactory, times(11)).openTelemetryContext(eq("jdbc-cache-lookup"), eq(TelemetryTraceLevel.TOP_LEVEL));
    verify(mockTelemetryFactory, times(1)).openTelemetryContext(eq("jdbc-database-query"), eq(TelemetryTraceLevel.TOP_LEVEL));
    verify(mockTelemetryContext, times(1)).setSuccess(false); // Cache miss
    verify(mockTelemetryContext, times(10)).setSuccess(true);  // Cache hit
    // Context closure: 2 cache contexts + 1 database context = 3 total
    verify(mockTelemetryContext, times(12)).closeContext();
  }

  void compareResults(final ResultSet expected, final ResultSet actual) throws SQLException {
    int i = 1;
    while (expected.next() && actual.next()) {
      assertEquals(expected.getObject(i), actual.getObject(i));
      i++;
    }
  }
}
