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
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class DataRemoteCachePluginTest {
  private static final Properties props = new Properties();
  private final String methodName = "Statement.executeQuery";
  private AutoCloseable closeable;

  private DataRemoteCachePlugin plugin;
  @Mock PluginService mockPluginService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryCounter mockHitCounter;
  @Mock TelemetryCounter mockMissCounter;
  @Mock TelemetryCounter mockTotalCallsCounter;
  @Mock TelemetryCounter mockMalformedHintCounter;
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
    props.setProperty("wrapperPlugins", "dataRemoteCache");
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");

    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter("remoteCache.cache.hit")).thenReturn(mockHitCounter);
    when(mockTelemetryFactory.createCounter("remoteCache.cache.miss")).thenReturn(mockMissCounter);
    when(mockTelemetryFactory.createCounter("remoteCache.cache.totalCalls")).thenReturn(mockTotalCallsCounter);
    when(mockTelemetryFactory.createCounter("JdbcCacheMalformedQueryHint")).thenReturn(mockMalformedHintCounter);
    when(mockResult1.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumnCount()).thenReturn(1);
    when(mockMetaData.getColumnLabel(1)).thenReturn("fooName");
    plugin = new DataRemoteCachePlugin(mockPluginService, props);
    plugin.setCacheConnection(mockCacheConn);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_getTTLFromQueryHint() throws Exception {
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
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1);

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"select * from mytable where ID = 2"});

    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1");
    compareResults(mockResult1, rs);
    verify(mockPluginService).isInTransaction();
    verify(mockCallable).call();
    verify(mockTotalCallsCounter, never()).inc();
    verify(mockHitCounter, never()).inc();
    verify(mockMissCounter, never()).inc();
  }

  @Test
  void test_execute_noCachingLongQuery() throws Exception {
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockCallable.call()).thenReturn(mockResult1);

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"/* CACHE_PARAM(ttl=20s) */ select * from T" + RandomStringUtils.randomAlphanumeric(15990)});

    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1");
    compareResults(mockResult1, rs);
    verify(mockCallable).call();
    verify(mockTotalCallsCounter, never()).inc();
    verify(mockHitCounter, never()).inc();
    verify(mockMissCounter, never()).inc();
  }

  @Test
  void test_execute_cachingMissAndHit() throws Exception {
    // Query is not cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty()).thenReturn(Optional.of("public"));
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockDbMetadata.getUserName()).thenReturn("user");
    when(mockCacheConn.readFromCache("public_user_select * from A")).thenReturn(null);
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
    when(mockCacheConn.readFromCache("public_user_select * from A")).thenReturn(serializedTestResultSet);
    ResultSet rs2 = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{" /*+CACHE_PARAM(ttl=50s)*/select * from A"});

    assertTrue(rs2.next());
    assertEquals("bar1", rs2.getString("fooName"));
    assertFalse(rs2.next());
    verify(mockPluginService, times(3)).getCurrentConnection();
    verify(mockPluginService, times(2)).isInTransaction();
    verify(mockCacheConn, times(2)).readFromCache("public_user_select * from A");
    verify(mockPluginService, times(3)).getSessionStateService();
    verify(mockSessionStateService, times(3)).getSchema();
    verify(mockConnection).getSchema();
    verify(mockSessionStateService).setSchema("public");
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("public_user_select * from A"), any(), eq(50));
    verify(mockTotalCallsCounter, times(2)).inc();
    verify(mockMissCounter).inc();
    verify(mockHitCounter).inc();
  }

  @Test
  void test_transaction_cacheQuery() throws Exception {
    // Query is cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockDbMetadata.getUserName()).thenReturn("user");
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
    verify(mockConnection).getSchema();
    verify(mockSessionStateService).setSchema("public");
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("public_user_select * from T"), any(), eq(300));
    verify(mockTotalCallsCounter, never()).inc();
    verify(mockHitCounter, never()).inc();
    verify(mockMissCounter, never()).inc();
  }

  @Test
  void test_transaction_cacheQuery_multiple_query_params() throws Exception {
    // Query is cacheable
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockDbMetadata.getUserName()).thenReturn("user");
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
    verify(mockSessionStateService).getSchema();
    verify(mockConnection).getSchema();
    verify(mockSessionStateService).setSchema("public");
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("public_user_select * from T"), any(), eq(300));
    verify(mockTotalCallsCounter, never()).inc();
    verify(mockHitCounter, never()).inc();
    verify(mockMissCounter, never()).inc();
  }

   @Test
   void test_transaction_cacheQuery_multiple_query_hints() throws Exception {// Query is cacheable
     when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
     when(mockPluginService.isInTransaction()).thenReturn(true);
     when(mockConnection.getMetaData()).thenReturn(mockDbMetadata);
     when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
     when(mockSessionStateService.getSchema()).thenReturn(Optional.empty());
     when(mockConnection.getSchema()).thenReturn("public");
     when(mockDbMetadata.getUserName()).thenReturn("user");
     when(mockCallable.call()).thenReturn(mockResult1);

     // Result set contains 1 row
     when(mockResult1.next()).thenReturn(true, false);
     when(mockResult1.getObject(1)).thenReturn("bar1");

     ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
     methodName, mockCallable, new String[]{"/*+ hello CACHE_PARAM(ttl=300s, otherParam=abc) world */ select * from T"});

     // Cached result set contains 1 row
     assertTrue(rs.next());
     assertEquals("bar1", rs.getString("fooName"));
     assertFalse(rs.next());
     verify(mockPluginService).getCurrentConnection();
     verify(mockPluginService).isInTransaction();
     verify(mockPluginService).getSessionStateService();
     verify(mockSessionStateService).getSchema();
     verify(mockConnection).getSchema();
     verify(mockSessionStateService).setSchema("public");
     verify(mockCacheConn, never()).readFromCache(anyString());
     verify(mockCallable).call();
     verify(mockCacheConn).writeToCache(eq("public_user_select * from T"), any(), eq(300));
     verify(mockTotalCallsCounter, never()).inc();
     verify(mockHitCounter, never()).inc();
     verify(mockMissCounter, never()).inc();
   }

    @Test
  void test_transaction_noCaching() throws Exception {
    // Query is not cacheable
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockCallable.call()).thenReturn(mockResult1);
    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"delete from mytable"});

    // Mock result set containing 1 row
    when(mockResult1.next()).thenReturn(true, true, false, false);
    when(mockResult1.getObject(1)).thenReturn("bar1", "bar1");
    compareResults(mockResult1, rs);
    verify(mockCacheConn, never()).readFromCache(anyString());
    verify(mockCallable).call();
    verify(mockTotalCallsCounter, never()).inc();
    verify(mockHitCounter, never()).inc();
    verify(mockMissCounter, never()).inc();
  }

  void compareResults(final ResultSet expected, final ResultSet actual) throws SQLException {
    int i = 1;
    while (expected.next() && actual.next()) {
      assertEquals(expected.getObject(i), actual.getObject(i));
      i++;
    }
  }
}
