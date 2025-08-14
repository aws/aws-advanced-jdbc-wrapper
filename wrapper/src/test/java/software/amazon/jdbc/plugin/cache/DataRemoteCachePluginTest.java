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
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
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
  @Mock ResultSet mockResult1;
  @Mock Statement mockStatement;
  @Mock ResultSetMetaData mockMetaData;
  @Mock Connection mockConnection;
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
    // Null and empty query string are not cacheable
    assertNull(plugin.getTtlForQuery(null));
    assertNull(plugin.getTtlForQuery(""));
    assertNull(plugin.getTtlForQuery("    "));
    // Some other query hint
    assertNull(plugin.getTtlForQuery("/* cacheNotEnabled */ select * from T"));
    // Rule set is empty. All select queries are cached with 300 seconds TTL
    String selectQuery1 = "cachettl=300s";
    String selectQuery2 = "  /*  CACHETTL=100s  */   SELECT ID from mytable2    ";
    String selectQuery3 = "/*CacheTTL=35s*/select * from table3 where ID = 1 and name = 'tom'";
    // Query hints that are not cacheable
    String selectQueryNoHint = "select * from table4";
    String selectQueryNoCache1 = "no cache";
    String selectQueryNoCache2 = " /* NO CACHE */   SELECT count(*) FROM (select player_id from roster where id = 1 FOR UPDATE) really_long_name_alias";
    String selectQueryNoCache3 = "/* cachettl=300 */ SELECT count(*) FROM (select player_id from roster where id = 1) really_long_name_alias";

    // Non select queries are not cacheable
    String veryShortQuery = "BEGIN";
    String insertQuery = "/* This is an insert query */ insert into mytable values (1, 2)";
    String updateQuery = "/* Update query */ Update /* Another hint */ mytable set val = 1";
    assertEquals(300, plugin.getTtlForQuery(selectQuery1));
    assertEquals(100, plugin.getTtlForQuery(selectQuery2));
    assertEquals(35, plugin.getTtlForQuery(selectQuery3));
    assertNull(plugin.getTtlForQuery(selectQueryNoHint));
    assertNull(plugin.getTtlForQuery(selectQueryNoCache1));
    assertNull(plugin.getTtlForQuery(selectQueryNoCache2));
    assertNull(plugin.getTtlForQuery(selectQueryNoCache3));
    assertNull(plugin.getTtlForQuery(veryShortQuery));
    assertNull(plugin.getTtlForQuery(insertQuery));
    assertNull(plugin.getTtlForQuery(updateQuery));
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
        methodName, mockCallable, new String[]{"/* cacheTTL=30s */ select * from T" + RandomStringUtils.randomAlphanumeric(15990)});

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
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockDbMetadata.getUserName()).thenReturn("user");
    when(mockCacheConn.readFromCache("public_user_select * from A")).thenReturn(null);
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"/*CACHETTL=100s*/ select * from A"});

    // Cached result set contains 1 row
    assertTrue(rs.next());
    assertEquals("bar1", rs.getString("fooName"));
    assertFalse(rs.next());
    rs.beforeFirst();
    byte[] serializedTestResultSet = ((CachedResultSet)rs).serializeIntoByteArray();
    when(mockCacheConn.readFromCache("public_user_select * from A")).thenReturn(serializedTestResultSet);
    ResultSet rs2 = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{" /* CacheTtl=50s */select * from A"});

    assertTrue(rs2.next());
    assertEquals("bar1", rs2.getString("fooName"));
    assertFalse(rs2.next());
    verify(mockPluginService, times(3)).getCurrentConnection();
    verify(mockPluginService, times(2)).isInTransaction();
    verify(mockCacheConn, times(2)).readFromCache("public_user_select * from A");
    verify(mockCallable).call();
    verify(mockCacheConn).writeToCache(eq("public_user_select * from A"), any(), eq(100));
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
    when(mockConnection.getSchema()).thenReturn("public");
    when(mockDbMetadata.getUserName()).thenReturn("user");
    when(mockCallable.call()).thenReturn(mockResult1);

    // Result set contains 1 row
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");

    ResultSet rs = plugin.execute(ResultSet.class, SQLException.class, mockStatement,
        methodName, mockCallable, new String[]{"/* cacheTTL=300s */ select * from T"});

    // Cached result set contains 1 row
    assertTrue(rs.next());
    assertEquals("bar1", rs.getString("fooName"));
    assertFalse(rs.next());
    verify(mockPluginService).getCurrentConnection();
    verify(mockPluginService).isInTransaction();
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
