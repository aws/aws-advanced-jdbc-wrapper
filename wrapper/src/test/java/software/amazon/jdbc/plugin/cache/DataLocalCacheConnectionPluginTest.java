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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

@SuppressWarnings("unchecked")
class DataLocalCacheConnectionPluginTest {

  private static final Properties props = new Properties();

  private AutoCloseable closeable;

  @Mock
  PluginService mockPluginService;
  @Mock
  TelemetryFactory mockTelemetryFactory;
  @Mock
  TelemetryCounter mockTelemetryCounter;
  @Mock
  ResultSet mockResult1;
  @Mock
  ResultSet mockResult2;
  @Mock
  Statement mockStatement;
  @Mock
  ResultSetMetaData mockMetaData;

  @Mock
  JdbcCallable mockCallable;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props.setProperty(DataLocalCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, "foo");
    DataLocalCacheConnectionPlugin.clearCache();

    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);

    when(mockResult1.getMetaData()).thenReturn(mockMetaData);
    when(mockResult2.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumnCount()).thenReturn(1);
    when(mockMetaData.getColumnName(1)).thenReturn("fooName");

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

  @Test
  void test_execute_withEmptyCache() throws SQLException {
    final String methodName = "Statement.executeQuery";

    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);

    final ResultSet rs = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement, methodName,
        () -> mockResult1,
        new String[]{"foo"}
    );

    compareResults(mockResult1, rs);
  }

  @Test
  void test_execute_withCache() throws Exception {
    final String methodName = "Statement.executeQuery";

    final DataLocalCacheConnectionPlugin plugin = new DataLocalCacheConnectionPlugin(mockPluginService, props);

    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    ResultSet rs = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement, methodName,
        mockCallable,
        new String[]{"foo"}
    );
    compareResults(mockResult1, rs);

    // Execute the query again with a different result set.
    rs = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement, methodName,
        mockCallable,
        new String[]{"foo"}
    );

    compareResults(mockResult1, rs);
    verify(mockCallable).call();
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

    plugin.tryCache("SELECT 1", mockResult1);

    assertSame(mockResult1, plugin.getIfFresh("SELECT 1"));
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

    plugin.tryCache("SELECT 1", mockResult1);

    assertSame(mockResult1, plugin.getIfFresh("SELECT 1"),
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

    plugin.tryCache("SELECT 1", mockResult1);
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

    plugin.tryCache("SELECT 1", mockResult1);
    Thread.sleep(20);

    assertSame(mockResult1, plugin.getIfFresh("SELECT 1"),
        "dataCacheTtlMs=0 opts out of expiration");
  }

  /**
   * Before {@code dataCacheMaxSize} existed, the cache grew without bound in the number of distinct
   * SQL strings the process had ever seen.
   */
  @Test
  void test_sizeLimitStopsCachingNewStatements() {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("60000", "2");

    plugin.tryCache("SELECT 1", mockResult1);
    plugin.tryCache("SELECT 2", mockResult2);
    plugin.tryCache("SELECT 3", mockResult1);

    assertEquals(2, DataLocalCacheConnectionPlugin.dataCache.size(),
        "The cache must not grow past dataCacheMaxSize");
    assertNotNull(plugin.getIfFresh("SELECT 1"));
    assertNotNull(plugin.getIfFresh("SELECT 2"));
    assertNull(plugin.getIfFresh("SELECT 3"), "The statement that did not fit is not cached");
  }

  @Test
  void test_sizeLimitStillRefreshesAnExistingKey() {
    final DataLocalCacheConnectionPlugin plugin = boundedPlugin("60000", "1");

    plugin.tryCache("SELECT 1", mockResult1);
    plugin.tryCache("SELECT 1", mockResult2);

    assertEquals(1, DataLocalCacheConnectionPlugin.dataCache.size());
    assertSame(mockResult2, plugin.getIfFresh("SELECT 1"),
        "Replacing the value of a key already present does not consume a new slot");
  }

  @Test
  void test_sizeLimitPurgesExpiredEntriesToMakeRoom() throws Exception {
    boundedPlugin("1", "1").tryCache("SELECT 1", mockResult1);
    Thread.sleep(20);

    // The cache is full, but only of an expired entry, so the new statement must displace it.
    final DataLocalCacheConnectionPlugin longLived = boundedPlugin("60000", "1");
    longLived.tryCache("SELECT 2", mockResult2);

    assertEquals(1, DataLocalCacheConnectionPlugin.dataCache.size());
    assertSame(mockResult2, longLived.getIfFresh("SELECT 2"));
    assertNull(longLived.getIfFresh("SELECT 1"));
  }

  void compareResults(final ResultSet expected, final ResultSet actual) throws SQLException {
    int i = 1;
    while (expected.next() && actual.next()) {
      assertEquals(expected.getObject(i), actual.getObject(i));
      i++;
    }
  }
}
