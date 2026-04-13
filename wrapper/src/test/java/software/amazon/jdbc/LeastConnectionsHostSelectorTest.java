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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.storage.SlidingExpirationCache;

public class LeastConnectionsHostSelectorTest {

  private static final int TEST_PORT = 5432;

  private SlidingExpirationCache<Pair, AutoCloseable> mockDatabasePools;
  private LeastConnectionsHostSelector selector;
  private Properties props;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer-host").hostId("writer-1").port(TEST_PORT).role(HostRole.WRITER).build();
  private final HostSpec readerHost1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-host-1").hostId("reader-1").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHost2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-host-2").hostId("reader-2").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHost3 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-host-3").hostId("reader-3").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec unavailableReader = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("unavailable-host").hostId("unavailable-1").port(TEST_PORT).role(HostRole.READER)
      .availability(HostAvailability.NOT_AVAILABLE).build();

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void setUp() {
    mockDatabasePools = mock(SlidingExpirationCache.class);
    when(mockDatabasePools.getEntries()).thenReturn(Collections.emptyMap());
    selector = new LeastConnectionsHostSelector(mockDatabasePools);
    props = new Properties();
  }

  @Test
  void testGetHost_emptyHostList_throwsSQLException() {
    assertThrows(
        SQLException.class,
        () -> selector.getHost(Collections.emptyList(), HostRole.READER, props));
  }

  @Test
  void testGetHost_noHostsMatchingRole_throwsSQLException() {
    final List<HostSpec> hosts = Collections.singletonList(writerHost);
    assertThrows(
        SQLException.class,
        () -> selector.getHost(hosts, HostRole.READER, props));
  }

  @Test
  void testGetHost_allHostsUnavailable_throwsSQLException() {
    final List<HostSpec> hosts = Collections.singletonList(unavailableReader);
    assertThrows(
        SQLException.class,
        () -> selector.getHost(hosts, HostRole.READER, props));
  }

  @Test
  void testGetHost_singleEligibleHost_returnsThatHost() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1);
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost1, result);
  }

  @Test
  void testGetHost_nullRole_considersAllAvailableHosts() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1);
    // With no pool entries, all have 0 connections — any available host is valid
    final HostSpec result = selector.getHost(hosts, null, props);
    assertTrue(hosts.contains(result));
  }

  @Test
  void testGetHost_selectsHostWithLeastConnections() throws SQLException {
    // Set up pool entries: readerHost1 has 5 connections, readerHost2 has 2 connections
    final Map<Pair, AutoCloseable> poolEntries = new HashMap<>();
    addPoolEntry(poolEntries, readerHost1, 5);
    addPoolEntry(poolEntries, readerHost2, 2);
    when(mockDatabasePools.getEntries()).thenReturn(poolEntries);

    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, readerHost2);
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost2, result);
  }

  @Test
  void testGetHost_selectsHostWithZeroConnectionsOverOthers() throws SQLException {
    // readerHost1 has 3 connections, readerHost2 has 0 (no pool entry)
    final Map<Pair, AutoCloseable> poolEntries = new HashMap<>();
    addPoolEntry(poolEntries, readerHost1, 3);
    when(mockDatabasePools.getEntries()).thenReturn(poolEntries);

    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, readerHost2);
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost2, result);
  }

  @Test
  void testGetHost_tiedConnections_returnsOneOfTiedHosts() throws SQLException {
    // Both readers have 3 connections
    final Map<Pair, AutoCloseable> poolEntries = new HashMap<>();
    addPoolEntry(poolEntries, readerHost1, 3);
    addPoolEntry(poolEntries, readerHost2, 3);
    when(mockDatabasePools.getEntries()).thenReturn(poolEntries);

    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, readerHost2);

    // Run multiple times to verify it picks from the tied set
    final Set<HostSpec> selectedHosts = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      selectedHosts.add(selector.getHost(hosts, HostRole.READER, props));
    }

    // Both hosts should be eligible; at minimum one must be selected
    assertTrue(selectedHosts.contains(readerHost1) || selectedHosts.contains(readerHost2));
    for (HostSpec selected : selectedHosts) {
      assertTrue(selected.equals(readerHost1) || selected.equals(readerHost2),
          "Selected host should be one of the tied hosts");
    }
  }

  @Test
  void testGetHost_filtersOutUnavailableHosts() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, unavailableReader);
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost1, result);
  }

  @Test
  void testGetHost_writerRole_returnsWriter() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, readerHost2);
    final HostSpec result = selector.getHost(hosts, HostRole.WRITER, props);
    assertEquals(writerHost, result);
  }

  @Test
  void testGetHost_multiplePoolEntriesForSameHost_sumsConnections() throws SQLException {
    // readerHost1 has two pool entries (e.g. different users/databases), totaling 7 connections
    // readerHost2 has one pool entry with 4 connections
    final Map<Pair, AutoCloseable> poolEntries = new HashMap<>();
    addPoolEntry(poolEntries, readerHost1, "pool-key-a", 3);
    addPoolEntry(poolEntries, readerHost1, "pool-key-b", 4);
    addPoolEntry(poolEntries, readerHost2, "pool-key-c", 4);
    when(mockDatabasePools.getEntries()).thenReturn(poolEntries);

    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, readerHost2);
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost2, result);
  }

  @Test
  void testGetHost_nonHikariPoolEntry_isIgnored() throws SQLException {
    // Add a non-HikariDataSource entry — should be ignored, so readerHost1 has 0 connections
    final Map<Pair, AutoCloseable> poolEntries = new HashMap<>();
    final AutoCloseable nonHikari = mock(AutoCloseable.class);
    poolEntries.put(Pair.create(readerHost1.getUrl(), "some-key"), nonHikari);
    addPoolEntry(poolEntries, readerHost2, 5);
    when(mockDatabasePools.getEntries()).thenReturn(poolEntries);

    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, readerHost2);
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost1, result);
  }

  @Test
  void testGetHost_poolEntryUrlDoesNotMatch_isIgnored() throws SQLException {
    // Pool entry URL doesn't match any host — all hosts have 0 connections
    final Map<Pair, AutoCloseable> poolEntries = new HashMap<>();
    final HikariDataSource ds = mock(HikariDataSource.class);
    final HikariPoolMXBean mxBean = mock(HikariPoolMXBean.class);
    when(ds.getHikariPoolMXBean()).thenReturn(mxBean);
    when(mxBean.getActiveConnections()).thenReturn(10);
    poolEntries.put(Pair.create("non-matching-url/", "key"), ds);
    when(mockDatabasePools.getEntries()).thenReturn(poolEntries);

    final List<HostSpec> hosts = Arrays.asList(readerHost1, readerHost2);
    // Both have 0 connections, either is valid
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertTrue(result.equals(readerHost1) || result.equals(readerHost2));
  }

  @Test
  void testGetHost_threeReaders_selectsMinConnections() throws SQLException {
    final Map<Pair, AutoCloseable> poolEntries = new HashMap<>();
    addPoolEntry(poolEntries, readerHost1, 10);
    addPoolEntry(poolEntries, readerHost2, 1);
    addPoolEntry(poolEntries, readerHost3, 5);
    when(mockDatabasePools.getEntries()).thenReturn(poolEntries);

    final List<HostSpec> hosts = Arrays.asList(writerHost, readerHost1, readerHost2, readerHost3);
    final HostSpec result = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost2, result);
  }

  @Test
  void testStrategyName() {
    assertEquals("leastConnections", LeastConnectionsHostSelector.STRATEGY_LEAST_CONNECTIONS);
  }

  // --- Helper methods ---

  private void addPoolEntry(
      final Map<Pair, AutoCloseable> poolEntries,
      final HostSpec hostSpec,
      final int activeConnections) {
    addPoolEntry(poolEntries, hostSpec, "default-pool-key", activeConnections);
  }

  private void addPoolEntry(
      final Map<Pair, AutoCloseable> poolEntries,
      final HostSpec hostSpec,
      final String poolKey,
      final int activeConnections) {
    final HikariDataSource ds = mock(HikariDataSource.class);
    final HikariPoolMXBean mxBean = mock(HikariPoolMXBean.class);
    when(ds.getHikariPoolMXBean()).thenReturn(mxBean);
    when(mxBean.getActiveConnections()).thenReturn(activeConnections);
    poolEntries.put(Pair.create(hostSpec.getUrl(), poolKey), ds);
  }
}
