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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import software.amazon.jdbc.HighestLoadHostSelector;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.events.TopologyRefreshedEvent;

public class HighestLoadHostSelectorTest {

  private static final Properties EMPTY_PROPS = new Properties();

  private HighestLoadHostSelector selector;

  @BeforeEach
  void setUp() {
    selector = new HighestLoadHostSelector();
  }

  private HostSpec reader(final String id, final long cpu, final float lag) {
    return reader(id, cpu, lag, HostAvailability.AVAILABLE);
  }

  private HostSpec reader(final String id, final float cpu, final float lag, final HostAvailability availability) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(id)
        .hostId(id)
        .role(HostRole.READER)
        .availability(availability)
        .cpuPercent(cpu)
        .lagMs(lag)
        .build();
  }

  @Test
  void emptyHostList_returnsNull() throws SQLException {
    assertNull(selector.getHost(Collections.emptyList(), HostRole.READER, EMPTY_PROPS));
  }

  @Test
  void noEligibleHosts_returnsNull() throws SQLException {
    final List<HostSpec> writers = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("w1").hostId("w1")
            .role(HostRole.WRITER).cpuPercent(1f).lagMs(1f).build());
    assertNull(selector.getHost(writers, HostRole.READER, EMPTY_PROPS));
  }

  @Test
  void oneHostClearlyDominates_winsMost() throws SQLException {
    // Single high-load reader vs low-load — the high-load one should be picked the vast majority of the time.
    final List<HostSpec> hosts = Arrays.asList(
        reader("high", 9000, 9000),
        reader("low1", 10, 10),
        reader("low2", 11, 11));

    final Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < 200; i++) {
      // Reset pending picks between iterations to measure steady-state preference, not herd protection.
      selector.processEvent(new TopologyRefreshedEvent("test"));
      final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      counts.merge(h.getHostId(), 1, Integer::sum);
    }

    final int highCount = counts.getOrDefault("high", 0);
    assertTrue(highCount > 150,
        "expected 'high' to win the strong majority of picks but got " + counts);
  }

  @Test
  void filtersByRoleAndAvailability() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(
        reader("avail-high", 9000, 9000),
        reader("not-avail-higher", 9999, 9999, HostAvailability.NOT_AVAILABLE),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("writer").hostId("writer")
            .role(HostRole.WRITER).cpuPercent(10000f).lagMs(10000f).build());

    for (int i = 0; i < 30; i++) {
      final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      assertEquals("avail-high", h.getHostId());
    }
  }

  @Test
  void noLoadDataAtAll_fallsBackToRandom() throws SQLException {
    // All hosts have unknown load. Strategy must not return null; must distribute over all of them.
    final HostSpec a = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("a").hostId("a").role(HostRole.READER).build();
    final HostSpec b = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("b").hostId("b").role(HostRole.READER).build();

    final Set<String> seen = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      final HostSpec h = selector.getHost(Arrays.asList(a, b), HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      seen.add(h.getHostId());
    }
    assertTrue(seen.contains("a") && seen.contains("b"),
        "expected fallback to spread picks across both hosts; saw " + seen);
  }

  @Test
  void pendingPicksDeflate_spreadAcrossTopK_onTiedLoad() throws SQLException {
    // Two readers tied at load=5000. Without herd protection a sequence might land on the same host.
    // With pending-picks deflation, after the first pick, the chosen host's effective load is reduced,
    // so the OTHER host becomes preferred next.
    final List<HostSpec> hosts = Arrays.asList(reader("a", 5000, 5000), reader("b", 5000, 5000));
    final Map<String, Integer> counts = new HashMap<>();

    for (int i = 0; i < 200; i++) {
      final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      counts.merge(h.getHostId(), 1, Integer::sum);
    }

    final int a = counts.getOrDefault("a", 0);
    final int b = counts.getOrDefault("b", 0);
    // Both hosts must each get a substantial share — herd protection prevents "all on one".
    assertTrue(a > 60 && b > 60,
        "expected pending-picks deflation to spread picks across both hosts; counts=" + counts);
  }

  @Test
  void topologyRefreshedEvent_clearsPendingPicks() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(reader("a", 5000, 5000), reader("b", 5000, 5000));

    // Drive a burst on host 'a' via repeated picks.
    for (int i = 0; i < 50; i++) {
      selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
    }

    // Reset.
    selector.processEvent(new TopologyRefreshedEvent("test"));

    // After reset, picks on perfectly tied loads should spread across both hosts.
    final Set<String> seen = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      seen.add(selector.getHost(hosts, HostRole.READER, EMPTY_PROPS).getHostId());
    }
    assertTrue(seen.contains("a") && seen.contains("b"),
        "post-reset distribution should reach both hosts; saw " + seen);
  }

  @Test
  void adaptiveK_twoReaders_kEqualsTwo() throws SQLException {
    // With only two readers, K is clamped to 2 — both candidates always considered. With moderately
    // different loads (ratio ~10:1), the direct-load weighting still gives the lighter reader a
    // small share, which should surface in many trials. If K were 1, the lighter reader would never
    // be picked.
    final List<HostSpec> hosts = Arrays.asList(reader("a", 100, 100), reader("b", 10, 10));
    final Set<String> seen = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      selector.processEvent(new TopologyRefreshedEvent("test"));
      seen.add(selector.getHost(hosts, HostRole.READER, EMPTY_PROPS).getHostId());
      if (seen.size() == 2) {
        break;
      }
    }
    assertTrue(seen.contains("b"),
        "with K=2 the lighter reader must be picked at least once across many trials; saw " + seen);
  }

  @Test
  void adaptiveK_largeCluster_capsAtMaxK() throws SQLException {
    // 12 readers, all with same load. Default maxK = 5.
    // The universe of picked hosts should be bounded by maxK.
    final List<HostSpec> hosts = new java.util.ArrayList<>();
    for (int i = 0; i < 12; i++) {
      hosts.add(reader("r" + i, 100, 100));
    }

    final Set<String> seen = new HashSet<>();
    for (int i = 0; i < 500; i++) {
      selector.processEvent(new TopologyRefreshedEvent("test"));
      seen.add(selector.getHost(hosts, HostRole.READER, EMPTY_PROPS).getHostId());
    }
    // Top-K cap is 5 (default), so we should NOT see all 12.
    assertTrue(seen.size() <= 5,
        "expected at most maxK=5 distinct hosts when load is uniform; saw " + seen);
  }

  @Test
  void prefersHigherLoad_overLowerLoad() throws SQLException {
    // Verify the strategy actually prefers the higher-loaded host (inverse of lowestLoad).
    final List<HostSpec> hosts = Arrays.asList(
        reader("low", 10, 10),
        reader("medium", 500, 500),
        reader("high", 9000, 9000));

    final Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < 300; i++) {
      selector.processEvent(new TopologyRefreshedEvent("test"));
      final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      counts.merge(h.getHostId(), 1, Integer::sum);
    }

    final int highCount = counts.getOrDefault("high", 0);
    final int lowCount = counts.getOrDefault("low", 0);
    assertTrue(highCount > lowCount,
        "expected 'high' to be picked more often than 'low'; counts=" + counts);
  }

  @Test
  void nullRole_considersAllHosts() throws SQLException {
    // When role is null, both writers and readers should be considered.
    final HostSpec writer = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer").hostId("writer").role(HostRole.WRITER).cpuPercent(9000f).lagMs(9000f).build();
    final HostSpec reader = reader("reader", 10, 10);

    final Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      selector.processEvent(new TopologyRefreshedEvent("test"));
      final HostSpec h = selector.getHost(Arrays.asList(writer, reader), null, EMPTY_PROPS);
      assertNotNull(h);
      counts.merge(h.getHostId(), 1, Integer::sum);
    }

    final int writerCount = counts.getOrDefault("writer", 0);
    assertTrue(writerCount > 70,
        "expected 'writer' (higher load) to win most picks with null role; counts=" + counts);
  }
}
