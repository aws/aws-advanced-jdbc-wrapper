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
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.LowestLoadHostSelector;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.events.TopologyRefreshedEvent;

public class LowestLoadHostSelectorTest {

  private static final Properties EMPTY_PROPS = new Properties();

  private LowestLoadHostSelector selector;

  @BeforeEach
  void setUp() {
    selector = new LowestLoadHostSelector();
  }

  private HostSpec reader(final String id, final long load) {
    return reader(id, load, HostAvailability.AVAILABLE);
  }

  private HostSpec reader(final String id, final long load, final HostAvailability availability) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(id)
        .hostId(id)
        .role(HostRole.READER)
        .availability(availability)
        .loadValue(load)
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
            .role(HostRole.WRITER).loadValue(10).build());
    assertNull(selector.getHost(writers, HostRole.READER, EMPTY_PROPS));
  }

  @Test
  void oneHostClearlyDominates_winsMost() throws SQLException {
    // Single low-load reader vs high-load — the low-load one should be picked the vast majority of the time.
    final List<HostSpec> hosts = Arrays.asList(
        reader("low", 10),
        reader("high1", 9000),
        reader("high2", 9001));

    final Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < 200; i++) {
      // Reset pending picks between iterations to measure steady-state preference, not herd protection.
      selector.processEvent(new TopologyRefreshedEvent("test"));
      final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      counts.merge(h.getHostId(), 1, Integer::sum);
    }

    final int lowCount = counts.getOrDefault("low", 0);
    assertTrue(lowCount > 150,
        "expected 'low' to win the strong majority of picks but got " + counts);
  }

  @Test
  void filtersByRoleAndAvailability() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(
        reader("avail-low", 10),
        reader("not-avail-lower", 1, HostAvailability.NOT_AVAILABLE),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("writer").hostId("writer")
            .role(HostRole.WRITER).loadValue(0).build());

    for (int i = 0; i < 30; i++) {
      final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      assertEquals("avail-low", h.getHostId());
    }
  }

  @Test
  void noLoadDataAtAll_fallsBackToRandom() throws SQLException {
    // All hosts have UNKNOWN_LOAD. Strategy must not return null; must distribute over all of them.
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
  void subsetWithLoad_picksOnlyFromThatSubset() throws SQLException {
    // Two readers have load, one is unknown. The unknown one must never be picked.
    final HostSpec withLoadLow = reader("with-load-low", 10);
    final HostSpec withLoadHigh = reader("with-load-high", 5000);
    final HostSpec unknown = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("unknown").hostId("unknown").role(HostRole.READER).build();

    for (int i = 0; i < 100; i++) {
      selector.processEvent(new TopologyRefreshedEvent("test"));
      final HostSpec h = selector.getHost(
          Arrays.asList(withLoadLow, withLoadHigh, unknown), HostRole.READER, EMPTY_PROPS);
      assertNotNull(h);
      assertTrue(!"unknown".equals(h.getHostId()),
          "unknown-load host must not be chosen when others have load");
    }
  }

  @Test
  void pendingPicksInflate_spreadAcrossTopK_onTiedLoad() throws SQLException {
    // Two readers tied at load=10. Without herd protection a sequence might land on the same host
    // due to weighted-random selection. With pending-picks inflation, after the first pick,
    // the second pick's effective load on the chosen host is much higher, so the OTHER host is
    // strongly preferred next.
    final List<HostSpec> hosts = Arrays.asList(reader("a", 10), reader("b", 10));
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
        "expected pending-picks inflation to spread picks across both hosts; counts=" + counts);
  }

  @Test
  void topologyRefreshedEvent_clearsPendingPicks() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(reader("a", 10), reader("b", 10));

    // Drive a burst on host 'a' via repeated picks.
    for (int i = 0; i < 50; i++) {
      selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
    }

    // Reset.
    selector.processEvent(new TopologyRefreshedEvent("test"));

    // After reset, a single pick on perfectly tied loads should be ~50/50 on average.
    // We just assert that the next pick succeeds and the selector isn't permanently biased away
    // from any host: do many picks and check both hostIds appear.
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
    // different loads (ratio ~10:1), the inverse-load weighting still gives the heavier reader a
    // ~10% share, which is overwhelmingly likely to surface in 1000 trials. If K were 1, the heavier
    // reader would never be picked.
    final List<HostSpec> hosts = Arrays.asList(reader("a", 10), reader("b", 100));
    final Set<String> seen = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      selector.processEvent(new TopologyRefreshedEvent("test"));
      seen.add(selector.getHost(hosts, HostRole.READER, EMPTY_PROPS).getHostId());
      if (seen.size() == 2) {
        break;
      }
    }
    assertTrue(seen.contains("b"),
        "with K=2 the heavier reader must be picked at least once across many trials; saw " + seen);
  }

  @Test
  void adaptiveK_largeCluster_capsAtMaxK() throws SQLException {
    // 12 readers, all with same load (so weighted-random is uniform over top-K). Default maxK = 5.
    // After resetting between picks, only the K with the lowest hostId-rank-tie should appear, but
    // since loads are identical the top-K is just the first K after sort-stable. We instead assert
    // the universe of picked hosts is bounded.
    final List<HostSpec> hosts = new java.util.ArrayList<>();
    for (int i = 0; i < 12; i++) {
      hosts.add(reader("r" + i, 100));
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
}
