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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.events.EventSubscriber;
import software.amazon.jdbc.util.events.TopologyRefreshedEvent;

/**
 * Host selector that picks the lowest-loaded reader using a calculated load derived from
 * {@link HostSpec#getCpuPercent()} and {@link HostSpec#getLag()}, with cold-start herd protection so a fleet
 * bringing up connection pools does not pile onto the same "least loaded" host.
 *
 * <p>Selection is weighted-random over the lowest-loaded top-K candidates, where K scales with the size of the
 * eligible pool. Each pick inflates the chosen host's effective load by a configurable amount, so subsequent picks in
 * a burst (e.g. a connection pool fill) spread across the top-K. The pending-pick counter resets when a fresh
 * topology refresh arrives, observed via {@link TopologyRefreshedEvent}.
 *
 * <p>Falls back to {@link RandomHostSelector} when no eligible host has a known load value (pre-first-refresh, or
 * non-Aurora dialects that do not populate {@code loadValue}).
 */
public class LowestLoadHostSelector implements HostSelector, EventSubscriber {

  private static final Logger LOGGER = Logger.getLogger(LowestLoadHostSelector.class.getName());

  public static final String STRATEGY_LOWEST_LOAD = "lowestLoad";

  public static final AwsWrapperProperty LOWEST_LOAD_MAX_K = new AwsWrapperProperty(
      "lowestLoadMaxK", "5",
      "Hard cap on the number of candidate hosts the lowestLoad strategy will randomize over. "
          + "The actual K is clamp(ceil(numEligibleWithLoad / 2), 2, lowestLoadMaxK).");

  public static final AwsWrapperProperty LOWEST_LOAD_PENDING_PICK_ALPHA = new AwsWrapperProperty(
      "lowestLoadPendingPickAlpha", "50.0",
      "Per-pending-pick load inflation. Each pick made by the lowestLoad strategy inflates the chosen "
          + "host's effective load by this amount until the next topology refresh resets the counter.");

  public static final AwsWrapperProperty LOWEST_LOAD_CPU_WEIGHT = new AwsWrapperProperty(
      "lowestLoadCpuWeight", "1",
      "The weight of CPU utilization percent in the calculation of a host's load.");

  public static final AwsWrapperProperty LOWEST_LOAD_LAG_WEIGHT = new AwsWrapperProperty(
      "lowestLoadLagWeight", "100",
      "The weight of lag in the calculation of a host's load.");

  // Safety belt: if the topology monitor stalls and pending-picks never reset, the counter must not
  // grow unbounded. Cleared and warned at this ceiling.
  static final int PENDING_PICKS_CEILING = 10000;

  static {
    PropertyDefinition.registerPluginProperties(LowestLoadHostSelector.class);
  }

  private final ConcurrentMap<String, AtomicInteger> pendingPicks = new ConcurrentHashMap<>();
  private final RandomHostSelector randomFallback = new RandomHostSelector();

  public LowestLoadHostSelector() {
    final EventPublisher publisher = CoreServicesContainer.getInstance().getEventPublisher();
    publisher.subscribe(this, new HashSet<>(Collections.singletonList(TopologyRefreshedEvent.class)));
  }

  @Override
  public HostSpec getHost(
      @NonNull final List<HostSpec> hosts,
      @Nullable final HostRole role,
      @Nullable final Properties props) throws SQLException {

    final List<HostSpec> eligible = hosts.stream()
        .filter(h -> (role == null || role.equals(h.getRole()))
            && h.getAvailability().equals(HostAvailability.AVAILABLE))
        .collect(Collectors.toList());

    if (eligible.isEmpty()) {
      return null;
    }

    final List<HostSpec> withLoad = eligible.stream()
        .filter(h -> calculateLoad(h, props) >= 0)
        .collect(Collectors.toList());

    if (withLoad.isEmpty()) {
      return this.randomFallback.getHost(eligible, role, props);
    }

    final int maxK = LOWEST_LOAD_MAX_K.getInteger(props);
    final double alpha = Double.parseDouble(LOWEST_LOAD_PENDING_PICK_ALPHA.getString(props));

    // Adaptive K: ceil(n/2), clamped to [2, maxK].
    final int k = Math.max(2, Math.min(maxK, (withLoad.size() + 1) / 2));

    enforcePendingPicksCeiling();

    // Compute effective load for each candidate.
    final List<HostEffectiveLoad> ranked = new ArrayList<>(withLoad.size());
    for (final HostSpec h : withLoad) {
      final int pending = pendingCount(h.getHostId());
      final double effective = (double) calculateLoad(h, props) + alpha * pending;
      ranked.add(new HostEffectiveLoad(h, effective));
    }
    ranked.sort((a, b) -> Double.compare(a.effectiveLoad, b.effectiveLoad));

    final List<HostEffectiveLoad> topK = ranked.subList(0, Math.min(k, ranked.size()));

    // Inverse-load weights — lighter hosts get higher probability mass. The +1 prevents
    // division by zero when a host reports zero load.
    double totalWeight = 0.0;
    final double[] weights = new double[topK.size()];
    for (int i = 0; i < topK.size(); i++) {
      weights[i] = 1.0 / (topK.get(i).effectiveLoad + 1.0);
      totalWeight += weights[i];
    }

    final double roll = ThreadLocalRandom.current().nextDouble() * totalWeight;
    double cumulative = 0.0;
    HostSpec selected = topK.get(topK.size() - 1).host;
    for (int i = 0; i < topK.size(); i++) {
      cumulative += weights[i];
      if (roll < cumulative) {
        selected = topK.get(i).host;
        break;
      }
    }

    pendingPicks.computeIfAbsent(selected.getHostId(), k2 -> new AtomicInteger()).incrementAndGet();
    return selected;
  }

  @Override
  public void processEvent(final Event event) {
    if (event instanceof TopologyRefreshedEvent) {
      pendingPicks.clear();
    }
  }

  private long calculateLoad(final HostSpec host, @Nullable final Properties props) {
    final long cpuPercentWeighted = (host.getCpuPercent() == HostSpec.UNKNOWN_CPU_PERCENT ? 0 : host.getCpuPercent())
        * LOWEST_LOAD_CPU_WEIGHT.getInteger(props);
    final long lagWeighted = (host.getCpuPercent() == HostSpec.UNKNOWN_LAG ? 0 : host.getCpuPercent())
        * LOWEST_LOAD_LAG_WEIGHT.getInteger(props);
    return cpuPercentWeighted + lagWeighted;
  }

  private int pendingCount(final String hostId) {
    final AtomicInteger c = pendingPicks.get(hostId);
    return c == null ? 0 : c.get();
  }

  private void enforcePendingPicksCeiling() {
    for (final AtomicInteger counter : pendingPicks.values()) {
      if (counter.get() >= PENDING_PICKS_CEILING) {
        LOGGER.warning(Messages.get("LowestLoadHostSelector.pendingPicksCeilingExceeded",
            new Object[] {PENDING_PICKS_CEILING}));
        pendingPicks.clear();
        return;
      }
    }
  }

  private static final class HostEffectiveLoad {
    final HostSpec host;
    final double effectiveLoad;

    HostEffectiveLoad(final HostSpec host, final double effectiveLoad) {
      this.host = host;
      this.effectiveLoad = effectiveLoad;
    }
  }
}
