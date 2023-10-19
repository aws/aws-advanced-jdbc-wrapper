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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class RoundRobinHostSelector implements HostSelector {
  public static final AwsWrapperProperty ROUND_ROBIN_HOST_WEIGHT_PAIRS = new AwsWrapperProperty(
      "roundRobinHostWeightPairs", null,
      "Comma separated list of database host-weight pairs in the format of `<host>:<weight>`.");
  public static final AwsWrapperProperty ROUND_ROBIN_DEFAULT_WEIGHT = new AwsWrapperProperty(
      "roundRobinDefaultWeight", "1",
      "The default weight for any hosts that have not been configured with the `roundRobinHostWeightPairs` parameter.");
  public static final String STRATEGY_ROUND_ROBIN = "roundRobin";
  private static final int DEFAULT_WEIGHT = 1;
  private static final long DEFAULT_ROUND_ROBIN_CACHE_EXPIRE_NANO = TimeUnit.MINUTES.toNanos(10);
  static final Pattern HOST_WEIGHT_PAIRS_PATTERN =
      Pattern.compile(
          "((?<host>[^:/?#]*):(?<weight>[0-9]*))");
  private static final CacheMap<String, RoundRobinClusterInfo> roundRobinCache = new CacheMap<>();

  static {
    PropertyDefinition.registerPluginProperties(RoundRobinHostSelector.class);
  }

  @Override
  public synchronized HostSpec getHost(
      final @NonNull List<HostSpec> hosts,
      final @NonNull HostRole role,
      final @Nullable Properties props) throws SQLException {
    final List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec -> role.equals(hostSpec.getRole()))
        .sorted(Comparator.comparing(HostSpec::getHost))
        .collect(Collectors.toList());

    if (eligibleHosts.isEmpty()) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[]{role}));
    }

    // Create new cache entries for provided hosts if necessary. All hosts point to the same cluster info.
    createCacheEntryForHosts(eligibleHosts, props);
    final String currentClusterInfoKey = eligibleHosts.get(0).getHost();
    final RoundRobinClusterInfo clusterInfo = roundRobinCache.get(currentClusterInfoKey);

    final HostSpec lastHost = clusterInfo.lastHost;
    int lastHostIndex = -1;

    // Check if lastHost is in list of eligible hosts. Update lastHostIndex.
    if (lastHost != null) {
      for (int i = 0; i < eligibleHosts.size(); i++) {
        if (eligibleHosts.get(i).getHost().equals(lastHost.getHost())) {
          lastHostIndex = i;
        }
      }
    }

    final int targetHostIndex;
    // If the host is weighted and the lastHost is in the eligibleHosts list.
    if (clusterInfo.weightCounter > 0 && lastHostIndex != -1) {
      targetHostIndex = lastHostIndex;
    } else {
      if (lastHostIndex != -1 && lastHostIndex != eligibleHosts.size() - 1) {
        targetHostIndex = lastHostIndex + 1;
      } else {
        targetHostIndex = 0;
      }

      final Integer weight = clusterInfo.clusterWeightsMap.get(eligibleHosts.get(targetHostIndex).getHost());
      clusterInfo.weightCounter = weight == null ? clusterInfo.defaultWeight : weight;
    }

    clusterInfo.weightCounter--;
    clusterInfo.lastHost = eligibleHosts.get(targetHostIndex);

    return eligibleHosts.get(targetHostIndex);
  }

  private void createCacheEntryForHosts(
      final @NonNull List<HostSpec> hosts,
      final @Nullable Properties props)
      throws SQLException {
    final List<HostSpec> hostsMissingCacheEntry = new ArrayList<>();
    final List<HostSpec> hostsWithCacheEntry = new ArrayList<>();
    for (final HostSpec host : hosts) {
      if (roundRobinCache.get(host.getHost()) != null) {
        hostsWithCacheEntry.add(host);
      } else {
        hostsMissingCacheEntry.add(host);
      }
    }

    if ((hostsMissingCacheEntry.isEmpty() && !hostsWithCacheEntry.isEmpty())) {
      for (final HostSpec host : hosts) {
        roundRobinCache.put(
            host.getHost(),
            roundRobinCache.get(hostsWithCacheEntry.get(0).getHost()),
            DEFAULT_ROUND_ROBIN_CACHE_EXPIRE_NANO);
      }
    } else if (hostsWithCacheEntry.isEmpty()) {
      final RoundRobinClusterInfo roundRobinClusterInfo = new RoundRobinClusterInfo();
      updateCachePropertiesForRoundRobinClusterInfo(roundRobinClusterInfo, props);
      for (final HostSpec host : hostsMissingCacheEntry) {
        roundRobinCache.put(
            host.getHost(),
            roundRobinClusterInfo,
            DEFAULT_ROUND_ROBIN_CACHE_EXPIRE_NANO);
      }
    }
  }

  private void updateCachePropertiesForRoundRobinClusterInfo(
      final @NonNull RoundRobinClusterInfo roundRobinClusterInfo,
      final @Nullable Properties props) throws SQLException {
    int defaultWeight = DEFAULT_WEIGHT;
    if (props != null) {
      final String defaultWeightString = ROUND_ROBIN_DEFAULT_WEIGHT.getString(props);
      if (!StringUtils.isNullOrEmpty(defaultWeightString)) {
        try {
          final int parsedWeight = Integer.parseInt(defaultWeightString);
          if (parsedWeight < DEFAULT_WEIGHT) {
            throw new SQLException(Messages.get("HostSelector.roundRobinInvalidDefaultWeight"));
          }
          defaultWeight = parsedWeight;
        } catch (NumberFormatException e) {
          throw new SQLException(Messages.get("HostSelector.roundRobinInvalidDefaultWeight"));
        }
      }
    }
    roundRobinClusterInfo.defaultWeight = defaultWeight;

    if (props != null) {
      final String hostWeights = ROUND_ROBIN_HOST_WEIGHT_PAIRS.getString(props);
      if (!StringUtils.isNullOrEmpty(hostWeights)) {
        final String[] hostWeightPairs = hostWeights.split(",");
        for (final String pair : hostWeightPairs) {
          final Matcher matcher = HOST_WEIGHT_PAIRS_PATTERN.matcher(pair);
          if (!matcher.matches()) {
            throw new SQLException(Messages.get("HostSelector.roundRobinInvalidHostWeightPairs"));
          }

          final String hostName = matcher.group("host").trim();
          final String hostWeight = matcher.group("weight").trim();
          if (hostName.isEmpty() || hostWeight.isEmpty()) {
            throw new SQLException(Messages.get("HostSelector.roundRobinInvalidHostWeightPairs"));
          }

          try {
            final int weight = Integer.parseInt(hostWeight);
            if (weight < DEFAULT_WEIGHT) {
              throw new SQLException(Messages.get("HostSelector.roundRobinInvalidHostWeightPairs"));
            }
            roundRobinClusterInfo.clusterWeightsMap.put(hostName, weight);
          } catch (NumberFormatException e) {
            throw new SQLException(Messages.get("HostSelector.roundRobinInvalidHostWeightPairs"));
          }
        }
      }
    }
  }

  // For testing purposes only
  public void clearCache() {
    roundRobinCache.clear();
  }

  public static class RoundRobinClusterInfo {
    public HostSpec lastHost;
    public HashMap<String, Integer> clusterWeightsMap = new HashMap<>();
    public int defaultWeight = 1;
    public int weightCounter = 0;
  }
}
