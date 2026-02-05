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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.StringUtils;

public class WeightedRandomHostSelector implements HostSelector {
  public static final AwsWrapperProperty WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS = new AwsWrapperProperty(
      "weightedRandomHostWeightPairs", null,
      "Comma separated list of database host-weight pairs in the format of `<host>:<weight>`.");
  public static final String STRATEGY_WEIGHTED_RANDOM = "weightedRandom";
  static final int DEFAULT_WEIGHT = 1;
  static final Pattern HOST_WEIGHT_PAIRS_PATTERN =
      Pattern.compile("((?<host>[^:/?#]*):(?<weight>[0-9]*))");

  private Map<String, Integer> cachedHostWeightMap;
  private String cachedHostWeightMapString;
  private Random random;

  private final ResourceLock lock = new ResourceLock();

  public WeightedRandomHostSelector() {
    this(new Random());
  }

  public WeightedRandomHostSelector(final Random random) {
    this.random = random;
  }

  public HostSpec getHost(
      @NonNull List<HostSpec> hosts,
      @Nullable HostRole role,
      @Nullable Properties props) throws SQLException {

    String hostWeightPairsValue = props == null ? null : WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.getString(props);

    // Parse property map if provided, otherwise use HostSpec.getWeight().
    // NOTE: If limitless plugin is used, weightedRandomHostWeightPairs must be null.
    Map<String, Integer> hostWeightMap = StringUtils.isNullOrEmpty(hostWeightPairsValue)
        ? null
        : getHostWeightPairMap(hostWeightPairsValue);

    HostSpec selected = selectWeightedRandom(hosts, role, hostWeightMap);

    if (selected == null) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[] {role}));
    }

    return selected;
  }

  // Selects a host using cumulative weight selection algorithm.
  private @Nullable HostSpec selectWeightedRandom(
      final List<HostSpec> hosts,
      final @Nullable HostRole role,
      final @Nullable Map<String, Integer> hostWeightMap) {

    if (this.random == null) {
      this.random = new Random();
    }

    // Build list of eligible hosts and calculate total weight
    final List<HostSpec> eligibleHosts = new ArrayList<>();
    long totalWeight = 0;
    for (HostSpec host : hosts) {
      if (isEligible(host, role)) {
        eligibleHosts.add(host);
        totalWeight += getWeight(host, hostWeightMap);
      }
    }

    if (eligibleHosts.isEmpty()) {
      return null;
    }

    // Generate random value in range [0, totalWeight)
    long roll = (long) (this.random.nextDouble() * totalWeight);

    // Find selected host by subtracting roll with weight until roll < weight
    for (HostSpec host : eligibleHosts) {
      long weight = getWeight(host, hostWeightMap);
      if (roll < weight) {
        return host;
      }
      roll -= weight;
    }

    return null;
  }

  private boolean isEligible(final HostSpec host, final @Nullable HostRole role) {
    return (role == null || role.equals(host.getRole()))
        && host.getAvailability().equals(HostAvailability.AVAILABLE);
  }

  private long getWeight(final HostSpec host, final @Nullable Map<String, Integer> hostWeightMap) {
    if (hostWeightMap != null) {
      Integer weight = hostWeightMap.get(host.getHost());
      return (weight != null && weight >= 1) ? weight : DEFAULT_WEIGHT;
    }
    long weight = host.getWeight();
    return weight < 1 ? DEFAULT_WEIGHT : weight;
  }

  private Map<String, Integer> getHostWeightPairMap(final @NonNull String hostWeightMapString) throws SQLException {
    try (ResourceLock ignored = lock.obtain()) {
      if (this.cachedHostWeightMapString != null
          && this.cachedHostWeightMapString.equals(
              hostWeightMapString == null ? "" : hostWeightMapString.trim())
          && this.cachedHostWeightMap != null
          && !this.cachedHostWeightMap.isEmpty()) {
        return this.cachedHostWeightMap;
      }

      final Map<String, Integer> hostWeightMap = new HashMap<>();
      if (hostWeightMapString == null || hostWeightMapString.trim().isEmpty()) {
        return hostWeightMap;
      }
      final String[] hostWeightPairs = hostWeightMapString.split(",");
      for (final String hostWeightPair : hostWeightPairs) {
        final Matcher matcher = HOST_WEIGHT_PAIRS_PATTERN.matcher(hostWeightPair);
        if (!matcher.matches()) {
          throw new SQLException(Messages.get("HostSelector.weightedRandomInvalidHostWeightPairs"));
        }

        final String hostName = matcher.group("host").trim();
        final String hostWeight = matcher.group("weight").trim();
        if (hostName.isEmpty() || hostWeight.isEmpty()) {
          throw new SQLException(Messages.get("HostSelector.weightedRandomInvalidHostWeightPairs"));
        }

        try {
          final int weight = Integer.parseInt(hostWeight);
          if (weight < DEFAULT_WEIGHT) {
            throw new SQLException(Messages.get("HostSelector.weightedRandomInvalidHostWeightPairs"));
          }
          hostWeightMap.put(hostName, weight);
        } catch (NumberFormatException e) {
          throw new SQLException(Messages.get("HostSelector.roundRobinInvalidHostWeightPairs"));
        }
      }
      this.cachedHostWeightMap = hostWeightMap;
      this.cachedHostWeightMapString = hostWeightMapString.trim();
      return hostWeightMap;
    }
  }
}
