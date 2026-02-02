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

    // Select by host weight, or by property.
    // This uses a weighted reservoir sampling algorithm.
    HostSpec selected = StringUtils.isNullOrEmpty(hostWeightPairsValue)
        ? selectByHostWeight(hosts, role)
        : selectByPropertyWeight(hosts, role, hostWeightPairsValue);

    if (selected == null) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[] {role}));
    }

    return selected;
  }

  private @Nullable HostSpec selectByHostWeight(
      final List<HostSpec> hosts,
      final @Nullable HostRole role) {

    if (this.random == null) {
      this.random = new Random();
    }

    HostSpec selected = null;
    long totalWeight = 0;

    for (HostSpec host : hosts) {
      if ((role == null || role.equals(host.getRole()))
          && host.getAvailability().equals(HostAvailability.AVAILABLE)) {

        long weight = host.getWeight();
        if (weight < 1) {
          weight = DEFAULT_WEIGHT;
        }

        totalWeight += weight;
        if ((long) (this.random.nextDouble() * totalWeight) < weight) {
          selected = host;
        }
      }
    }

    return selected;
  }

  private @Nullable HostSpec selectByPropertyWeight(
      final List<HostSpec> hosts,
      final @Nullable HostRole role,
      final String hostWeightPairsValue) throws SQLException {

    final Map<String, Integer> hostWeightMap = this.getHostWeightPairMap(hostWeightPairsValue);

    if (this.random == null) {
      this.random = new Random();
    }

    HostSpec selected = null;
    long totalWeight = 0;

    for (HostSpec host : hosts) {
      if ((role == null || role.equals(host.getRole()))
          && host.getAvailability().equals(HostAvailability.AVAILABLE)) {

        Integer weight = hostWeightMap.get(host.getHost());
        if (weight == null || weight < 1) {
          continue;
        }

        totalWeight += weight;
        if ((long) (this.random.nextDouble() * totalWeight) < weight) {
          selected = host;
        }
      }
    }

    return selected;
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
