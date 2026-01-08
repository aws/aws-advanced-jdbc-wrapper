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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;

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

  private final ReentrantLock lock = new ReentrantLock();

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
    if (StringUtils.isNullOrEmpty(hostWeightPairsValue)) {
      throw new SQLException(Messages.get("WeightedRandomHostSelector.hostWeightPairsPropertyRequired"));
    }

    final Map<String, Integer> hostWeightMap = this.getHostWeightPairMap(hostWeightPairsValue);

    // Get and check eligible hosts
    final List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec ->
            (role == null || role.equals(hostSpec.getRole()))
            && hostSpec.getAvailability().equals(HostAvailability.AVAILABLE))
        .sorted(Comparator.comparing(HostSpec::getHost))
        .collect(Collectors.toList());

    if (eligibleHosts.isEmpty()) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[] {role}));
    }

    final Map<String, NumberRange> hostWeightRangeMap = new HashMap<>();
    int counter = 1;
    for (HostSpec host : eligibleHosts) {
      if (!hostWeightMap.containsKey(host.getHost())) {
        continue;
      }
      final int hostWeight = hostWeightMap.get(host.getHost());
      if (hostWeight > 0) {
        final int rangeStart = counter;
        final int rangeEnd = counter + hostWeight - 1;
        hostWeightRangeMap.put(host.getHost(), new NumberRange(rangeStart, rangeEnd));
        counter = counter + hostWeight;
      } else {
        hostWeightRangeMap.put(host.getHost(), new NumberRange(counter, counter));
        counter++;
      }
    }

    if (this.random == null) {
      this.random = new Random();
    }
    int randomInt = this.random.nextInt(counter);

    // Check random number is in host weight range map
    for (final HostSpec host : eligibleHosts) {
      NumberRange range = hostWeightRangeMap.get(host.getHost());
      if (range != null && range.isInRange(randomInt)) {
        return host;
      }
    }

    throw new SQLException(Messages.get("HostSelector.weightedRandomUnableToGetHost", new Object[] {role}));
  }

  private Map<String, Integer> getHostWeightPairMap(final @NonNull String hostWeightMapString) throws SQLException {
    try {
      lock.lock();
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
    } finally {
      lock.unlock();
    }
  }

  private static class NumberRange {
    private int start;
    private int end;

    public NumberRange(int start, int end) {
      this.start = start;
      this.end = end;
    }

    public boolean isInRange(int value) {
      return start <= value && value <= end;
    }
  }
}
