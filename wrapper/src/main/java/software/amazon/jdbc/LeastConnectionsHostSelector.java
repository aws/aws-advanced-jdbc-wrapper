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

import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.kms.endpoints.internal.Value.Int;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.storage.SlidingExpirationCache;

public class LeastConnectionsHostSelector implements HostSelector {
  public static final String STRATEGY_LEAST_CONNECTIONS = "leastConnections";
  private final SlidingExpirationCache<Pair, AutoCloseable> databasePools;

  public LeastConnectionsHostSelector(
      SlidingExpirationCache<Pair, AutoCloseable> databasePools) {
    this.databasePools = databasePools;
  }

  @Override
  public HostSpec getHost(
      @NonNull final List<HostSpec> hosts,
      @Nullable final HostRole role,
      @Nullable final Properties props) throws SQLException {

    // Get all eligible hosts along with number of active connections
    final List<Pair<HostSpec, Integer>> eligibleHosts = hosts.stream()
        .filter(hostSpec ->
            (role == null || role.equals(hostSpec.getRole()))
            && hostSpec.getAvailability().equals(HostAvailability.AVAILABLE))
        .map(hostSpec -> Pair.create(hostSpec, this.getNumConnections(hostSpec, this.databasePools)))
        .collect(Collectors.toList());

    if (eligibleHosts.isEmpty()) {
      return null;
    }

    // Get min number of connections
    final Pair<HostSpec, Integer> minNumConnectionPair =
        eligibleHosts.stream().min(Comparator.comparingInt(Pair::getValue2)).orElse(null);

    // Get all hosts with found min number of connections
    final List<HostSpec> hostsWithMinConnections = eligibleHosts.stream()
        .filter(x -> Objects.equals(x.getValue2(), minNumConnectionPair.getValue2()))
        .map(Pair::getValue1)
        .collect(Collectors.toList());

    // Randomly choose one of hosts with the same min number of connections
    final int randomIndex = new Random().nextInt(hostsWithMinConnections.size());
    return hostsWithMinConnections.get(randomIndex);
  }

  private int getNumConnections(
      final HostSpec hostSpec,
      final SlidingExpirationCache<Pair, AutoCloseable> databasePools) {
    int numConnections = 0;
    final String url = hostSpec.getUrl();
    for (final Map.Entry<Pair, AutoCloseable> entry :
        databasePools.getEntries().entrySet()) {
      if (!url.equals(entry.getKey().getValue1())) {
        continue;
      }
      if (!(entry.getValue() instanceof HikariDataSource)) {
        continue;
      }
      numConnections += ((HikariDataSource) entry.getValue()).getHikariPoolMXBean().getActiveConnections();
    }
    return numConnections;
  }
}
