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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SlidingExpirationCache;

public class LeastConnectionsHostSelector implements HostSelector {
  public static final String STRATEGY_LEAST_CONNECTIONS = "leastConnections";
  private final SlidingExpirationCache<HikariPooledConnectionProvider.PoolKey, HikariDataSource> databasePools;

  public LeastConnectionsHostSelector(
      SlidingExpirationCache<HikariPooledConnectionProvider.PoolKey, HikariDataSource> databasePools) {
    this.databasePools = databasePools;
  }

  @Override
  public HostSpec getHost(
      @NonNull final List<HostSpec> hosts,
      @NonNull final HostRole role,
      @Nullable final Properties props) throws SQLException {
    final List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec ->
            role.equals(hostSpec.getRole()) && hostSpec.getAvailability().equals(HostAvailability.AVAILABLE))
        .sorted((hostSpec1, hostSpec2) ->
            getNumConnections(hostSpec1, this.databasePools) - getNumConnections(hostSpec2, this.databasePools))
        .collect(Collectors.toList());

    if (eligibleHosts.size() == 0) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[]{role}));
    }

    return eligibleHosts.get(0);
  }

  private int getNumConnections(
      final HostSpec hostSpec,
      final SlidingExpirationCache<HikariPooledConnectionProvider.PoolKey, HikariDataSource> databasePools) {
    int numConnections = 0;
    final String url = hostSpec.getUrl();
    for (final Map.Entry<HikariPooledConnectionProvider.PoolKey, HikariDataSource> entry :
        databasePools.getEntries().entrySet()) {
      if (!url.equals(entry.getKey().getUrl())) {
        continue;
      }
      numConnections += entry.getValue().getHikariPoolMXBean().getActiveConnections();
    }
    return numConnections;
  }
}
