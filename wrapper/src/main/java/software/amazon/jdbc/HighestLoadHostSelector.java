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
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;

/**
 * Host selector that picks the highest-loaded reader using a calculated load derived from
 * {@link HostSpec#getCpuPercent()} and {@link HostSpec#getLagMs()}
 *
 * <p>Falls back to {@link RandomHostSelector} when no eligible host has a known load value (or non-Aurora dialects
 * that do not populate {@code cpuPercent} or {@code lagMs} values).
 */
public class HighestLoadHostSelector implements HostSelector {

  private static final Logger LOGGER = Logger.getLogger(HighestLoadHostSelector.class.getName());

  public static final String STRATEGY_HIGHEST_LOAD = "highestLoad";

  public static final AwsWrapperProperty HIGHEST_LOAD_CPU_WEIGHT = new AwsWrapperProperty(
      "highestLoadCpuWeight", "1",
      "The weight of CPU utilization percent in the calculation of a host's load.");

  public static final AwsWrapperProperty HIGHEST_LOAD_LAG_WEIGHT = new AwsWrapperProperty(
      "highestLoadLagWeight", "100",
      "The weight of lag in the calculation of a host's load.");

  protected static final long CPU_DEFAULT = 40; // TODO: find good value

  protected static final long LAG_MS_DEFAULT = 1000; // TODO: find good value

  static {
    PropertyDefinition.registerPluginProperties(HighestLoadHostSelector.class);
  }

  @Override
  public HostSpec getHost(
      @NonNull final List<HostSpec> hosts,
      @Nullable final HostRole role,
      @Nullable final Properties props) throws SQLException {

    final List<HostSpec> eligible = hosts.stream()
        .filter(h -> (role == null || role.equals(h.getRole()))
            && !(h.getCpuPercent() == null && h.getLagMs() == null)
            && h.getAvailability().equals(HostAvailability.AVAILABLE))
        .collect(Collectors.toList());

    if (eligible.isEmpty()) {
      return null;
    }

    HostSpec highestLoadHost = null;
    long highestLoad = -1;
    for (final HostSpec host : eligible) {
      final long currentLoad = calculateLoad(host, props);
      if (highestLoad < 0 || highestLoad < currentLoad) {
        highestLoadHost = host;
        highestLoad = currentLoad;
      }
    }

    if (highestLoad < 0) {
      return null;
    }
    return highestLoadHost;
  }

  private long calculateLoad(final HostSpec host, @Nullable final Properties props) {
    final long cpuPercentWeighted = (host.getCpuPercent() == null ? CPU_DEFAULT : Math.round(host.getCpuPercent()))
        * HIGHEST_LOAD_CPU_WEIGHT.getLong(props);
    final long lagWeighted = (host.getLagMs() == null ? LAG_MS_DEFAULT : Math.round(host.getLagMs()))
        * HIGHEST_LOAD_LAG_WEIGHT.getLong(props);
    return cpuPercentWeighted + lagWeighted;
  }
}
