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
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;

/**
 * Host selector that picks the highest-loaded reader using a calculated load derived from
 * {@link HostSpec#getCpuPercent()} and {@link HostSpec#getLagMs()}
 *
 * <p>Note: Non-Aurora dialects may not be compatible as they do not populate host {@code cpuPercent} or {@code lagMs}.
 */
public class HighestLoadHostSelector implements HostSelector {

  public static final String STRATEGY_HIGHEST_LOAD = "highestLoad";
  public static final String STRATEGY_HIGHEST_LOAD_BY_CPU = "highestLoadByCpu";
  public static final String STRATEGY_HIGHEST_LOAD_BY_LAG = "highestLoadByLag";

  public static final AwsWrapperProperty HIGHEST_LOAD_CPU_WEIGHT = new AwsWrapperProperty(
      "highestLoadCpuWeight", "1",
      "The weight of CPU utilization percent in the calculation of a host's load.");

  public static final AwsWrapperProperty HIGHEST_LOAD_LAG_WEIGHT = new AwsWrapperProperty(
      "highestLoadLagWeight", "100",
      "The weight of lag in the calculation of a host's load.");

  protected static final long CPU_DEFAULT = 40;

  protected static final long LAG_MS_DEFAULT = 50;

  private static final long DEFAULT_CPU_WEIGHT = 1;
  private static final long DEFAULT_LAG_WEIGHT = 100;

  private final long defaultCpuWeight;
  private final long defaultLagWeight;

  static {
    PropertyDefinition.registerPluginProperties(HighestLoadHostSelector.class);
  }

  public HighestLoadHostSelector() {
    this(DEFAULT_CPU_WEIGHT, DEFAULT_LAG_WEIGHT);
  }

  public HighestLoadHostSelector(final long defaultCpuWeight, final long defaultLagWeight) {
    this.defaultCpuWeight = defaultCpuWeight;
    this.defaultLagWeight = defaultLagWeight;
  }

  public static HighestLoadHostSelector byCpu() {
    return new HighestLoadHostSelector(100, 1);
  }

  public static HighestLoadHostSelector byLag() {
    return new HighestLoadHostSelector(1, 100);
  }

  @Override
  public @Nullable HostSpec getHost(
      final @NonNull List<HostSpec> hosts,
      final @Nullable HostRole role,
      final @Nullable Properties props) throws SQLException {

    final List<HostSpec> eligible = hosts.stream()
        .filter(h -> (role == null || role.equals(h.getRole()))
            && h.getAvailability().equals(HostAvailability.AVAILABLE))
        .collect(Collectors.toList());

    if (eligible.isEmpty()) {
      return null;
    }

    @Nullable HostSpec highestLoadHost = null;
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

  private long calculateLoad(final HostSpec host, final @Nullable Properties props) {
    final long cpuWeight = getCpuWeight(props);
    final long lagWeight = getLagWeight(props);
    final Float cpuPercent = host.getCpuPercent();
    final Float lagMs = host.getLagMs();
    final long cpuPercentWeighted = (cpuPercent == null ? CPU_DEFAULT : Math.round(cpuPercent))
        * cpuWeight;
    final long lagWeighted = (lagMs == null ? LAG_MS_DEFAULT : Math.round(lagMs))
        * lagWeight;
    return cpuPercentWeighted + lagWeighted;
  }

  private long getCpuWeight(final @Nullable Properties props) {
    if (props != null && props.containsKey(HIGHEST_LOAD_CPU_WEIGHT.name)) {
      return HIGHEST_LOAD_CPU_WEIGHT.getLong(props);
    }
    return this.defaultCpuWeight;
  }

  private long getLagWeight(final @Nullable Properties props) {
    if (props != null && props.containsKey(HIGHEST_LOAD_LAG_WEIGHT.name)) {
      return HIGHEST_LOAD_LAG_WEIGHT.getLong(props);
    }
    return this.defaultLagWeight;
  }
}
