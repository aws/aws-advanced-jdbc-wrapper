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

package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.AccessibleRegions;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;


public class GlobalAuroraTopologyMonitor extends ClusterTopologyMonitorImpl {
  private static final Logger LOGGER = Logger.getLogger(GlobalAuroraTopologyMonitor.class.getName());

  private static final long STABLE_TOPOLOGIES_DURATION_NANO = TimeUnit.SECONDS.toNanos(30);

  protected final Map<String, HostSpec> instanceTemplatesByRegion;
  protected final GlobalAuroraTopologyUtils topologyUtils;
  protected final @Nullable Set<String> accessibleRegions;
  protected final RdsUtils rdsUtils = new RdsUtils();

  public GlobalAuroraTopologyMonitor(
      final FullServicesContainer servicesContainer,
      final GlobalAuroraTopologyUtils topologyUtils,
      final String clusterId,
      final HostSpec initialHostSpec,
      final Properties properties,
      final HostSpec instanceTemplate,
      final long refreshRateNano,
      final long highRefreshRateNano,
      final Map<String, HostSpec> instanceTemplatesByRegion) {
    super(servicesContainer,
        topologyUtils,
        clusterId,
        initialHostSpec,
        properties,
        instanceTemplate,
        refreshRateNano,
        highRefreshRateNano);

    this.instanceTemplatesByRegion = instanceTemplatesByRegion;
    this.topologyUtils = topologyUtils;

    this.accessibleRegions = AccessibleRegions.parse(properties);
  }

  @Override
  protected HostSpec getInstanceTemplate(@Nullable String instanceId, Connection connection) throws SQLException {
    String region = this.topologyUtils.getRegion(instanceId, connection);
    if (!StringUtils.isNullOrEmpty(region)) {
      final @Nullable HostSpec instanceTemplate = this.instanceTemplatesByRegion.get(region);
      if (instanceTemplate == null) {
        throw new SQLException(
            Messages.get("GlobalAuroraTopologyMonitor.cannotFindRegionTemplate", new Object[] {region}));
      }

      return instanceTemplate;
    }

    return this.instanceTemplate;
  }

  @Override
  protected @Nullable List<HostSpec> queryForTopology(Connection connection) throws SQLException {
    return this.topologyUtils.queryForTopology(connection, this.initialHostSpec, this.instanceTemplatesByRegion);
  }

  @Override
  protected @Nullable List<HostSpec> openAnyConnectionAndUpdateTopology() {
    if (this.accessibleRegions != null) {
      final String region = this.rdsUtils.getRdsRegion(this.initialHostSpec.getHost());
      if (region != null && !this.accessibleRegions.contains(region.toLowerCase(Locale.ROOT))) {
        throw new RuntimeException(
            Messages.get("GlobalAuroraTopologyMonitor.initialHostNotInAccessibleRegion",
                new Object[]{this.initialHostSpec.getHost(), region, this.accessibleRegions}));
      }
    }
    return super.openAnyConnectionAndUpdateTopology();
  }

  @Override
  protected List<HostSpec> filterHostsForNodeMonitoring(final List<HostSpec> hosts) {
    final Set<String> regions = this.accessibleRegions;
    if (regions == null) {
      return hosts;
    }
    return hosts.stream()
        .filter(host -> {
          final String region = this.rdsUtils.getRdsRegion(host.getHost());
          return region != null && regions.contains(region.toLowerCase(Locale.ROOT));
        })
        .collect(Collectors.toList());
  }

  @Override
  protected MonitoringConnectionHandler createConnectionHandler() {
    return new GdbMonitoringConnectionHandler(
        this.monitoringConnection,
        this.servicesContainer.getPluginService(),
        this.topologyUtils,
        this.properties,
        this.monitoringProperties,
        this.writerHostSpec,
        this::wakeUpMonitoringLoop);
  }

  @Override
  protected long getStableTopologiesDurationNano() {
    return STABLE_TOPOLOGIES_DURATION_NANO;
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    List<Pair<String, Object>> state = super.getSnapshotState();
    if (state == null) {
      state = new ArrayList<>();
    }
    if (this.accessibleRegions != null) {
      state.add(Pair.create("accessibleRegions", this.accessibleRegions.toString()));
    }
    if (this.instanceTemplatesByRegion != null) {
      List<Pair<String, Object>> propsMap = new ArrayList<>();
      for (Map.Entry<String, HostSpec> entry : instanceTemplatesByRegion.entrySet()) {
        propsMap.add(Pair.create(entry.getKey(), entry.getValue().toString()));
      }
      state.add(Pair.create("instanceTemplatesByRegion", propsMap));
    }
    return state;
  }

}
