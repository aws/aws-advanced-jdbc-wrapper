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

package software.amazon.jdbc.hostlistprovider.monitoring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.BlockingHostListProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.hostlistprovider.TopologyUtils;
import software.amazon.jdbc.util.FullServicesContainer;

public class MonitoringRdsHostListProvider
    extends RdsHostListProvider implements BlockingHostListProvider, CanReleaseResources {

  public static final AwsWrapperProperty CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS =
      new AwsWrapperProperty(
          "clusterTopologyHighRefreshRateMs",
          "100",
          "Cluster topology high refresh rate in millis.");

  static {
    PropertyDefinition.registerPluginProperties(MonitoringRdsHostListProvider.class);
  }

  protected final FullServicesContainer servicesContainer;
  protected final PluginService pluginService;
  protected final long highRefreshRateNano;

  public MonitoringRdsHostListProvider(
      final TopologyUtils topologyUtils,
      final Properties properties,
      final String originalUrl,
      final FullServicesContainer servicesContainer) {
    super(topologyUtils, properties, originalUrl, servicesContainer);
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();
    this.highRefreshRateNano = TimeUnit.MILLISECONDS.toNanos(
        CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.getLong(this.properties));
  }

  protected ClusterTopologyMonitor initMonitor() throws SQLException {
    return this.servicesContainer.getMonitorService().runIfAbsent(
        ClusterTopologyMonitorImpl.class,
        this.clusterId,
        this.servicesContainer,
        this.properties,
        (servicesContainer) -> new ClusterTopologyMonitorImpl(
            this.servicesContainer,
            this.topologyUtils,
            this.clusterId,
            this.initialHostSpec,
            this.properties,
            this.instanceTemplate,
            this.refreshRateNano,
            this.highRefreshRateNano));
  }

  @Override
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    ClusterTopologyMonitor monitor = this.initMonitor();

    try {
      return monitor.forceRefresh(conn, defaultTopologyQueryTimeoutMs);
    } catch (TimeoutException ex) {
      return null;
    }
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException {

    ClusterTopologyMonitor monitor =
        this.servicesContainer.getMonitorService().get(ClusterTopologyMonitorImpl.class, this.clusterId);
    if (monitor == null) {
      monitor = this.initMonitor();
    }
    assert monitor != null;
    return monitor.forceRefresh(shouldVerifyWriter, timeoutMs);
  }

  @Override
  public void releaseResources() {
    // Do nothing.
  }
}
