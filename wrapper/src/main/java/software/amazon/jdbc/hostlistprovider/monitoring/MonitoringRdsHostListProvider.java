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
import java.util.logging.Logger;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.BlockingHostListProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.util.ServiceContainer;
import software.amazon.jdbc.util.connection.ConnectionService;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.Topology;

public class MonitoringRdsHostListProvider extends RdsHostListProvider
    implements BlockingHostListProvider, CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(MonitoringRdsHostListProvider.class.getName());

  public static final AwsWrapperProperty CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS =
      new AwsWrapperProperty(
          "clusterTopologyHighRefreshRateMs",
          "100",
          "Cluster topology high refresh rate in millis.");

  static {
    PropertyDefinition.registerPluginProperties(MonitoringRdsHostListProvider.class);
  }

  protected final ServiceContainer serviceContainer;
  protected final MonitorService monitorService;
  protected final PluginService pluginService;
  protected final long highRefreshRateNano;
  protected final String writerTopologyQuery;

  public MonitoringRdsHostListProvider(
      final Properties properties,
      final String originalUrl,
      final ServiceContainer serviceContainer,
      final String topologyQuery,
      final String nodeIdQuery,
      final String isReaderQuery,
      final String writerTopologyQuery) {
    super(properties, originalUrl, serviceContainer, topologyQuery, nodeIdQuery, isReaderQuery);
    this.serviceContainer = serviceContainer;
    this.monitorService = serviceContainer.getMonitorService();
    this.pluginService = serviceContainer.getPluginService();
    this.writerTopologyQuery = writerTopologyQuery;
    this.highRefreshRateNano = TimeUnit.MILLISECONDS.toNanos(
        CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.getLong(this.properties));
  }

  public static void clearCache() {
    clearAll();
  }

  @Override
  protected void init() throws SQLException {
    super.init();
  }

  protected ClusterTopologyMonitor initMonitor() throws SQLException {
    return monitorService.runIfAbsent(
        ClusterTopologyMonitorImpl.class,
        this.clusterId,
        this.storageService,
        this.pluginService.getTelemetryFactory(),
        this.originalUrl,
        this.pluginService.getDriverProtocol(),
        this.pluginService.getTargetDriverDialect(),
        this.pluginService.getDialect(),
        this.properties,
        (ConnectionService connectionService, PluginService monitorPluginService) -> new ClusterTopologyMonitorImpl(
            this.clusterId,
            this.storageService,
            this.monitorService,
            connectionService,
            this.initialHostSpec,
            this.properties,
            monitorPluginService,
            this.serviceContainer.getHostListProviderService(),
            this.clusterInstanceTemplate,
            this.refreshRateNano,
            this.highRefreshRateNano,
            this.topologyQuery,
            this.writerTopologyQuery,
            this.nodeIdQuery));
  }

  @Override
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    ClusterTopologyMonitor monitor = monitorService.get(ClusterTopologyMonitorImpl.class, this.clusterId);
    if (monitor == null) {
      monitor = this.initMonitor();
    }

    try {
      return monitor.forceRefresh(conn, defaultTopologyQueryTimeoutMs);
    } catch (TimeoutException ex) {
      return null;
    }
  }

  @Override
  protected void clusterIdChanged(final String oldClusterId) throws SQLException {
    final ClusterTopologyMonitorImpl existingMonitor =
        monitorService.get(ClusterTopologyMonitorImpl.class, oldClusterId);
    if (existingMonitor != null) {
      monitorService.runIfAbsent(
          ClusterTopologyMonitorImpl.class,
          this.clusterId,
          this.storageService,
          this.pluginService.getTelemetryFactory(),
          this.originalUrl,
          this.pluginService.getDriverProtocol(),
          this.pluginService.getTargetDriverDialect(),
          this.pluginService.getDialect(),
          this.properties,
          (connectionService, pluginService) -> existingMonitor);
      assert monitorService.get(ClusterTopologyMonitorImpl.class, this.clusterId) == existingMonitor;
      existingMonitor.setClusterId(this.clusterId);
      monitorService.remove(ClusterTopologyMonitorImpl.class, oldClusterId);
    }

    final Topology existingTopology = storageService.get(Topology.class, oldClusterId);
    final List<HostSpec> existingHosts = existingTopology == null ? null : existingTopology.getHosts();
    if (existingHosts != null) {
      storageService.set(this.clusterId, new Topology(existingHosts));
    }
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException {

    ClusterTopologyMonitor monitor = monitorService.get(ClusterTopologyMonitorImpl.class, this.clusterId);
    if (monitor == null) {
      monitor = this.initMonitor();
    }
    assert monitor != null;
    return monitor.forceRefresh(shouldVerifyWriter, timeoutMs);
  }

  @Override
  public void releaseResources() {
    // do nothing
  }
}
