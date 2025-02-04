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
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;

public class MonitoringRdsHostListProvider extends RdsHostListProvider
    implements BlockingHostListProvider, CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(MonitoringRdsHostListProvider.class.getName());

  public static final AwsWrapperProperty CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS =
      new AwsWrapperProperty(
          "clusterTopologyHighRefreshRateMs",
          "100",
          "Cluster topology high refresh rate in millis.");

  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);
  protected static final long MONITOR_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(15);

  protected static final long TOPOLOGY_CACHE_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(5);

  protected static final SlidingExpirationCacheWithCleanupThread<String, ClusterTopologyMonitor> monitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          ClusterTopologyMonitor::canDispose,
          (monitor) -> {
            try {
              monitor.close();
            } catch (Exception ex) {
              // ignore
            }
          },
          CACHE_CLEANUP_NANO);

  static {
    PropertyDefinition.registerPluginProperties(MonitoringRdsHostListProvider.class);
  }

  protected final PluginService pluginService;
  protected final long highRefreshRateNano;
  protected final String writerTopologyQuery;

  public MonitoringRdsHostListProvider(
      final Properties properties,
      final String originalUrl,
      final HostListProviderService hostListProviderService,
      final String topologyQuery,
      final String nodeIdQuery,
      final String isReaderQuery,
      final String writerTopologyQuery,
      final PluginService pluginService) {
    super(properties, originalUrl, hostListProviderService, topologyQuery, nodeIdQuery, isReaderQuery);
    this.pluginService = pluginService;
    this.writerTopologyQuery = writerTopologyQuery;
    this.highRefreshRateNano = TimeUnit.MILLISECONDS.toNanos(
        CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.getLong(this.properties));
  }

  public static void closeAllMonitors() {
    monitors.getEntries().values().forEach(monitor -> {
      try {
        monitor.close();
      } catch (Exception ex) {
        // ignore
      }
    });
    monitors.clear();
    clearAll();
  }

  @Override
  protected void init() throws SQLException {
    super.init();
  }

  protected ClusterTopologyMonitor initMonitor() {
    return monitors.computeIfAbsent(this.clusterId,
        (key) -> new ClusterTopologyMonitorImpl(
            key, topologyCache, this.initialHostSpec, this.properties, this.pluginService,
            this.hostListProviderService, this.clusterInstanceTemplate,
            this.refreshRateNano, this.highRefreshRateNano, TOPOLOGY_CACHE_EXPIRATION_NANO,
            this.topologyQuery,
            this.writerTopologyQuery,
            this.nodeIdQuery),
        MONITOR_EXPIRATION_NANO);
  }

  @Override
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    ClusterTopologyMonitor monitor = monitors.get(this.clusterId, MONITOR_EXPIRATION_NANO);
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
  protected void clusterIdChanged(final String oldClusterId) {
    super.clusterIdChanged(oldClusterId);

    if (this.clusterId.equals(oldClusterId)) {
      // clusterId is the same
      return;
    }

    final ClusterTopologyMonitor existingMonitor = monitors.get(oldClusterId, MONITOR_EXPIRATION_NANO);
    if (existingMonitor != null) {
      monitors.computeIfAbsent(
          this.clusterId,
          (key) -> {
            existingMonitor.setClusterId(this.clusterId);
            return existingMonitor;
          },
          MONITOR_EXPIRATION_NANO);
      monitors.remove(oldClusterId);
    }

    final List<HostSpec> existingHosts = topologyCache.get(oldClusterId);
    if (existingHosts != null) {
      topologyCache.put(this.clusterId, existingHosts, TOPOLOGY_CACHE_EXPIRATION_NANO);
    }
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException {

    ClusterTopologyMonitor monitor = monitors.get(this.clusterId, MONITOR_EXPIRATION_NANO);
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
