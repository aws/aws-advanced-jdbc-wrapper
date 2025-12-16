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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.Utils;

public class RdsHostListProvider implements DynamicHostListProvider, CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(RdsHostListProvider.class.getName());

  public static final AwsWrapperProperty CLUSTER_TOPOLOGY_REFRESH_RATE_MS =
      new AwsWrapperProperty(
          "clusterTopologyRefreshRateMs",
          "30000",
          "Cluster topology refresh rate in millis. "
              + "The cached topology for the cluster will be invalidated after the specified time, "
              + "after which it will be updated during the next interaction with the connection.");

  public static final AwsWrapperProperty CLUSTER_ID = new AwsWrapperProperty(
      "clusterId", "1",
      "A unique identifier for the cluster. "
          + "Connections with the same cluster id share a cluster topology cache. "
          + "If unspecified, a cluster id is '1'.");

  public static final AwsWrapperProperty CLUSTER_INSTANCE_HOST_PATTERN =
      new AwsWrapperProperty(
          "clusterInstanceHostPattern",
          null,
          "The cluster instance DNS pattern that will be used to build a complete instance endpoint. "
              + "A \"?\" character in this pattern should be used as a placeholder for cluster instance names. "
              + "This pattern is required to be specified for IP address or custom domain connections to AWS RDS "
              + "clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS clusters.");

  public static final AwsWrapperProperty CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS =
      new AwsWrapperProperty(
          "clusterTopologyHighRefreshRateMs",
          "100",
          "Cluster topology high refresh rate in millis.");

  protected static final RdsUtils rdsHelper = new RdsUtils();
  protected static final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  protected static final int DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS = 5000;

  static {
    PropertyDefinition.registerPluginProperties(RdsHostListProvider.class);
  }

  protected final ReentrantLock lock = new ReentrantLock();
  protected final Properties properties;
  protected final String originalUrl;
  protected final FullServicesContainer servicesContainer;
  protected final PluginService pluginService;
  protected final HostListProviderService hostListProviderService;
  protected final TopologyUtils topologyUtils;
  protected final long refreshRateNano;
  protected final long highRefreshRateNano;

  protected RdsUrlType rdsUrlType;
  protected List<HostSpec> initialHostList = new ArrayList<>();
  protected HostSpec initialHostSpec;
  protected String clusterId;
  protected HostSpec instanceTemplate;
  protected volatile boolean isInitialized = false;

  public RdsHostListProvider(
      final TopologyUtils topologyUtils,
      final Properties properties,
      final String originalUrl,
      final FullServicesContainer servicesContainer) {
    this.topologyUtils = topologyUtils;
    this.properties = properties;
    this.originalUrl = originalUrl;
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();
    this.hostListProviderService = servicesContainer.getHostListProviderService();
    this.refreshRateNano =
        TimeUnit.MILLISECONDS.toNanos(CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInteger(properties));
    this.highRefreshRateNano = TimeUnit.MILLISECONDS.toNanos(
        CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.getLong(this.properties));
  }

  protected void init() throws SQLException {
    if (this.isInitialized) {
      return;
    }

    lock.lock();
    try {
      if (this.isInitialized) {
        return;
      }
      this.initSettings();
      this.isInitialized = true;
    } finally {
      lock.unlock();
    }
  }

  protected void initSettings() throws SQLException {
    // The initial topology is based on the connection string.
    this.initialHostList =
        connectionUrlParser.getHostsFromConnectionUrl(this.originalUrl, false,
            this.hostListProviderService::getHostSpecBuilder);
    if (this.initialHostList == null || this.initialHostList.isEmpty()) {
      throw new SQLException(Messages.get("RdsHostListProvider.parsedListEmpty", new Object[] {this.originalUrl}));
    }
    this.initialHostSpec = this.initialHostList.get(0);
    this.hostListProviderService.setInitialConnectionHostSpec(this.initialHostSpec);

    this.clusterId = CLUSTER_ID.getString(this.properties);
    HostSpecBuilder hostSpecBuilder = this.hostListProviderService.getHostSpecBuilder();
    String clusterInstancePattern = CLUSTER_INSTANCE_HOST_PATTERN.getString(this.properties);
    if (clusterInstancePattern != null) {
      this.instanceTemplate =
          ConnectionUrlParser.parseHostPortPair(clusterInstancePattern, () -> hostSpecBuilder);
    } else {
      this.instanceTemplate =
          hostSpecBuilder
              .host(rdsHelper.getRdsInstanceHostPattern(this.initialHostSpec.getHost()))
              .hostId(this.initialHostSpec.getHostId())
              .port(this.initialHostSpec.getPort())
              .build();
    }

    validateHostPatternSetting(this.instanceTemplate.getHost());
    this.rdsUrlType = rdsHelper.identifyRdsType(this.initialHostSpec.getHost());
  }

  protected ClusterTopologyMonitor getOrCreateMonitor() throws SQLException {
    return this.servicesContainer.getMonitorService().runIfAbsent(
        ClusterTopologyMonitorImpl.class,
        this.clusterId,
        this.servicesContainer,
        this.properties,
        (servicesContainer) -> new ClusterTopologyMonitorImpl(
            servicesContainer,
            this.topologyUtils,
            this.clusterId,
            this.initialHostSpec,
            this.properties,
            this.instanceTemplate,
            this.refreshRateNano,
            this.highRefreshRateNano));
  }

  @Override
  public List<HostSpec> getCurrentTopology(Connection conn, HostSpec initialHostSpec) throws SQLException {
    init();
    return this.topologyUtils.queryForTopology(conn, initialHostSpec, this.instanceTemplate);
  }

  protected List<HostSpec> forceRefreshMonitor(boolean shouldVerifyWriter, long timeoutMs) throws SQLException {
    ClusterTopologyMonitor monitor = this.getOrCreateMonitor();
    try {
      return monitor.forceRefresh(shouldVerifyWriter, timeoutMs);
    } catch (TimeoutException ex) {
      return null;
    }
  }


  /**
   * Get cluster topology. It may require an extra call to database to fetch the latest topology. A
   * cached copy of topology is returned if it's not yet outdated (controlled by {@link
   * #refreshRateNano}).
   *
   * @return a list of hosts that describes cluster topology. A writer is always at position 0.
   *     Returns an empty list if isn't available or is invalid (doesn't contain a writer).
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  protected FetchTopologyResult getTopology() throws SQLException {
    init();

    final List<HostSpec> storedHosts = this.getStoredTopology();
    if (storedHosts == null) {
      // We need to re-fetch topology.
      if (!this.pluginService.isDialectConfirmed()) {
        // We need to confirm the dialect before creating a topology monitor so that it uses the correct SQL queries.
        // We will return the original hosts parsed from the connections string until the dialect has been confirmed.
        return new FetchTopologyResult(false, this.initialHostList);
      }

      final List<HostSpec> hosts = this.forceRefreshMonitor(false, DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS);
      if (!Utils.isNullOrEmpty(hosts)) {
        return new FetchTopologyResult(false, hosts);
      }
    }

    if (storedHosts == null) {
      return new FetchTopologyResult(false, this.initialHostList);
    } else {
      // Return the cached data.
      return new FetchTopologyResult(true, storedHosts);
    }
  }

  /**
   * Get cached topology.
   *
   * @return list of hosts that represents topology. If there's no topology in the cache or the
   *     cached topology is outdated, it returns null.
   */
  public @Nullable List<HostSpec> getStoredTopology() throws SQLException {
    init();
    Topology topology = this.servicesContainer.getStorageService().get(Topology.class, this.clusterId);
    return topology == null ? null : topology.getHosts();
  }

  /**
   * Clear topology cache for the current cluster.
   */
  public void clear() {
    this.servicesContainer.getStorageService().remove(Topology.class, this.clusterId);
  }

  @Override
  public List<HostSpec> refresh() throws SQLException {
    init();
    final FetchTopologyResult results = getTopology();
    LOGGER.finest(() -> LogUtils.logTopology(results.hosts, results.isCachedData ? "[From cache] Topology:" : null));
    return Collections.unmodifiableList(results.hosts);
  }

  public RdsUrlType getRdsUrlType() throws SQLException {
    init();
    return this.rdsUrlType;
  }

  protected void validateHostPatternSetting(final String hostPattern) {
    if (!rdsHelper.isDnsPatternValid(hostPattern)) {
      final String message = Messages.get("RdsHostListProvider.invalidPattern");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }

    final RdsUrlType rdsUrlType = rdsHelper.identifyRdsType(hostPattern);
    if (rdsUrlType == RdsUrlType.RDS_PROXY || rdsUrlType == RdsUrlType.RDS_PROXY_ENDPOINT) {
      final String message = Messages.get("RdsHostListProvider.clusterInstanceHostPatternNotSupportedForRDSProxy");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }

    if (rdsUrlType == RdsUrlType.RDS_CUSTOM_CLUSTER) {
      final String message = Messages.get("RdsHostListProvider.clusterInstanceHostPatternNotSupportedForRdsCustom");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }
  }

  protected static class FetchTopologyResult {

    public List<HostSpec> hosts;
    public boolean isCachedData;

    public FetchTopologyResult(final boolean isCachedData, final List<HostSpec> hosts) {
      this.isCachedData = isCachedData;
      this.hosts = hosts;
    }
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException, TimeoutException {
    return this.forceRefresh(false, DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS);
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException {
    init();
    if (!this.pluginService.isDialectConfirmed()) {
      // We need to confirm the dialect before creating a topology monitor so that it uses the correct SQL queries.
      // We will return the original hosts parsed from the connections string until the dialect has been confirmed.
      return this.initialHostList;
    }

    return this.forceRefreshMonitor(shouldVerifyWriter, timeoutMs);
  }

  @Override
  public void releaseResources() {
    // Do nothing.
  }

  @Override
  public HostRole getHostRole(Connection conn) throws SQLException {
    init();
    return this.topologyUtils.getHostRole(conn);
  }

  @Override
  public @Nullable HostSpec identifyConnection(Connection connection) throws SQLException {
    init();
    try {
      Pair<String, String> instanceIds = this.topologyUtils.getInstanceId(connection);
      if (instanceIds == null) {
        throw new SQLException(Messages.get("RdsHostListProvider.errorIdentifyConnection"));
      }

      List<HostSpec> topology = this.refresh();
      boolean isForcedRefresh = false;
      if (topology == null) {
        topology = this.forceRefresh();
        isForcedRefresh = true;
      }

      if (topology == null) {
        return null;
      }

      String instanceName = instanceIds.getValue2();
      HostSpec foundHost = topology
          .stream()
          .filter(host -> Objects.equals(instanceName, host.getHostId()))
          .findAny()
          .orElse(null);

      if (foundHost == null && !isForcedRefresh) {
        topology = this.forceRefresh();
        if (topology == null) {
          return null;
        }

        foundHost = topology
            .stream()
            .filter(host -> Objects.equals(instanceName, host.getHostId()))
            .findAny()
            .orElse(null);
      }

      return foundHost;
    } catch (final SQLException | TimeoutException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.errorIdentifyConnection"), e);
    }
  }

  @Override
  public String getClusterId() throws SQLException {
    init();
    return this.clusterId;
  }
}
