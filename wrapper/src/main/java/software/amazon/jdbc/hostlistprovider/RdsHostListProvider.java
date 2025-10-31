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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.TopologyDialect;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.TopologyUtils;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.storage.CacheMap;

public class RdsHostListProvider implements DynamicHostListProvider {

  private static final Logger LOGGER = Logger.getLogger(RdsHostListProvider.class.getName());

  public static final AwsWrapperProperty CLUSTER_TOPOLOGY_REFRESH_RATE_MS =
      new AwsWrapperProperty(
          "clusterTopologyRefreshRateMs",
          "30000",
          "Cluster topology refresh rate in millis. "
              + "The cached topology for the cluster will be invalidated after the specified time, "
              + "after which it will be updated during the next interaction with the connection.");

  public static final AwsWrapperProperty CLUSTER_ID = new AwsWrapperProperty(
      "clusterId", "",
      "A unique identifier for the cluster. "
          + "Connections with the same cluster id share a cluster topology cache. "
          + "If unspecified, a cluster id is automatically created for AWS RDS clusters.");

  public static final AwsWrapperProperty CLUSTER_INSTANCE_HOST_PATTERN =
      new AwsWrapperProperty(
          "clusterInstanceHostPattern",
          null,
          "The cluster instance DNS pattern that will be used to build a complete instance endpoint. "
              + "A \"?\" character in this pattern should be used as a placeholder for cluster instance names. "
              + "This pattern is required to be specified for IP address or custom domain connections to AWS RDS "
              + "clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS clusters.");

  protected static final RdsUtils rdsHelper = new RdsUtils();
  protected static final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  protected static final int defaultTopologyQueryTimeoutMs = 5000;
  protected static final long suggestedClusterIdRefreshRateNano = TimeUnit.MINUTES.toNanos(10);
  protected static final CacheMap<String, String> suggestedPrimaryClusterIdCache = new CacheMap<>();
  protected static final CacheMap<String, Boolean> primaryClusterIdCache = new CacheMap<>();

  protected final ReentrantLock lock = new ReentrantLock();
  protected final TopologyDialect dialect;
  protected final Properties properties;
  protected final String originalUrl;
  protected final FullServicesContainer servicesContainer;
  protected final HostListProviderService hostListProviderService;

  protected RdsUrlType rdsUrlType;
  protected long refreshRateNano = CLUSTER_TOPOLOGY_REFRESH_RATE_MS.defaultValue != null
      ? TimeUnit.MILLISECONDS.toNanos(Long.parseLong(CLUSTER_TOPOLOGY_REFRESH_RATE_MS.defaultValue))
      : TimeUnit.MILLISECONDS.toNanos(30000);
  protected List<HostSpec> hostList = new ArrayList<>();
  protected List<HostSpec> initialHostList = new ArrayList<>();
  protected HostSpec initialHostSpec;
  protected String clusterId;
  protected HostSpec clusterInstanceTemplate;
  protected TopologyUtils topologyUtils;

  // A primary clusterId is a clusterId that is based off of a cluster endpoint URL
  // (rather than a GUID or a value provided by the user).
  protected boolean isPrimaryClusterId;

  protected volatile boolean isInitialized = false;

  static {
    PropertyDefinition.registerPluginProperties(RdsHostListProvider.class);
  }

  public RdsHostListProvider(
      final TopologyDialect dialect,
      final Properties properties,
      final String originalUrl,
      final FullServicesContainer servicesContainer) {
    this.dialect = dialect;
    this.properties = properties;
    this.originalUrl = originalUrl;
    this.servicesContainer = servicesContainer;
    this.hostListProviderService = servicesContainer.getHostListProviderService();
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

      // initial topology is based on connection string
      this.initialHostList =
          connectionUrlParser.getHostsFromConnectionUrl(this.originalUrl, false,
              this.hostListProviderService::getHostSpecBuilder);
      if (this.initialHostList == null || this.initialHostList.isEmpty()) {
        throw new SQLException(Messages.get("RdsHostListProvider.parsedListEmpty",
            new Object[] {this.originalUrl}));
      }
      this.initialHostSpec = this.initialHostList.get(0);
      this.hostListProviderService.setInitialConnectionHostSpec(this.initialHostSpec);

      this.clusterId = UUID.randomUUID().toString();
      this.isPrimaryClusterId = false;
      this.refreshRateNano =
          TimeUnit.MILLISECONDS.toNanos(CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInteger(properties));

      HostSpecBuilder hostSpecBuilder = this.hostListProviderService.getHostSpecBuilder();
      String clusterInstancePattern = CLUSTER_INSTANCE_HOST_PATTERN.getString(this.properties);
      if (clusterInstancePattern != null) {
        this.clusterInstanceTemplate =
            ConnectionUrlParser.parseHostPortPair(clusterInstancePattern, () -> hostSpecBuilder);
      } else {
        this.clusterInstanceTemplate =
            hostSpecBuilder
                .host(rdsHelper.getRdsInstanceHostPattern(this.initialHostSpec.getHost()))
                .hostId(this.initialHostSpec.getHostId())
                .port(this.initialHostSpec.getPort())
                .build();
      }

      validateHostPatternSetting(this.clusterInstanceTemplate.getHost());

      this.rdsUrlType = rdsHelper.identifyRdsType(this.initialHostSpec.getHost());

      final String clusterIdSetting = CLUSTER_ID.getString(this.properties);
      if (!StringUtils.isNullOrEmpty(clusterIdSetting)) {
        this.clusterId = clusterIdSetting;
      } else if (rdsUrlType == RdsUrlType.RDS_PROXY) {
        // Each proxy is associated with a single cluster, so it's safe to use RDS Proxy Url as cluster
        // identification
        this.clusterId = this.initialHostSpec.getUrl();
      } else if (rdsUrlType.isRds()) {
        final ClusterSuggestedResult clusterSuggestedResult =
            getSuggestedClusterId(this.initialHostSpec.getHostAndPort());
        if (clusterSuggestedResult != null && !StringUtils.isNullOrEmpty(clusterSuggestedResult.clusterId)) {
          this.clusterId = clusterSuggestedResult.clusterId;
          this.isPrimaryClusterId = clusterSuggestedResult.isPrimaryClusterId;
        } else {
          final String clusterRdsHostUrl =
              rdsHelper.getRdsClusterHostUrl(this.initialHostSpec.getHost());
          if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
            this.clusterId = this.clusterInstanceTemplate.isPortSpecified()
                ? String.format("%s:%s", clusterRdsHostUrl, this.clusterInstanceTemplate.getPort())
                : clusterRdsHostUrl;
            this.isPrimaryClusterId = true;
            primaryClusterIdCache.put(this.clusterId, true, suggestedClusterIdRefreshRateNano);
          }
        }
      }

      this.topologyUtils = new TopologyUtils(
          this.dialect,
          this.clusterInstanceTemplate,
          this.initialHostSpec,
          this.servicesContainer.getPluginService().getHostSpecBuilder());

      this.isInitialized = true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get cluster topology. It may require an extra call to database to fetch the latest topology. A
   * cached copy of topology is returned if it's not yet outdated (controlled by {@link
   * #refreshRateNano}).
   *
   * @param conn A connection to database to fetch the latest topology, if needed.
   * @param forceUpdate If true, it forces a service to ignore cached copy of topology and to fetch
   *     a fresh one.
   * @return a list of hosts that describes cluster topology. A writer is always at position 0.
   *     Returns an empty list if isn't available or is invalid (doesn't contain a writer).
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  protected FetchTopologyResult getTopology(final Connection conn, final boolean forceUpdate) throws SQLException {
    init();

    final String suggestedPrimaryClusterId = suggestedPrimaryClusterIdCache.get(this.clusterId);

    // Change clusterId by accepting a suggested one
    if (!StringUtils.isNullOrEmpty(suggestedPrimaryClusterId)
        && !this.clusterId.equals(suggestedPrimaryClusterId)) {

      final String oldClusterId = this.clusterId;
      this.clusterId = suggestedPrimaryClusterId;
      this.isPrimaryClusterId = true;
      this.clusterIdChanged(oldClusterId);
    }

    final List<HostSpec> storedHosts = this.getStoredTopology();

    // This clusterId is a primary one and is about to create a new entry in the cache.
    // When a primary entry is created it needs to be suggested for other (non-primary) entries.
    // Remember a flag to do suggestion after cache is updated.
    final boolean needToSuggest = storedHosts == null && this.isPrimaryClusterId;

    if (storedHosts == null || forceUpdate) {

      // need to re-fetch topology

      if (conn == null) {
        // can't fetch the latest topology since no connection
        // return original hosts parsed from connection string
        return new FetchTopologyResult(false, this.initialHostList);
      }

      // fetch topology from the DB
      final List<HostSpec> hosts = this.topologyUtils.queryForTopology(conn);

      if (!Utils.isNullOrEmpty(hosts)) {
        this.servicesContainer.getStorageService().set(this.clusterId, new Topology(hosts));
        if (needToSuggest) {
          this.suggestPrimaryCluster(hosts);
        }
        return new FetchTopologyResult(false, hosts);
      }
    }

    if (storedHosts == null) {
      return new FetchTopologyResult(false, this.initialHostList);
    } else {
      // use cached data
      return new FetchTopologyResult(true, storedHosts);
    }
  }

  protected void clusterIdChanged(final String oldClusterId) throws SQLException {
    // do nothing
  }

  protected @Nullable ClusterSuggestedResult getSuggestedClusterId(final String url) {
    Map<String, Topology> entries = this.servicesContainer.getStorageService().getEntries(Topology.class);
    if (entries == null) {
      return null;
    }

    for (final Entry<String, Topology> entry : entries.entrySet()) {
      final String key = entry.getKey(); // clusterId
      final List<HostSpec> hosts = entry.getValue().getHosts();
      final boolean isPrimaryCluster = primaryClusterIdCache.get(key, false,
          suggestedClusterIdRefreshRateNano);
      if (key.equals(url)) {
        return new ClusterSuggestedResult(url, isPrimaryCluster);
      }

      for (final HostSpec host : hosts) {
        if (host.getHostAndPort().equals(url)) {
          LOGGER.finest(() -> Messages.get("RdsHostListProvider.suggestedClusterId",
              new Object[] {key, url}));
          return new ClusterSuggestedResult(key, isPrimaryCluster);
        }
      }
    }
    return null;
  }

  protected void suggestPrimaryCluster(final @NonNull List<HostSpec> primaryClusterHosts) {
    if (Utils.isNullOrEmpty(primaryClusterHosts)) {
      return;
    }

    Map<String, Topology> entries = this.servicesContainer.getStorageService().getEntries(Topology.class);
    if (entries == null) {
      return;
    }

    for (final Entry<String, Topology> entry : entries.entrySet()) {
      final String clusterId = entry.getKey();
      final List<HostSpec> clusterHosts = entry.getValue().getHosts();
      final boolean isPrimaryCluster = primaryClusterIdCache.get(clusterId, false,
          suggestedClusterIdRefreshRateNano);
      final String suggestedPrimaryClusterId = suggestedPrimaryClusterIdCache.get(clusterId);
      if (isPrimaryCluster
          || !StringUtils.isNullOrEmpty(suggestedPrimaryClusterId)
          || Utils.isNullOrEmpty(clusterHosts)) {
        continue;
      }

      // The entry is non-primary
      for (final HostSpec host : clusterHosts) {
        if (Utils.containsHostAndPort(primaryClusterHosts, host.getHostAndPort())) {
          // Instance on this cluster matches with one of the instance on primary cluster
          // Suggest the primary clusterId to this entry
          suggestedPrimaryClusterIdCache.put(clusterId, this.clusterId,
              suggestedClusterIdRefreshRateNano);
          break;
        }
      }
    }
  }

  /**
   * Obtain a cluster topology from database.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return a list of {@link HostSpec} objects representing the topology
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    return this.topologyUtils.queryForTopology(conn);
  }

  /**
   * Get cached topology.
   *
   * @return list of hosts that represents topology. If there's no topology in the cache or the
   *     cached topology is outdated, it returns null.
   */
  public @Nullable List<HostSpec> getStoredTopology() {
    Topology topology = this.servicesContainer.getStorageService().get(Topology.class, this.clusterId);
    return topology == null ? null : topology.getHosts();
  }

  /**
   * Clear topology cache for all clusters.
   */
  public static void clearAll() {
    primaryClusterIdCache.clear();
    suggestedPrimaryClusterIdCache.clear();
  }

  /**
   * Clear topology cache for the current cluster.
   */
  public void clear() {
    this.servicesContainer.getStorageService().remove(Topology.class, this.clusterId);
  }

  @Override
  public List<HostSpec> refresh() throws SQLException {
    return this.refresh(null);
  }

  @Override
  public List<HostSpec> refresh(final Connection connection) throws SQLException {
    init();
    final Connection currentConnection = connection != null
        ? connection
        : this.hostListProviderService.getCurrentConnection();

    final FetchTopologyResult results = getTopology(currentConnection, false);
    LOGGER.finest(() -> Utils.logTopology(results.hosts, results.isCachedData ? "[From cache] Topology:" : null));

    this.hostList = results.hosts;
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    return this.forceRefresh(null);
  }

  @Override
  public List<HostSpec> forceRefresh(final Connection connection) throws SQLException {
    init();
    final Connection currentConnection = connection != null
        ? connection
        : this.hostListProviderService.getCurrentConnection();

    final FetchTopologyResult results = getTopology(currentConnection, true);
    LOGGER.finest(() -> Utils.logTopology(results.hosts));
    this.hostList = results.hosts;
    return Collections.unmodifiableList(this.hostList);
  }

  public RdsUrlType getRdsUrlType() throws SQLException {
    init();
    return this.rdsUrlType;
  }

  private void validateHostPatternSetting(final String hostPattern) {
    if (!rdsHelper.isDnsPatternValid(hostPattern)) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the host
      // pattern must contain a '?'
      // character as a placeholder for the DB instance identifiers of the instances in the cluster"
      final String message = Messages.get("RdsHostListProvider.invalidPattern");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }

    final RdsUrlType rdsUrlType = rdsHelper.identifyRdsType(hostPattern);
    if (rdsUrlType == RdsUrlType.RDS_PROXY) {
      // "An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration setting."
      final String message =
          Messages.get("RdsHostListProvider.clusterInstanceHostPatternNotSupportedForRDSProxy");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }

    if (rdsUrlType == RdsUrlType.RDS_CUSTOM_CLUSTER) {
      // "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern'
      // configuration setting."
      final String message =
          Messages.get("RdsHostListProvider.clusterInstanceHostPatternNotSupportedForRdsCustom");
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
  public HostRole getHostRole(Connection conn) throws SQLException {
    return this.topologyUtils.getHostRole(conn);
  }

  @Override
  public @Nullable HostSpec identifyConnection(Connection connection) throws SQLException {
    try {
      String instanceId = this.topologyUtils.getInstanceId(connection);
      if (instanceId == null) {
        throw new SQLException(Messages.get("RdsHostListProvider.errorIdentifyConnection"));
      }

      List<HostSpec> topology = this.refresh(connection);
      boolean isForcedRefresh = false;
      if (topology == null) {
        topology = this.forceRefresh(connection);
        isForcedRefresh = true;
      }

      if (topology == null) {
        // TODO: above, we throw an exception, but here, we return null. Should we stick with just one?
        return null;
      }

      HostSpec foundHost = topology
          .stream()
          .filter(host -> Objects.equals(instanceId, host.getHostId()))
          .findAny()
          .orElse(null);

      if (foundHost == null && !isForcedRefresh) {
        topology = this.forceRefresh(connection);
        if (topology == null) {
          return null;
        }

        foundHost = topology
            .stream()
            .filter(host -> Objects.equals(instanceId, host.getHostId()))
            .findAny()
            .orElse(null);
      }

      return foundHost;
    } catch (final SQLException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.errorIdentifyConnection"), e);
    }
  }

  @Override
  public String getClusterId() throws UnsupportedOperationException, SQLException {
    init();
    return this.clusterId;
  }

  public static class ClusterSuggestedResult {

    public String clusterId;
    public boolean isPrimaryClusterId;

    public ClusterSuggestedResult(final String clusterId, final boolean isPrimaryClusterId) {
      this.clusterId = clusterId;
      this.isPrimaryClusterId = isPrimaryClusterId;
    }
  }
}
