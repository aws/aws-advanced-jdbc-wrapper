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
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
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
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.Utils;

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

  protected static final RdsUtils rdsHelper = new RdsUtils();
  protected static final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  protected static final int defaultTopologyQueryTimeoutMs = 5000;

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

  // For testing purposes only
  public RdsHostListProvider(
      final TopologyDialect dialect,
      final Properties properties,
      final String originalUrl,
      final FullServicesContainer servicesContainer,
      final TopologyUtils topologyUtils) {
    this(dialect, properties, originalUrl, servicesContainer);
    this.topologyUtils = topologyUtils;
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

    this.clusterId = CLUSTER_ID.getString(this.properties);
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

    if (this.topologyUtils == null) {
      this.topologyUtils = new TopologyUtils(
          this.dialect,
          this.initialHostSpec,
          this.servicesContainer.getPluginService().getHostSpecBuilder());
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

    final List<HostSpec> storedHosts = this.getStoredTopology();

    if (storedHosts == null || forceUpdate) {

      // need to re-fetch topology

      if (conn == null) {
        // can't fetch the latest topology since no connection
        // return original hosts parsed from connection string
        return new FetchTopologyResult(false, this.initialHostList);
      }

      // fetch topology from the DB
      final List<HostSpec> hosts = this.queryForTopology(conn);

      if (!Utils.isNullOrEmpty(hosts)) {
        this.servicesContainer.getStorageService().set(this.clusterId, new Topology(hosts));
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

  /**
   * Obtain a cluster topology from database.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return a list of {@link HostSpec} objects representing the topology
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    init();
    return this.topologyUtils.queryForTopology(conn, this.clusterInstanceTemplate);
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

  protected void validateHostPatternSetting(final String hostPattern) {
    if (!rdsHelper.isDnsPatternValid(hostPattern)) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the host
      // pattern must contain a '?'
      // character as a placeholder for the DB instance identifiers of the instances in the cluster"
      final String message = Messages.get("RdsHostListProvider.invalidPattern");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }

    final RdsUrlType rdsUrlType = rdsHelper.identifyRdsType(hostPattern);
    if (rdsUrlType == RdsUrlType.RDS_PROXY || rdsUrlType == RdsUrlType.RDS_PROXY_ENDPOINT) {
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

      String instanceName = instanceIds.getValue2();
      HostSpec foundHost = topology
          .stream()
          .filter(host -> Objects.equals(instanceName, host.getHostId()))
          .findAny()
          .orElse(null);

      if (foundHost == null && !isForcedRefresh) {
        topology = this.forceRefresh(connection);
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
    } catch (final SQLException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.errorIdentifyConnection"), e);
    }
  }

  @Override
  public String getClusterId() throws UnsupportedOperationException, SQLException {
    init();
    return this.clusterId;
  }
}
