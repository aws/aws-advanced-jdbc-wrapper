/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc.hostlistprovider;

import com.amazon.awslabs.jdbc.HostAvailability;
import com.amazon.awslabs.jdbc.HostListProvider;
import com.amazon.awslabs.jdbc.HostRole;
import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.PluginService;
import com.amazon.awslabs.jdbc.PropertyDefinition;
import com.amazon.awslabs.jdbc.ProxyDriverProperty;
import com.amazon.awslabs.jdbc.util.ExpiringCache;
import com.amazon.awslabs.jdbc.util.Messages;
import com.amazon.awslabs.jdbc.util.RdsUrlType;
import com.amazon.awslabs.jdbc.util.RdsUtils;
import com.amazon.awslabs.jdbc.util.StringUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

public class AuroraHostListProvider implements HostListProvider, DynamicHostListProvider {

  public static final ProxyDriverProperty CLUSTER_TOPOLOGY_REFRESH_RATE_MS =
      new ProxyDriverProperty(
          "clusterTopologyRefreshRateMs",
          "30000",
          "Cluster topology refresh rate in millis. "
              + "The cached topology for the cluster will be invalidated after the specified time, "
              + "after which it will be updated during the next interaction with the connection.");
  static final String PG_RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "ORDER BY LAST_UPDATE_TIMESTAMP DESC";

  static final String MYSQL_RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID, LAST_UPDATE_TIMESTAMP, REPLICA_LAG_IN_MILLISECONDS "
          + "FROM information_schema.replica_host_status "
          + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 " // 5 min
          + "ORDER BY LAST_UPDATE_TIMESTAMP DESC";
  static final int DEFAULT_CACHE_EXPIRE_MS = 5 * 60 * 1000; // 5 min
  static final int WRITER_CONNECTION_INDEX = 0;
  static final String MYSQL_GET_INSTANCE_NAME_SQL = "SELECT @@aurora_server_id";
  static final String MYSQL_GET_INSTANCE_NAME_COL = "@@aurora_server_id";
  static final String PG_GET_INSTANCE_NAME_SQL = "SELECT aurora_db_instance_identifier()";
  static final String PG_INSTANCE_NAME_COL = "aurora_db_instance_identifier";
  static final String WRITER_SESSION_ID = "MASTER_SESSION_ID";
  static final String FIELD_SERVER_ID = "SERVER_ID";
  static final String FIELD_SESSION_ID = "SESSION_ID";
  private final PluginService pluginService;
  private final String originalUrl;
  private final RdsUrlType rdsUrlType;
  private final RdsUtils rdsHelper;

  private final int refreshRateInMilliseconds;
  private List<HostSpec> hostList = new ArrayList<>();

  static final ExpiringCache<String, ClusterTopologyInfo> topologyCache =
      new ExpiringCache<>(DEFAULT_CACHE_EXPIRE_MS);
  private static final Object cacheLock = new Object();

  private static final String PG_DRIVER_PROTOCOL = "postgresql";
  private final String retrieveTopologyQuery;
  private final String retrieveInstanceQuery;
  private final String instanceNameCol;
  protected String clusterId;
  protected HostSpec clusterInstanceTemplate;

  private static final Logger LOGGER = Logger.getLogger(AuroraHostListProvider.class.getName());

  Properties properties;

  public AuroraHostListProvider(
      final String driverProtocol,
      final PluginService pluginService,
      final Properties properties,
      final String originalUrl) {
    this.rdsHelper = new RdsUtils();
    this.pluginService = pluginService;
    this.properties = properties;
    this.originalUrl = originalUrl;
    this.clusterId = UUID.randomUUID().toString();
    this.refreshRateInMilliseconds = CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInteger(properties);
    this.clusterInstanceTemplate =
        PropertyDefinition.CLUSTER_INSTANCE_HOST_PATTERN.get(this.properties) == null
            ? new HostSpec(rdsHelper.getRdsInstanceHostPattern(originalUrl))
            : new HostSpec(
                PropertyDefinition.CLUSTER_INSTANCE_HOST_PATTERN.getString(this.properties));
    validateHostPatternSetting(this.clusterInstanceTemplate.getHost());

    this.rdsUrlType = rdsHelper.identifyRdsType(originalUrl);

    final String clusterIdSetting = PropertyDefinition.CLUSTER_ID.get(this.properties);
    if (!StringUtils.isNullOrEmpty(clusterIdSetting)) {
      this.clusterId = clusterIdSetting;
    } else if (rdsUrlType == RdsUrlType.RDS_PROXY) {
      // Each proxy is associated with a single cluster so it's safe to use RDS Proxy Url as cluster
      // identification
      this.clusterId = this.pluginService.getCurrentHostSpec().getUrl();
    } else if (rdsUrlType.isRds()) {
      this.clusterId = this.rdsHelper.getRdsClusterHostUrl(originalUrl);
      if (!StringUtils.isNullOrEmpty(this.clusterId)) {
        this.clusterId = this.clusterId + ":" + this.clusterInstanceTemplate.getPort();
      }
    }

    if (driverProtocol.contains(PG_DRIVER_PROTOCOL)) {
      retrieveTopologyQuery = PG_RETRIEVE_TOPOLOGY_SQL;
      retrieveInstanceQuery = PG_GET_INSTANCE_NAME_SQL;
      instanceNameCol = PG_INSTANCE_NAME_COL;
    } else {
      retrieveTopologyQuery = MYSQL_RETRIEVE_TOPOLOGY_SQL;
      retrieveInstanceQuery = MYSQL_GET_INSTANCE_NAME_SQL;
      instanceNameCol = MYSQL_GET_INSTANCE_NAME_COL;
    }
  }

  /**
   * Get cluster topology. It may require an extra call to database to fetch the latest topology. A
   * cached copy of topology is returned if it's not yet outdated (controlled by {@link
   * #refreshRateInMilliseconds}).
   *
   * @param conn A connection to database to fetch the latest topology, if needed.
   * @param forceUpdate If true, it forces a service to ignore cached copy of topology and to fetch
   *     a fresh one.
   * @return a list of hosts that describes cluster topology. A writer is always at position 0.
   *     Returns an empty list if isn't available or is invalid (doesn't contain a writer).
   */
  public List<HostSpec> getTopology(final Connection conn, final boolean forceUpdate)
      throws SQLException {
    final HostSpec hostSpec = this.pluginService.getCurrentHostSpec();

    if (rdsUrlType.isRdsCluster()) {
      final HostSpec instance = this.getHostByName(this.pluginService.getCurrentConnection());
      if (instance != null && instance.getAliases() != null) {
        hostSpec.addAlias(instance.getAliases().toArray(new String[] {}));
      }
    }

    ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);

    if (clusterTopologyInfo == null
        || clusterTopologyInfo.hosts.isEmpty()
        || forceUpdate
        || refreshNeeded(clusterTopologyInfo)) {

      final ClusterTopologyInfo latestTopologyInfo = queryForTopology(conn);

      if (!latestTopologyInfo.hosts.isEmpty()) {
        clusterTopologyInfo = updateCache(clusterTopologyInfo, latestTopologyInfo);
      } else {
        return (clusterTopologyInfo == null || forceUpdate)
            ? new ArrayList<>()
            : clusterTopologyInfo.hosts;
      }
    }

    return clusterTopologyInfo.hosts;
  }

  private boolean refreshNeeded(final ClusterTopologyInfo info) {
    final Instant lastUpdateTime = info.lastUpdated;
    return info.hosts.isEmpty()
        || Duration.between(lastUpdateTime, Instant.now()).toMillis() > refreshRateInMilliseconds;
  }

  /**
   * Obtain a cluster topology from database.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return a {@link ClusterTopologyInfo} instance which contains details of the fetched topology
   */
  protected ClusterTopologyInfo queryForTopology(final Connection conn) throws SQLException {
    List<HostSpec> hosts = new ArrayList<>();
    try (final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery(retrieveTopologyQuery)) {
      hosts = processQueryResults(resultSet);
    } catch (final SQLSyntaxErrorException e) {
      // ignore
    }

    return new ClusterTopologyInfo(hosts, new HashSet<>(), Instant.now(), false);
  }

  /**
   * Form a list of hosts from the results of the topology query.
   *
   * @param resultSet The results of the topology query
   * @return a list of {@link HostSpec} objects representing the topology that was returned by the
   *     topology query. The list will be empty if the topology query returned an invalid topology
   *     (no writer instance).
   */
  private List<HostSpec> processQueryResults(final ResultSet resultSet) throws SQLException {
    int writerCount = 0;
    final List<HostSpec> hosts = new ArrayList<>();

    while (resultSet.next()) {
      if (!WRITER_SESSION_ID.equalsIgnoreCase(resultSet.getString(FIELD_SESSION_ID))) {
        hosts.add(createHost(resultSet));
        continue;
      }

      if (writerCount == 0) {
        // store the first writer to its expected position [0]
        hosts.add(WRITER_CONNECTION_INDEX, createHost(resultSet));
      } else {
        // during failover, there could temporarily be two writers. Because we sorted by the last
        // updated timestamp, this host should be the obsolete writer, and it is about to become a
        // reader
        hosts.add(createHost(resultSet, false));
      }
      writerCount++;
    }

    if (writerCount == 0) {
      LOGGER.log(
          Level.SEVERE,
          "[AuroraHostListProvider] The topology query returned an invalid topology - no writer instance detected");
      hosts.clear();
    }
    return hosts;
  }

  /**
   * Creates an instance of HostSpec which captures details about a connectable host.
   *
   * @param resultSet the result set from querying the topology
   * @return a {@link HostSpec} instance for a specific instance from the cluster
   * @throws SQLException If unable to retrieve the hostName from the result set
   */
  private HostSpec createHost(final ResultSet resultSet) throws SQLException {
    return createHost(resultSet, WRITER_SESSION_ID.equals(resultSet.getString(FIELD_SESSION_ID)));
  }

  /**
   * Creates an instance of HostSpec which captures details about a connectable host.
   *
   * @param resultSet the result set from querying the topology
   * @param isWriter true if the session_ID is the writer, else it will return false
   * @return a {@link HostSpec} instance for a specific instance from the cluster
   * @throws SQLException If unable to retrieve the hostName from the result set
   */
  private HostSpec createHost(final ResultSet resultSet, final boolean isWriter)
      throws SQLException {
    String hostName = resultSet.getString(FIELD_SERVER_ID);
    hostName = hostName == null ? "?" : hostName;
    final String endpoint = getHostEndpoint(hostName);
    final int port =
        this.clusterInstanceTemplate.isPortSpecified()
            ? this.clusterInstanceTemplate.getPort()
            : this.pluginService.getCurrentHostSpec().getPort();

    final HostSpec hostSpec =
        new HostSpec(endpoint, port, isWriter ? HostRole.WRITER : HostRole.READER);
    hostSpec.addAlias(hostName, endpoint, hostSpec.getUrl());
    if (!endpoint.endsWith("/")) {
      hostSpec.addAlias(endpoint + "/");
    }
    return hostSpec;
  }

  /**
   * Build a host dns endpoint based on host/node name.
   *
   * @param nodeName A host name.
   * @return Host dns endpoint
   */
  private String getHostEndpoint(final String nodeName) {
    final String host = this.clusterInstanceTemplate.getHost();
    return host.replace("?", nodeName);
  }

  /**
   * Store the information for the topology in the cache, creating the information object if it did
   * not previously exist in the cache.
   *
   * @param clusterTopologyInfo The cluster topology info that existed in the cache before the
   *     topology query. This parameter will be null if no topology info for the cluster has been
   *     created in the cache yet.
   * @param latestTopologyInfo The results of the current topology query
   * @return the {@link ClusterTopologyInfo} stored in the cache by this method, representing the
   *     most up-to-date information we have about the topology.
   */
  private ClusterTopologyInfo updateCache(
      @Nullable ClusterTopologyInfo clusterTopologyInfo,
      final ClusterTopologyInfo latestTopologyInfo) {
    if (clusterTopologyInfo == null) {
      clusterTopologyInfo = latestTopologyInfo;
    } else {
      clusterTopologyInfo.hosts = latestTopologyInfo.hosts;
    }
    clusterTopologyInfo.lastUpdated = Instant.now();

    synchronized (cacheLock) {
      topologyCache.put(this.clusterId, clusterTopologyInfo);
    }
    return clusterTopologyInfo;
  }

  /**
   * Get cached topology.
   *
   * @return list of hosts that represents topology. If there's no topology in the cache or the
   *     cached topology is outdated, it returns null.
   */
  public @Nullable List<HostSpec> getCachedTopology() {
    final ClusterTopologyInfo info = topologyCache.get(this.clusterId);
    return info == null || refreshNeeded(info) ? null : info.hosts;
  }

  /**
   * Get a set of instance names that were marked down.
   *
   * @return A set of instance dns names with port (example: "instance-1.my-domain.com:3306")
   */
  public Set<String> getDownHosts() {
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      return clusterTopologyInfo != null && clusterTopologyInfo.downHosts != null
          ? clusterTopologyInfo.downHosts
          : new HashSet<>();
    }
  }

  /**
   * Mark the host as down. Host stays marked down until the next topology refreshes.
   *
   * @param downHost The {@link HostSpec} object representing the host to mark as down
   */
  public void addToDownHostList(HostSpec downHost) {
    if (downHost == null) {
      return;
    }
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      if (clusterTopologyInfo == null) {
        clusterTopologyInfo =
            new ClusterTopologyInfo(new ArrayList<>(), new HashSet<>(), Instant.now(), false);
        topologyCache.put(this.clusterId, clusterTopologyInfo);
      } else if (clusterTopologyInfo.downHosts == null) {
        clusterTopologyInfo.downHosts = new HashSet<>();
      }
      clusterTopologyInfo.downHosts.add(downHost.getUrl());
      this.pluginService.setAvailability(downHost.getAliases(), HostAvailability.NOT_AVAILABLE);
    }
  }

  /**
   * Unmark host as down. The host is removed from the list of down hosts
   *
   * @param host The {@link HostSpec} object representing the host to remove from the list of down
   *     hosts
   */
  public void removeFromDownHostList(HostSpec host) {
    if (host == null) {
      return;
    }
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      if (clusterTopologyInfo != null && clusterTopologyInfo.downHosts != null) {
        clusterTopologyInfo.downHosts.remove(host.getUrl());
        this.pluginService.setAvailability(host.getAliases(), HostAvailability.AVAILABLE);
      }
    }
  }

  /**
   * Check if cached topology belongs to multi-writer cluster.
   *
   * @return True, if it's multi-writer cluster.
   */
  public boolean isMultiWriterCluster() {
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      return (clusterTopologyInfo != null
          && clusterTopologyInfo.downHosts != null
          && clusterTopologyInfo.isMultiWriterCluster);
    }
  }

  /**
   * Return the {@link HostSpec} object that is associated with a provided connection from the
   * topology host list.
   *
   * @param conn A connection to database.
   * @return The HostSpec object from the topology host list. Returns null if the connection host is
   *     not found in the latest topology.
   */
  public HostSpec getHostByName(Connection conn) {
    try (Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(retrieveInstanceQuery)) {
      String instanceName = null;
      if (resultSet.next()) {
        instanceName = resultSet.getString(instanceNameCol);
      }
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      return instanceNameToHost(
          instanceName, clusterTopologyInfo == null ? null : clusterTopologyInfo.hosts);
    } catch (SQLException e) {
      return null;
    }
  }

  private HostSpec instanceNameToHost(String name, List<HostSpec> hosts) {
    if (name == null || hosts == null) {
      return null;
    }

    for (HostSpec host : hosts) {
      if (host != null && host.getAliases().stream().anyMatch(name::equalsIgnoreCase)) {
        return host;
      }
    }
    return null;
  }

  /** Clear topology cache for all clusters. */
  public void clearAll() {
    synchronized (cacheLock) {
      topologyCache.clear();
    }
  }

  /** Clear topology cache for the current cluster. */
  public void clear() {
    synchronized (cacheLock) {
      topologyCache.remove(this.clusterId);
    }
  }

  @Override
  public List<HostSpec> refresh() throws SQLException {
    Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection == null) {
      currentConnection =
          this.pluginService.connect(this.pluginService.getCurrentHostSpec(), this.properties);
      this.pluginService.setCurrentConnection(
          currentConnection, this.pluginService.getCurrentHostSpec());
    }
    final ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
    hostList =
        getTopology(
            currentConnection, clusterTopologyInfo != null && refreshNeeded(clusterTopologyInfo));
    for (HostSpec hostSpec : hostList) {
      this.pluginService.setAvailability(hostSpec.getAliases(), hostSpec.getAvailability());
    }
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection == null) {
      currentConnection =
          this.pluginService.connect(this.pluginService.getCurrentHostSpec(), this.properties);
      this.pluginService.setCurrentConnection(
          currentConnection, this.pluginService.getCurrentHostSpec());
    }
    hostList = getTopology(currentConnection, true);
    for (HostSpec hostSpec : hostList) {
      this.pluginService.setAvailability(hostSpec.getAliases(), hostSpec.getAvailability());
    }
    return Collections.unmodifiableList(hostList);
  }

  public RdsUrlType getRdsUrlType() {
    return this.rdsUrlType;
  }

  private void validateHostPatternSetting(String hostPattern) {
    if (!this.rdsHelper.isDnsPatternValid(hostPattern)) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the host
      // pattern must contain a '?'
      // character as a placeholder for the DB instance identifiers of the instances in the cluster"
      final String message = Messages.get("AuroraHostListProvider.invalidPattern");
      LOGGER.severe(message);
      throw new RuntimeException(Messages.get("AuroraHostListProvider.invalidPattern"));
    }

    final RdsUrlType rdsUrlType = this.rdsHelper.identifyRdsType(hostPattern);
    if (rdsUrlType == RdsUrlType.RDS_PROXY) {
      // "An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration setting."
      final String message =
          Messages.get("AuroraHostListProvider.clusterInstanceHostPatternNotSupportedForRDSProxy");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }

    if (rdsUrlType == RdsUrlType.RDS_CUSTOM_CLUSTER) {
      // "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern'
      // configuration setting."
      final String message =
          Messages.get("AuroraHostListProvider.clusterInstanceHostPatternNotSupportedForRdsCustom");
      LOGGER.severe(message);
      throw new RuntimeException(message);
    }
  }

  /** Class that holds the topology and additional information about the topology. */
  static class ClusterTopologyInfo {

    public Set<String> downHosts;
    public List<HostSpec> hosts;
    public Instant lastUpdated;
    public boolean isMultiWriterCluster;

    /**
     * Constructor for ClusterTopologyInfo.
     *
     * @param hosts List of available instance hosts
     * @param lastUpdated Last updated topology time
     */
    ClusterTopologyInfo(
        List<HostSpec> hosts,
        Set<String> downHosts,
        final Instant lastUpdated,
        final boolean isMultiWriterCluster) {
      this.hosts = hosts;
      this.downHosts = downHosts;
      this.lastUpdated = lastUpdated;
      this.isMultiWriterCluster = isMultiWriterCluster;
    }
  }
}
