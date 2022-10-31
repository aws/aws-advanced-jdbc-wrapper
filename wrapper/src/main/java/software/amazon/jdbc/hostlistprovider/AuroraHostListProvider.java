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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.ExpiringCache;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;

public class AuroraHostListProvider implements DynamicHostListProvider {

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

  static final String PG_RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "ORDER BY LAST_UPDATE_TIMESTAMP";

  static final String MYSQL_RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID "
          + "FROM information_schema.replica_host_status "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "ORDER BY LAST_UPDATE_TIMESTAMP";
  static final int DEFAULT_CACHE_EXPIRE_MS = 5 * 60 * 1000; // 5 min
  static final String MYSQL_GET_INSTANCE_NAME_SQL = "SELECT @@aurora_server_id";
  static final String MYSQL_GET_INSTANCE_NAME_COL = "@@aurora_server_id";
  static final String PG_GET_INSTANCE_NAME_SQL = "SELECT aurora_db_instance_identifier()";
  static final String PG_INSTANCE_NAME_COL = "aurora_db_instance_identifier";
  static final String WRITER_SESSION_ID = "MASTER_SESSION_ID";
  static final String FIELD_SERVER_ID = "SERVER_ID";
  static final String FIELD_SESSION_ID = "SESSION_ID";
  private final HostListProviderService hostListProviderService;
  private final String originalUrl;
  private RdsUrlType rdsUrlType;
  private final RdsUtils rdsHelper;

  private int refreshRateInMilliseconds = Integer.parseInt(
      CLUSTER_TOPOLOGY_REFRESH_RATE_MS.defaultValue != null
          ? CLUSTER_TOPOLOGY_REFRESH_RATE_MS.defaultValue
          : "30000");
  private List<HostSpec> hostList = new ArrayList<>();
  private List<HostSpec> lastReturnedHostList;
  private List<HostSpec> initialHostList = new ArrayList<>();
  private HostSpec initialHostSpec;

  protected static final ExpiringCache<String, ClusterTopologyInfo> topologyCache =
      new ExpiringCache<>(DEFAULT_CACHE_EXPIRE_MS, getOnEvict());
  private static final Object cacheLock = new Object();

  private static final String PG_DRIVER_PROTOCOL = "postgresql";
  private final String retrieveTopologyQuery;
  private final String retrieveInstanceQuery;
  private final String instanceNameCol;
  protected String clusterId;
  protected HostSpec clusterInstanceTemplate;
  protected ConnectionUrlParser connectionUrlParser;

  // A primary clusterId is a clusterId that is based off of a cluster endpoint URL
  // (rather than a GUID or a value provided by the user).
  protected boolean isPrimaryClusterId;

  protected boolean isInitialized = false;

  private static final Logger LOGGER = Logger.getLogger(AuroraHostListProvider.class.getName());

  Properties properties;

  public AuroraHostListProvider(
      final String driverProtocol,
      final HostListProviderService hostListProviderService,
      final Properties properties,
      final String originalUrl) {
    this(driverProtocol, hostListProviderService, properties, originalUrl, new ConnectionUrlParser());
  }

  public AuroraHostListProvider(
      final String driverProtocol,
      final HostListProviderService hostListProviderService,
      final Properties properties,
      final String originalUrl,
      final ConnectionUrlParser connectionUrlParser) {
    this.rdsHelper = new RdsUtils();
    this.hostListProviderService = hostListProviderService;
    this.properties = properties;
    this.originalUrl = originalUrl;
    this.connectionUrlParser = connectionUrlParser;

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

  private static ExpiringCache.OnEvictRunnable<ClusterTopologyInfo> getOnEvict() {
    return (ClusterTopologyInfo evictedEntry) -> {
      LOGGER.finest(() -> "Entry with clusterId '" + evictedEntry.clusterId
          + "' has been evicted from the topology cache.");
    };
  }

  protected void init() throws SQLException {
    if (this.isInitialized) {
      return;
    }

    // initial topology is based on connection string
    this.initialHostList = this.connectionUrlParser.getHostsFromConnectionUrl(this.originalUrl, false);
    if (this.initialHostList == null || this.initialHostList.isEmpty()) {
      throw new SQLException(Messages.get("AuroraHostListProvider.parsedListEmpty",
          new Object[]{this.originalUrl}));
    }
    this.initialHostSpec = this.initialHostList.get(0);
    this.hostListProviderService.setInitialConnectionHostSpec(this.initialHostSpec);

    this.clusterId = UUID.randomUUID().toString();
    this.isPrimaryClusterId = false;
    this.refreshRateInMilliseconds = CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInteger(properties);
    this.clusterInstanceTemplate = CLUSTER_INSTANCE_HOST_PATTERN.getString(this.properties) == null
        ? new HostSpec(rdsHelper.getRdsInstanceHostPattern(originalUrl))
        : new HostSpec(CLUSTER_INSTANCE_HOST_PATTERN.getString(this.properties));
    validateHostPatternSetting(this.clusterInstanceTemplate.getHost());

    this.rdsUrlType = rdsHelper.identifyRdsType(originalUrl);

    final String clusterIdSetting = CLUSTER_ID.getString(this.properties);
    if (!StringUtils.isNullOrEmpty(clusterIdSetting)) {
      this.clusterId = clusterIdSetting;
    } else if (rdsUrlType == RdsUrlType.RDS_PROXY) {
      // Each proxy is associated with a single cluster, so it's safe to use RDS Proxy Url as cluster
      // identification
      this.clusterId = this.initialHostSpec.getUrl();
    } else if (rdsUrlType.isRds()) {
      final ClusterSuggestedResult clusterSuggestedResult =
          getSuggestedClusterId(this.initialHostSpec.getUrl());
      if (clusterSuggestedResult != null && !StringUtils.isNullOrEmpty(clusterSuggestedResult.clusterId)) {
        this.clusterId = clusterSuggestedResult.clusterId;
        this.isPrimaryClusterId = clusterSuggestedResult.isPrimaryClusterId;
      } else {
        final String clusterRdsHostUrl =
            this.rdsHelper.getRdsClusterHostUrl(this.initialHostSpec.getUrl());
        if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
          this.clusterId = this.clusterInstanceTemplate.isPortSpecified()
              ? String.format("%s:%s", clusterRdsHostUrl, this.clusterInstanceTemplate.getPort())
              : clusterRdsHostUrl;
          this.isPrimaryClusterId = true;
        }
      }
    }

    this.isInitialized = true;
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
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  public FetchTopologyResult getTopology(final Connection conn, final boolean forceUpdate) throws SQLException {

    ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);

    // Change clusterId by accepting a suggested one
    if (clusterTopologyInfo != null
        && !StringUtils.isNullOrEmpty(clusterTopologyInfo.suggestedPrimaryClusterId)
        && !this.clusterId.equals(clusterTopologyInfo.suggestedPrimaryClusterId)) {

      this.clusterId = clusterTopologyInfo.suggestedPrimaryClusterId;
      this.isPrimaryClusterId = true;

      clusterTopologyInfo = topologyCache.get(this.clusterId);
    }

    // This clusterId is a primary one and is about to create a new entry in the cache.
    // When a primary entry is created it needs to be suggested for other (non-primary) entries.
    // Remember a flag to do suggestion after cache is updated.
    boolean needToSuggest = clusterTopologyInfo == null && this.isPrimaryClusterId;

    if (clusterTopologyInfo == null
        || clusterTopologyInfo.hosts.isEmpty()
        || forceUpdate
        || refreshNeeded(clusterTopologyInfo)) {

      // need to re-fetch topology

      if (conn == null) {
        // can't fetch the latest topology since no connection
        // return original hosts parsed from connection string
        return new FetchTopologyResult(false, this.initialHostList);

      } else {
        // fetch topology from the DB
        final ClusterTopologyInfo latestTopologyInfo = queryForTopology(conn);

        if (latestTopologyInfo != null && !latestTopologyInfo.hosts.isEmpty()) {
          // topology looks valid
          clusterTopologyInfo = updateCache(clusterTopologyInfo, latestTopologyInfo);

          if (needToSuggest) {
            this.suggestPrimaryCluster(clusterTopologyInfo);
          }

          return new FetchTopologyResult(false, clusterTopologyInfo.hosts);

        } else {
          if (clusterTopologyInfo != null && !forceUpdate) {
            // use cached data
            return new FetchTopologyResult(true, clusterTopologyInfo.hosts);
          } else {
            return new FetchTopologyResult(false, this.initialHostList);
          }
        }
      }
    }

    // return cached hosts
    return new FetchTopologyResult(true, clusterTopologyInfo.hosts);
  }

  private ClusterSuggestedResult getSuggestedClusterId(final String url) {
    for (Entry<String, ClusterTopologyInfo> entry : topologyCache.entrySet()) {
      final String key = entry.getKey();
      final ClusterTopologyInfo value = entry.getValue();
      if (key.equals(url)) {
        return new ClusterSuggestedResult(url, value.isPrimaryCluster);
      }
      if (value.hosts == null) {
        continue;
      }
      for (HostSpec host : value.hosts) {
        if (host.getUrl().equals(url)) {
          LOGGER.finest(() -> Messages.get("AuroraHostListProvider.suggestedClusterId",
              new Object[]{key, url}));
          return new ClusterSuggestedResult(key, value.isPrimaryCluster);
        }
      }
    }
    return null;
  }

  protected void suggestPrimaryCluster(final @NonNull ClusterTopologyInfo primaryClusterTopologyInfo) {
    if (Utils.isNullOrEmpty(primaryClusterTopologyInfo.hosts)) {
      return;
    }

    final Set<String> primaryClusterHostUrls = new HashSet<>();
    for (final HostSpec hostSpec : primaryClusterTopologyInfo.hosts) {
      primaryClusterHostUrls.add(hostSpec.getUrl());
    }

    for (Entry<String, ClusterTopologyInfo> entry : topologyCache.entrySet()) {
      if (entry.getValue().isPrimaryCluster
          || !StringUtils.isNullOrEmpty(entry.getValue().suggestedPrimaryClusterId)
          || Utils.isNullOrEmpty(entry.getValue().hosts)) {
        continue;
      }

      // The entry is non-primary
      for (HostSpec host : entry.getValue().hosts) {
        if (primaryClusterHostUrls.contains(host.getUrl())) {
          // Instance on this cluster matches with one of the instance on primary cluster
          // Suggest the primary clusterId to this entry
          try {
            topologyCache.getLock().lock();
            entry.getValue().suggestedPrimaryClusterId = primaryClusterTopologyInfo.clusterId;
          } finally {
            topologyCache.getLock().unlock();
          }
          break;
        }
      }
    }
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
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  protected ClusterTopologyInfo queryForTopology(final Connection conn) throws SQLException {
    init();
    try (final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery(retrieveTopologyQuery)) {
      return processQueryResults(resultSet);
    } catch (final SQLSyntaxErrorException e) {
      // ignore
    }

    return new ClusterTopologyInfo(
        this.clusterId, new ArrayList<>(), Instant.now(), false, this.isPrimaryClusterId);
  }

  /**
   * Form a list of hosts from the results of the topology query.
   *
   * @param resultSet The results of the topology query
   * @return topology details {@link ClusterTopologyInfo} with a list of {@link HostSpec} objects representing
   *     the topology that was returned by the
   *     topology query. The list will be empty if the topology query returned an invalid topology
   *     (no writer instance).
   */
  private ClusterTopologyInfo processQueryResults(final ResultSet resultSet) throws SQLException {

    final HashMap<String, HostSpec> hostMap = new HashMap<>();

    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    while (resultSet.next()) {
      final HostSpec host = createHost(resultSet);
      hostMap.put(host.getHost(), host);
    }

    int writerCount = 0;
    final List<HostSpec> hosts = new ArrayList<>();

    for (HostSpec host : hostMap.values()) {
      if (host.getRole() == HostRole.WRITER) {
        writerCount++;
      }
      hosts.add(host);
    }

    if (writerCount == 0) {
      LOGGER.severe(
          () -> Messages.get(
              "AuroraHostListProvider.invalidTopology"));
      hosts.clear();
    }
    return new ClusterTopologyInfo(
        this.clusterId, hosts, Instant.now(), writerCount > 1, this.isPrimaryClusterId);
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
    final int port = this.clusterInstanceTemplate.isPortSpecified()
        ? this.clusterInstanceTemplate.getPort()
        : this.initialHostSpec.getPort();

    final HostSpec hostSpec =
        new HostSpec(endpoint, port, isWriter ? HostRole.WRITER : HostRole.READER);
    hostSpec.addAlias(hostName);
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
      // Add a new entry to the cache
      topologyCache.put(latestTopologyInfo.clusterId, latestTopologyInfo);
      return latestTopologyInfo;

    }

    if (clusterTopologyInfo.clusterId.equals(latestTopologyInfo.clusterId)) {
      // Updating the same item in the cache
      try {
        topologyCache.getLock().lock();

        clusterTopologyInfo.hosts = latestTopologyInfo.hosts;
        clusterTopologyInfo.lastUpdated = Instant.now();

        // It's not necessary, but it forces the cache to check for entries to evict
        topologyCache.put(clusterTopologyInfo.clusterId, clusterTopologyInfo);

      } finally {
        topologyCache.getLock().unlock();
      }
      return clusterTopologyInfo;

    }

    // Update existing entry in the cache
    // This instance of AuroraHostListProvider accepts suggested clusterId
    // and effectively needs to update the primary entry

    ClusterTopologyInfo primaryClusterTopologyInfo = topologyCache.get(this.clusterId);

    if (primaryClusterTopologyInfo != null) {
      try {
        topologyCache.getLock().lock();

        primaryClusterTopologyInfo.hosts = latestTopologyInfo.hosts;
        primaryClusterTopologyInfo.lastUpdated = Instant.now();

        // It's not necessary, but it forces the cache to check for entries to evict
        topologyCache.put(primaryClusterTopologyInfo.clusterId, primaryClusterTopologyInfo);

      } finally {
        topologyCache.getLock().unlock();
      }
      return primaryClusterTopologyInfo;
    }

    // That's suspicious path. Primary entry doesn't exist.
    // Let's create it.
    topologyCache.put(latestTopologyInfo.clusterId, latestTopologyInfo);
    return latestTopologyInfo;
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
   * Check if cached topology belongs to multi-writer cluster.
   *
   * @return True, if it's multi-writer cluster.
   */
  public boolean isMultiWriterCluster() {
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      return (clusterTopologyInfo != null
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
      return instanceNameToHost(instanceName, clusterTopologyInfo == null ? null : clusterTopologyInfo.hosts);
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

  /**
   * Clear topology cache for all clusters.
   */
  public void clearAll() {
    synchronized (cacheLock) {
      topologyCache.clear();
    }
  }

  /**
   * Clear topology cache for the current cluster.
   */
  public void clear() {
    synchronized (cacheLock) {
      topologyCache.remove(this.clusterId);
    }
  }

  @Override
  public List<HostSpec> refresh() throws SQLException {
    return this.refresh(null);
  }

  @Override
  public List<HostSpec> refresh(Connection connection) throws SQLException {
    init();
    Connection currentConnection = connection != null
        ? connection
        : this.hostListProviderService.getCurrentConnection();

    final FetchTopologyResult results = getTopology(currentConnection, false);
    LOGGER.finest(() -> Utils.logTopology(results.hosts));

    if (results.isCachedData && this.lastReturnedHostList == results.hosts) {
      return null; // no topology update
    }

    this.hostList = results.hosts;
    this.lastReturnedHostList = this.hostList;
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    return this.forceRefresh(null);
  }

  @Override
  public List<HostSpec> forceRefresh(Connection connection) throws SQLException {
    init();
    Connection currentConnection = connection != null
        ? connection
        : this.hostListProviderService.getCurrentConnection();

    final FetchTopologyResult results = getTopology(currentConnection, true);
    LOGGER.finest(() -> Utils.logTopology(results.hosts));
    this.hostList = results.hosts;
    this.lastReturnedHostList = this.hostList;
    return Collections.unmodifiableList(this.hostList);
  }

  public RdsUrlType getRdsUrlType() throws SQLException {
    init();
    return this.rdsUrlType;
  }

  private void validateHostPatternSetting(String hostPattern) {
    if (!this.rdsHelper.isDnsPatternValid(hostPattern)) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the host
      // pattern must contain a '?'
      // character as a placeholder for the DB instance identifiers of the instances in the cluster"
      final String message = Messages.get("AuroraHostListProvider.invalidPattern");
      LOGGER.severe(message);
      throw new RuntimeException(message);
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

  public static void logCache() {
    LOGGER.finest(() -> {
      StringBuilder sb = new StringBuilder();
      final Set<Entry<String, ClusterTopologyInfo>> cacheEntries = topologyCache.entrySet();

      if (cacheEntries.isEmpty()) {
        sb.append("Cache is empty.");
        return sb.toString();
      }

      for (Entry<String, ClusterTopologyInfo> entry : cacheEntries) {
        if (sb.length() > 0) {
          sb.append("\n");
        }
        sb.append("[").append(entry.getKey()).append("]:\n")
            .append("\tlastUpdated: ")
            .append(entry.getValue().lastUpdated).append("\n")
            .append("\tisMultiWriterCluster: ")
            .append(entry.getValue().isMultiWriterCluster).append("\n")
            .append("\tisPrimaryCluster: ")
            .append(entry.getValue().isPrimaryCluster).append("\n")
            .append("\tsuggestedPrimaryCluster: ")
            .append(entry.getValue().suggestedPrimaryClusterId).append("\n")
            .append("\tHosts: ");

        if (entry.getValue().hosts == null) {
          sb.append("<null>");
        } else {
          for (HostSpec h : entry.getValue().hosts) {
            sb.append("\n\t").append(h);
          }
        }
      }
      return sb.toString();
    });
  }

  static class FetchTopologyResult {

    public List<HostSpec> hosts;
    public boolean isCachedData;

    public FetchTopologyResult(boolean isCachedData, List<HostSpec> hosts) {
      this.isCachedData = isCachedData;
      this.hosts = hosts;
    }
  }

  /**
   * Class that holds the topology and additional information about the topology.
   */
  static class ClusterTopologyInfo {

    public String clusterId;
    public List<HostSpec> hosts;
    public Instant lastUpdated;
    public boolean isMultiWriterCluster;
    public boolean isPrimaryCluster;
    public String suggestedPrimaryClusterId;

    /**
     * Constructor for ClusterTopologyInfo.
     *
     * @param clusterId Associated ClusterId
     * @param hosts List of available instance hosts
     * @param lastUpdated Last updated topology time
     * @param isMultiWriterCluster true if this cluster has more than one writer in the topology
     * @param isPrimaryCluster true if ClusterId is a cluster endpoint url
     */
    ClusterTopologyInfo(
        final String clusterId,
        final List<HostSpec> hosts,
        final Instant lastUpdated,
        final boolean isMultiWriterCluster,
        final boolean isPrimaryCluster) {
      this.clusterId = clusterId;
      this.hosts = hosts;
      this.lastUpdated = lastUpdated;
      this.isMultiWriterCluster = isMultiWriterCluster;
      this.isPrimaryCluster = isPrimaryCluster;
    }
  }

  static class ClusterSuggestedResult {
    public String clusterId;
    public boolean isPrimaryClusterId;

    public ClusterSuggestedResult(final String clusterId, final boolean isPrimaryClusterId) {
      this.clusterId = clusterId;
      this.isPrimaryClusterId = isPrimaryClusterId;
    }
  }
}
