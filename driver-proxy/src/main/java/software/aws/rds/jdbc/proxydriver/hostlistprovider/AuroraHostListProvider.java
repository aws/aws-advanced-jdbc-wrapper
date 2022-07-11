/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.hostlistprovider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.HostListProvider;
import software.aws.rds.jdbc.proxydriver.HostRole;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.PluginService;
import software.aws.rds.jdbc.proxydriver.util.ExpiringCache;

public class AuroraHostListProvider implements HostListProvider, DynamicHostListProvider {

  static final Pattern AURORA_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
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
  static final int DEFAULT_REFRESH_RATE_IN_MILLISECONDS = 30000;
  static final int DEFAULT_CACHE_EXPIRE_MS = 5 * 60 * 1000; // 5 min
  static final int WRITER_CONNECTION_INDEX = 0;

  static final String WRITER_SESSION_ID = "MASTER_SESSION_ID";
  static final String FIELD_SERVER_ID = "SERVER_ID";
  static final String FIELD_SESSION_ID = "SESSION_ID";
  private final PluginService pluginService;
  private final String originalUrl;

  private int refreshRateInMilliseconds;
  private List<HostSpec> hostList = new ArrayList<>();

  static final ExpiringCache<String, ClusterTopologyInfo> topologyCache =
      new ExpiringCache<>(DEFAULT_CACHE_EXPIRE_MS);
  private static final Object cacheLock = new Object();

  private static final String PG_DRIVER_PROTOCOL = "postgresql";
  private final String retrieveTopologyQuery;
  protected String clusterId;
  protected HostSpec clusterInstanceTemplate;

  private static final Logger LOGGER = Logger.getLogger(AuroraHostListProvider.class.getName());

  Properties properties;

  public AuroraHostListProvider(
      final String driverProtocol,
      final PluginService pluginService,
      final Properties properties,
      final String originalUrl) {
    this(driverProtocol, pluginService, properties, originalUrl, DEFAULT_REFRESH_RATE_IN_MILLISECONDS);
  }

  public AuroraHostListProvider(
      final String driverProtocol,
      final PluginService pluginService,
      final Properties properties,
      final String originalUrl,
      final int refreshRateInMilliseconds) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.originalUrl = originalUrl;
    this.clusterId = UUID.randomUUID().toString();
    this.clusterInstanceTemplate = new HostSpec(getRdsInstanceHostPattern(originalUrl));
    this.refreshRateInMilliseconds = refreshRateInMilliseconds;
    retrieveTopologyQuery =
        driverProtocol.contains(PG_DRIVER_PROTOCOL)
            ? PG_RETRIEVE_TOPOLOGY_SQL
            : MYSQL_RETRIEVE_TOPOLOGY_SQL;
  }

  public void setClusterId(final String clusterId) {
    LOGGER.log(Level.FINER, "[AuroraHostListProvider] clusterId=''{0}''", clusterId);
    this.clusterId = clusterId;
  }

  /**
   * Sets host details common to each host in the cluster, including the host dns pattern. "?" (question mark) in a host
   * dns pattern will be replaced with a host name to form a fully qualified dns host endpoint.
   *
   * <p>Examples: "?.mydomain.com", "db-host.?.mydomain.com"
   *
   * @param clusterInstanceTemplate Cluster host details including host dns pattern.
   */
  public void setClusterInstanceTemplate(final HostSpec clusterInstanceTemplate) {
    this.clusterInstanceTemplate = clusterInstanceTemplate;
  }

  /**
   * Get cluster topology. It may require an extra call to database to fetch the latest topology. A cached copy of
   * topology is returned if it's not yet outdated (controlled by {@link #refreshRateInMilliseconds}).
   *
   * @param conn        A connection to database to fetch the latest topology, if needed.
   * @param forceUpdate If true, it forces a service to ignore cached copy of topology and to fetch a fresh one.
   * @return a list of hosts that describes cluster topology. A writer is always at position 0. Returns an empty list if
   *     isn't available or is invalid (doesn't contain a writer).
   */
  public List<HostSpec> getTopology(final Connection conn, final boolean forceUpdate) {
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
  protected ClusterTopologyInfo queryForTopology(final Connection conn) {
    List<HostSpec> hosts = new ArrayList<>();
    try (final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery(retrieveTopologyQuery)) {
      hosts = processQueryResults(resultSet);
    } catch (final SQLException e) {
      // ignore
    }

    return new ClusterTopologyInfo(hosts, null, Instant.now());
  }

  /**
   * Form a list of hosts from the results of the topology query.
   *
   * @param resultSet The results of the topology query
   * @return a list of {@link HostSpec} objects representing the topology that was returned by the topology query. The
   *     list will be empty if the topology query returned an invalid topology (no writer instance).
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
          "[AuroraTopologyService] The topology query returned an invalid topology - no writer instance detected");
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
   * @param isWriter  true if the session_ID is the writer, else it will return false
   * @return a {@link HostSpec} instance for a specific instance from the cluster
   * @throws SQLException If unable to retrieve the hostName from the result set
   */
  private HostSpec createHost(final ResultSet resultSet, final boolean isWriter)
      throws SQLException {
    String hostName = resultSet.getString(FIELD_SERVER_ID);
    hostName = hostName == null ? "NULL" : hostName;
    return new HostSpec(
        getHostEndpoint(hostName),
        this.clusterInstanceTemplate.getPort(),
        isWriter ? HostRole.WRITER : HostRole.READER);
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
   * Store the information for the topology in the cache, creating the information object if it did not previously exist
   * in the cache.
   *
   * @param clusterTopologyInfo The cluster topology info that existed in the cache before the topology query. This
   *                            parameter will be null if no topology info for the cluster has been created in the cache
   *                            yet.
   * @param latestTopologyInfo  The results of the current topology query
   * @return the {@link ClusterTopologyInfo} stored in the cache by this method, representing the most up-to-date
   *     information we have about the topology.
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
   * @return list of hosts that represents topology. If there's no topology in the cache or the cached topology is
   *     outdated, it returns null.
   */
  public @Nullable List<HostSpec> getCachedTopology() {
    final ClusterTopologyInfo info = topologyCache.get(this.clusterId);
    return info == null || refreshNeeded(info) ? null : info.hosts;
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
    Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection == null) {
      currentConnection = this.pluginService.connect(this.pluginService.getCurrentHostSpec(), this.properties);
      this.pluginService.setCurrentConnection(currentConnection, this.pluginService.getCurrentHostSpec());
    }
    final ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
    hostList =
        getTopology(
            currentConnection,
            clusterTopologyInfo != null && refreshNeeded(clusterTopologyInfo));
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection == null) {
      currentConnection = this.pluginService.connect(this.pluginService.getCurrentHostSpec(), this.properties);
      this.pluginService.setCurrentConnection(currentConnection, this.pluginService.getCurrentHostSpec());
    }
    hostList = getTopology(currentConnection, true);
    return Collections.unmodifiableList(hostList);
  }

  private String getRdsInstanceHostPattern(String host) {
    final Matcher matcher = AURORA_DNS_PATTERN.matcher(host);
    if (matcher.find()) {
      return "?." + matcher.group("domain");
    }
    return "?";
  }

  /**
   * Class that holds the topology and additional information about the topology.
   */
  static class ClusterTopologyInfo {

    public List<HostSpec> hosts;
    public @Nullable HostSpec lastUsedReader;
    public Instant lastUpdated;

    /**
     * Constructor for ClusterTopologyInfo.
     *
     * @param hosts          List of available instance hosts
     * @param lastUsedReader The previously used reader host
     * @param lastUpdated    Last updated topology time
     */
    ClusterTopologyInfo(
        final List<HostSpec> hosts,
        @Nullable final HostSpec lastUsedReader,
        final Instant lastUpdated) {
      this.hosts = hosts;
      this.lastUsedReader = lastUsedReader;
      this.lastUpdated = lastUpdated;
    }
  }
}
