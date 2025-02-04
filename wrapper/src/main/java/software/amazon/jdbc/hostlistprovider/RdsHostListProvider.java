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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.SynchronousExecutor;
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
      "clusterId", null,
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

  protected static final Executor networkTimeoutExecutor = new SynchronousExecutor();
  protected static final RdsUtils rdsHelper = new RdsUtils();
  protected static final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  protected static final int defaultTopologyQueryTimeoutMs = 5000;
  protected static final CacheMap<String, List<HostSpec>> topologyCache = new CacheMap<>();
  protected static final CacheMap<String, String> clusterIdByHostAndPort = new CacheMap<>();

  protected final HostListProviderService hostListProviderService;
  protected final String originalUrl;
  protected final String topologyQuery;
  protected final String nodeIdQuery;
  protected final String isReaderQuery;
  protected RdsUrlType rdsUrlType;
  protected long refreshRateNano = CLUSTER_TOPOLOGY_REFRESH_RATE_MS.defaultValue != null
      ? TimeUnit.MILLISECONDS.toNanos(Long.parseLong(CLUSTER_TOPOLOGY_REFRESH_RATE_MS.defaultValue))
      : TimeUnit.MILLISECONDS.toNanos(30000);
  protected List<HostSpec> hostList = new ArrayList<>();
  protected List<HostSpec> initialHostList = new ArrayList<>();
  protected HostSpec initialHostSpec;

  protected final ReentrantLock lock = new ReentrantLock();
  protected String clusterId;
  protected HostSpec clusterInstanceTemplate;

  protected volatile boolean isInitialized = false;

  protected Properties properties;

  static {
    PropertyDefinition.registerPluginProperties(RdsHostListProvider.class);
  }

  public RdsHostListProvider(
      final Properties properties,
      final String originalUrl,
      final HostListProviderService hostListProviderService,
      final String topologyQuery,
      final String nodeIdQuery,
      final String isReaderQuery) {
    this.hostListProviderService = hostListProviderService;
    this.properties = properties;
    this.originalUrl = originalUrl;
    this.topologyQuery = topologyQuery;
    this.nodeIdQuery = nodeIdQuery;
    this.isReaderQuery = isReaderQuery;
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

    this.clusterId = UUID.randomUUID().toString();
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
    } else if (this.rdsUrlType != RdsUrlType.IP_ADDRESS) {
      final String suggestedClusterId = this.getSuggestedClusterId(this.initialHostSpec.getHostAndPort());
      if (!StringUtils.isNullOrEmpty(suggestedClusterId)) {
        this.clusterId = suggestedClusterId;
      }
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
  public FetchTopologyResult getTopology(final Connection conn, final boolean forceUpdate) throws SQLException {
    init();

    final List<HostSpec> cachedHosts = topologyCache.get(this.clusterId);
    final boolean needToSuggest = cachedHosts == null;

    if (cachedHosts == null || forceUpdate) {

      // need to re-fetch topology

      if (conn == null) {
        // can't fetch the latest topology since no connection
        // return original hosts parsed from connection string
        return new FetchTopologyResult(false, this.initialHostList);
      }

      // fetch topology from the DB
      final List<HostSpec> hosts = queryForTopology(conn);

      if (!Utils.isNullOrEmpty(hosts)) {

        if (needToSuggest) {
          // check if host list matches any existing topologies
          String matchClusterId = this.getMatchedClusterId(hosts);

          // Change clusterId by accepting a suggested one
          if (!StringUtils.isNullOrEmpty(matchClusterId)) {

            final String oldClusterId = this.clusterId;
            this.clusterId = matchClusterId;
            this.clusterIdChanged(oldClusterId);
          }
          clusterIdByHostAndPort.put(this.initialHostSpec.getHostAndPort(), this.clusterId, this.refreshRateNano);
        }

        for (HostSpec hostSpec : hosts) {
          clusterIdByHostAndPort.put(hostSpec.getHostAndPort(), this.clusterId, this.refreshRateNano);
        }

        topologyCache.put(this.clusterId, hosts, this.refreshRateNano);

        return new FetchTopologyResult(false, hosts);
      }
    }

    if (cachedHosts == null) {
      return new FetchTopologyResult(false, this.initialHostList);
    } else {
      // use cached data
      return new FetchTopologyResult(true, cachedHosts);
    }
  }

  protected String getMatchedClusterId(final @NonNull List<HostSpec> hosts) {
    if (Utils.isNullOrEmpty(hosts)) {
      return null;
    }

    for (HostSpec hostSpec : hosts) {
      // TODO: use template pattern port?
      String existingClusterId = clusterIdByHostAndPort.get(hostSpec.getHostAndPort());
      if (!StringUtils.isNullOrEmpty(existingClusterId)) {
        return existingClusterId;
      }

      for (final Entry<String, List<HostSpec>> entry : topologyCache.getEntries().entrySet()) {
        for (final HostSpec existingHostSpec : entry.getValue()) {
          if (hostSpec.getHostAndPort().equals(existingHostSpec.getHostAndPort())) {
            return entry.getKey();
          }
        }
      }
    }

    return null;
  }

  protected void clusterIdChanged(final String oldClusterId) {
    // do nothing
  }

  protected String getSuggestedClusterId(final String hostAndPort) {
    return clusterIdByHostAndPort.get(hostAndPort);
  }

  /**
   * Obtain a cluster topology from database.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return a list of {@link HostSpec} objects representing the topology
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    int networkTimeout = -1;
    try {
      networkTimeout = conn.getNetworkTimeout();
      // The topology query is not monitored by the EFM plugin, so it needs a socket timeout
      if (networkTimeout == 0) {
        conn.setNetworkTimeout(networkTimeoutExecutor, defaultTopologyQueryTimeoutMs);
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("RdsHostListProvider.errorGettingNetworkTimeout",
          new Object[] {e.getMessage()}));
    }

    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(this.topologyQuery)) {
      return processQueryResults(resultSet);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  /**
   * Form a list of hosts from the results of the topology query.
   *
   * @param resultSet The results of the topology query
   * @return a list of {@link HostSpec} objects representing
   *     the topology that was returned by the
   *     topology query. The list will be empty if the topology query returned an invalid topology
   *     (no writer instance).
   */
  private List<HostSpec> processQueryResults(final ResultSet resultSet) throws SQLException {

    final HashMap<String, HostSpec> hostMap = new HashMap<>();

    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    while (resultSet.next()) {
      final HostSpec host = createHost(resultSet);
      hostMap.put(host.getHost(), host);
    }

    final List<HostSpec> hosts = new ArrayList<>();
    final List<HostSpec> writers = new ArrayList<>();

    for (final HostSpec host : hostMap.values()) {
      if (host.getRole() != HostRole.WRITER) {
        hosts.add(host);
      } else {
        writers.add(host);
      }
    }

    int writerCount = writers.size();

    if (writerCount == 0) {
      LOGGER.severe(
          () -> Messages.get(
              "RdsHostListProvider.invalidTopology"));
      hosts.clear();
    } else if (writerCount == 1) {
      hosts.add(writers.get(0));
    } else {
      // Take the latest updated writer node as the current writer. All others will be ignored.
      List<HostSpec> sortedWriters = writers.stream()
          .sorted(Comparator.comparing(HostSpec::getLastUpdateTime, Comparator.nullsLast(Comparator.reverseOrder())))
          .collect(Collectors.toList());
      hosts.add(sortedWriters.get(0));
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
  protected HostSpec createHost(final ResultSet resultSet) throws SQLException {
    // According to the topology query the result set
    // should contain 4 columns: node ID, 1/0 (writer/reader), CPU utilization, node lag in time.
    String hostName = resultSet.getString(1);
    final boolean isWriter = resultSet.getBoolean(2);
    final float cpuUtilization = resultSet.getFloat(3);
    final float nodeLag = resultSet.getFloat(4);
    Timestamp lastUpdateTime;
    try {
      lastUpdateTime = resultSet.getTimestamp(5);
    } catch (Exception e) {
      lastUpdateTime = Timestamp.from(Instant.now());
    }

    // Calculate weight based on node lag in time and CPU utilization.
    final long weight = Math.round(nodeLag) * 100L + Math.round(cpuUtilization);

    return createHost(hostName, isWriter, weight, lastUpdateTime, this.clusterInstanceTemplate);
  }

  protected HostSpec createHost(
      String host,
      final boolean isWriter,
      final long weight,
      final Timestamp lastUpdateTime,
      final HostSpec clusterInstanceTemplate) {

    host = host == null ? "?" : host;
    final String endpoint = getHostEndpoint(host, clusterInstanceTemplate);
    final int port = clusterInstanceTemplate.isPortSpecified()
        ? clusterInstanceTemplate.getPort()
        : this.initialHostSpec.getPort();

    final HostSpec hostSpec = this.hostListProviderService.getHostSpecBuilder()
        .host(endpoint)
        .port(port)
        .role(isWriter ? HostRole.WRITER : HostRole.READER)
        .availability(HostAvailability.AVAILABLE)
        .weight(weight)
        .lastUpdateTime(lastUpdateTime)
        .build();
    hostSpec.addAlias(host);
    hostSpec.setHostId(host);
    return hostSpec;
  }

  /**
   * Build a host dns endpoint based on host/node name.
   *
   * @param nodeName A host name.
   * @return Host dns endpoint
   */
  protected String getHostEndpoint(final String nodeName, final HostSpec clusterInstanceTemplate) {
    final String host = clusterInstanceTemplate.getHost();
    return host.replace("?", nodeName);
  }

  /**
   * Get cached topology.
   *
   * @return list of hosts that represents topology. If there's no topology in the cache or the
   *     cached topology is outdated, it returns null.
   */
  public @Nullable List<HostSpec> getCachedTopology() {
    return topologyCache.get(this.clusterId);
  }

  /**
   * Clear topology cache for all clusters.
   */
  public static void clearAll() {
    topologyCache.clear();
    clusterIdByHostAndPort.clear();
  }

  /**
   * Clear topology cache for the current cluster.
   */
  public void clear() {
    topologyCache.remove(this.clusterId);
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

  public static void logCache() {
    LOGGER.finest(() -> {
      final StringBuilder sb = new StringBuilder();
      final Set<Entry<String, List<HostSpec>>> cacheEntries = topologyCache.getEntries().entrySet();

      if (cacheEntries.isEmpty()) {
        sb.append("Cache is empty.");
        return sb.toString();
      }

      for (final Entry<String, List<HostSpec>> entry : cacheEntries) {
        final List<HostSpec> hosts = entry.getValue();

        if (sb.length() > 0) {
          sb.append("\n");
        }
        sb.append("[").append(entry.getKey()).append("]:\n")
            .append("\tHosts: ");

        if (hosts == null) {
          sb.append("<null>");
        } else {
          for (final HostSpec h : hosts) {
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

    public FetchTopologyResult(final boolean isCachedData, final List<HostSpec> hosts) {
      this.isCachedData = isCachedData;
      this.hosts = hosts;
    }
  }

  @Override
  public HostRole getHostRole(Connection conn) throws SQLException {
    try (final Statement stmt = conn.createStatement();
         final ResultSet rs = stmt.executeQuery(this.isReaderQuery)) {
      if (rs.next()) {
        boolean isReader = rs.getBoolean(1);
        return isReader ? HostRole.READER : HostRole.WRITER;
      }
    } catch (SQLException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.errorGettingHostRole"), e);
    }

    throw new SQLException(Messages.get("RdsHostListProvider.errorGettingHostRole"));
  }

  @Override
  public HostSpec identifyConnection(Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement();
         final ResultSet resultSet = stmt.executeQuery(this.nodeIdQuery)) {
      if (resultSet.next()) {
        final String instanceName = resultSet.getString(1);

        List<HostSpec> topology = this.refresh(connection);

        boolean isForcedRefresh = false;
        if (topology == null) {
          topology = this.forceRefresh(connection);
          isForcedRefresh = true;
        }

        if (topology == null) {
          return null;
        }

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
      }
    } catch (final SQLException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.errorIdentifyConnection"), e);
    }

    throw new SQLException(Messages.get("RdsHostListProvider.errorIdentifyConnection"));
  }

  @Override
  public String getClusterId() throws UnsupportedOperationException, SQLException {
    init();
    return this.clusterId;
  }
}
