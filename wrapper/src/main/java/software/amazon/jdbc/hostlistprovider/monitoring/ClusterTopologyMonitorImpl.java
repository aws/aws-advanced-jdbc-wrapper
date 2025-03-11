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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.Utils;

public class ClusterTopologyMonitorImpl implements ClusterTopologyMonitor {

  private static final Logger LOGGER = Logger.getLogger(ClusterTopologyMonitorImpl.class.getName());

  protected static final String MONITORING_PROPERTY_PREFIX = "topology-monitoring-";
  protected static final Executor networkTimeoutExecutor = new SynchronousExecutor();
  protected static final RdsUtils rdsHelper = new RdsUtils();


  protected static final int defaultTopologyQueryTimeoutMs = 1000;
  protected static final int closeConnectionNetworkTimeoutMs = 500;

  protected static final int defaultConnectionTimeoutMs = 5000;
  protected static final int defaultSocketTimeoutMs = 5000;

  // Keep monitoring topology with a high rate for 30s after failover.
  protected static final long highRefreshPeriodAfterPanicNano = TimeUnit.SECONDS.toNanos(30);
  protected static final long ignoreTopologyRequestNano = TimeUnit.SECONDS.toNanos(10);

  protected final long refreshRateNano;
  protected final long highRefreshRateNano;
  protected final long topologyCacheExpirationNano;
  protected final Properties properties;
  protected final Properties monitoringProperties;
  protected final PluginService pluginService;
  protected final HostSpec initialHostSpec;
  protected final CacheMap<String, List<HostSpec>> topologyMap;
  protected final String topologyQuery;
  protected final String nodeIdQuery;
  protected final String writerTopologyQuery;
  protected final HostListProviderService hostListProviderService;
  protected final HostSpec clusterInstanceTemplate;

  protected String clusterId;
  protected final AtomicReference<HostSpec> writerHostSpec = new AtomicReference<>(null);
  protected final AtomicReference<Connection> monitoringConnection = new AtomicReference<>(null);
  protected boolean isVerifiedWriterConnection = false;
  protected final AtomicBoolean stop = new AtomicBoolean(false);
  protected long highRefreshRateEndTimeNano = 0;
  protected final Object topologyUpdated = new Object();
  protected final AtomicBoolean requestToUpdateTopology = new AtomicBoolean(false);
  protected final AtomicLong ignoreNewTopologyRequestsEndTimeNano = new AtomicLong(-1);
  protected final ConcurrentHashMap<String /* host */, Thread> nodeThreads = new ConcurrentHashMap<>();
  protected final AtomicBoolean nodeThreadsStop = new AtomicBoolean(false);
  protected final AtomicReference<Connection> nodeThreadsWriterConnection = new AtomicReference<>(null);
  protected final AtomicReference<HostSpec> nodeThreadsWriterHostSpec = new AtomicReference<>(null);
  protected final AtomicReference<Connection> nodeThreadsReaderConnection = new AtomicReference<>(null);
  protected final AtomicReference<List<HostSpec>> nodeThreadsLatestTopology = new AtomicReference<>(null);


  protected final ExecutorService monitorExecutor = Executors.newSingleThreadExecutor(runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    if (!StringUtils.isNullOrEmpty(monitoringThread.getName())) {
      monitoringThread.setName(monitoringThread.getName() + "-m");
    }
    return monitoringThread;
  });

  public ClusterTopologyMonitorImpl(
      final String clusterId,
      final CacheMap<String, List<HostSpec>> topologyMap,
      final HostSpec initialHostSpec,
      final Properties properties,
      final PluginService pluginService,
      final HostListProviderService hostListProviderService,
      final HostSpec clusterInstanceTemplate,
      final long refreshRateNano,
      final long highRefreshRateNano,
      final long topologyCacheExpirationNano,
      final String topologyQuery,
      final String writerTopologyQuery,
      final String nodeIdQuery) {

    this.clusterId = clusterId;
    this.topologyMap = topologyMap;
    this.initialHostSpec = initialHostSpec;
    this.pluginService = pluginService;
    this.hostListProviderService = hostListProviderService;
    this.clusterInstanceTemplate = clusterInstanceTemplate;
    this.properties = properties;
    this.refreshRateNano = refreshRateNano;
    this.highRefreshRateNano = highRefreshRateNano;
    this.topologyCacheExpirationNano = topologyCacheExpirationNano;
    this.topologyQuery = topologyQuery;
    this.writerTopologyQuery = writerTopologyQuery;
    this.nodeIdQuery = nodeIdQuery;

    this.monitoringProperties = PropertyUtils.copyProperties(properties);
    this.properties.stringPropertyNames().stream()
        .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
        .forEach(
            p -> {
              this.monitoringProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  this.properties.getProperty(p));
              this.monitoringProperties.remove(p);
            });

    // Set default values if they are not provided.
    if (PropertyDefinition.SOCKET_TIMEOUT.getString(this.monitoringProperties) == null) {
      PropertyDefinition.SOCKET_TIMEOUT.set(
          this.monitoringProperties, String.valueOf(defaultSocketTimeoutMs));
    }
    if (PropertyDefinition.CONNECT_TIMEOUT.getString(this.monitoringProperties) == null) {
      PropertyDefinition.CONNECT_TIMEOUT.set(
          this.monitoringProperties, String.valueOf(defaultConnectionTimeoutMs));
    }

    this.monitorExecutor.submit(this);
    this.monitorExecutor.shutdown(); // No more tasks are accepted by the pool.
  }

  @Override
  public boolean canDispose() {
    return true;
  }

  @Override
  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException {

    if (this.ignoreNewTopologyRequestsEndTimeNano.get() > 0
        && System.nanoTime() < this.ignoreNewTopologyRequestsEndTimeNano.get()) {

      // Previous failover has just completed. We can use results of it without triggering a new topology update.
      List<HostSpec> currentHosts = this.topologyMap.get(this.clusterId);
      LOGGER.finest(
          Utils.logTopology(currentHosts, Messages.get("ClusterTopologyMonitorImpl.ignoringTopologyRequest")));
      if (currentHosts != null) {
        return currentHosts;
      }
    }

    if (shouldVerifyWriter) {
      final Connection monitoringConnection = this.monitoringConnection.get();
      this.monitoringConnection.set(null);
      this.isVerifiedWriterConnection = false;
      this.closeConnection(monitoringConnection, true);
    }

    return this.waitTillTopologyGetsUpdated(timeoutMs);
  }

  @Override
  public List<HostSpec> forceRefresh(@Nullable Connection connection, final long timeoutMs)
      throws SQLException, TimeoutException {

    if (this.isVerifiedWriterConnection) {
      // Push monitoring thread to refresh topology with a verified connection
      return this.waitTillTopologyGetsUpdated(timeoutMs);
    }

    // Otherwise use provided unverified connection to update topology
    return this.fetchTopologyAndUpdateCache(connection);
  }

  protected List<HostSpec> waitTillTopologyGetsUpdated(final long timeoutMs) throws TimeoutException {

    List<HostSpec> currentHosts = this.topologyMap.get(this.clusterId);
    List<HostSpec> latestHosts;

    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);

      // Notify monitoring thread (that might be sleeping) that topology should be refreshed immediately.
      this.requestToUpdateTopology.notifyAll();
    }

    if (timeoutMs == 0) {
      LOGGER.finest(Utils.logTopology(currentHosts, Messages.get("ClusterTopologyMonitorImpl.timeoutSetToZero")));
      return currentHosts;
    }

    final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);

    // Note that we are checking reference equality instead of value equality here. We will break out of the loop if
    // there is a new entry in the topology map, even if the value of the hosts in latestHosts is the same as
    // currentHosts.
    while (currentHosts == (latestHosts = this.topologyMap.get(this.clusterId))
        && System.nanoTime() < end) {
      try {
        synchronized (this.topologyUpdated) {
          this.topologyUpdated.wait(1000);
        }
      } catch (InterruptedException ex) {
        LOGGER.fine(Messages.get("ClusterTopologyMonitorImpl.interrupted"));
        Thread.currentThread().interrupt();
        return null;
      }
    }

    if (System.nanoTime() >= end) {
      throw new TimeoutException(Messages.get(
          "ClusterTopologyMonitorImpl.topologyNotUpdated",
          new Object[]{timeoutMs}));
    }

    return latestHosts;
  }

  @Override
  public void close() throws Exception {
    this.stop.set(true);
    this.nodeThreadsStop.set(true);

    // It breaks a waiting/sleeping cycles in monitoring thread
    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);
      this.requestToUpdateTopology.notifyAll();
    }

    // Waiting for 30s gives a thread enough time to exit monitoring loop and close database connection.
    if (!this.monitorExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
      this.monitorExecutor.shutdownNow();
    }

    this.nodeThreads.clear();
  }

  @Override
  public void run() {
    try {
      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.startMonitoringThread",
          new Object[]{this.initialHostSpec.getHost()}));

      while (!this.stop.get()) {

        if (this.isInPanicMode()) {

          if (this.nodeThreads.isEmpty()) {
            LOGGER.finest(Messages.get("ClusterTopologyMonitorImpl.startingNodeMonitoringThreads"));

            // start node threads
            this.nodeThreadsStop.set(false);
            this.nodeThreadsWriterConnection.set(null);
            this.nodeThreadsReaderConnection.set(null);
            this.nodeThreadsWriterHostSpec.set(null);
            this.nodeThreadsLatestTopology.set(null);

            List<HostSpec> hosts = this.topologyMap.get(this.clusterId);
            if (hosts == null) {
              // need any connection to get topology
              hosts = this.openAnyConnectionAndUpdateTopology();
            }

            if (hosts != null && !this.isVerifiedWriterConnection) {
              for (HostSpec hostSpec : hosts) {
                this.nodeThreads.computeIfAbsent(hostSpec.getHost(),
                    (key) -> {
                      final Thread thread = this.getNodeMonitoringThread(hostSpec, this.writerHostSpec.get());
                      thread.start();
                      return thread;
                    });
              }
            }
            // otherwise let's try it again the next round

          } else {
            // node threads are running
            // check if writer is already detected
            final Connection writerConnection = this.nodeThreadsWriterConnection.get();
            final HostSpec writerConnectionHostSpec = this.nodeThreadsWriterHostSpec.get();
            if (writerConnection != null && writerConnectionHostSpec != null) {
              LOGGER.finest(
                  Messages.get(
                      "ClusterTopologyMonitorImpl.writerPickedUpFromNodeMonitors",
                      new Object[]{writerConnectionHostSpec}));

              this.closeConnection(this.monitoringConnection.get());
              this.monitoringConnection.set(writerConnection);
              this.writerHostSpec.set(writerConnectionHostSpec);
              this.isVerifiedWriterConnection = true;
              this.highRefreshRateEndTimeNano = System.nanoTime() + highRefreshPeriodAfterPanicNano;

              // We verify the writer on initial connection and on failover, but we only want to ignore new topology
              // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
              // time to 0. Any future writer verifications will set it to a positive value.
              if (!this.ignoreNewTopologyRequestsEndTimeNano.compareAndSet(-1, 0)) {
                this.ignoreNewTopologyRequestsEndTimeNano.set(System.nanoTime() + ignoreTopologyRequestNano);
              }

              this.nodeThreadsStop.set(true);
              for (Thread thread : this.nodeThreads.values()) {
                thread.interrupt();
              }
              this.nodeThreads.clear();

              continue;

            } else {
              // update node threads with new nodes in the topology
              List<HostSpec> hosts = this.nodeThreadsLatestTopology.get();
              if (hosts != null && !this.nodeThreadsStop.get()) {
                for (HostSpec hostSpec : hosts) {
                  this.nodeThreads.computeIfAbsent(hostSpec.getHost(),
                      (key) -> {
                        final Thread thread = this.getNodeMonitoringThread(hostSpec, this.writerHostSpec.get());
                        thread.start();
                        return thread;
                      });
                }
              }
            }
          }

          this.delay(true);

        } else {
          // regular mode (not panic mode)

          if (!this.nodeThreads.isEmpty()) {
            // stop node threads
            for (Thread thread : this.nodeThreads.values()) {
              thread.interrupt();
            }
            this.nodeThreads.clear();
          }

          final List<HostSpec> hosts = this.fetchTopologyAndUpdateCache(this.monitoringConnection.get());
          if (hosts == null) {
            // can't get topology
            // let's switch to panic mode
            Connection conn = this.monitoringConnection.get();
            this.monitoringConnection.set(null);
            this.isVerifiedWriterConnection = false;
            this.closeConnection(conn);
            continue;
          }

          if (this.highRefreshRateEndTimeNano > 0 && System.nanoTime() > this.highRefreshRateEndTimeNano) {
            this.highRefreshRateEndTimeNano = 0;
          }

          // Do not log topology while in high refresh rate. It's noisy!
          if (this.highRefreshRateEndTimeNano == 0) {
            LOGGER.finest(Utils.logTopology(this.topologyMap.get(this.clusterId)));
          }

          this.delay(false);
        }

        if (this.ignoreNewTopologyRequestsEndTimeNano.get() > 0
            && System.nanoTime() > this.ignoreNewTopologyRequestsEndTimeNano.get()) {
          this.ignoreNewTopologyRequestsEndTimeNano.set(0);
        }
      }

    } catch (final InterruptedException intEx) {
      Thread.currentThread().interrupt();

    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (LOGGER.isLoggable(Level.FINEST)) {
        // We want to print full trace stack of the exception.
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "ClusterTopologyMonitorImpl.exceptionDuringMonitoringStop",
                new Object[]{this.initialHostSpec.getHost()}),
            ex);
      }

    } finally {
      this.stop.set(true);

      final Connection conn = this.monitoringConnection.get();
      this.monitoringConnection.set(null);
      this.closeConnection(conn);

      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.stopMonitoringThread",
          new Object[]{this.initialHostSpec.getHost()}));
    }
  }

  protected boolean isInPanicMode() {
    return this.monitoringConnection.get() == null
        || !this.isVerifiedWriterConnection;
  }

  protected Thread getNodeMonitoringThread(final HostSpec hostSpec, final @Nullable HostSpec writerHostSpec) {
    return new NodeMonitoringThread(this, hostSpec, writerHostSpec);
  }

  protected List<HostSpec> openAnyConnectionAndUpdateTopology() {
    boolean writerVerifiedByThisThread = false;
    if (this.monitoringConnection.get() == null) {

      Connection conn;

      // open a new connection
      try {
        conn = this.pluginService.forceConnect(this.initialHostSpec, this.monitoringProperties);
      } catch (SQLException ex) {
        // can't connect
        return null;
      }

      if (this.monitoringConnection.compareAndSet(null, conn)) {
        LOGGER.finest(() -> Messages.get(
            "ClusterTopologyMonitorImpl.openedMonitoringConnection",
            new Object[]{this.initialHostSpec.getHost()}));

        try {
          if (!StringUtils.isNullOrEmpty(this.getWriterNodeId(this.monitoringConnection.get()))) {
            this.isVerifiedWriterConnection = true;
            writerVerifiedByThisThread = true;

            if (rdsHelper.isRdsInstance(this.initialHostSpec.getHost())) {
              this.writerHostSpec.set(this.initialHostSpec);
              LOGGER.finest(
                  Messages.get(
                      "ClusterTopologyMonitorImpl.writerMonitoringConnection",
                      new Object[]{this.writerHostSpec.get().getHost()}));
            } else {
              final String nodeId = this.getNodeId(this.monitoringConnection.get());
              if (!StringUtils.isNullOrEmpty(nodeId)) {
                this.writerHostSpec.set(this.createHost(nodeId, true, 0, null));
                LOGGER.finest(
                    Messages.get(
                        "ClusterTopologyMonitorImpl.writerMonitoringConnection",
                        new Object[]{this.writerHostSpec.get().getHost()}));
              }
            }
          }
        } catch (SQLException ex) {
          // do nothing
        }

      } else {
        // monitoring connection has already been set by other thread
        // close new connection as we don't need it
        this.closeConnection(conn);
      }
    }

    final List<HostSpec> hosts = this.fetchTopologyAndUpdateCache(this.monitoringConnection.get());
    if (writerVerifiedByThisThread) {
      // We verify the writer on initial connection and on failover, but we only want to ignore new topology
      // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
      // time to 0. Any future writer verifications will set it to a positive value.
      if (!this.ignoreNewTopologyRequestsEndTimeNano.compareAndSet(-1, 0)) {
        this.ignoreNewTopologyRequestsEndTimeNano.set(System.nanoTime() + ignoreTopologyRequestNano);
      }
    }

    if (hosts == null) {
      // can't get topology; it might be something's wrong with a connection
      // close connection
      Connection connToClose = this.monitoringConnection.get();
      this.monitoringConnection.set(null);
      this.closeConnection(connToClose);
      this.isVerifiedWriterConnection = false;
    }

    return hosts;
  }

  protected String getNodeId(final Connection connection) {
    try {
      try (final Statement stmt = connection.createStatement();
          final ResultSet resultSet = stmt.executeQuery(this.nodeIdQuery)) {
        if (resultSet.next()) {
          return resultSet.getString(1);
        }
      }
    } catch (SQLException ex) {
      // do nothing
    }
    return null;
  }

  protected void closeConnection(final @Nullable Connection connection) {
    this.closeConnection(connection, false);
  }

  protected void closeConnection(final @Nullable Connection connection, final boolean unstableConnection) {
    try {
      if (connection != null && !connection.isClosed()) {
        if (unstableConnection) {
          try {
            connection.setNetworkTimeout(networkTimeoutExecutor, closeConnectionNetworkTimeoutMs);
          } catch (SQLException ex) {
            // do nothing
          }
        }
        connection.close();
      }
    } catch (final SQLException ex) {
      // ignore
    }
  }

  // Sleep that can be easily interrupted
  protected void delay(boolean useHighRefreshRate) throws InterruptedException {
    if (this.highRefreshRateEndTimeNano > 0 && System.nanoTime() < this.highRefreshRateEndTimeNano) {
      useHighRefreshRate = true;
    }

    if (this.requestToUpdateTopology.get()) {
      useHighRefreshRate = true;
    }

    long start = System.nanoTime();
    long end = start + (useHighRefreshRate ? this.highRefreshRateNano : this.refreshRateNano);
    do {
      synchronized (this.requestToUpdateTopology) {
        this.requestToUpdateTopology.wait(50);
      }
    } while (!this.requestToUpdateTopology.get() && System.nanoTime() < end && !this.stop.get());
  }

  protected @Nullable List<HostSpec> fetchTopologyAndUpdateCache(final Connection connection) {
    if (connection == null) {
      return null;
    }
    try {
      final List<HostSpec> hosts = this.queryForTopology(connection);
      if (!Utils.isNullOrEmpty(hosts)) {
        this.updateTopologyCache(hosts);
      }
      return hosts;
    } catch (SQLException ex) {
      // do nothing
      LOGGER.finest(Messages.get("ClusterTopologyMonitorImpl.errorFetchingTopology", new Object[]{ex}));
    }
    return null;
  }

  protected void updateTopologyCache(final @NonNull List<HostSpec> hosts) {
    synchronized (this.requestToUpdateTopology) {
      this.topologyMap.put(this.clusterId, hosts, this.topologyCacheExpirationNano);
      synchronized (this.topologyUpdated) {
        this.requestToUpdateTopology.set(false);

        // Notify all threads that are waiting for a topology update.
        this.topologyUpdated.notifyAll();
      }
    }
  }

  // Returns a writer node ID if connected to a writer node. Returns null otherwise.
  protected String getWriterNodeId(final Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.writerTopologyQuery)) {
        if (resultSet.next()) {
          return resultSet.getString(1);
        }
      }
    }
    return null;
  }

  protected @Nullable List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    int networkTimeout = -1;
    try {
      networkTimeout = conn.getNetworkTimeout();
      // The topology query is not monitored by the EFM plugin, so it needs a socket timeout
      if (networkTimeout == 0) {
        conn.setNetworkTimeout(networkTimeoutExecutor, defaultTopologyQueryTimeoutMs);
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("ClusterTopologyMonitorImpl.errorGettingNetworkTimeout",
          new Object[] {e.getMessage()}));
    }

    final String suggestedWriterNodeId = this.getSuggestedWriterNodeId(conn);
    try (final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery(this.topologyQuery)) {
      return this.processQueryResults(resultSet, suggestedWriterNodeId);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("ClusterTopologyMonitorImpl.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  protected String getSuggestedWriterNodeId(final Connection connection) throws SQLException {
    // Aurora topology query can detect a writer for itself so it doesn't need any suggested writer node ID.
    return null; // intentionally null
  }

  protected @Nullable List<HostSpec> processQueryResults(
      final ResultSet resultSet,
      final String suggestedWriterNodeId) throws SQLException {

    final HashMap<String, HostSpec> hostMap = new HashMap<>();

    if (resultSet.getMetaData().getColumnCount() == 0) {
      // We expect at least 4 columns. Note that the server may return 0 columns if failover has occurred.
      LOGGER.finest(Messages.get("ClusterTopologyMonitorImpl.unexpectedTopologyQueryColumnCount"));
      return null;
    }

    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    while (resultSet.next()) {
      try {
        final HostSpec host = createHost(resultSet, suggestedWriterNodeId);
        hostMap.put(host.getHost(), host);
      } catch (Exception e) {
        LOGGER.finest(
            Messages.get("ClusterTopologyMonitorImpl.errorProcessingQueryResults", new Object[]{e.getMessage()}));
        return null;
      }
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
      LOGGER.warning(() -> Messages.get("ClusterTopologyMonitorImpl.invalidTopology"));
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

  protected HostSpec createHost(
      final ResultSet resultSet,
      final String suggestedWriterNodeId) throws SQLException {

    // suggestedWriterNodeId is not used for Aurora clusters. Topology query can detect a writer for itself.

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

    return createHost(hostName, isWriter, weight, lastUpdateTime);
  }

  protected HostSpec createHost(
      String nodeName,
      final boolean isWriter,
      final long weight,
      final Timestamp lastUpdateTime) {

    nodeName = nodeName == null ? "?" : nodeName;
    final String endpoint = getHostEndpoint(nodeName);
    final int port = this.clusterInstanceTemplate.isPortSpecified()
        ? this.clusterInstanceTemplate.getPort()
        : this.initialHostSpec.getPort();

    final HostSpec hostSpec = this.hostListProviderService.getHostSpecBuilder()
        .host(endpoint)
        .port(port)
        .role(isWriter ? HostRole.WRITER : HostRole.READER)
        .availability(HostAvailability.AVAILABLE)
        .weight(weight)
        .lastUpdateTime(lastUpdateTime)
        .build();
    hostSpec.addAlias(nodeName);
    hostSpec.setHostId(nodeName);
    return hostSpec;
  }

  protected String getHostEndpoint(final String nodeName) {
    final String host = this.clusterInstanceTemplate.getHost();
    return host.replace("?", nodeName);
  }

  private static class NodeMonitoringThread extends Thread {

    private static final Logger LOGGER = Logger.getLogger(NodeMonitoringThread.class.getName());

    protected final ClusterTopologyMonitorImpl monitor;
    protected final HostSpec hostSpec;
    protected final @Nullable HostSpec writerHostSpec;
    protected boolean writerChanged = false;

    public NodeMonitoringThread(
        final ClusterTopologyMonitorImpl monitor,
        final HostSpec hostSpec,
        final @Nullable HostSpec writerHostSpec
    ) {
      this.monitor = monitor;
      this.hostSpec = hostSpec;
      this.writerHostSpec = writerHostSpec;

      if (!StringUtils.isNullOrEmpty(this.getName())) {
        this.setName(this.getName() + "-nm");
      }
    }

    @Override
    public void run() {
      Connection connection = null;
      boolean updateTopology = false;

      final long start = System.nanoTime();
      try {
        while (!this.monitor.nodeThreadsStop.get()) {

          if (connection == null) {

            try {
              connection = this.monitor.pluginService.forceConnect(
                  hostSpec, this.monitor.monitoringProperties);
              this.monitor.pluginService.setAvailability(
                  hostSpec.asAliases(), HostAvailability.AVAILABLE);
            } catch (SQLException ex) {
              // connect issues
              this.monitor.pluginService.setAvailability(
                  hostSpec.asAliases(), HostAvailability.NOT_AVAILABLE);
            }
          }

          if (connection != null) {

            String writerId = null;
            try {
              writerId = this.monitor.getWriterNodeId(connection);

            } catch (SQLSyntaxErrorException ex) {
              LOGGER.severe(() -> Messages.get("NodeMonitoringThread.invalidWriterQuery",
                  new Object[] {ex.getMessage()}));
              throw new RuntimeException(ex);

            } catch (SQLException ex) {
              this.monitor.closeConnection(connection);
              connection = null;
            }

            if (!StringUtils.isNullOrEmpty(writerId)) {
              try {
                if (this.monitor.pluginService.getHostRole(connection) != HostRole.WRITER) {
                  // The first connection after failover may be stale.
                  writerId = null;
                }
              } catch (SQLException e) {
                // Invalid connection, retry.
                continue;
              }
            }

            if (!StringUtils.isNullOrEmpty(writerId)) {
              // this prevents closing connection in finally block
              if (!this.monitor.nodeThreadsWriterConnection.compareAndSet(null, connection)) {
                // writer connection is already setup
                this.monitor.closeConnection(connection);

              } else {
                // writer connection is successfully set to writerConnection
                LOGGER.fine(Messages.get("NodeMonitoringThread.detectedWriter", new Object[]{writerId}));
                // When nodeThreadsWriterConnection and nodeThreadsWriterHostSpec are both set, the topology monitor may
                // set ignoreNewTopologyRequestsEndTimeNano, in which case other threads will use the cached topology
                // for the ignore duration, so we need to update the topology before setting nodeThreadsWriterHostSpec.
                this.monitor.fetchTopologyAndUpdateCache(connection);
                this.monitor.nodeThreadsWriterHostSpec.set(hostSpec);
                this.monitor.nodeThreadsStop.set(true);
                LOGGER.fine(Utils.logTopology(
                    this.monitor.topologyMap.get(this.monitor.clusterId)));
              }

              // Setting the connection to null here prevents the finally block
              // from closing nodeThreadsWriterConnection.
              connection = null;
              return;

            } else if (connection != null) {
              // this connection is a reader connection
              if (this.monitor.nodeThreadsWriterConnection.get() == null) {
                // while writer connection isn't yet established this reader connection may update topology
                if (updateTopology) {
                  this.readerThreadFetchTopology(connection, writerHostSpec);
                } else if (this.monitor.nodeThreadsReaderConnection.get() == null) {
                  if (this.monitor.nodeThreadsReaderConnection.compareAndSet(null, connection)) {
                    // let's use this connection to update topology
                    updateTopology = true;
                    this.readerThreadFetchTopology(connection, writerHostSpec);
                  }
                }
              }
            }
          }
          TimeUnit.MILLISECONDS.sleep(100);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } finally {
        this.monitor.closeConnection(connection);
        final long end = System.nanoTime();
        LOGGER.finest(() -> Messages.get("NodeMonitoringThread.threadCompleted",
            new Object[] {TimeUnit.NANOSECONDS.toMillis(end - start)}));
      }
    }

    private void readerThreadFetchTopology(final Connection connection, final @Nullable HostSpec writerHostSpec) {
      if (connection == null) {
        return;
      }

      List<HostSpec> hosts;
      try {
        hosts = this.monitor.queryForTopology(connection);
        if (hosts == null) {
          return;
        }
      } catch (SQLException ex) {
        return;
      }

      // share this topology so the main monitoring thread be able to adjust node monitoring threads
      this.monitor.nodeThreadsLatestTopology.set(hosts);

      if (this.writerChanged) {
        this.monitor.updateTopologyCache(hosts);
        LOGGER.finest(Utils.logTopology(hosts));
        return;
      }

      final HostSpec latestWriterHostSpec = hosts.stream()
          .filter(x -> x.getRole() == HostRole.WRITER)
          .findFirst()
          .orElse(null);
      if (latestWriterHostSpec != null
          && writerHostSpec != null
          && !latestWriterHostSpec.getHostAndPort().equals(writerHostSpec.getHostAndPort())) {

        // writer node has changed
        this.writerChanged = true;

        LOGGER.fine(() -> Messages.get("NodeMonitoringThread.writerNodeChanged",
            new Object[] {writerHostSpec.getHost(), latestWriterHostSpec.getHost()}));

        // we can update topology cache and notify all waiting threads
        this.monitor.updateTopologyCache(hosts);
        LOGGER.fine(Utils.logTopology(hosts));
      }
    }
  }
}
