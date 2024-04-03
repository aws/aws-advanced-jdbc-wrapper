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

package software.amazon.jdbc.plugin.bluegreen;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.SupportBlueGreen;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class BlueGreenStatusMonitor {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusMonitor.class.getName());
  protected static final long DEFAULT_CHECK_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
  protected static final String BG_CLUSTER_ID = "941d00a8-8238-4f7d-bf59-771bff783a8e";

  protected static final HashMap<String, BlueGreenPhases> blueGreenStatusMapping =
      new HashMap<String, BlueGreenPhases>() {
        {
          put("AVAILABLE", BlueGreenPhases.CREATED);
          put("SWITCHOVER_INITIATED", BlueGreenPhases.PREPARATION);
          put("SWITCHOVER_IN_PROGRESS", BlueGreenPhases.IN_PROGRESS);
          put("SWITCHOVER_IN_POST_PROCESSING", BlueGreenPhases.POST);
          put("SWITCHOVER_COMPLETED", BlueGreenPhases.COMPLETED);
        }
      };

  protected static final String latestKnownVersion = "1.0";

  // Add more versions here if needed.
  protected static final Set<String> knownVersions = new HashSet<>(Collections.singletonList(latestKnownVersion));
  protected final SupportBlueGreen supportBlueGreen;
  protected final PluginService pluginService;
  protected final String bgdId;
  protected final Properties props;
  protected final BlueGreenRole role;
  protected final OnStatusChange onStatusChangeFunc;
  protected final Map<IntervalType, Long> checkIntervalMap;

  protected final HostSpec initialHostSpec;

  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);
  protected final ExecutorService openConnectionExecutorService = Executors.newFixedThreadPool(1);

  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();

  protected final HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(new SimpleHostAvailabilityStrategy());
  protected final AtomicBoolean collectIpAddresses = new AtomicBoolean(true);
  protected final AtomicBoolean collectTopology = new AtomicBoolean(true);
  protected final AtomicReference<IntervalType> intervalType = new AtomicReference<>(IntervalType.BASELINE);
  protected final AtomicBoolean stop = new AtomicBoolean(false);
  protected final AtomicBoolean useIpAddress = new AtomicBoolean(false);
  protected final Object sleepWaitObj = new Object();

  protected HostListProvider hostListProvider = null;
  protected List<HostSpec> startTopology = new ArrayList<>();
  protected final AtomicReference<List<HostSpec>> currentTopology = new AtomicReference<>(new ArrayList<>());
  protected Map<String, Optional<String>> startIpAddressesByHostMap = new ConcurrentHashMap<>();
  protected Map<String, Optional<String>> currentIpAddressesByHostMap = new ConcurrentHashMap<>();

  // Track all endpoints in startTopology and check whether all their IP addresses have changed.
  protected boolean allStartTopologyIpChanged = false;

  // Track all endpoints in startTopology and check whether they are removed (i.e. could not be resolved ayt DNS).
  protected boolean allStartTopologyEndpointsRemoved = false;
  protected boolean allTopologyChanged = false;
  protected BlueGreenPhases currentPhase = BlueGreenPhases.NOT_CREATED;
  protected Set<String> endpoints = ConcurrentHashMap.newKeySet();

  protected String version = "1.0";
  protected int port = -1;

  protected final AtomicReference<Connection> connection = new AtomicReference<>(null);
  protected final AtomicReference<HostSpec> connectionHostSpec = new AtomicReference<>(null);
  protected final AtomicReference<String> connectedIpAddress = new AtomicReference<>(null);
  protected final AtomicBoolean connectionHostSpecCorrect = new AtomicBoolean(false);
  protected final AtomicBoolean panicMode = new AtomicBoolean(true);
  protected Future openConnectionFuture = null;

  public BlueGreenStatusMonitor(
      final @NonNull BlueGreenRole role,
      final @NonNull String bgdId,
      final @NonNull HostSpec initialHostSpec,
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull Map<IntervalType, Long> checkIntervalMap,
      final @Nullable OnStatusChange onStatusChangeFunc) {

    this.role = role;
    this.bgdId = bgdId;
    this.initialHostSpec = initialHostSpec;
    this.pluginService = pluginService;
    this.props = props;
    this.checkIntervalMap = checkIntervalMap;
    this.onStatusChangeFunc = onStatusChangeFunc;

    this.supportBlueGreen = (SupportBlueGreen) this.pluginService.getDialect();

    executorService.submit(this::runMonitoringLoop);
    executorService.shutdown(); // executor accepts no more tasks
  }

  protected void runMonitoringLoop() {
    try {
      while (!this.stop.get()) {
        try {
          final BlueGreenPhases oldPhase = this.currentPhase;
          this.openConnection();
          this.collectStatus();
          this.collectTopology();
          this.collectHostIpAddresses();
          this.updateIpAddressFlags();

          if ((oldPhase == null && this.currentPhase != null)
              || (this.currentPhase != null && oldPhase != this.currentPhase)) {
            LOGGER.finest(() -> Messages.get("bgd.statusChanged", new Object[] {this.role, this.currentPhase}));
          }

          if (this.onStatusChangeFunc != null) {
            this.onStatusChangeFunc.onStatusChanged(
                this.role,
                new BlueGreenInterimStatus(
                    this.currentPhase,
                    this.version,
                    this.port,
                    this.startTopology,
                    this.currentTopology.get(),
                    this.startIpAddressesByHostMap,
                    this.currentIpAddressesByHostMap,
                    this.endpoints,
                    this.allStartTopologyIpChanged,
                    this.allStartTopologyEndpointsRemoved,
                    this.allTopologyChanged));
          }

          long delayMs = checkIntervalMap.getOrDefault(
              this.panicMode.get() ? IntervalType.HIGH : this.intervalType.get(),
              DEFAULT_CHECK_INTERVAL_MS);

          this.delay(delayMs);

        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          LOGGER.finest(() -> Messages.get("bgd.interrupted", new Object[] {this.role}));
          return;
        } catch (Exception ex) {
          if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.log(
                Level.WARNING,
                Messages.get("bgd.monitoringUnhandledException", new Object[] {this.role}),
                ex);
          }
        }
      }
    } finally {
      this.closeConnection();
      LOGGER.finest(() -> Messages.get("bgd.threadCompleted", new Object[] {this.role}));
    }
  }

  protected void delay(long delayMs) throws InterruptedException {
    long start = System.nanoTime();
    long end = start + TimeUnit.MILLISECONDS.toNanos(delayMs);
    IntervalType currentIntervalType = this.intervalType.get();
    boolean currentPanic = this.panicMode.get();
    long minDelay = Math.min(delayMs, 50);

    // Check whether intervalType or stop flag change, or until waited specified delay time.
    do {
      synchronized (this.sleepWaitObj) {
        this.sleepWaitObj.wait(minDelay);
      }
    } while (this.intervalType.get() == currentIntervalType
        && System.nanoTime() < end
        && !this.stop.get()
        && currentPanic == this.panicMode.get());
  }

  public void setIntervalType(final @NonNull IntervalType intervalType) {
    this.intervalType.set(intervalType);
    this.notifyChanges();
  }

  public void setCollectIpAddresses(final boolean collectIpAddresses) {
    this.collectIpAddresses.set(collectIpAddresses);
  }

  public void setCollectTopology(boolean collectTopology) {
    this.collectTopology.set(collectTopology);
  }

  public void setUseIpAddress(boolean useIpAddress) {
    this.useIpAddress.set(useIpAddress);
  }

  public void setStop(boolean stop) {
    this.stop.set(stop);
    this.notifyChanges();
  }

  public void resetCollectedData() {
    this.startIpAddressesByHostMap.clear();
    this.startTopology = new ArrayList<>();
    this.endpoints.clear();
  }

  protected void collectHostIpAddresses() {

    this.currentIpAddressesByHostMap.clear();

    if (this.endpoints != null) {
      for (String host : this.endpoints) {
        this.currentIpAddressesByHostMap.putIfAbsent(host, this.getIpAddress(host));
      }
    }

    if (this.collectIpAddresses.get()) {
      this.startIpAddressesByHostMap.clear();
      this.startIpAddressesByHostMap.putAll(this.currentIpAddressesByHostMap);
    }
  }

  protected void updateIpAddressFlags() {
    if (this.collectTopology.get()) {
      this.allStartTopologyIpChanged = false;
      this.allStartTopologyEndpointsRemoved = false;
      this.allTopologyChanged = false;
    } else {
      if (!this.collectIpAddresses.get()) {
        // All hosts in startTopology should resolve to different IP address.
        this.allStartTopologyIpChanged = !this.startTopology.isEmpty()
            && this.startTopology.stream()
            .allMatch(x -> this.startIpAddressesByHostMap.get(x.getHost()) != null
                && this.startIpAddressesByHostMap.get(x.getHost()).isPresent()
                && this.currentIpAddressesByHostMap.get(x.getHost()) != null
                && this.currentIpAddressesByHostMap.get(x.getHost()).isPresent()
                && !this.startIpAddressesByHostMap.get(x.getHost()).get()
                .equals(this.currentIpAddressesByHostMap.get(x.getHost()).get()));
      }

      // All hosts in startTopology should have no IP address. That means that host endpoint
      // couldn't be resolved since DNS entry doesn't exist anymore.
      this.allStartTopologyEndpointsRemoved = !this.startTopology.isEmpty()
          && this.startTopology.stream()
          .allMatch(x -> this.startIpAddressesByHostMap.get(x.getHost()) != null
              && this.startIpAddressesByHostMap.get(x.getHost()).isPresent()
              && this.currentIpAddressesByHostMap.get(x.getHost()) != null
              && !this.currentIpAddressesByHostMap.get(x.getHost()).isPresent());

      if (!this.collectTopology.get()) {
        // All hosts in currentTopology should have no same host in startTopology.
        // All hosts in currentTopology should have changed.
        final Set<String> startTopologyNodes = this.startTopology == null
            ? new HashSet<>()
            : this.startTopology.stream().map(HostSpec::getHost).collect(Collectors.toSet());
        final List<HostSpec> currentTopologyCopy = this.currentTopology.get();
        this.allTopologyChanged = currentTopologyCopy != null
            && !currentTopologyCopy.isEmpty()
            && !startTopologyNodes.isEmpty()
            && currentTopologyCopy.stream().noneMatch(x -> startTopologyNodes.contains(x.getHost()));
      }
    }
  }

  protected Optional<String> getIpAddress(String host) {
    try {
      return Optional.of(InetAddress.getByName(host).getHostAddress());
    } catch (UnknownHostException ex) {
      return Optional.empty();
    }
  }

  protected void collectTopology() throws SQLException {

    if (this.hostListProvider == null || this.connection.get() == null) {
      return;
    }

    final Connection conn = this.connection.get();
    if (conn == null || conn.isClosed()) {
      return;
    }
    this.currentTopology.set(this.hostListProvider.forceRefresh(conn));

    if (this.collectTopology.get()) {
      this.startTopology = this.currentTopology.get();
    }

    // Do not update endpoints when topology is frozen.
    final List<HostSpec> currentTopologyCopy = this.currentTopology.get();
    if (currentTopologyCopy != null && this.collectTopology.get()) {
      this.endpoints.addAll(currentTopologyCopy.stream().map(HostSpec::getHost).collect(Collectors.toSet()));
    }
  }

  protected void closeConnection() {
    final Connection conn = this.connection.get();
    this.connection.set(null);
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException sqlException) {
      // ignore
    }
  }

  protected void collectStatus() {
    final Connection conn = this.connection.get();
    try {
      if (conn == null || conn.isClosed()) {
        return;
      }

      if (!supportBlueGreen.isStatusAvailable(conn)) {
        if (!conn.isClosed()) {
          this.currentPhase = BlueGreenPhases.NOT_CREATED;
          LOGGER.finest(() -> Messages.get("bgd.statusNotAvailable",
              new Object[] {this.role, BlueGreenPhases.NOT_CREATED}));
        } else {
          this.connection.set(null);
          this.currentPhase = null;
          this.panicMode.set(true);
          return;
        }
      }

      final Statement statement = conn.createStatement();
      final ResultSet resultSet = statement.executeQuery(this.supportBlueGreen.getBlueGreenStatusQuery());

      final List<StatusInfo> statusEntries = new ArrayList<>();
      while (resultSet.next()) {
        String version = resultSet.getString("version");
        if (!knownVersions.contains(version)) {
          final String versionCopy = version;
          version = latestKnownVersion;
          LOGGER.warning(() -> Messages.get("bgd.usesVersion",
                  new Object[] {this.role, versionCopy, latestKnownVersion}));
        }

        final String endpoint = resultSet.getString("endpoint");
        final int port = resultSet.getInt("port");
        final BlueGreenRole role = BlueGreenRole.parseRole(resultSet.getString("role"), version);
        final BlueGreenPhases phase = this.parsePhase(resultSet.getString("status"), version);

        if (this.role != role) {
          continue;
        }

        statusEntries.add(new StatusInfo(version, endpoint, port, phase, role));
      }

      // Check if there's a cluster writer endpoint.
      StatusInfo statusInfo = statusEntries.stream()
          .filter(x -> rdsUtils.isWriterClusterDns(x.endpoint) && !rdsUtils.isOldInstance(x.endpoint))
          .findFirst()
          .orElse(null);

      if (statusInfo != null) {
        // Cluster writer endpoint found.
        // Add cluster reader endpoint as well.
        this.endpoints.add(statusInfo.endpoint.toLowerCase().replace(".cluster-", ".cluster-ro-"));
      }

      if (statusInfo == null) {
        // maybe it's an instance endpoint?
        statusInfo = statusEntries.stream()
            .filter(x -> rdsUtils.isRdsInstance(x.endpoint) && !rdsUtils.isOldInstance(x.endpoint))
            .findFirst()
            .orElse(null);
      }

      if (statusInfo == null) {

        if (statusEntries.isEmpty()) {
          // It's normal to expect that the status table has no entries after BGD is completed.
          // Old1 cluster/instance has been separated and no longer receives
          // updates from related green cluster/instance.
          if (this.role != BlueGreenRole.SOURCE) {
            LOGGER.warning(() -> Messages.get("bgd.noEntriesInStatusTable", new Object[] {this.role}));
          }
          this.currentPhase = null;
        }

      } else {
        this.currentPhase = statusInfo.phase;
        this.version = statusInfo.version;
        this.port = statusInfo.port;
      }

      if (this.collectTopology.get()) {
        this.endpoints.addAll(
            statusEntries.stream()
                .filter(x -> x.endpoint != null && !rdsUtils.isOldInstance(x.endpoint))
                .map(x -> x.endpoint.toLowerCase())
                .collect(Collectors.toSet()));
      }

      if (!this.connectionHostSpecCorrect.get() && statusInfo != null) {
        // We connected to an initialHostSpec that might be not the desired Blue or Green cluster.
        // We need to reconnect to a correct one.

        String statusInfoHostIpAddress = this.getIpAddress(statusInfo.endpoint).orElse(null);
        String connectedIpAddressCopy = this.connectedIpAddress.get();
        if (connectedIpAddressCopy != null && !connectedIpAddressCopy.equals(statusInfoHostIpAddress)) {
          // Found endpoint confirms that we're connected to a different node, and we need to reconnect.
          this.connectionHostSpec.set(this.hostSpecBuilder
              .host(statusInfo.endpoint)
              .port(statusInfo.port)
              .build());
          this.connectionHostSpecCorrect.set(true);
          this.closeConnection();
          this.panicMode.set(true);

        } else {
          // We're already connected to a correct node.
          this.connectionHostSpecCorrect.set(true);
          this.panicMode.set(false);
        }
      }

      if (this.connectionHostSpecCorrect.get() && this.hostListProvider == null) {
        // A connection to a correct cluster (blue or green) is established.
        // Let's initialize HostListProvider
        this.initHostListProvider();
      }

    } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
      this.currentPhase = BlueGreenPhases.NOT_CREATED;
      if (LOGGER.isLoggable(Level.WARNING)) {
        LOGGER.log(
            Level.WARNING,
            Messages.get("bgd.exception", new Object[] {this.role, BlueGreenPhases.NOT_CREATED}),
            sqlSyntaxErrorException);
      }
    } catch (SQLException e) {
      if (!this.isConnectionClosed(conn)) {
        // It's normal to get connection closed during BGD switchover.
        // If connection isn't closed but there's an exception then let's log it.
        if (LOGGER.isLoggable(Level.FINEST)) {
          LOGGER.log(Level.FINEST, Messages.get("bgd.unhandledSqlException", new Object[] {this.role}), e);
        }
      }
      this.closeConnection();
      this.panicMode.set(true);
    } catch (Exception e) {
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(Level.FINEST, Messages.get("bgd.unhandledException", new Object[] {this.role}), e);
      }
    }
  }

  protected boolean isConnectionClosed(Connection conn) {
    try {
      return conn == null || conn.isClosed();
    } catch (SQLException ex) {
      // do nothing
    }
    return true;
  }

  protected void openConnection() {
    final Connection conn = this.connection.get();
    if (!this.isConnectionClosed(conn)) {
      return;
    }

    if (this.openConnectionFuture != null) {
      if (this.openConnectionFuture.isDone()) {
        if (!this.panicMode.get()) {
          return; // Connection should be open by now.
        }
      } else if (!this.openConnectionFuture.isCancelled()) {
        // Opening a new connection is in progress. Let's wait.
        return;
      } else {
        this.openConnectionFuture = null;
      }
    }

    this.connection.set(null);
    this.panicMode.set(true);

    this.openConnectionFuture = openConnectionExecutorService.submit(() -> {

      HostSpec connectionHostSpecCopy = this.connectionHostSpec.get();
      String connectedIpAddressCopy = this.connectedIpAddress.get();

      if (connectionHostSpecCopy == null) {
        this.connectionHostSpec.set(this.initialHostSpec);
        connectionHostSpecCopy = this.initialHostSpec;
        this.connectedIpAddress.set(null);
        connectedIpAddressCopy = null;
        this.connectionHostSpecCorrect.set(false);
      }
      try {

        if (this.useIpAddress.get() && connectedIpAddressCopy != null) {
          final HostSpec connectionWithIpHostSpec = this.hostSpecBuilder.copyFrom(connectionHostSpecCopy)
              .host(connectedIpAddressCopy)
              .build();
          final Properties connectWithIpProperties = PropertyUtils.copyProperties(this.props);
          IamAuthConnectionPlugin.IAM_HOST.set(connectWithIpProperties, connectionHostSpecCopy.getHost());

          LOGGER.finest(() -> Messages.get("bgd.openingConnectionWithIp",
              new Object[] {this.role, connectionWithIpHostSpec.getHost()}));

          this.connection.set(this.pluginService.forceConnect(connectionWithIpHostSpec, connectWithIpProperties));
          LOGGER.finest(() -> Messages.get("bgd.openedConnectionWithIp",
                  new Object[] {this.role, connectionWithIpHostSpec.getHost()}));

        } else {

          final HostSpec finalConnectionHostSpecCopy = connectionHostSpecCopy;
          LOGGER.finest(() -> Messages.get("bgd.openingConnection",
                  new Object[] {this.role, finalConnectionHostSpecCopy.getHost()}));

          connectedIpAddressCopy = this.getIpAddress(connectionHostSpecCopy.getHost()).orElse(null);
          this.connection.set(this.pluginService.forceConnect(connectionHostSpecCopy, this.props));
          this.connectedIpAddress.set(connectedIpAddressCopy);

          LOGGER.finest(() -> Messages.get("bgd.openedConnection",
                  new Object[] {this.role, finalConnectionHostSpecCopy.getHost()}));
        }
        this.panicMode.set(false);
        this.notifyChanges();

      } catch (SQLException ex) {
        // can't open connection
        this.connection.set(null);
        this.panicMode.set(true);
        this.notifyChanges();
      }
    });
  }

  protected void notifyChanges() {
    synchronized (this.sleepWaitObj) {
      this.sleepWaitObj.notifyAll();
    }
  }

  protected void initHostListProvider() {
    if (this.hostListProvider != null || !this.connectionHostSpecCorrect.get()) {
      return;
    }

    final Properties hostListProperties = PropertyUtils.copyProperties(this.props);

    // Need to instantiate a separate HostListProvider with
    // a special unique clusterId to avoid interference with other HostListProviders opened for this cluster.
    // Blue and Green clusters are expected to have different clusterId.

    RdsHostListProvider.CLUSTER_ID.set(hostListProperties,
        String.format("%s::%s::%s", this.bgdId, this.role, BG_CLUSTER_ID));

    LOGGER.finest(() -> Messages.get("bgd.createHostListProvider",
            new Object[] {this.role, RdsHostListProvider.CLUSTER_ID.getString(hostListProperties)}));

    String protocol = this.connectionUrlParser.getProtocol(this.pluginService.getOriginalUrl());
    final HostSpec connectionHostSpecCopy = this.connectionHostSpec.get();
    if (connectionHostSpecCopy != null) {
      String hostListProviderUrl = String.format("%s%s/", protocol, connectionHostSpecCopy.getHostAndPort());
      this.hostListProvider = this.pluginService.getDialect()
          .getHostListProvider()
          .getProvider(
              hostListProperties,
              hostListProviderUrl,
              (HostListProviderService) this.pluginService,
              this.pluginService);
    }
  }

  protected BlueGreenPhases parsePhase(final String value, final String version) {
    if (StringUtils.isNullOrEmpty(value)) {
      return BlueGreenPhases.NOT_CREATED;
    }
    final BlueGreenPhases phase = blueGreenStatusMapping.get(value.toUpperCase());

    if (phase == null) {
      throw new IllegalArgumentException(Messages.get("bgd.unknownStatus", new Object[] {value}));
    }
    return phase;
  }

  private static class StatusInfo {
    public String version;
    public String endpoint;
    public int port;
    public BlueGreenPhases phase;
    public BlueGreenRole role;

    StatusInfo(String version, String endpoint, int port, BlueGreenPhases phase, BlueGreenRole role) {
      this.version = version;
      this.endpoint = endpoint;
      this.port = port;
      this.phase = phase;
      this.role = role;
    }
  }
}
