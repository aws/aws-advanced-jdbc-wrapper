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
  protected final Properties props;
  protected final BlueGreenRole role;
  protected final OnStatusChange onStatusChangeFunc;
  protected final Map<IntervalType, Long> checkIntervalMap;

  protected final HostSpec initialHostSpec;

  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);

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
  protected List<HostSpec> currentTopology = new ArrayList<>();
  protected Map<String, Optional<String>> startIpAddressesByHostMap = new ConcurrentHashMap<>();
  protected Map<String, Optional<String>> currentIpAddressesByHostMap = new ConcurrentHashMap<>();

  // Track all endpoints in startTopology and check whether all their IP addresses have changed.
  protected boolean allStartTopologyIpChanged = false;

  // Track all endpoints in startTopology and check whether they are removed (i.e. could not be resolved ayt DNS).
  protected boolean allStartTopologyEndpointsRemoved = false;
  protected BlueGreenPhases currentPhase = BlueGreenPhases.NOT_CREATED;
  protected Set<String> endpoints = ConcurrentHashMap.newKeySet();

  protected String version = "1.0";
  protected int port = -1;

  protected Connection connection = null;
  protected HostSpec connectionHostSpec = null;
  protected String connectedIpAddress = null;
  protected boolean connectionHostSpecCorrect = false;
  protected boolean panicMode = false;

  public BlueGreenStatusMonitor(
      final @NonNull BlueGreenRole role,
      final @NonNull HostSpec initialHostSpec,
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull Map<IntervalType, Long> checkIntervalMap,
      final @Nullable OnStatusChange onStatusChangeFunc) {

    this.role = role;
    this.initialHostSpec = initialHostSpec;
    this.pluginService = pluginService;
    this.props = props;
    this.checkIntervalMap = checkIntervalMap;
    this.onStatusChangeFunc = onStatusChangeFunc;

    this.supportBlueGreen = (SupportBlueGreen) this.pluginService.getDialect();

    executorService.submit(() -> {
      try {
        while (!this.stop.get()) {
          try {
            BlueGreenPhases oldPhase = this.currentPhase;
            this.collectStatus();

            // TODO: do we need this condition?
            //if (!this.panicMode) {
              this.collectTopology();
            //}

            this.collectHostIpAddresses();
            this.updateIpAddressFlags();

            if ((oldPhase == null && this.currentPhase != null)
                || (this.currentPhase != null && oldPhase != this.currentPhase)) {
              LOGGER.finest(() -> String.format("[%s] Status changed to: %s", this.role, this.currentPhase));
            }

            if (this.onStatusChangeFunc != null) {
              this.onStatusChangeFunc.onStatusChanged(
                  this.role,
                  new BlueGreenInterimStatus(
                      this.currentPhase,
                      this.version,
                      this.port,
                      this.startTopology,
                      this.currentTopology,
                      this.startIpAddressesByHostMap,
                      this.currentIpAddressesByHostMap,
                      this.endpoints,
                      this.allStartTopologyIpChanged,
                      this.allStartTopologyEndpointsRemoved));
            }

            long delayMs = checkIntervalMap.getOrDefault(
                this.panicMode ? IntervalType.HIGH : this.intervalType.get(),
                DEFAULT_CHECK_INTERVAL_MS);

            this.delay(delayMs);

          } catch (InterruptedException ex) {
            LOGGER.finest(() -> String.format("[%s] Interrupted.", this.role));
            return;
          } catch (Exception ex) {
            if (LOGGER.isLoggable(Level.WARNING)) {
              LOGGER.log(
                  Level.WARNING,
                  String.format("[%s] Unhandled exception while monitoring blue/green status.", this.role),
                  ex);
            }
          }
        }
      } finally {
        this.closeConnection();
        LOGGER.finest(() -> String.format("[%s] Blue/green status monitoring thread is completed.", this.role));
      }
    });
    executorService.shutdown(); // executor accepts no more tasks
  }

  protected void delay(long delayMs) throws InterruptedException {
    long start = System.nanoTime();
    long end = start + TimeUnit.MILLISECONDS.toNanos(delayMs);
    IntervalType currentIntervalType = this.intervalType.get();
    long minDelay = Math.min(delayMs, 50);

    // Check whether intervalType or stop flag change, or until waited specified delay time.
    do {
      synchronized (this.sleepWaitObj) {
        this.sleepWaitObj.wait(minDelay);
      }
    } while (this.intervalType.get() == currentIntervalType
        && System.nanoTime() < end
        && !this.stop.get());
  }

  public void setIntervalType(final @NonNull IntervalType intervalType) {
    this.intervalType.set(intervalType);

    synchronized (this.sleepWaitObj) {
      this.sleepWaitObj.notifyAll();
    }
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

    synchronized (this.sleepWaitObj) {
      this.sleepWaitObj.notifyAll();
    }
  }

  public void resetCollectedData() {
    this.startIpAddressesByHostMap.clear();
    this.startTopology = new ArrayList<>();
    this.endpoints.clear();
  }

  protected void collectHostIpAddresses() {

    this.currentIpAddressesByHostMap.clear();

    for (HostSpec hostSpec : this.currentTopology) {
      this.currentIpAddressesByHostMap.putIfAbsent(hostSpec.getHost(), this.getIpAddress(hostSpec.getHost()));
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

    if (this.hostListProvider == null || this.connection == null) {
      return;
    }

    this.currentTopology = this.hostListProvider.forceRefresh(this.connection);

    if (this.collectTopology.get()) {
      this.startTopology = this.currentTopology;
    }

    // Do not update endpoints when topology is frozen.
    if (this.currentTopology != null && this.collectTopology.get()) {
      this.endpoints.addAll(this.currentTopology.stream().map(HostSpec::getHost).collect(Collectors.toSet()));
    }
  }

  protected void closeConnection() {
    try {
      if (this.connection != null && !this.connection.isClosed()) {
        this.connection.close();
      }
    } catch (SQLException sqlException) {
      // ignore
    }
    this.connection = null;
  }

  protected void collectStatus() {
    try {
      if (this.connection == null || this.connection.isClosed()) {
        this.connection = null;
        if (this.connectionHostSpec == null) {
          this.connectionHostSpec = this.initialHostSpec;
          this.connectedIpAddress = null;
          this.connectionHostSpecCorrect = false;
        }
        try {

          if (this.useIpAddress.get() && this.connectedIpAddress != null) {
            final HostSpec connectionWithIpHostSpec = this.hostSpecBuilder.copyFrom(this.connectionHostSpec)
                .host(this.connectedIpAddress)
                .build();
            final Properties connectWithIpProperties = PropertyUtils.copyProperties(this.props);
            IamAuthConnectionPlugin.IAM_HOST.set(connectWithIpProperties, this.connectionHostSpec.getHost());

            LOGGER.finest(() -> String.format(
                "[%s] Opening monitoring connection (IP) to %s", this.role, connectionWithIpHostSpec.getHost()));

            this.connection = this.pluginService.forceConnect(connectionWithIpHostSpec, connectWithIpProperties);
            LOGGER.finest(() -> String.format(
                "[%s] Opened monitoring connection (IP) to %s", this.role, connectionWithIpHostSpec.getHost()));

          } else {
            LOGGER.finest(() -> String.format(
                "[%s] Opening monitoring connection to %s", this.role, this.connectionHostSpec.getHost()));

            this.connectedIpAddress = this.getIpAddress(this.connectionHostSpec.getHost()).orElse(null);
            this.connection = this.pluginService.forceConnect(this.connectionHostSpec, this.props);
            LOGGER.finest(() -> String.format(
                "[%s] Opened monitoring connection to %s", this.role, this.connectionHostSpec.getHost()));
          }
          this.panicMode = false;

        } catch (SQLException ex) {
          // can't open connection
          //this.connectedIpAddress = null;
          this.panicMode = true;
          return;
        }
      }

      if (!supportBlueGreen.isStatusAvailable(this.connection)) {
          if (!this.connection.isClosed()) {
            this.currentPhase = BlueGreenPhases.NOT_CREATED;
            LOGGER.finest(() -> String.format(
                "[%s] (status not available) currentPhase: %s", this.role, BlueGreenPhases.NOT_CREATED));
          } else {
            LOGGER.finest(() -> String.format("[%s] Status is not available since connection is closed.", this.role));
            this.closeConnection();
            this.currentPhase = null;
            this.panicMode = true;
            return;
          }
      }

      final Statement statement = this.connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(this.supportBlueGreen.getBlueGreenStatusQuery());

      final List<StatusInfo> statusEntries = new ArrayList<>();
      while (resultSet.next()) {
        final String version = resultSet.getString("version");
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
          .filter(x -> rdsUtils.isWriterClusterDns(x.endpoint))
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
            .filter(x -> rdsUtils.isRdsInstance(x.endpoint))
            .findFirst()
            .orElse(null);
      }

      if (statusInfo == null) {

        if (statusEntries.isEmpty()) {
          // It's normal to expect that the status table has no entries after BGD is completed.
          // Old1 cluster/instance has been separated and no longer receives
          // updates from related green cluster/instance.
          if (this.role != BlueGreenRole.SOURCE) {
            LOGGER.warning(() -> String.format("[%s] No entries in status table.", this.role));
          }
          this.currentPhase = null;
        }

      } else {
        this.currentPhase = statusInfo.phase;
        this.version = statusInfo.version;
        this.port = statusInfo.port;
      }

      if (!knownVersions.contains(this.version)) {
        this.version = latestKnownVersion;
        LOGGER.warning(() -> String.format(
            "[%s] Blue/Green deployment uses version '%s' that the driver doesn't support. '%s' version is used instead.",
            this.role, this.version, latestKnownVersion));
      }

      if (this.collectTopology.get()) {
        this.endpoints.addAll(
            statusEntries.stream()
                .filter(x -> x.endpoint != null)
                .map(x -> x.endpoint.toLowerCase())
                .collect(Collectors.toSet()));
      }

      if (!this.connectionHostSpecCorrect && statusInfo != null) {
        // We connected to an initialHostSpec that might be not the desired Blue or Green cluster.
        // We need to reconnect to a correct one.

        String statusInfoHostIpAddress = this.getIpAddress(statusInfo.endpoint).orElse(null);
        if (this.connectedIpAddress != null && !this.connectedIpAddress.equals(statusInfoHostIpAddress)) {
          // Found endpoint confirms that we're connected to a different node, and we need to reconnect.
          this.connectionHostSpec = this.hostSpecBuilder
              .host(statusInfo.endpoint)
              .port(statusInfo.port)
              .build();
          this.connectionHostSpecCorrect = true;
          this.closeConnection();
          this.panicMode = true;

        } else {
          // We're already connected to a correct node.
          this.connectionHostSpecCorrect = true;
          this.panicMode = false;
        }
      }

      if (this.connectionHostSpecCorrect && this.connection != null && this.hostListProvider == null) {
        // A connection to a correct cluster (blue or green) is established.
        // Let''s initialize HostListProvider
        this.initHostListProvider();
      }

    } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
      this.currentPhase = BlueGreenPhases.NOT_CREATED;
      LOGGER.finest(() -> String.format(
          "[%s] (SQLSyntaxErrorException) currentPhase: %s", this.role, BlueGreenPhases.NOT_CREATED));
    } catch (SQLException e) {
      if (!this.isConnectionClosed()) {
        // It's normal to get connection closed during BGD switchover.
        // If connection isn't closed but there's an exception then let's log it.
        if (LOGGER.isLoggable(Level.FINEST)) {
          LOGGER.log(Level.FINEST, String.format("[%s] Unhandled SQLException.", this.role), e);
        }
      }
      this.closeConnection();
      this.panicMode = true;
    } catch (Exception e) {
      // do nothing
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(Level.FINEST, String.format("[%s] Unhandled exception.", this.role), e);
      }
    }
  }

  protected boolean isConnectionClosed() {
    try {
      return this.connection == null || this.connection.isClosed();
    } catch (SQLException ex) {
      // do nothing
    }
    return true;
  }

  protected void initHostListProvider() {
    if (this.hostListProvider != null) {
      return;
    }

    final Properties hostListProperties = PropertyUtils.copyProperties(this.props);

    // TODO: do we need a more solid way to identify what cluster, blue or green, we're connected to?
    if (rdsUtils.isGreenInstance(this.pluginService.getCurrentHostSpec().getHost())
          && this.role == BlueGreenRole.TARGET
        || rdsUtils.isNoPrefixInstance(this.pluginService.getCurrentHostSpec().getHost())
          && this.role == BlueGreenRole.SOURCE) {

      // We can reuse the same HostListProvider as PluginService provides
      LOGGER.finest(() -> String.format("[%s] Reuse HostListProvider from PluginService", this.role));
      this.hostListProvider = this.pluginService.getHostListProvider();

    } else {
      // Need to instantiate a separate HostListProvider.
      // Let''s a special unique clusterId to avoid interference with other HostListProviders opened for this cluster.
      // Blue and Green clusters are expected to have different clusterId.


      String pluginServiceClusterId = "none";
      try {
        pluginServiceClusterId = this.pluginService.getHostListProvider().getClusterId();
      } catch (Exception ex) {
        // ignore
      }

      RdsHostListProvider.CLUSTER_ID.set(hostListProperties,
          String.format("%s::%s::%s", this.role, pluginServiceClusterId, BG_CLUSTER_ID));

      LOGGER.finest(() -> String.format(
          "[%s] Creating a new HostListProvider, clusterId: %s",
          this.role,
          RdsHostListProvider.CLUSTER_ID.getString(hostListProperties)));

      String protocol = this.connectionUrlParser.getProtocol(this.pluginService.getOriginalUrl());
      String hostListProviderUrl = String.format("%s%s/", protocol, this.connectionHostSpec.getHostAndPort());
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
      throw new IllegalArgumentException("Unknown blue/green status " + value);
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
