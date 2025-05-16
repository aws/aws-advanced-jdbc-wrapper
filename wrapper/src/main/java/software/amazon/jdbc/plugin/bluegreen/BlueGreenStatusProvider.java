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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.SupportBlueGreen;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.bluegreen.routing.ConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.ExecuteRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.HoldConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.HoldExecuteRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.RejectConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.SubstituteConnectRouting;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;

public class BlueGreenStatusProvider {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusProvider.class.getName());
  public static final AwsWrapperProperty BG_INTERVAL_BASELINE = new AwsWrapperProperty(
      "bgBaseline", "60000",
      "Baseline Blue/Green Deployment status checking interval (in msec).");

  public static final AwsWrapperProperty BG_INTERVAL_INCREASED = new AwsWrapperProperty(
      "bgIncreased", "1000",
      "Increased Blue/Green Deployment status checking interval (in msec).");

  public static final AwsWrapperProperty BG_INTERVAL_HIGH = new AwsWrapperProperty(
      "bgHigh", "100",
      "High Blue/Green Deployment status checking interval (in msec).");

  private static final String MONITORING_PROPERTY_PREFIX = "blue-green-monitoring-";
  private static final String DEFAULT_CONNECT_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));
  private static final String DEFAULT_SOCKET_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));

  public static final AwsWrapperProperty BG_SWITCHOVER_TIMEOUT = new AwsWrapperProperty(
      "bgSwitchoverTimeout", "180000", // 3min
      "Blue/Green Deployment switchover timeout (in msec).");

  public static final AwsWrapperProperty BG_HOLD_NEW_BLUE_CONNECTIONS = new AwsWrapperProperty(
      "bgHoldNewBlueConnections", "false",
      "Blue/Green Deployment switchover can put opening new connections on hold"
      + " when switchover is in progress.");

  protected final HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(new SimpleHostAvailabilityStrategy());

  protected final BlueGreenStatusMonitor[] monitors = { null, null };
  protected int[] interimStatusHashes = { 0, 0 };
  protected int lastContextHash = 0;
  protected BlueGreenInterimStatus[] interimStatuses = { null, null };
  protected final Map<String, Optional<String>> hostIpAddresses = new ConcurrentHashMap<>();
  protected final Map<String, Pair<HostSpec, HostSpec>> correspondingNodes = new ConcurrentHashMap<>();

  // all known endpoints; host and port
  protected final Map<String, BlueGreenRole> roleByEndpoint = new ConcurrentHashMap<>();
  protected final Map<String, Set<String>> iamHostSuccessfulConnects = new ConcurrentHashMap<>();
  protected final Map<String, Instant> greenNodeChangeNameTimes = new ConcurrentHashMap<>();
  protected BlueGreenStatus summaryStatus = null;
  protected BlueGreenPhases latestStatusPhase = BlueGreenPhases.NOT_CREATED;

  protected boolean rollback = false;
  protected boolean blueDnsUpdateCompleted = false;
  protected boolean greenDnsRemoved = false;
  protected boolean greenTopologyChanged = false;
  protected final AtomicBoolean allGreenNodesChangedName = new AtomicBoolean(false);
  protected long postStatusEndTimeNano = 0;
  protected final ReentrantLock monitorInitLock = new ReentrantLock();
  protected final ReentrantLock processStatusLock = new ReentrantLock();
  protected final Map<IntervalType, Long> checkIntervalMap = new HashMap<>();
  protected final long switchoverTimeoutNano;
  protected final boolean holdNewBlueConnectionsWhenInProgress;

  protected final PluginService pluginService;
  protected final Properties props;
  protected final String bgdId;
  protected Map<String, PhaseTimeInfo> phaseTimeNano = new ConcurrentHashMap<>();
  protected final RdsUtils rdsUtils = new RdsUtils();

  public BlueGreenStatusProvider(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull String bgdId) {

    this.pluginService = pluginService;
    this.props = props;
    this.bgdId = bgdId;

    this.checkIntervalMap.put(IntervalType.BASELINE, BG_INTERVAL_BASELINE.getLong(props));
    this.checkIntervalMap.put(IntervalType.INCREASED, BG_INTERVAL_INCREASED.getLong(props));
    this.checkIntervalMap.put(IntervalType.HIGH, BG_INTERVAL_HIGH.getLong(props));

    this.switchoverTimeoutNano = TimeUnit.MILLISECONDS.toNanos(BG_SWITCHOVER_TIMEOUT.getLong(props));
    this.holdNewBlueConnectionsWhenInProgress = BG_HOLD_NEW_BLUE_CONNECTIONS.getBoolean(props);

    final Dialect dialect = this.pluginService.getDialect();
    if (dialect instanceof SupportBlueGreen) {
      this.initMonitoring();
    } else {
      LOGGER.warning(() -> Messages.get("bgd.unsupportedDialect",
          new Object[] {this.bgdId, dialect.getClass().getSimpleName()}));
    }
  }

  protected void initMonitoring() {
    if (monitors[BlueGreenRole.SOURCE.getValue()] == null || monitors[BlueGreenRole.TARGET.getValue()] == null) {
      monitorInitLock.lock();
      try {
        if (monitors[BlueGreenRole.SOURCE.getValue()] == null) {
          monitors[BlueGreenRole.SOURCE.getValue()] =
              new BlueGreenStatusMonitor(
                  BlueGreenRole.SOURCE,
                  this.bgdId,
                  this.pluginService.getCurrentHostSpec(),
                  this.pluginService,
                  this.getMonitoringProperties(),
                  checkIntervalMap,
                  this::prepareStatus);
        }
        if (monitors[BlueGreenRole.TARGET.getValue()] == null) {
          monitors[BlueGreenRole.TARGET.getValue()] =
              new BlueGreenStatusMonitor(
                  BlueGreenRole.TARGET,
                  this.bgdId,
                  this.pluginService.getCurrentHostSpec(),
                  this.pluginService,
                  this.getMonitoringProperties(),
                  checkIntervalMap,
                  this::prepareStatus);
        }
      } finally {
        monitorInitLock.unlock();
      }
    }
  }

  protected Properties getMonitoringProperties() {
    final Properties monitoringConnProperties = PropertyUtils.copyProperties(this.props);
    this.props.stringPropertyNames().stream()
        .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
        .forEach(
            p -> {
              monitoringConnProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  this.props.getProperty(p));
              monitoringConnProperties.remove(p);
            });

    if (!monitoringConnProperties.containsKey(PropertyDefinition.CONNECT_TIMEOUT)) {
      monitoringConnProperties.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, DEFAULT_CONNECT_TIMEOUT_MS);
    }
    if (!monitoringConnProperties.containsKey(PropertyDefinition.SOCKET_TIMEOUT)) {
      monitoringConnProperties.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, DEFAULT_SOCKET_TIMEOUT_MS);
    }

    return monitoringConnProperties;
  }

  protected void prepareStatus(
      final BlueGreenRole role,
      final BlueGreenInterimStatus interimStatus) {

    this.processStatusLock.lock();
    try {

      // Detect changes
      int statusHash = this.getInterimStatusHash(interimStatus);
      int contextHash = this.getContextHash();
      if (this.interimStatusHashes[role.getValue()] == statusHash
          && this.lastContextHash == contextHash) {
        // no changes detected
        return;
      }

      // There are some changes detected. Let's update summary status.

      LOGGER.finest(() -> Messages.get("bgd.interimStatus",
          new Object[] {this.bgdId, role, interimStatus}));

      this.updatePhase(role, interimStatus);

      // Store interimStatus and corresponding hash
      this.interimStatuses[role.getValue()] = interimStatus;
      this.interimStatusHashes[role.getValue()] = statusHash;
      this.lastContextHash = contextHash;

      // Update map of IP addresses.
      this.hostIpAddresses.putAll(interimStatus.startIpAddressesByHostMap);

      // Update roleByEndpoint based on provided endpoints.
      interimStatus.endpoints.forEach(x -> this.roleByEndpoint.put(x.toLowerCase(), role));

      this.updateCorrespondingNodes();
      this.updateSummaryStatus(role, interimStatus);
      this.updateMonitors();
      this.updateStatusCache();
      this.logCurrentContext();

      // Log final switchover results.
      this.logSwitchoverFinalSummary();

      this.resetContextWhenCompleted();

    } finally {
      this.processStatusLock.unlock();
    }
  }

  protected void updatePhase(BlueGreenRole role, BlueGreenInterimStatus interimStatus) {

    BlueGreenPhases latestInterimPhase = this.interimStatuses[role.getValue()] == null
        ? BlueGreenPhases.NOT_CREATED
        : this.interimStatuses[role.getValue()].blueGreenPhase;

    if (latestInterimPhase != null
        && interimStatus.blueGreenPhase != null
        && interimStatus.blueGreenPhase.getValue() < latestInterimPhase.getValue()) {
      this.rollback = true;
      LOGGER.finest(() -> Messages.get("bgd.rollback", new Object[] {this.bgdId}));
    }

    // Do not allow status moves backward (unless it's rollback).
    // That could be caused by updating blue/green nodes delays.
    if (!this.rollback) {
      if (interimStatus.blueGreenPhase != null
          && interimStatus.blueGreenPhase.getValue() >= this.latestStatusPhase.getValue()) {
        this.latestStatusPhase = interimStatus.blueGreenPhase;
      }
    } else {
      if (interimStatus.blueGreenPhase != null
          && interimStatus.blueGreenPhase.getValue() < this.latestStatusPhase.getValue()) {
        this.latestStatusPhase = interimStatus.blueGreenPhase;
      }
    }
  }

  protected void updateStatusCache() {
    final BlueGreenStatus latestStatus = this.pluginService.getStatus(BlueGreenStatus.class, this.bgdId);
    this.pluginService.setStatus(BlueGreenStatus.class, this.summaryStatus, this.bgdId);
    this.storePhaseTime(this.summaryStatus.getCurrentPhase());

    // Notify all waiting threads that status is changed.
    // Those waiting threads are waiting on an existing status so we need to notify on it.
    if (latestStatus != null) {
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (latestStatus) {
        latestStatus.notifyAll();
      }
    }
  }

  /**
   * Update corresponding nodes
   * Blue writer node is mapped to a green writer node.
   * Blue reader nodes are mapped to green reader nodes.
   */
  protected void updateCorrespondingNodes() {
    this.correspondingNodes.clear();

    if (this.interimStatuses[BlueGreenRole.SOURCE.getValue()] != null
        && !Utils.isNullOrEmpty(this.interimStatuses[BlueGreenRole.SOURCE.getValue()].startTopology)
        && this.interimStatuses[BlueGreenRole.TARGET.getValue()] != null
        && !Utils.isNullOrEmpty(this.interimStatuses[BlueGreenRole.TARGET.getValue()].startTopology)) {

      HostSpec blueWriterHostSpec =
          this.interimStatuses[BlueGreenRole.SOURCE.getValue()].startTopology.stream()
              .filter(x -> x.getRole() == HostRole.WRITER)
              .findFirst()
              .orElse(null);

      HostSpec greenWriterHostSpec =
          this.interimStatuses[BlueGreenRole.TARGET.getValue()].startTopology.stream()
              .filter(x -> x.getRole() == HostRole.WRITER)
              .findFirst()
              .orElse(null);

      List<HostSpec> sortedBlueReaderHostSpecs =
          this.interimStatuses[BlueGreenRole.SOURCE.getValue()].startTopology.stream()
              .filter(x -> x.getRole() != HostRole.WRITER)
              .sorted(Comparator.comparing(HostSpec::getHost))
              .collect(Collectors.toList());

      List<HostSpec> sortedGreenReaderHostSpecs =
          this.interimStatuses[BlueGreenRole.TARGET.getValue()].startTopology.stream()
              .filter(x -> x.getRole() != HostRole.WRITER)
              .sorted(Comparator.comparing(HostSpec::getHost))
              .collect(Collectors.toList());

      if (blueWriterHostSpec != null) {
        if (greenWriterHostSpec != null) {
          this.correspondingNodes.put(
              blueWriterHostSpec.getHost(), Pair.create(blueWriterHostSpec, greenWriterHostSpec));
        } else {
          sortedGreenReaderHostSpecs.stream()
              .findFirst()
              .ifPresent(anyGreenHostSpec -> this.correspondingNodes.put(
                  blueWriterHostSpec.getHost(), Pair.create(blueWriterHostSpec, anyGreenHostSpec)));
        }
      }

      int greenIndex = 0;
      for (HostSpec blueHostSpec : sortedBlueReaderHostSpecs) {
        this.correspondingNodes.put(
            blueHostSpec.getHost(), Pair.create(blueHostSpec, sortedGreenReaderHostSpecs.get(greenIndex++)));
        greenIndex = greenIndex % sortedGreenReaderHostSpecs.size();
      }
    }

    if (this.interimStatuses[BlueGreenRole.SOURCE.getValue()] != null
        && !Utils.isNullOrEmpty(this.interimStatuses[BlueGreenRole.SOURCE.getValue()].endpoints)
        && this.interimStatuses[BlueGreenRole.TARGET.getValue()] != null
        && !Utils.isNullOrEmpty(this.interimStatuses[BlueGreenRole.TARGET.getValue()].endpoints)) {

      Set<String> blueEndpoints = this.interimStatuses[BlueGreenRole.SOURCE.getValue()].endpoints;
      Set<String> greenEndpoints = this.interimStatuses[BlueGreenRole.TARGET.getValue()].endpoints;

      // Find corresponding cluster endpoints
      String blueClusterEndpoint = blueEndpoints.stream().filter(this.rdsUtils::isWriterClusterDns)
          .findFirst()
          .orElse(null);
      String greenClusterEndpoint = greenEndpoints.stream().filter(this.rdsUtils::isWriterClusterDns)
          .findFirst()
          .orElse(null);

      if (!StringUtils.isNullOrEmpty(blueClusterEndpoint) && !StringUtils.isNullOrEmpty(greenClusterEndpoint)) {
        this.correspondingNodes.putIfAbsent(blueClusterEndpoint,
            Pair.create(
                this.hostSpecBuilder.host(blueClusterEndpoint).build(),
                this.hostSpecBuilder.host(greenClusterEndpoint).build()));
      }

      // Find corresponding cluster reader endpoints
      String blueClusterReaderEndpoint = blueEndpoints.stream().filter(this.rdsUtils::isReaderClusterDns)
          .findFirst()
          .orElse(null);
      String greenClusterReaderEndpoint = greenEndpoints.stream().filter(this.rdsUtils::isReaderClusterDns)
          .findFirst()
          .orElse(null);

      if (!StringUtils.isNullOrEmpty(blueClusterReaderEndpoint)
          && !StringUtils.isNullOrEmpty(greenClusterReaderEndpoint)) {
        this.correspondingNodes.putIfAbsent(blueClusterReaderEndpoint,
            Pair.create(
                this.hostSpecBuilder.host(blueClusterReaderEndpoint).build(),
                this.hostSpecBuilder.host(greenClusterReaderEndpoint).build()));
      }

      blueEndpoints.stream().filter(this.rdsUtils::isRdsCustomClusterDns).forEach(blueEndpoint -> {
        final String customClusterName = this.rdsUtils.getRdsClusterId(blueEndpoint);
        if (customClusterName != null) {
          greenEndpoints.stream()
              .filter(x -> this.rdsUtils.isRdsCustomClusterDns(x)
                  && x.startsWith(customClusterName))
              .findFirst()
              .ifPresent(y -> this.correspondingNodes.putIfAbsent(blueEndpoint,
                  Pair.create(
                      this.hostSpecBuilder.host(y).build(),
                      this.hostSpecBuilder.host(y).build())));
        }
      });
    }
  }

  protected void updateSummaryStatus(BlueGreenRole role, BlueGreenInterimStatus interimStatus) {
    switch (this.latestStatusPhase) {
      case NOT_CREATED:
        this.summaryStatus = new BlueGreenStatus(this.bgdId, BlueGreenPhases.NOT_CREATED);
        break;
      case CREATED:
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfCreated();
        break;
      case PREPARATION:
        this.startSwitchoverTimer();
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfPreparation();
        break;
      case IN_PROGRESS:
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfInProgress();
        break;
      case POST:
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfPost();
        break;
      case COMPLETED:
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfCompleted();
        break;
      default:
        throw new UnsupportedOperationException(Messages.get("bgd.unknownPhase",
            new Object[] {this.bgdId, this.latestStatusPhase}));
    }
  }

  protected void updateMonitors() {
    switch (this.summaryStatus.getCurrentPhase()) {
      case NOT_CREATED:
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.BASELINE);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(false);
        });
        break;
      case CREATED:
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.INCREASED);
          x.setCollectIpAddresses(true);
          x.setCollectTopology(true);
          x.setUseIpAddress(false);
          if (this.rollback) {
            x.resetCollectedData();
          }
        });
        break;
      case PREPARATION:
      case IN_PROGRESS:
      case POST:
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.HIGH);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(true);
        });
        break;
      case COMPLETED:
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.BASELINE);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(false);
          x.resetCollectedData();
        });

        // Stop monitoring old1 cluster/instance.
        if (!this.rollback && monitors[BlueGreenRole.SOURCE.getValue()] != null) {
          monitors[BlueGreenRole.SOURCE.getValue()].setStop(true);
        }
        break;
      default:
        throw new UnsupportedOperationException(Messages.get("bgd.unknownPhase",
            new Object[] {this.bgdId, this.summaryStatus.getCurrentPhase()}));
    }
  }

  protected void updateDnsFlags(BlueGreenRole role, BlueGreenInterimStatus interimStatus) {
    if (role == BlueGreenRole.SOURCE && !this.blueDnsUpdateCompleted && interimStatus.allStartTopologyIpChanged) {
      LOGGER.finest(() -> Messages.get("bgd.blueDnsCompleted", new Object[] {this.bgdId}));
      this.blueDnsUpdateCompleted = true;
      this.storeBlueDnsUpdateTime();
    }

    if (role == BlueGreenRole.TARGET && !this.greenDnsRemoved && interimStatus.allStartTopologyEndpointsRemoved) {
      LOGGER.finest(() -> Messages.get("bgd.greenDnsRemoved", new Object[] {this.bgdId}));
      this.greenDnsRemoved = true;
      this.storeGreenDnsRemoveTime();
    }

    if (role == BlueGreenRole.TARGET && !this.greenTopologyChanged && interimStatus.allTopologyChanged) {
      LOGGER.finest(() -> Messages.get("bgd.greenTopologyChanged", new Object[] {this.bgdId}));
      this.greenTopologyChanged = true;
      this.storeGreenTopologyChangeTime();
    }
  }

  protected int getInterimStatusHash(BlueGreenInterimStatus interimStatus) {

    int result = this.getValueHash(1,
        interimStatus.blueGreenPhase == null ? "" : interimStatus.blueGreenPhase.toString());
    result = this.getValueHash(result,
        interimStatus.version == null ? "" : interimStatus.version);
    result = this.getValueHash(result, String.valueOf(interimStatus.port));
    result = this.getValueHash(result, String.valueOf(interimStatus.allStartTopologyIpChanged));
    result = this.getValueHash(result, String.valueOf(interimStatus.allStartTopologyEndpointsRemoved));
    result = this.getValueHash(result, String.valueOf(interimStatus.allTopologyChanged));

    result = this.getValueHash(result,
        interimStatus.endpoints == null
            ? ""
            : interimStatus.endpoints.stream()
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getValueHash(result,
        interimStatus.startTopology == null
            ? ""
            : interimStatus.startTopology.stream()
                .map(x -> x.getHostAndPort() + x.getRole())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getValueHash(result,
        interimStatus.currentTopology == null
            ? ""
            : interimStatus.currentTopology.stream()
                .map(x -> x.getHostAndPort() + x.getRole())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getValueHash(result,
        interimStatus.startIpAddressesByHostMap == null
            ? ""
            : interimStatus.startIpAddressesByHostMap.entrySet().stream()
                .map(x -> x.getKey() + x.getValue())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getValueHash(result,
        interimStatus.currentIpAddressesByHostMap == null
            ? ""
            : interimStatus.currentIpAddressesByHostMap.entrySet().stream()
                .map(x -> x.getKey() + x.getValue())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    return result;
  }

  protected int getContextHash() {
    int result = this.getValueHash(1, String.valueOf(this.allGreenNodesChangedName.get()));
    result = this.getValueHash(result, String.valueOf(this.iamHostSuccessfulConnects.size()));
    return result;
  }

  protected int getValueHash(int currentHash, String val) {
    return currentHash * 31 + val.hashCode();
  }

  protected String getHostAndPort(String host, int port) {
    if (port > 0) {
      return String.format("%s:%d", host, port);
    }
    return host;
  }

  // New connect requests: go to blue or green nodes; default behaviour; no routing
  // Existing connections: default behaviour; no action
  // Execute JDBC calls: default behaviour; no action
  protected BlueGreenStatus getStatusOfCreated() {
    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhases.CREATED,
        new ArrayList<>(),
        new ArrayList<>(),
        this.roleByEndpoint);
  }

  /**
   * New connect requests to blue: route to corresponding IP address.
   * New connect requests to green: route to corresponding IP address
   * New connect requests with IP address: default behaviour; no routing
   * Existing connections: default behaviour; no action
   * Execute JDBC calls: default behaviour; no action
   */
  protected BlueGreenStatus getStatusOfPreparation() {

    // We want to limit switchover duration to DEFAULT_POST_STATUS_DURATION_NANO.
    if (this.isSwitchoverTimerExpired()) {
      LOGGER.finest(Messages.get("bgd.switchoverTimeout"));
      if (this.rollback) {
        return this.getStatusOfCreated();
      }
      return this.getStatusOfCompleted();
    }

    List<ConnectRouting> connectRouting = new ArrayList<>();

    this.roleByEndpoint.entrySet().stream()
        .filter(x -> this.correspondingNodes.containsKey(x.getKey()))
        .forEach(x -> {
          HostSpec hostSpec = this.correspondingNodes.get(x.getKey()).getValue1();
          Optional<String> blueIp = this.hostIpAddresses.get(hostSpec.getHost());
          HostSpec substituteHostSpecWithIp = blueIp == null || !blueIp.isPresent()
              ? hostSpec
              : this.hostSpecBuilder.copyFrom(hostSpec).host(blueIp.get()).build();

          connectRouting.add(new SubstituteConnectRouting(
              x.getKey(),
              x.getValue(),
              substituteHostSpecWithIp,
              Collections.singletonList(hostSpec),
              null));

          connectRouting.add(new SubstituteConnectRouting(
              this.getHostAndPort(
                  x.getKey(),
                  this.interimStatuses[x.getValue().getValue()].port),
              x.getValue(),
              substituteHostSpecWithIp,
              Collections.singletonList(hostSpec),
              null));
        });

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhases.PREPARATION,
        connectRouting,
        new ArrayList<>(),
        this.roleByEndpoint);
  }

  /**
   * New connect requests to blue: route to corresponding IP address.
   * New connect requests to green: route to corresponding IP address
   * New connect requests with IP address: default behaviour; no routing
   * Existing connections: default behaviour; no action
   * Execute JDBC calls: default behaviour; no action
   */
  protected BlueGreenStatus getStatusOfInProgress() {

    // We want to limit switchover duration to DEFAULT_POST_STATUS_DURATION_NANO.
    if (this.isSwitchoverTimerExpired()) {
      LOGGER.finest(Messages.get("bgd.switchoverTimeout"));
      if (this.rollback) {
        return this.getStatusOfCreated();
      }
      return this.getStatusOfCompleted();
    }

    // All blue and green connect calls should be on hold.
    List<ConnectRouting> connectRouting = new ArrayList<>();

    if (this.holdNewBlueConnectionsWhenInProgress) {
      connectRouting.add(new HoldConnectRouting(null, BlueGreenRole.SOURCE, this.bgdId));
    } else {
      // If we're not suspending new connections then at least we need to use IP addresses.
      this.roleByEndpoint.entrySet().stream()
          .filter(x -> x.getValue() == BlueGreenRole.SOURCE
              && this.correspondingNodes.containsKey(x.getKey()))
          .forEach(x -> {
            HostSpec hostSpec = this.correspondingNodes.get(x.getKey()).getValue1();
            Optional<String> blueIp = this.hostIpAddresses.get(hostSpec.getHost());
            HostSpec substituteHostSpecWithIp = blueIp == null || !blueIp.isPresent()
                ? hostSpec
                : this.hostSpecBuilder.copyFrom(hostSpec).host(blueIp.get()).build();

            connectRouting.add(new SubstituteConnectRouting(
                x.getKey(),
                x.getValue(),
                substituteHostSpecWithIp,
                Collections.singletonList(hostSpec),
                null));

            connectRouting.add(new SubstituteConnectRouting(
                this.getHostAndPort(
                    x.getKey(),
                    this.interimStatuses[x.getValue().getValue()].port),
                x.getValue(),
                substituteHostSpecWithIp,
                Collections.singletonList(hostSpec),
                null));
          });
    }

    connectRouting.add(new HoldConnectRouting(null, BlueGreenRole.TARGET, this.bgdId));

    // All connect calls with IP address that belongs to blue or green node should be on hold.
    this.hostIpAddresses.values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .distinct()
        .forEach(ipAddress -> {

          BlueGreenInterimStatus interimStatus;

          if (this.holdNewBlueConnectionsWhenInProgress) {
            // Try to confirm tht ipAddress belongs to one of the blue nodes
            interimStatus = this.interimStatuses[BlueGreenRole.SOURCE.getValue()];
            if (interimStatus != null) {
              if (interimStatus.startIpAddressesByHostMap.values().stream()
                  .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                  .map(x -> true)
                  .findFirst()
                  .orElse(false)) {

                connectRouting.add(new HoldConnectRouting(ipAddress, null, this.bgdId));
                connectRouting.add(new HoldConnectRouting(
                    this.getHostAndPort(ipAddress, interimStatus.port),
                    null,
                    this.bgdId));

                return;
              }
            }
          }

          // Try to confirm tht ipAddress belongs to one of the green nodes
          interimStatus = this.interimStatuses[BlueGreenRole.TARGET.getValue()];
          if (interimStatus != null) {
            if (interimStatus.startIpAddressesByHostMap.values().stream()
                .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                .map(x -> true)
                .findFirst()
                .orElse(false)) {

              connectRouting.add(new HoldConnectRouting(ipAddress, null, this.bgdId));
              connectRouting.add(new HoldConnectRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null,
                  this.bgdId));

              return;
            }
          }

          connectRouting.add(new HoldConnectRouting(ipAddress, null, this.bgdId));
        });

    // All blue and green traffic should be on hold.
    List<ExecuteRouting> executeRouting = new ArrayList<>();
    executeRouting.add(new HoldExecuteRouting(null, BlueGreenRole.SOURCE, this.bgdId));
    executeRouting.add(new HoldExecuteRouting(null, BlueGreenRole.TARGET, this.bgdId));

    // All traffic through connections with IP addresses that belong to blue or green nodes should be on hold.
    this.hostIpAddresses.values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .distinct()
        .forEach(ipAddress -> {

          // Try to confirm tht ipAddress belongs to one of the blue nodes
          BlueGreenInterimStatus interimStatus = this.interimStatuses[BlueGreenRole.SOURCE.getValue()];
          if (interimStatus != null) {
            if (interimStatus.startIpAddressesByHostMap.values().stream()
                .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                .map(x -> true)
                .findFirst()
                .orElse(false)) {

              executeRouting.add(new HoldExecuteRouting(ipAddress, null, this.bgdId));
              executeRouting.add(new HoldExecuteRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null,
                  this.bgdId));

              return;
            }
          }

          // Try to confirm tht ipAddress belongs to one of the green nodes
          interimStatus = this.interimStatuses[BlueGreenRole.TARGET.getValue()];
          if (interimStatus != null) {
            if (interimStatus.startIpAddressesByHostMap.values().stream()
                .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                .map(x -> true)
                .findFirst()
                .orElse(false)) {

              executeRouting.add(new HoldExecuteRouting(ipAddress, null, this.bgdId));
              executeRouting.add(new HoldExecuteRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null,
                  this.bgdId));

              return;
            }
          }

          executeRouting.add(new HoldExecuteRouting(ipAddress, null, this.bgdId));
        });

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhases.IN_PROGRESS,
        connectRouting,
        executeRouting,
        this.roleByEndpoint);
  }

  protected BlueGreenStatus getStatusOfPost() {

    // We want to limit switchover duration to DEFAULT_POST_STATUS_DURATION_NANO.
    if (this.isSwitchoverTimerExpired()) {
      LOGGER.finest(Messages.get("bgd.switchoverTimeout"));
      if (this.rollback) {
        return this.getStatusOfCreated();
      }
      return this.getStatusOfCompleted();
    }

    List<ConnectRouting> connectRouting = new ArrayList<>();
    List<ExecuteRouting> executeRouting = new ArrayList<>();
    this.createPostRouting(connectRouting, executeRouting);

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhases.POST,
        connectRouting,
        executeRouting,
        this.roleByEndpoint);
  }

  protected void createPostRouting(
      List<ConnectRouting> connectRouting,
      List<ExecuteRouting> executeRouting) {

    if (!this.blueDnsUpdateCompleted || !this.allGreenNodesChangedName.get()) {
      // New connect calls to blue nodes should be routed to green nodes.
      this.roleByEndpoint.entrySet().stream()
          .filter(x -> x.getValue() == BlueGreenRole.SOURCE)
          .filter(x -> this.correspondingNodes.containsKey(x.getKey()))
          .forEach(x -> {
            final String blueHost = x.getKey();
            final boolean isBlueHostInstance = rdsUtils.isRdsInstance(blueHost);
            HostSpec blueHostSpec = this.correspondingNodes.get(x.getKey()).getValue1();
            HostSpec greenHostSpec = this.correspondingNodes.get(x.getKey()).getValue2();
            final String greenHost = greenHostSpec.getHost();
            Optional<String> greenIp = this.hostIpAddresses.get(greenHostSpec.getHost());
            HostSpec greenHostSpecWithIp = greenIp == null || !greenIp.isPresent()
                ? greenHostSpec
                : this.hostSpecBuilder.copyFrom(greenHostSpec).host(greenIp.get()).build();

            // Check whether green host is already been connected with blue (no-prefixes) IAM host name.
            List<HostSpec> iamHosts = this.isAlreadySuccessfullyConnected(greenHost, blueHost)
                // Green node has already changed its name and not it's a new blue node (no prefixes).
                ? Collections.singletonList(blueHostSpec)
                // Green node isn't yet changed its name, so we need to try both possible IAM host options.
                : Arrays.asList(greenHostSpec, blueHostSpec);

            connectRouting.add(new SubstituteConnectRouting(
                blueHost,
                x.getValue(),
                greenHostSpecWithIp,
                iamHosts,
                isBlueHostInstance ? (iamHost) -> this.registerIamHost(greenHost, iamHost) : null));

            connectRouting.add(new SubstituteConnectRouting(
                this.getHostAndPort(
                    blueHost,
                    this.interimStatuses[x.getValue().getValue()].port),
                x.getValue(),
                greenHostSpecWithIp,
                iamHosts,
                isBlueHostInstance ? (iamHost) -> this.registerIamHost(greenHost, iamHost) : null));
          });
    }

    if (!this.greenDnsRemoved) {
      // New connect calls to green endpoints should be rejected.
      connectRouting.add(new RejectConnectRouting(null, BlueGreenRole.TARGET));
    }
  }

  protected BlueGreenStatus getStatusOfCompleted() {

    // We want to limit switchover duration to DEFAULT_POST_STATUS_DURATION_NANO.
    if (this.isSwitchoverTimerExpired()) {
      LOGGER.finest(Messages.get("bgd.switchoverTimeout"));
      if (this.rollback) {
        return this.getStatusOfCreated();
      }

      return new BlueGreenStatus(
          this.bgdId,
          BlueGreenPhases.COMPLETED,
          new ArrayList<>(),
          new ArrayList<>(),
          this.roleByEndpoint);
    }

    // BGD reports that it's completed but DNS hasn't yet updated completely.
    // Pretend that status isn't (yet) completed.
    if (!this.blueDnsUpdateCompleted || !this.greenDnsRemoved) {
      return this.getStatusOfPost();
    }

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhases.COMPLETED,
        new ArrayList<>(),
        new ArrayList<>(),
        this.roleByEndpoint);
  }

  protected void registerIamHost(String connectHost, String iamHost) {

    boolean differentNodeNames = connectHost != null && !connectHost.equals(iamHost);
    if (differentNodeNames) {
      boolean alreadyChangedName = this.iamHostSuccessfulConnects
          .computeIfAbsent(connectHost, (key) -> ConcurrentHashMap.newKeySet())
          .contains(iamHost);

      if (!alreadyChangedName) {
        this.greenNodeChangeNameTimes.computeIfAbsent(connectHost, (key) -> Instant.now());
        LOGGER.finest(() -> Messages.get("bgd.greenNodeChangedName", new Object[] {connectHost, iamHost}));
      }
    }

    this.iamHostSuccessfulConnects.computeIfAbsent(connectHost, (key) -> ConcurrentHashMap.newKeySet())
        .add(iamHost);

    if (differentNodeNames) {

      // Check all IAM host changed their names
      boolean allHostChangedNames = this.iamHostSuccessfulConnects.entrySet().stream()
          .filter(x -> !x.getValue().isEmpty())
          .allMatch(x -> x.getValue().stream().anyMatch(y -> !x.getKey().equals(y)));

      if (allHostChangedNames && !this.allGreenNodesChangedName.get()) {
        LOGGER.finest("allGreenNodesChangedName: true");
        this.allGreenNodesChangedName.set(true);
        this.storeGreenNodeChangeNameTime();
      }
    }
  }

  protected boolean isAlreadySuccessfullyConnected(String connectHost, String iamHost) {
    return this.iamHostSuccessfulConnects.computeIfAbsent(connectHost, (key) -> ConcurrentHashMap.newKeySet())
        .contains(iamHost);
  }

  /**
   * For testing purposes.
   */
  protected long getNanoTime() {
    return System.nanoTime();
  }

  protected void storePhaseTime(BlueGreenPhases phase) {
    if (phase == null) {
      return;
    }
    this.phaseTimeNano.putIfAbsent(
        phase.name() + (this.rollback ? " (rollback)" : ""),
        new PhaseTimeInfo(Instant.now(), this.getNanoTime(), phase));
  }

  protected void storeBlueDnsUpdateTime() {
    this.phaseTimeNano.putIfAbsent(
        "Blue DNS updated" + (this.rollback ? " (rollback)" : ""),
        new PhaseTimeInfo(Instant.now(), this.getNanoTime(), null));
  }

  protected void storeGreenDnsRemoveTime() {
    this.phaseTimeNano.putIfAbsent(
        "Green DNS removed" + (this.rollback ? " (rollback)" : ""),
        new PhaseTimeInfo(Instant.now(), this.getNanoTime(), null));
  }

  protected void storeGreenNodeChangeNameTime() {
    this.phaseTimeNano.putIfAbsent(
        "Green node certificates changed" + (this.rollback ? " (rollback)" : ""),
        new PhaseTimeInfo(Instant.now(), this.getNanoTime(), null));
  }

  protected void storeGreenTopologyChangeTime() {
    this.phaseTimeNano.putIfAbsent(
        "Green topology changed" + (this.rollback ? " (rollback)" : ""),
        new PhaseTimeInfo(Instant.now(), this.getNanoTime(), null));
  }

  protected void logSwitchoverFinalSummary() {
    final boolean switchoverCompleted =
        (!this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhases.COMPLETED)
        || (this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhases.CREATED);

    final boolean hasActiveSwitchoverPhases = this.phaseTimeNano.entrySet().stream()
        .anyMatch(x -> x.getValue().phase != null && x.getValue().phase.isActiveSwitchover());

    if (!switchoverCompleted || !hasActiveSwitchoverPhases) {
      return;
    }

    PhaseTimeInfo timeZero = this.rollback
        ? this.phaseTimeNano.get(BlueGreenPhases.PREPARATION.name())
        : this.phaseTimeNano.get(BlueGreenPhases.IN_PROGRESS.name());
    String divider = "----------------------------------------------------------------------------------\n";

    String logMessage =
        String.format("[bgdId: '%s']", this.bgdId)
        + "\n" + divider
        + String.format("%-28s %21s %31s\n",
        "timestamp",
        "time offset (ms)",
        "event")
        + divider
        + this.phaseTimeNano.entrySet().stream()
        .sorted(Comparator.comparing(y -> y.getValue().timestampNano))
        .map(x -> String.format("%28s %18s ms %31s",
            x.getValue().timestamp,
            timeZero == null ? "" : TimeUnit.NANOSECONDS.toMillis(x.getValue().timestampNano - timeZero.timestampNano),
            x.getKey()))
        .collect(Collectors.joining("\n"))
        + "\n" + divider;
    LOGGER.fine(logMessage);
  }

  protected void resetContextWhenCompleted() {
    final boolean switchoverCompleted =
        (!this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhases.COMPLETED)
        || (this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhases.CREATED);

    final boolean hasActiveSwitchoverPhases = this.phaseTimeNano.entrySet().stream()
        .anyMatch(x -> x.getValue().phase != null && x.getValue().phase.isActiveSwitchover());

    if (switchoverCompleted && hasActiveSwitchoverPhases) {
      LOGGER.finest(Messages.get("bgd.resetContext"));
      this.rollback = false;
      this.summaryStatus = null;
      this.latestStatusPhase = BlueGreenPhases.NOT_CREATED;
      this.phaseTimeNano.clear();
      this.blueDnsUpdateCompleted = false;
      this.greenDnsRemoved = false;
      this.greenTopologyChanged = false;
      this.allGreenNodesChangedName.set(false);
      this.postStatusEndTimeNano = 0;
      this.interimStatusHashes = new int[]{0, 0};
      this.lastContextHash = 0;
      this.interimStatuses = new BlueGreenInterimStatus[]{null, null};
      this.hostIpAddresses.clear();
      this.correspondingNodes.clear();
      this.roleByEndpoint.clear();
      this.iamHostSuccessfulConnects.clear();
      this.greenNodeChangeNameTimes.clear();
    }
  }

  protected void startSwitchoverTimer() {
    if (this.postStatusEndTimeNano == 0) {
      this.postStatusEndTimeNano = this.getNanoTime() + this.switchoverTimeoutNano;
    }
  }

  protected boolean isSwitchoverTimerExpired() {
    return this.postStatusEndTimeNano > 0 && this.postStatusEndTimeNano < this.getNanoTime();
  }

  protected void logCurrentContext() {
    if (!LOGGER.isLoggable(Level.FINEST)) {
      // We can skip this log message if FINEST level is in effect
      // and more detailed message is going to be printed few lines below.
      //noinspection ConstantValue
      LOGGER.fine(() -> String.format("[bgdId: '%s'] BG status: %s",
          this.bgdId,
          this.summaryStatus == null || this.summaryStatus.getCurrentPhase() == null
              ? "<null>"
              : this.summaryStatus.getCurrentPhase()));
    }

    LOGGER.finest(() -> String.format("[bgdId: '%s'] Summary status:\n%s",
        this.bgdId,
        this.summaryStatus == null ? "<null>" : this.summaryStatus.toString()));

    LOGGER.finest(() -> "Corresponding nodes:\n"
        + this.correspondingNodes.entrySet().stream()
        .map(x -> String.format("   %s -> %s",
            x.getKey(),
            x.getValue().getValue2().getHostAndPort()))
        .collect(Collectors.joining("\n")));

    LOGGER.finest(() -> "Phase times:\n"
        + this.phaseTimeNano.entrySet().stream()
        .map(x -> String.format("   %s -> %s",
            x.getKey(),
            x.getValue().timestamp))
        .collect(Collectors.joining("\n")));

    LOGGER.finest(() -> "Green node certificate change times:\n"
        + this.greenNodeChangeNameTimes.entrySet().stream()
        .map(x -> String.format("   %s -> %s",
            x.getKey(),
            x.getValue()))
        .collect(Collectors.joining("\n")));

    LOGGER.finest(() -> String.format("\n"
        + "   latestStatusPhase: %s\n"
        + "   blueDnsUpdateCompleted: %s\n"
        + "   greenDnsRemoved: %s\n"
        + "   greenNodeChangedName: %s\n"
        + "   greenTopologyChanged: %s",
        this.latestStatusPhase,
        this.blueDnsUpdateCompleted,
        this.greenDnsRemoved,
        this.allGreenNodesChangedName.get(),
        this.greenTopologyChanged));
  }

  private static class PhaseTimeInfo {
    public final Instant timestamp;
    public final long timestampNano;
    public final @Nullable BlueGreenPhases phase;

    public PhaseTimeInfo(final Instant timestamp, final long timestampNano, final @Nullable BlueGreenPhases phase) {
      this.timestamp = timestamp;
      this.timestampNano = timestampNano;
      this.phase = phase;
    }
  }
}
