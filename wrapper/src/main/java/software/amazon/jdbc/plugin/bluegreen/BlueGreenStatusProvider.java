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
import software.amazon.jdbc.dialect.BlueGreenDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.bluegreen.routing.ConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.ExecuteRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.RejectConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.SubstituteConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.SuspendConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.SuspendExecuteRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.SuspendUntilCorrespondingNodeFoundConnectRouting;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.storage.StorageService;

public class BlueGreenStatusProvider {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusProvider.class.getName());

  public static final AwsWrapperProperty BG_INTERVAL_BASELINE_MS = new AwsWrapperProperty(
      "bgBaselineMs", "60000",
      "Baseline Blue/Green Deployment status checking interval (in msec).");

  public static final AwsWrapperProperty BG_INTERVAL_INCREASED_MS = new AwsWrapperProperty(
      "bgIncreasedMs", "1000",
      "Increased Blue/Green Deployment status checking interval (in msec).");

  public static final AwsWrapperProperty BG_INTERVAL_HIGH_MS = new AwsWrapperProperty(
      "bgHighMs", "100",
      "High Blue/Green Deployment status checking interval (in msec).");

  private static final String MONITORING_PROPERTY_PREFIX = "blue-green-monitoring-";
  private static final String DEFAULT_CONNECT_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));
  private static final String DEFAULT_SOCKET_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));

  public static final AwsWrapperProperty BG_SWITCHOVER_TIMEOUT_MS = new AwsWrapperProperty(
      "bgSwitchoverTimeoutMs", "180000", // 3min
      "Blue/Green Deployment switchover timeout (in msec).");

  public static final AwsWrapperProperty BG_SUSPEND_NEW_BLUE_CONNECTIONS = new AwsWrapperProperty(
      "bgSuspendNewBlueConnections", "false",
      "Enables Blue/Green Deployment switchover to suspend new blue connection requests"
          + " while the switchover process is in progress.");

  protected final HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(new SimpleHostAvailabilityStrategy());

  protected final BlueGreenStatusMonitor[] monitors = { null, null };
  protected int[] interimStatusHashes = { 0, 0 };
  protected int lastContextHash = 0;
  protected BlueGreenInterimStatus[] interimStatuses = { null, null };
  protected final Map<String, Optional<String>> hostIpAddresses = new ConcurrentHashMap<>();

  // The second parameter of Pair is null when no corresponding node is found.
  protected final Map<String, Pair<HostSpec, HostSpec>> correspondingNodes = new ConcurrentHashMap<>();

  // all known host names; host with no port
  protected final Map<String, BlueGreenRole> roleByHost = new ConcurrentHashMap<>();
  protected final Map<String, Set<String>> iamHostSuccessfulConnects = new ConcurrentHashMap<>();
  protected final Map<String, Instant> greenNodeChangeNameTimes = new ConcurrentHashMap<>();
  protected BlueGreenStatus summaryStatus = null;
  protected BlueGreenPhase latestStatusPhase = BlueGreenPhase.NOT_CREATED;

  protected boolean rollback = false;
  protected boolean blueDnsUpdateCompleted = false;
  protected boolean greenDnsRemoved = false;
  protected boolean greenTopologyChanged = false;
  protected final AtomicBoolean allGreenNodesChangedName = new AtomicBoolean(false);
  protected long postStatusEndTimeNano = 0;
  protected final ReentrantLock processStatusLock = new ReentrantLock();

  // Status check interval time in millis for each BlueGreenIntervalRate.
  protected final Map<BlueGreenIntervalRate, Long> statusCheckIntervalMap = new HashMap<>();
  protected final long switchoverTimeoutNano;
  protected final boolean suspendNewBlueConnectionsWhenInProgress;

  protected final FullServicesContainer servicesContainer;
  protected final StorageService storageService;
  protected final PluginService pluginService;
  protected final Properties props;
  protected final String bgdId;
  protected Map<String, PhaseTimeInfo> phaseTimeNano = new ConcurrentHashMap<>();
  protected final RdsUtils rdsUtils = new RdsUtils();

  public BlueGreenStatusProvider(
      final @NonNull FullServicesContainer servicesContainer,
      final @NonNull Properties props,
      final @NonNull String bgdId) {

    this.servicesContainer = servicesContainer;
    this.storageService = servicesContainer.getStorageService();
    this.pluginService = servicesContainer.getPluginService();
    this.props = props;
    this.bgdId = bgdId;

    this.statusCheckIntervalMap.put(BlueGreenIntervalRate.BASELINE, BG_INTERVAL_BASELINE_MS.getLong(props));
    this.statusCheckIntervalMap.put(BlueGreenIntervalRate.INCREASED, BG_INTERVAL_INCREASED_MS.getLong(props));
    this.statusCheckIntervalMap.put(BlueGreenIntervalRate.HIGH, BG_INTERVAL_HIGH_MS.getLong(props));

    this.switchoverTimeoutNano = TimeUnit.MILLISECONDS.toNanos(BG_SWITCHOVER_TIMEOUT_MS.getLong(props));
    this.suspendNewBlueConnectionsWhenInProgress = BG_SUSPEND_NEW_BLUE_CONNECTIONS.getBoolean(props);

    final Dialect dialect = this.pluginService.getDialect();
    if (dialect instanceof BlueGreenDialect) {
      this.initMonitoring();
    } else {
      LOGGER.warning(() -> Messages.get("bgd.unsupportedDialect",
          new Object[] {this.bgdId, dialect.getClass().getSimpleName()}));
    }
  }

  protected void initMonitoring() {
    monitors[BlueGreenRole.SOURCE.getValue()] =
        new BlueGreenStatusMonitor(
            BlueGreenRole.SOURCE,
            this.bgdId,
            this.pluginService.getCurrentHostSpec(),
            this.servicesContainer,
            this.getMonitoringProperties(),
            statusCheckIntervalMap,
            this::prepareStatus);
    monitors[BlueGreenRole.TARGET.getValue()] =
        new BlueGreenStatusMonitor(
            BlueGreenRole.TARGET,
            this.bgdId,
            this.pluginService.getCurrentHostSpec(),
            this.servicesContainer,
            this.getMonitoringProperties(),
            statusCheckIntervalMap,
            this::prepareStatus);
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

    monitoringConnProperties.putIfAbsent(PropertyDefinition.CONNECT_TIMEOUT.name, DEFAULT_CONNECT_TIMEOUT_MS);
    monitoringConnProperties.putIfAbsent(PropertyDefinition.SOCKET_TIMEOUT.name, DEFAULT_SOCKET_TIMEOUT_MS);

    return monitoringConnProperties;
  }

  protected void prepareStatus(
      final @NonNull BlueGreenRole role,
      final @NonNull BlueGreenInterimStatus interimStatus) {

    this.processStatusLock.lock();
    try {

      // Detect changes
      int statusHash = interimStatus.getCustomHashCode();
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

      // Update roleByHost based on provided host names.
      interimStatus.hostNames.forEach(x -> this.roleByHost.put(x.toLowerCase(), role));

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
    BlueGreenInterimStatus roleStatus = this.interimStatuses[role.getValue()];
    BlueGreenPhase latestInterimPhase = roleStatus == null ? BlueGreenPhase.NOT_CREATED : roleStatus.blueGreenPhase;

    if (latestInterimPhase != null
        && interimStatus.blueGreenPhase != null
        && interimStatus.blueGreenPhase.getValue() < latestInterimPhase.getValue()) {
      this.rollback = true;
      LOGGER.finest(() -> Messages.get("bgd.rollback", new Object[] {this.bgdId}));
    }

    if (interimStatus.blueGreenPhase == null) {
      return;
    }

    // Do not allow status moves backward (unless it's rollback).
    // That could be caused by updating blue/green nodes delays.
    if (!this.rollback) {
      if (interimStatus.blueGreenPhase.getValue() >= this.latestStatusPhase.getValue()) {
        this.latestStatusPhase = interimStatus.blueGreenPhase;
      }
    } else {
      if (interimStatus.blueGreenPhase.getValue() < this.latestStatusPhase.getValue()) {
        this.latestStatusPhase = interimStatus.blueGreenPhase;
      }
    }
  }

  protected void updateStatusCache() {
    final BlueGreenStatus latestStatus = this.storageService.get(BlueGreenStatus.class, this.bgdId);
    this.storageService.set(this.bgdId, this.summaryStatus);
    this.storePhaseTime(this.summaryStatus.getCurrentPhase());

    // Notify all waiting threads that status is updated.
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

    BlueGreenInterimStatus sourceInterimStatus = this.interimStatuses[BlueGreenRole.SOURCE.getValue()];
    BlueGreenInterimStatus targetInterimStatus = this.interimStatuses[BlueGreenRole.TARGET.getValue()];
    if (sourceInterimStatus != null && !Utils.isNullOrEmpty(sourceInterimStatus.startTopology)
        && targetInterimStatus != null && !Utils.isNullOrEmpty(targetInterimStatus.startTopology)) {

      HostSpec blueWriterHostSpec = getWriterHost(BlueGreenRole.SOURCE);
      HostSpec greenWriterHostSpec = getWriterHost(BlueGreenRole.TARGET);
      List<HostSpec> sortedBlueReaderHostSpecs = getReaderHosts(BlueGreenRole.SOURCE);
      List<HostSpec> sortedGreenReaderHostSpecs = getReaderHosts(BlueGreenRole.TARGET);

      if (blueWriterHostSpec != null) {
        // greenWriterHostSpec can be null but that will be handled properly by corresponding routing.
        this.correspondingNodes.put(
            blueWriterHostSpec.getHost(), Pair.create(blueWriterHostSpec, greenWriterHostSpec));
      }

      if (!Utils.isNullOrEmpty(sortedBlueReaderHostSpecs)) {
        // Map blue readers to green nodes.
        if (!Utils.isNullOrEmpty(sortedGreenReaderHostSpecs)) {
          int greenIndex = 0;
          for (HostSpec blueHostSpec : sortedBlueReaderHostSpecs) {
            this.correspondingNodes.put(
                blueHostSpec.getHost(), Pair.create(blueHostSpec, sortedGreenReaderHostSpecs.get(greenIndex++)));
            greenIndex %= sortedGreenReaderHostSpecs.size();
          }
        } else {
          // There's no green reader nodes. We have to map all blue reader nodes to the green writer.
          for (HostSpec blueHostSpec : sortedBlueReaderHostSpecs) {
            this.correspondingNodes.put(
                blueHostSpec.getHost(), Pair.create(blueHostSpec, greenWriterHostSpec));
          }
        }
      }
    }

    if (sourceInterimStatus != null && !Utils.isNullOrEmpty(sourceInterimStatus.hostNames)
        && targetInterimStatus != null && !Utils.isNullOrEmpty(targetInterimStatus.hostNames)) {

      Set<String> blueHosts = sourceInterimStatus.hostNames;
      Set<String> greenHosts = targetInterimStatus.hostNames;

      // Find corresponding cluster hosts
      String blueClusterHost = blueHosts.stream().filter(this.rdsUtils::isWriterClusterDns)
          .findFirst()
          .orElse(null);
      String greenClusterHost = greenHosts.stream().filter(this.rdsUtils::isWriterClusterDns)
          .findFirst()
          .orElse(null);

      if (!StringUtils.isNullOrEmpty(blueClusterHost) && !StringUtils.isNullOrEmpty(greenClusterHost)) {
        this.correspondingNodes.putIfAbsent(blueClusterHost,
            Pair.create(
                this.hostSpecBuilder.host(blueClusterHost).build(),
                this.hostSpecBuilder.host(greenClusterHost).build()));
      }

      // Find corresponding cluster reader hosts
      String blueClusterReaderHost = blueHosts.stream().filter(this.rdsUtils::isReaderClusterDns)
          .findFirst()
          .orElse(null);
      String greenClusterReaderHost = greenHosts.stream().filter(this.rdsUtils::isReaderClusterDns)
          .findFirst()
          .orElse(null);

      if (!StringUtils.isNullOrEmpty(blueClusterReaderHost)
          && !StringUtils.isNullOrEmpty(greenClusterReaderHost)) {
        this.correspondingNodes.putIfAbsent(blueClusterReaderHost,
            Pair.create(
                this.hostSpecBuilder.host(blueClusterReaderHost).build(),
                this.hostSpecBuilder.host(greenClusterReaderHost).build()));
      }

      blueHosts.stream().filter(this.rdsUtils::isRdsCustomClusterDns).forEach(blueHost -> {
        final String customClusterName = this.rdsUtils.getRdsClusterId(blueHost);
        if (customClusterName != null) {
          greenHosts.stream()
              .filter(x -> this.rdsUtils.isRdsCustomClusterDns(x)
                  && customClusterName.equals(
                      this.rdsUtils.removeGreenInstancePrefix(this.rdsUtils.getRdsClusterId(x))))
              .findFirst()
              .ifPresent(y -> this.correspondingNodes.putIfAbsent(blueHost,
                  Pair.create(
                      this.hostSpecBuilder.host(blueHost).build(),
                      this.hostSpecBuilder.host(y).build())));
        }
      });
    }
  }

  protected @Nullable HostSpec getWriterHost(final BlueGreenRole role) {
    BlueGreenInterimStatus interimStatus = this.interimStatuses[role.getValue()];
    if (interimStatus == null) {
      return null;
    }

    return interimStatus.startTopology.stream()
        .filter(x -> x.getRole() == HostRole.WRITER)
        .findFirst()
        .orElse(null);
  }

  protected @Nullable List<HostSpec> getReaderHosts(final BlueGreenRole role) {
    BlueGreenInterimStatus interimStatus = this.interimStatuses[role.getValue()];
    if (interimStatus == null) {
      return null;
    }

    return interimStatus.startTopology.stream()
        .filter(x -> x.getRole() != HostRole.WRITER)
        .sorted(Comparator.comparing(HostSpec::getHost))
        .collect(Collectors.toList());
  }

  protected void updateSummaryStatus(BlueGreenRole role, BlueGreenInterimStatus interimStatus) {
    switch (this.latestStatusPhase) {
      case NOT_CREATED:
        this.summaryStatus = new BlueGreenStatus(this.bgdId, BlueGreenPhase.NOT_CREATED);
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
          x.setIntervalRate(BlueGreenIntervalRate.BASELINE);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(false);
        });
        break;
      case CREATED:
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalRate(BlueGreenIntervalRate.INCREASED);
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
          x.setIntervalRate(BlueGreenIntervalRate.HIGH);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(true);
        });
        break;
      case COMPLETED:
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalRate(BlueGreenIntervalRate.BASELINE);
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
        interimStatus.hostNames == null
            ? ""
            : interimStatus.hostNames.stream()
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
        BlueGreenPhase.CREATED,
        new ArrayList<>(),
        new ArrayList<>(),
        this.roleByHost,
        this.correspondingNodes);
  }

  /**
   * New connect requests to blue: route to corresponding IP address.
   * New connect requests to green: route to corresponding IP address
   * New connect requests with IP address: default behaviour; no routing
   * Existing connections: default behaviour; no action
   * Execute JDBC calls: default behaviour; no action
   *
   * @return Blue/Green status
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

    List<ConnectRouting> connectRouting = this.addSubstituteBlueWithIpAddressConnectRouting();

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhase.PREPARATION,
        connectRouting,
        new ArrayList<>(),
        this.roleByHost,
        this.correspondingNodes);
  }

  protected List<ConnectRouting> addSubstituteBlueWithIpAddressConnectRouting() {
    List<ConnectRouting> connectRouting = new ArrayList<>();
    for (Map.Entry<String, BlueGreenRole> entry : this.roleByHost.entrySet()) {
      String host = entry.getKey();
      BlueGreenRole role = entry.getValue();
      Pair<HostSpec, HostSpec> nodePair = this.correspondingNodes.get(host);
      if (role != BlueGreenRole.SOURCE || nodePair == null) {
        continue;
      }

      HostSpec blueHostSpec = nodePair.getValue1();
      Optional<String> blueIp = this.hostIpAddresses.get(blueHostSpec.getHost());
      HostSpec blueIpHostSpec;
      if (blueIp == null || !blueIp.isPresent()) {
        blueIpHostSpec = blueHostSpec;
      } else {
        blueIpHostSpec = this.hostSpecBuilder.copyFrom(blueHostSpec).host(blueIp.get()).build();
      }

      connectRouting.add(new SubstituteConnectRouting(
          host,
          role,
          blueIpHostSpec,
          Collections.singletonList(blueHostSpec),
          null));

      BlueGreenInterimStatus interimStatus = this.interimStatuses[role.getValue()];
      if (interimStatus == null) {
        continue;
      }

      connectRouting.add(new SubstituteConnectRouting(
          this.getHostAndPort(host, interimStatus.port),
          role,
          blueIpHostSpec,
          Collections.singletonList(blueHostSpec),
          null));
    }

    return connectRouting;
  }

  /**
   * New connect requests to blue: suspend or route to corresponding IP address (depending on settings).
   * New connect requests to green: suspend
   * New connect requests with IP address: suspend
   * Existing connections: default behaviour; no action
   * Execute JDBC calls: suspend
   *
   * @return Blue/Green status
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

    List<ConnectRouting> connectRouting;

    if (this.suspendNewBlueConnectionsWhenInProgress) {
      // All blue and green connect calls should be suspended.
      connectRouting = new ArrayList<>();
      connectRouting.add(new SuspendConnectRouting(null, BlueGreenRole.SOURCE, this.bgdId));
    } else {
      // If we're not suspending new connections then, at least, we need to use IP addresses.
      connectRouting = this.addSubstituteBlueWithIpAddressConnectRouting();
    }

    connectRouting.add(new SuspendConnectRouting(null, BlueGreenRole.TARGET, this.bgdId));

    // All connect calls with IP address that belongs to blue or green node should be suspended.
    this.hostIpAddresses.values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .distinct()
        .forEach(ipAddress -> {

          BlueGreenInterimStatus interimStatus;

          if (this.suspendNewBlueConnectionsWhenInProgress) {
            // Try to confirm that the ipAddress belongs to one of the blue nodes
            interimStatus = this.interimStatuses[BlueGreenRole.SOURCE.getValue()];
            if (interimStatus != null) {
              if (interimStatus.startIpAddressesByHostMap.values().stream()
                  .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                  .map(x -> true)
                  .findFirst()
                  .orElse(false)) {

                connectRouting.add(new SuspendConnectRouting(ipAddress, null, this.bgdId));
                connectRouting.add(new SuspendConnectRouting(
                    this.getHostAndPort(ipAddress, interimStatus.port),
                    null,
                    this.bgdId));

                return;
              }
            }
          }

          // Try to confirm that the ipAddress belongs to one of the green nodes
          interimStatus = this.interimStatuses[BlueGreenRole.TARGET.getValue()];
          if (interimStatus != null) {
            if (interimStatus.startIpAddressesByHostMap.values().stream()
                .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                .map(x -> true)
                .findFirst()
                .orElse(false)) {

              connectRouting.add(new SuspendConnectRouting(ipAddress, null, this.bgdId));
              connectRouting.add(new SuspendConnectRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null,
                  this.bgdId));

              return;
            }
          }
        });

    // All blue and green traffic should be on hold.
    List<ExecuteRouting> executeRouting = new ArrayList<>();
    executeRouting.add(new SuspendExecuteRouting(null, BlueGreenRole.SOURCE, this.bgdId));
    executeRouting.add(new SuspendExecuteRouting(null, BlueGreenRole.TARGET, this.bgdId));

    // All traffic through connections with IP addresses that belong to blue or green nodes should be on hold.
    this.hostIpAddresses.values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .distinct()
        .forEach(ipAddress -> {

          // Try to confirm that the ipAddress belongs to one of the blue nodes
          BlueGreenInterimStatus interimStatus = this.interimStatuses[BlueGreenRole.SOURCE.getValue()];
          if (interimStatus != null) {
            if (interimStatus.startIpAddressesByHostMap.values().stream()
                .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                .map(x -> true)
                .findFirst()
                .orElse(false)) {

              executeRouting.add(new SuspendExecuteRouting(ipAddress, null, this.bgdId));
              executeRouting.add(new SuspendExecuteRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null,
                  this.bgdId));

              return;
            }
          }

          // Try to confirm that the ipAddress belongs to one of the green nodes
          interimStatus = this.interimStatuses[BlueGreenRole.TARGET.getValue()];
          if (interimStatus != null) {
            if (interimStatus.startIpAddressesByHostMap.values().stream()
                .filter(x -> x.isPresent() && x.get().equals(ipAddress))
                .map(x -> true)
                .findFirst()
                .orElse(false)) {

              executeRouting.add(new SuspendExecuteRouting(ipAddress, null, this.bgdId));
              executeRouting.add(new SuspendExecuteRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null,
                  this.bgdId));

              return;
            }
          }

          executeRouting.add(new SuspendExecuteRouting(ipAddress, null, this.bgdId));
        });

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhase.IN_PROGRESS,
        connectRouting,
        executeRouting,
        this.roleByHost,
        this.correspondingNodes);
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
    this.createPostRouting(connectRouting);

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhase.POST,
        connectRouting,
        executeRouting,
        this.roleByHost,
        this.correspondingNodes);
  }

  protected void createPostRouting(List<ConnectRouting> connectRouting) {

    if (!this.blueDnsUpdateCompleted || !this.allGreenNodesChangedName.get()) {
      // New connect calls to blue nodes should be routed to green nodes.
      this.roleByHost.entrySet().stream()
          .filter(x -> x.getValue() == BlueGreenRole.SOURCE)
          .filter(x -> this.correspondingNodes.containsKey(x.getKey()))
          .forEach(x -> {
            final String blueHost = x.getKey();
            final boolean isBlueHostInstance = rdsUtils.isRdsInstance(blueHost);

            Pair<HostSpec, HostSpec> nodePair = this.correspondingNodes.get(x.getKey());
            HostSpec blueHostSpec = nodePair == null ? null : nodePair.getValue1();
            HostSpec greenHostSpec = nodePair == null ? null : nodePair.getValue2();

            if (greenHostSpec == null) {

              // A corresponding host is not found. We need to suspend this call.
              connectRouting.add(new SuspendUntilCorrespondingNodeFoundConnectRouting(
                  blueHost,
                  x.getValue(),
                  this.bgdId));

              BlueGreenInterimStatus interimStatus = this.interimStatuses[x.getValue().getValue()];
              if (interimStatus != null) {
                connectRouting.add(new SuspendUntilCorrespondingNodeFoundConnectRouting(
                    this.getHostAndPort(blueHost, interimStatus.port),
                    x.getValue(),
                    this.bgdId));
              }
            } else {
              final String greenHost = greenHostSpec.getHost();
              Optional<String> greenIp = this.hostIpAddresses.get(greenHostSpec.getHost());
              HostSpec greenHostSpecWithIp = greenIp == null || !greenIp.isPresent()
                  ? greenHostSpec
                  : this.hostSpecBuilder.copyFrom(greenHostSpec).host(greenIp.get()).build();

              // Check whether green host is already been connected with blue (no-prefixes) IAM host name.
              List<HostSpec> iamHosts;
              if (this.isAlreadySuccessfullyConnected(greenHost, blueHost)) {
                // Green node has already changed its name, and it's not a new blue node (no prefixes).
                iamHosts = blueHostSpec == null ? null : Collections.singletonList(blueHostSpec);
              } else {
                // Green node isn't yet changed its name, so we need to try both possible IAM host options.
                iamHosts = blueHostSpec == null
                    ? Collections.singletonList(greenHostSpec)
                    : Arrays.asList(greenHostSpec, blueHostSpec);
              }

              connectRouting.add(new SubstituteConnectRouting(
                  blueHost,
                  x.getValue(),
                  greenHostSpecWithIp,
                  iamHosts,
                  isBlueHostInstance ? (iamHost) -> this.registerIamHost(greenHost, iamHost) : null));

              BlueGreenInterimStatus interimStatus = this.interimStatuses[x.getValue().getValue()];
              if (interimStatus != null) {
                connectRouting.add(new SubstituteConnectRouting(
                    this.getHostAndPort(blueHost, interimStatus.port),
                    x.getValue(),
                    greenHostSpecWithIp,
                    iamHosts,
                    isBlueHostInstance ? (iamHost) -> this.registerIamHost(greenHost, iamHost) : null));
              }
            }
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
          BlueGreenPhase.COMPLETED,
          new ArrayList<>(),
          new ArrayList<>(),
          this.roleByHost,
          this.correspondingNodes);
    }

    // BGD reports that it's completed but DNS hasn't yet updated completely.
    // Pretend that status isn't (yet) completed.
    if (!this.blueDnsUpdateCompleted || !this.greenDnsRemoved) {
      return this.getStatusOfPost();
    }

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhase.COMPLETED,
        new ArrayList<>(),
        new ArrayList<>(),
        this.roleByHost,
        new HashMap<>());
  }

  protected void registerIamHost(String connectHost, String iamHost) {

    boolean differentNodeNames = connectHost != null && !connectHost.equals(iamHost);
    if (differentNodeNames) {
      if (!isAlreadySuccessfullyConnected(connectHost, iamHost)) {
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
   *
   * @return current time in nanoseconds
   */
  protected long getNanoTime() {
    return System.nanoTime();
  }

  protected void storePhaseTime(BlueGreenPhase phase) {
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
        (!this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhase.COMPLETED)
        || (this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhase.CREATED);

    final boolean hasActiveSwitchoverPhases = this.phaseTimeNano.entrySet().stream()
        .anyMatch(x -> x.getValue().phase != null && x.getValue().phase.isActiveSwitchoverOrCompleted());

    if (!switchoverCompleted || !hasActiveSwitchoverPhases) {
      return;
    }

    BlueGreenPhase timeZeroPhase = this.rollback ? BlueGreenPhase.PREPARATION : BlueGreenPhase.IN_PROGRESS;
    String timeZeroKey = this.rollback ? timeZeroPhase.name() + " (rollback)" : timeZeroPhase.name();
    PhaseTimeInfo timeZero = this.phaseTimeNano.get(timeZeroKey);
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
        (!this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhase.COMPLETED)
        || (this.rollback && this.summaryStatus.getCurrentPhase() == BlueGreenPhase.CREATED);

    final boolean hasActiveSwitchoverPhases = this.phaseTimeNano.entrySet().stream()
        .anyMatch(x -> x.getValue().phase != null && x.getValue().phase.isActiveSwitchoverOrCompleted());

    if (switchoverCompleted && hasActiveSwitchoverPhases) {
      LOGGER.finest(Messages.get("bgd.resetContext"));
      this.rollback = false;
      this.summaryStatus = null;
      this.latestStatusPhase = BlueGreenPhase.NOT_CREATED;
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
      this.roleByHost.clear();
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
            x.getValue().getValue2() == null ? "<null>" : x.getValue().getValue2().getHostAndPort()))
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

  public static class PhaseTimeInfo {
    public final Instant timestamp;
    public final long timestampNano;
    public final @Nullable BlueGreenPhase phase;

    public PhaseTimeInfo(final Instant timestamp, final long timestampNano, final @Nullable BlueGreenPhase phase) {
      this.timestamp = timestamp;
      this.timestampNano = timestampNano;
      this.phase = phase;
    }
  }
}
