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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
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
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
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
  private static final long DEFAULT_POST_STATUS_DURATION_NANO = TimeUnit.MINUTES.toNanos(3); // TODO: make it configurable?

  protected final HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(new SimpleHostAvailabilityStrategy());

  protected final BlueGreenStatusMonitor[] monitors = { null, null };
  protected int[] statusHashes = { 0, 0 };
  protected BlueGreenInterimStatus[] interimStatuses = { null, null };
  protected final Map<String, Optional<String>> hostIpAddresses = new ConcurrentHashMap<>();
  protected final Map<String, Pair<HostSpec, HostSpec>> correspondingNodes = new ConcurrentHashMap<>();
  protected final Map<String, BlueGreenRole> roleByEndpoint = new ConcurrentHashMap<>(); // all known endpoints; host and port
  protected final Map<String, Set<String>> iamHostSuccessfulConnects = new ConcurrentHashMap<>();
  protected BlueGreenStatus summaryStatus = null;
  protected BlueGreenPhases latestStatusPhase = BlueGreenPhases.NOT_CREATED;

  protected boolean rollback = false;
  protected boolean blueDnsUpdateCompleted = false;
  protected boolean greenDnsRemoved = false;
  protected final AtomicBoolean greenNodeChangedName = new AtomicBoolean(false);
  protected long postStatusEndTimeNano = 0;
  protected final ReentrantLock monitorInitLock = new ReentrantLock();
  protected final ReentrantLock processStatusLock = new ReentrantLock();
  protected final Map<IntervalType, Long> checkIntervalMap = new HashMap<>();

  protected final PluginService pluginService;
  protected final Properties props;
  protected final String bgdId;
  protected Map<String, Pair<Instant, Long>> phaseTimeNano = new ConcurrentHashMap<>();
  protected boolean loggedSwitchoverSummary = false;

  public BlueGreenStatusProvider(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull String bgdId) {

    this.pluginService = pluginService;
    this.props = props;
    this.bgdId = bgdId;

    checkIntervalMap.put(IntervalType.BASELINE, BG_INTERVAL_BASELINE.getLong(props));
    checkIntervalMap.put(IntervalType.INCREASED, BG_INTERVAL_INCREASED.getLong(props));
    checkIntervalMap.put(IntervalType.HIGH, BG_INTERVAL_HIGH.getLong(props));

    final Dialect dialect = this.pluginService.getDialect();
    if (dialect instanceof SupportBlueGreen) {
      this.initMonitoring();
    } else {
      LOGGER.warning(() -> String.format(
          "[bgdId: '%s'] Blue/Green Deployments isn't supported by database dialect %s.",
          this.bgdId,
          dialect.getClass().getSimpleName()));
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
      //LOGGER.finest(() -> String.format("#1 [bgdId: '%s', role: %s] %s", this.bgdId, role, interimStatus));

      int statusHash = this.getStatusHash(interimStatus);
      if (this.statusHashes[role.getValue()] == statusHash) {
        // no changes detected
        //LOGGER.finest("No changes detected.");
        return;
      }

      LOGGER.finest(() -> String.format("[bgdId: '%s', role: %s] %s", this.bgdId, role, interimStatus));

      this.statusHashes[role.getValue()] = statusHash;

      BlueGreenPhases latestInterimPhase = this.interimStatuses[role.getValue()] == null
          ? BlueGreenPhases.NOT_CREATED
          : this.interimStatuses[role.getValue()].blueGreenPhase;

      if (latestInterimPhase != null
          && interimStatus.blueGreenPhase != null
          && interimStatus.blueGreenPhase.getValue() < latestInterimPhase.getValue()) {
        this.rollback = true;
        LOGGER.finest(() -> String.format("[bgdId: '%s'] Blue/Green deployment is in rollback mode.", this.bgdId));
      }

      // Do not allow status moves backward (unless it's rollback).
      // That could be caused by updating blue/green nodes delays.
      if (!this.rollback) {
        if (interimStatus.blueGreenPhase != null
            && interimStatus.blueGreenPhase.getValue() >= this.latestStatusPhase.getValue()) {
          this.latestStatusPhase = interimStatus.blueGreenPhase;
          LOGGER.finest("latestStatusPhase=" + this.latestStatusPhase);
        }
      } else {
        if (interimStatus.blueGreenPhase != null
            && interimStatus.blueGreenPhase.getValue() < this.latestStatusPhase.getValue()) {
          this.latestStatusPhase = interimStatus.blueGreenPhase;
          LOGGER.finest("(rollback) latestStatusPhase=" + this.latestStatusPhase);
        }
      }

      this.interimStatuses[role.getValue()] = interimStatus;

      this.hostIpAddresses.putAll(interimStatus.startIpAddressesByHostMap);
      interimStatus.endpoints.stream().forEach(x -> this.roleByEndpoint.put(x.toLowerCase(), role));

      // Update corresponding nodes
      // Blue writer node is mapped to a green writer node.
      // Blue reader nodes are mapped to green reader nodes.
      if (this.interimStatuses[BlueGreenRole.SOURCE.getValue()] != null
          && !Utils.isNullOrEmpty(this.interimStatuses[BlueGreenRole.SOURCE.getValue()].currentTopology)
          && this.interimStatuses[BlueGreenRole.TARGET.getValue()] != null
          && !Utils.isNullOrEmpty(this.interimStatuses[BlueGreenRole.TARGET.getValue()].currentTopology)) {

        HostSpec blueWriterHostSpec =
            this.interimStatuses[BlueGreenRole.SOURCE.getValue()].currentTopology.stream()
                .filter(x -> x.getRole() == HostRole.WRITER)
                .findFirst()
                .orElse(null);

        HostSpec greenWriterHostSpec =
            this.interimStatuses[BlueGreenRole.TARGET.getValue()].currentTopology.stream()
                .filter(x -> x.getRole() == HostRole.WRITER)
                .findFirst()
                .orElse(null);

        List<HostSpec> sortedBlueReaderHostSpecs =
            this.interimStatuses[BlueGreenRole.SOURCE.getValue()].currentTopology.stream()
                .filter(x -> x.getRole() != HostRole.WRITER)
                .sorted(Comparator.comparing(HostSpec::getHostAndPort))
                .collect(Collectors.toList());

        List<HostSpec> sortedGreenReaderHostSpecs =
            this.interimStatuses[BlueGreenRole.TARGET.getValue()].currentTopology.stream()
                .filter(x -> x.getRole() != HostRole.WRITER)
                .sorted(Comparator.comparing(HostSpec::getHostAndPort))
                .collect(Collectors.toList());

        this.correspondingNodes.clear();

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
              blueHostSpec.getHostAndPort(), Pair.create(blueHostSpec, sortedGreenReaderHostSpecs.get(greenIndex++)));
          greenIndex = greenIndex % sortedGreenReaderHostSpecs.size();
        }
      }

      switch (this.latestStatusPhase) {
        case PREPARATION:
        case IN_PROGRESS:
        case POST:
        case COMPLETED:
          this.updateDnsFlags(role, interimStatus);
        default:
          // do nothing
      }

      switch (this.latestStatusPhase) {
        case NOT_CREATED:
          this.summaryStatus = new BlueGreenStatus(this.bgdId, BlueGreenPhases.NOT_CREATED);
          Arrays.stream(this.monitors).forEach(x -> {
            x.setIntervalType(IntervalType.BASELINE);
            x.setCollectIpAddresses(false);
            x.setCollectTopology(false);
            x.setUseIpAddress(false);
          });
          break;
        case CREATED:
          this.summaryStatus = this.getStatusOfCreated();
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
          if (this.postStatusEndTimeNano == 0) {
            this.postStatusEndTimeNano = this.getNanoTime() + DEFAULT_POST_STATUS_DURATION_NANO;
          }
          this.summaryStatus = this.getStatusOfPreparation();
          Arrays.stream(this.monitors).forEach(x -> {
            x.setIntervalType(IntervalType.HIGH);
            x.setCollectIpAddresses(false);
            x.setCollectTopology(false);
            x.setUseIpAddress(true);
          });
          break;
        case IN_PROGRESS:
          this.summaryStatus = this.getStatusOfInProgress();
          Arrays.stream(this.monitors).forEach(x -> {
            x.setIntervalType(IntervalType.HIGH);
            x.setCollectIpAddresses(false);
            x.setCollectTopology(false);
            x.setUseIpAddress(true);
          });
          break;
        case POST:
          this.summaryStatus = this.getStatusOfPost();
          Arrays.stream(this.monitors).forEach(x -> {
            x.setIntervalType(IntervalType.HIGH);
            x.setCollectIpAddresses(false);
            x.setCollectTopology(false);
            x.setUseIpAddress(true);
          });
          break;
        case COMPLETED:
          this.summaryStatus = this.getStatusOfCompleted();
          Arrays.stream(this.monitors).forEach(x -> {
            x.setIntervalType(IntervalType.BASELINE);
            x.setCollectIpAddresses(false);
            x.setCollectTopology(false);
            x.setUseIpAddress(false);

            if (this.summaryStatus.getCurrentPhase() == BlueGreenPhases.COMPLETED) {
              x.resetCollectedData();
            }
          });
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("[bgdId: '%s'] Unknown BG phase '%s'", this.bgdId, this.latestStatusPhase));
      }

      final BlueGreenStatus latestStatus = this.pluginService.getStatus(BlueGreenStatus.class, this.bgdId);
      this.pluginService.setStatus(BlueGreenStatus.class, this.summaryStatus, this.bgdId);
      this.storePhaseTime(this.summaryStatus.getCurrentPhase());

      // Notify all waiting threads that status is changed.
      // Those waiting threads are waiting on an existing status so we need to notify on it.
      if (latestStatus != null) {
        synchronized (latestStatus) {
          latestStatus.notifyAll();
        }
      }

      LOGGER.finest(() -> String.format("[bgdId: '%s'] Summary status:\n%s",
          this.bgdId,
          this.summaryStatus == null ? "<null>" : this.summaryStatus.toString()));

      LOGGER.finest(() -> "Corresponding nodes:\n"
          + this.correspondingNodes.entrySet().stream()
          .map(x -> String.format("%s -> %s",
              x.getKey(),
              x.getValue().getValue2().getHostAndPort()))
          .collect(Collectors.joining("\n")));

      LOGGER.finest(() -> "Phase time:\n"
          + this.phaseTimeNano.entrySet().stream()
          .map(x -> String.format("%s -> %s",
              x.getKey(),
              x.getValue().getValue1()))
          .collect(Collectors.joining("\n")));

      LOGGER.finest(() -> String.format(
          "\nlatestStatusPhase: %s\nblueDnsUpdateCompleted: %s\ngreenDnsRemoved: %s\ngreenNodeChangedName: %s",
          this.latestStatusPhase,
          this.blueDnsUpdateCompleted,
          this.greenDnsRemoved,
          this.greenNodeChangedName.get()));

      if (this.summaryStatus.getCurrentPhase() == BlueGreenPhases.COMPLETED) {
        this.logSwitchoverSummary();
      }

    } finally {
      this.processStatusLock.unlock();
    }
  }

  protected void updateDnsFlags(BlueGreenRole role, BlueGreenInterimStatus interimStatus) {
    if (role == BlueGreenRole.SOURCE && !this.blueDnsUpdateCompleted && interimStatus.allStartTopologyIpChanged) {
      LOGGER.finest(() -> String.format("[bgdId: '%s'] Blue DNS update completed.", this.bgdId));
      this.blueDnsUpdateCompleted = true;
      this.storeBlueDnsUpdateTime();
    }

    if (role == BlueGreenRole.TARGET && !this.greenDnsRemoved && interimStatus.allStartTopologyEndpointsRemoved) {
      LOGGER.finest(() -> String.format("[bgdId: '%s'] Green DNS update completed.", this.bgdId));
      this.greenDnsRemoved = true;
      this.storeGreenDnsRemoveTime();
    }
  }

  protected int getStatusHash(BlueGreenInterimStatus interimStatus) {

    int result = this.getStatusHash(1,
        interimStatus.blueGreenPhase == null ? "" : interimStatus.blueGreenPhase.toString());
    result = this.getStatusHash(result,
        interimStatus.version == null ? "" : interimStatus.version);
    result = this.getStatusHash(result, String.valueOf(interimStatus.port));
    result = this.getStatusHash(result, String.valueOf(interimStatus.allStartTopologyIpChanged));
    result = this.getStatusHash(result, String.valueOf(interimStatus.allStartTopologyEndpointsRemoved));

    // Provider context
    result = this.getStatusHash(result, String.valueOf(this.greenNodeChangedName.get()));
    result = this.getStatusHash(result, String.valueOf(this.iamHostSuccessfulConnects.size()));

    result = this.getStatusHash(result,
        interimStatus.endpoints == null
            ? ""
            : interimStatus.endpoints.stream()
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getStatusHash(result,
        interimStatus.startTopology == null
            ? ""
            : interimStatus.startTopology.stream()
                .map(x -> x.getHostAndPort() + x.getRole())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getStatusHash(result,
        interimStatus.currentTopology == null
            ? ""
            : interimStatus.currentTopology.stream()
                .map(x -> x.getHostAndPort() + x.getRole())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getStatusHash(result,
        interimStatus.startIpAddressesByHostMap == null
            ? ""
            : interimStatus.startIpAddressesByHostMap.entrySet().stream()
                .map(x -> x.getKey() + x.getValue())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    result = this.getStatusHash(result,
        interimStatus.currentIpAddressesByHostMap == null
            ? ""
            : interimStatus.currentIpAddressesByHostMap.entrySet().stream()
                .map(x -> x.getKey() + x.getValue())
                .sorted(Comparator.comparing(x -> x))
                .collect(Collectors.joining(",")));
    return result;
  }

  protected int getStatusHash(int currentHash, String val) {
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

  // New connect requests to blue: route to corresponding IP address
  // New connect requests to green: route to corresponding IP address
  // New connect requests with IP address: default behaviour; no routing
  // Existing connections: default behaviour; no action
  // Execute JDBC calls: default behaviour; no action
  protected BlueGreenStatus getStatusOfPreparation() {

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

  // New connect requests to blue: route to corresponding IP address
  // New connect requests to green: route to corresponding IP address
  // New connect requests with IP address: default behaviour; no routing
  // Existing connections: default behaviour; no action
  // Execute JDBC calls: default behaviour; no action
  protected BlueGreenStatus getStatusOfInProgress() {

    List<ConnectRouting> connectRouting = new ArrayList<>();
    List<ExecuteRouting> executeRouting = new ArrayList<>();

    // All blue and green connect calls should be on hold.
    connectRouting.add(new HoldConnectRouting(null, BlueGreenRole.SOURCE, this.bgdId));
    connectRouting.add(new HoldConnectRouting(null, BlueGreenRole.TARGET, this.bgdId));

    // All connect calls with IP address that belongs to blue or green node should be on hold.
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

              connectRouting.add(new HoldConnectRouting(ipAddress, null, this.bgdId));
              connectRouting.add(new HoldConnectRouting(
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

    if (!this.blueDnsUpdateCompleted) {
      // New connect calls to blue nodes should be routed to green nodes.
      this.roleByEndpoint.entrySet().stream()
          .filter(x -> x.getValue() == BlueGreenRole.SOURCE)
          .filter(x -> this.correspondingNodes.containsKey(x.getKey()))
          .forEach(x -> {
            HostSpec hostSpec = this.correspondingNodes.get(x.getKey()).getValue1();
            HostSpec greenHostSpec = this.correspondingNodes.get(x.getKey()).getValue2();
            final String greenHost = greenHostSpec.getHost();
            Optional<String> greenIp = this.hostIpAddresses.get(greenHostSpec.getHost());
            HostSpec greenHostSpecWithIp = greenIp == null || !greenIp.isPresent()
                ? greenHostSpec
                : this.hostSpecBuilder.copyFrom(greenHostSpec).host(greenIp.get()).build();

            // Check whether green host is already been connected with blue (no-prefixes) IAM host name.
            List<HostSpec> iamHosts = this.isAlreadySuccessfullyConnected(greenHost, x.getKey())
                // Green node has already changed its name and not it's a new blue node (no prefixes).
                ? Collections.singletonList(hostSpec)
                // Green node isn't yet changed its name, so we need to try both possible IAM host options.
                : Arrays.asList(greenHostSpec, hostSpec);

            connectRouting.add(new SubstituteConnectRouting(
                x.getKey(),
                x.getValue(),
                greenHostSpecWithIp,
                iamHosts,
                (iamHost) -> this.registerIamHost(greenHost, iamHost)));

            connectRouting.add(new SubstituteConnectRouting(
                this.getHostAndPort(
                    x.getKey(),
                    this.interimStatuses[x.getValue().getValue()].port),
                x.getValue(),
                greenHostSpecWithIp,
                iamHosts,
                (iamHost) -> this.registerIamHost(greenHost, iamHost)));
          });
    }

    if (!this.greenDnsRemoved) {
      // New connect calls to green endpoints should be rejected.
      connectRouting.add(new RejectConnectRouting(null, BlueGreenRole.TARGET));
    }
  }

  protected BlueGreenStatus getStatusOfCompleted() {

    // BGD reports that it's completed but DNS hasn't yet updated completely.
    // Pretend that status isn't (yet) completed.
    if (!this.blueDnsUpdateCompleted
        || !this.greenDnsRemoved
        || (!this.iamHostSuccessfulConnects.isEmpty() && !this.greenNodeChangedName.get())) {

      // We want to limit duration in POST status up to DEFAULT_POST_STATUS_DURATION_NANO.
      if (this.postStatusEndTimeNano > 0 && this.postStatusEndTimeNano >= this.getNanoTime()) {
        LOGGER.finest("BG switchover max time is over."); // TODO: debug
        return this.getStatusOfPost();
      }
    }

    return new BlueGreenStatus(
        this.bgdId,
        BlueGreenPhases.COMPLETED,
        new ArrayList<>(),
        new ArrayList<>(),
        this.roleByEndpoint);
  }

  protected void registerIamHost(String greenHost, String iamHost) {
    if (greenHost != null && !greenHost.equals(iamHost)) {
      if (!this.greenNodeChangedName.get()) {
        LOGGER.finest(() -> String.format("Green node %s has changed its name to %s", greenHost, iamHost));
        LOGGER.finest("greenNodeChangedName: true");
      }
      this.greenNodeChangedName.set(true);
      this.storeGreenNodeChangeNameTime();
    }
    this.iamHostSuccessfulConnects.computeIfAbsent(greenHost, (key) -> ConcurrentHashMap.newKeySet())
        .add(iamHost);
  }

  protected boolean isAlreadySuccessfullyConnected(String greenHost, String iamHost) {
    return this.iamHostSuccessfulConnects.computeIfAbsent(greenHost, (key) -> ConcurrentHashMap.newKeySet())
        .contains(iamHost);
  }

  // For testing purposes
  protected long getNanoTime() {
    return System.nanoTime();
  }

  protected void storePhaseTime(BlueGreenPhases phase) {
    if (phase == null) {
      return;
    }
    this.phaseTimeNano.putIfAbsent(phase.name(), Pair.create(Instant.now(), this.getNanoTime()));
  }

  protected void storeBlueDnsUpdateTime() {
    this.phaseTimeNano.putIfAbsent("Blue DNS updated", Pair.create(Instant.now(), this.getNanoTime()));
  }

  protected void storeGreenDnsRemoveTime() {
    this.phaseTimeNano.putIfAbsent("Green DNS removed", Pair.create(Instant.now(), this.getNanoTime()));
  }

  protected void storeGreenNodeChangeNameTime() {
    this.phaseTimeNano.putIfAbsent("Green node changed name", Pair.create(Instant.now(), this.getNanoTime()));
  }

  protected void logSwitchoverSummary() {
    if (this.loggedSwitchoverSummary) {
      return;
    }

    this.loggedSwitchoverSummary = true;
    Pair<Instant, Long> timeZero = this.phaseTimeNano.get(BlueGreenPhases.IN_PROGRESS.name());
    String divider = "---------------------------------------------------------------------------\n";

    String logMessage =
        "\n" + divider
        + String.format("%-28s %15s %25s\n",
        "timestamp",
        "time offset (ms)",
        "event")
        + divider
        + this.phaseTimeNano.entrySet().stream()
        .sorted(Comparator.comparing(y -> y.getValue().getValue2()))
        .map(x -> String.format("%28s %18s ms %25s",
            x.getValue().getValue1(),
            timeZero == null ? "" : TimeUnit.NANOSECONDS.toMillis(x.getValue().getValue2() - timeZero.getValue2()),
            x.getKey()))
        .collect(Collectors.joining("\n"))
        + "\n" + divider;
    LOGGER.info(logMessage);
  }
}
