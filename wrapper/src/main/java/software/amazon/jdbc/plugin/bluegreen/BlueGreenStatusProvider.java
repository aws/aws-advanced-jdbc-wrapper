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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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

  protected static final HashMap<BlueGreenPhases, IntervalType> checkIntervalTypeMap =
      new HashMap<BlueGreenPhases, IntervalType>() {
        {
          put(BlueGreenPhases.NOT_CREATED, IntervalType.BASELINE);
          put(BlueGreenPhases.CREATED, IntervalType.INCREASED);
          put(BlueGreenPhases.PREPARATION, IntervalType.INCREASED);
          put(BlueGreenPhases.IN_PROGRESS, IntervalType.HIGH);
          put(BlueGreenPhases.COMPLETED, IntervalType.INCREASED);
        }
      };

  protected final HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(new SimpleHostAvailabilityStrategy());

  protected final BlueGreenStatusMonitor[] monitors = { null, null };
  protected int[] statusHashes = { 0, 0 };
  protected BlueGreenInterimStatus[] interimStatuses = { null, null };
  protected final Map<String, Optional<String>> hostIpAddresses = new ConcurrentHashMap<>();
  protected final Map<String, Pair<HostSpec, HostSpec>> correspondingNodes = new ConcurrentHashMap<>();
  protected Map<String, BlueGreenRole> roleByEndpoint = new ConcurrentHashMap<>(); // all known endpoints; host and port
  protected BlueGreenStatus summaryStatus = null;

  protected boolean rollback = false;
  protected boolean blueDnsUpdateCompleted = false;
  protected boolean greenDnsRemoved = false;
  protected final ReentrantLock monitorInitLock = new ReentrantLock();
  protected final HashMap<IntervalType, Long> checkIntervalMap = new HashMap<>();

  protected final PluginService pluginService;
  protected final Properties props;

  public BlueGreenStatusProvider(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) {

    this.pluginService = pluginService;
    this.props = props;

    checkIntervalMap.put(IntervalType.BASELINE, BG_INTERVAL_BASELINE.getLong(props));
    checkIntervalMap.put(IntervalType.INCREASED, BG_INTERVAL_INCREASED.getLong(props));
    checkIntervalMap.put(IntervalType.HIGH, BG_INTERVAL_HIGH.getLong(props));

    final Dialect dialect = this.pluginService.getDialect();
    if (dialect instanceof SupportBlueGreen) {
      this.initMonitoring();
    } else {
      LOGGER.warning("Blue/Green Deployments isn't supported by this database engine.");
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

    //LOGGER.finest(String.format("[%s] %s", role, interimStatus));

    int statusHash = this.getStatusHash(interimStatus);
    if (this.statusHashes[role.getValue()] == statusHash) {
      // no changes detected
      return;
    }

    LOGGER.finest(String.format("[%s] %s", role, interimStatus));

    this.statusHashes[role.getValue()] = statusHash;

    BlueGreenPhases latestInterimPhase = this.interimStatuses[role.getValue()] == null
        ? BlueGreenPhases.NOT_CREATED
        : this.interimStatuses[role.getValue()].blueGreenPhase;
    if (interimStatus.blueGreenPhase.getValue() < latestInterimPhase.getValue()) {
      this.rollback = true;
      LOGGER.finest("Blue/Green deployment is in rollback mode.");
    }

    BlueGreenPhases latestStatusPhase = this.summaryStatus == null
        ? BlueGreenPhases.NOT_CREATED
        : this.summaryStatus.getCurrentPhase();

    // Do not allow status moves backward (that could be caused by updating blue/green nodes delays)
    if (interimStatus.blueGreenPhase.getValue() >= latestStatusPhase.getValue()) {
      latestInterimPhase = interimStatus.blueGreenPhase;
    }

    this.interimStatuses[role.getValue()] = interimStatus;

    this.hostIpAddresses.putAll(interimStatus.startIpAddressesByHostMap);
    interimStatus.endpoints.stream().forEach(x -> this.roleByEndpoint.put(x, role));

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

      int greenIndex = 0;
      this.correspondingNodes.clear();

      if (blueWriterHostSpec != null) {
        if (greenWriterHostSpec != null) {
          this.correspondingNodes.put(
              blueWriterHostSpec.getHostAndPort(), Pair.create(blueWriterHostSpec, greenWriterHostSpec));
          greenIndex++;
          greenIndex = greenIndex % sortedGreenReaderHostSpecs.size();
        } else {
          sortedGreenReaderHostSpecs.stream()
              .findFirst()
              .ifPresent(anyGreenHostSpec -> this.correspondingNodes.put(
                  blueWriterHostSpec.getHostAndPort(), Pair.create(blueWriterHostSpec, anyGreenHostSpec)));
        }
      }

      for (HostSpec blueHostSpec : sortedBlueReaderHostSpecs) {
        this.correspondingNodes.put(
            blueHostSpec.getHostAndPort(), Pair.create(blueHostSpec, sortedGreenReaderHostSpecs.get(greenIndex++)));
        greenIndex = greenIndex % sortedGreenReaderHostSpecs.size();
      }
    }

    // TODO: debug
//     if (latestInterimPhase == BlueGreenPhases.CREATED
//         && this.interimStatuses[BlueGreenRole.SOURCE.getValue()] != null
//         && !this.interimStatuses[BlueGreenRole.SOURCE.getValue()].startIpAddressesByHostMap.isEmpty()
//         && this.interimStatuses[BlueGreenRole.TARGET.getValue()] != null
//         && !this.interimStatuses[BlueGreenRole.TARGET.getValue()].startIpAddressesByHostMap.isEmpty()) {
//       latestInterimPhase = BlueGreenPhases.COMPLETED;
//     }

    switch (latestInterimPhase) {
      case NOT_CREATED:
        this.summaryStatus = new BlueGreenStatus(BlueGreenPhases.NOT_CREATED);
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
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfPreparation();
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.HIGH);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(true);
        });
        break;
      case IN_PROGRESS:
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfInProgress();
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.HIGH);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(true);
        });
        break;
      case POST:
        this.updateDnsFlags(role, interimStatus);
        this.summaryStatus = this.getStatusOfPost();
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.HIGH);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(true);
        });
        break;
      case COMPLETED:
        this.updateDnsFlags(role, interimStatus);

        //TODO: debug
        //this.blueDnsUpdateCompleted = true;
        //this.greenDnsRemoved = true;

        this.summaryStatus = this.getStatusOfCompleted();
        Arrays.stream(this.monitors).forEach(x -> {
          x.setIntervalType(IntervalType.BASELINE);
          x.setCollectIpAddresses(false);
          x.setCollectTopology(false);
          x.setUseIpAddress(!(this.blueDnsUpdateCompleted && this.greenDnsRemoved));
          if (this.blueDnsUpdateCompleted && this.greenDnsRemoved) {
            x.resetCollectedData();
          }
        });
        break;
      default:
        throw new UnsupportedOperationException("Unknown BG phase " + interimStatus.blueGreenPhase);
    }

    final BlueGreenStatus latestStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);
    this.pluginService.setStatus(BlueGreenStatus.class, this.summaryStatus, true);

    // Notify all waiting threads that status is changed.
    // Those waiting threads are waiting on an existing status so we need to notify on it.
    if (latestStatus != null) {
      synchronized (latestStatus) {
        latestStatus.notifyAll();
      }
    }

    LOGGER.finest("Summary status:\n" + (this.summaryStatus == null ? "<null>" : this.summaryStatus.toString()));
  }

  protected void updateDnsFlags(BlueGreenRole role, BlueGreenInterimStatus interimStatus) {
    if (role == BlueGreenRole.SOURCE) {
      if (this.blueDnsUpdateCompleted != interimStatus.allStartTopologyIpChanged) {
        LOGGER.finest("Blue DNS update completed.");
      }
      this.blueDnsUpdateCompleted = interimStatus.allStartTopologyIpChanged;
    }

    if (role == BlueGreenRole.TARGET) {
      if (this.greenDnsRemoved != interimStatus.allStartTopologyEndpointsRemoved) {
        LOGGER.finest("Green DNS update completed.");
      }
      this.greenDnsRemoved = interimStatus.allStartTopologyEndpointsRemoved;
    }
  }

  protected int getStatusHash(BlueGreenInterimStatus interimStatus) {

    int result = this.getStatusHash(1,
        interimStatus.blueGreenPhase == null ? "" : interimStatus.blueGreenPhase.toString());
    result = this.getStatusHash(result,
        interimStatus.version == null ? "" : interimStatus.version);
    result = this.getStatusHash(result, String.valueOf(interimStatus.port));
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
              Collections.singletonList(hostSpec)));
          connectRouting.add(new SubstituteConnectRouting(
              this.getHostAndPort(
                  x.getKey(),
                  this.interimStatuses[x.getValue().getValue()].port),
              x.getValue(),
              substituteHostSpecWithIp,
              Collections.singletonList(hostSpec)));
        });

    return new BlueGreenStatus(
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
    connectRouting.add(new HoldConnectRouting(null, BlueGreenRole.SOURCE));
    connectRouting.add(new HoldConnectRouting(null, BlueGreenRole.TARGET));

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

              connectRouting.add(new HoldConnectRouting(ipAddress, null));
              connectRouting.add(new HoldConnectRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null));

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

              connectRouting.add(new HoldConnectRouting(ipAddress, null));
              connectRouting.add(new HoldConnectRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null));

              return;
            }
          }

          connectRouting.add(new HoldConnectRouting(ipAddress, null));
        });

    // All blue and green traffic should be on hold.
    executeRouting.add(new HoldExecuteRouting(null, BlueGreenRole.SOURCE));
    executeRouting.add(new HoldExecuteRouting(null, BlueGreenRole.TARGET));

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

              executeRouting.add(new HoldExecuteRouting(ipAddress, null));
              executeRouting.add(new HoldExecuteRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null));

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

              executeRouting.add(new HoldExecuteRouting(ipAddress, null));
              executeRouting.add(new HoldExecuteRouting(
                  this.getHostAndPort(ipAddress, interimStatus.port),
                  null));

              return;
            }
          }

          executeRouting.add(new HoldExecuteRouting(ipAddress, null));
        });

    return new BlueGreenStatus(
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
            HostSpec substituteHostSpec = this.correspondingNodes.get(x.getKey()).getValue2();
            Optional<String> greenIp = this.hostIpAddresses.get(substituteHostSpec.getHost());
            HostSpec substituteHostSpecWithIp = greenIp == null || !greenIp.isPresent()
                ? substituteHostSpec
                : this.hostSpecBuilder.copyFrom(substituteHostSpec).host(greenIp.get()).build();

            connectRouting.add(new SubstituteConnectRouting(
                x.getKey(),
                x.getValue(),
                substituteHostSpecWithIp,
                Arrays.asList(substituteHostSpec, hostSpec)));
            connectRouting.add(new SubstituteConnectRouting(
                this.getHostAndPort(
                    x.getKey(),
                    this.interimStatuses[x.getValue().getValue()].port),
                x.getValue(),
                substituteHostSpecWithIp,
                Arrays.asList(substituteHostSpec, hostSpec)));
          });
    }

    if (!this.greenDnsRemoved) {
      // New connect calls to green endpoints should be rejected.
      connectRouting.add(new RejectConnectRouting(null, BlueGreenRole.TARGET));
    }
  }

  protected BlueGreenStatus getStatusOfCompleted() {
    List<ConnectRouting> connectRouting = new ArrayList<>();
    List<ExecuteRouting> executeRouting = new ArrayList<>();

    // Continue with rerouting of blue and green nodes until DNS gets updated.
    this.createPostRouting(connectRouting, executeRouting);

    return new BlueGreenStatus(
        BlueGreenPhases.COMPLETED,
        connectRouting,
        executeRouting,
        this.roleByEndpoint);
  }
}
