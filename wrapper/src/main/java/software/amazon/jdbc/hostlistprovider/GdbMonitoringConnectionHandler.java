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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUtils;

/**
 * GDB-specific implementation of {@link MonitoringConnectionHandler} that uses
 * {@link GdbMonitoringConnectionPriority} with region and primary/secondary awareness.
 */
public class GdbMonitoringConnectionHandler implements MonitoringConnectionHandler {

  private static final Logger LOGGER = Logger.getLogger(GdbMonitoringConnectionHandler.class.getName());

  public static final AwsWrapperProperty GDB_MONITORING_CONNECTION_PRIORITY =
      new AwsWrapperProperty(
          "gdbMonitoringConnectionPriority", "strict-writer-primary",
          "Comma-separated list of monitoring connection priorities for Global Aurora Databases, "
              + "in order of preference. Possible values: strict-writer-primary, strict-reader-primary, "
              + "strict-reader-secondary, strict-writer-<region>, strict-reader-<region>, <region>.",
          false,
          null);

  static {
    PropertyDefinition.registerPluginProperties(GdbMonitoringConnectionHandler.class);
  }

  protected final AtomicConnection monitoringConnection;
  protected final AtomicConnection upgradeConnection;
  protected final List<GdbMonitoringConnectionPriority> priorities;
  protected final PluginService pluginService;
  protected final TopologyUtils topologyUtils;
  protected final Properties monitoringProperties;
  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final AtomicReference<HostSpec> writerHostSpec;
  protected final @Nullable Runnable upgradeReadyNotifier;

  protected int currentPriorityIndex = -1;
  protected volatile Future<?> upgradeFuture = null;
  protected volatile HostSpec upgradeConnectedHost = null;

  /**
   * Formats a priority index for logging. {@link Integer#MAX_VALUE} is shown as {@code <none>}
   * to avoid confusing users with a 10-digit number.
   */
  protected static String formatPriorityIndex(final int index) {
    return index == Integer.MAX_VALUE ? "<none>" : Integer.toString(index);
  }

  public GdbMonitoringConnectionHandler(
      final AtomicConnection monitoringConnection,
      final PluginService pluginService,
      final TopologyUtils topologyUtils,
      final Properties properties,
      final Properties monitoringProperties,
      final AtomicReference<HostSpec> writerHostSpec) {
    this(monitoringConnection, pluginService, topologyUtils, properties, monitoringProperties, writerHostSpec, null);
  }

  public GdbMonitoringConnectionHandler(
      final AtomicConnection monitoringConnection,
      final PluginService pluginService,
      final TopologyUtils topologyUtils,
      final Properties properties,
      final Properties monitoringProperties,
      final AtomicReference<HostSpec> writerHostSpec,
      final @Nullable Runnable upgradeReadyNotifier) {
    this.monitoringConnection = monitoringConnection;
    this.upgradeConnection = new AtomicConnection(null, false);
    this.pluginService = pluginService;
    this.topologyUtils = topologyUtils;
    this.monitoringProperties = monitoringProperties;
    this.writerHostSpec = writerHostSpec;
    this.upgradeReadyNotifier = upgradeReadyNotifier;
    this.priorities = GdbMonitoringConnectionPriority.parseList(
        GDB_MONITORING_CONNECTION_PRIORITY.getString(properties));
  }

  @Override
  public boolean acceptConnection(Connection conn, boolean isWriter, HostSpec hostSpec) {
    final int priorityIndex = determinePriorityIndex(hostSpec, isWriter);
    // A connection that doesn't satisfy any priority is treated as worse than the lowest priority.
    final int effectiveIndex = priorityIndex >= 0 ? priorityIndex : Integer.MAX_VALUE;

    // No active monitoring connection — accept whatever is offered.
    if (this.monitoringConnection.get() == null || this.currentPriorityIndex < 0) {
      this.monitoringConnection.set(conn);
      this.currentPriorityIndex = effectiveIndex;
      LOGGER.fine(() -> Messages.get(
          "ClusterTopologyMonitorImpl.connectionAccepted",
          new Object[]{
              hostSpec != null ? hostSpec.getHost() : "unknown", isWriter, formatPriorityIndex(effectiveIndex)}));
      return true;
    }

    // We already have an active connection. Replace only if the offered one is at a strictly higher priority
    // (i.e., a smaller index value).
    if (effectiveIndex < this.currentPriorityIndex) {
      this.monitoringConnection.set(conn);
      this.currentPriorityIndex = effectiveIndex;
      LOGGER.fine(() -> Messages.get(
          "ClusterTopologyMonitorImpl.connectionAccepted",
          new Object[]{
              hostSpec != null ? hostSpec.getHost() : "unknown", isWriter, formatPriorityIndex(effectiveIndex)}));
      return true;
    }

    final int finalCurrent = this.currentPriorityIndex;
    LOGGER.fine(() -> Messages.get(
        "ClusterTopologyMonitorImpl.connectionRejected",
        new Object[]{
            hostSpec != null ? hostSpec.getHost() : "unknown",
            isWriter,
            formatPriorityIndex(finalCurrent),
            formatPriorityIndex(effectiveIndex)}));
    return false;
  }

  @Override
  public @Nullable HostSpec acceptConnections(
      Map<HostSpec, AtomicConnection> connections,
      @Nullable HostSpec writerHostSpec,
      List<HostSpec> topology) {
    if (connections == null || connections.isEmpty()) {
      return null;
    }

    // The primary region is determined by the current writer (if any).
    final String primaryRegion = writerHostSpec != null
        ? this.rdsUtils.getRdsRegion(writerHostSpec.getHost())
        : getPrimaryRegion();

    // Find the best matching connection. If nothing satisfies any priority, we still pick the first available
    // connection as a fallback so monitoring can proceed; the regular-mode upgrade loop will then try to
    // replace it with a higher-priority connection once one becomes available.
    HostSpec bestHost = null;
    int bestIndex = Integer.MAX_VALUE;
    for (Map.Entry<HostSpec, AtomicConnection> entry : connections.entrySet()) {
      final HostSpec host = entry.getKey();
      final AtomicConnection atomicConn = entry.getValue();
      if (atomicConn == null || atomicConn.get() == null) {
        continue;
      }
      final boolean isWriter = writerHostSpec != null
          && writerHostSpec.getHostAndPort().equals(host.getHostAndPort());
      // Build an effective host with the correct role for priority evaluation.
      final HostSpec effectiveHost = new HostSpec(host, isWriter ? HostRole.WRITER : HostRole.READER);

      int priorityIndex = -1;
      for (int i = 0; i < this.priorities.size(); i++) {
        if (this.priorities.get(i).isSatisfiedBy(effectiveHost, primaryRegion, this.rdsUtils)) {
          priorityIndex = i;
          break;
        }
      }
      final int effectiveIndex = priorityIndex >= 0 ? priorityIndex : Integer.MAX_VALUE;
      if (bestHost == null || effectiveIndex < bestIndex) {
        bestIndex = effectiveIndex;
        bestHost = host;
      }
    }

    if (bestHost == null) {
      return null;
    }

    final AtomicConnection bestAtomic = connections.get(bestHost);
    final Connection bestConn = bestAtomic.get();
    // Detach from the AtomicConnection so it doesn't close the connection when cleaned up.
    bestAtomic.set(null, false);
    this.monitoringConnection.set(bestConn);
    this.currentPriorityIndex = bestIndex;
    final HostSpec acceptedHost = bestHost;
    final int acceptedIndex = bestIndex;
    LOGGER.fine(() -> Messages.get(
        "ClusterTopologyMonitorImpl.connectionAccepted",
        new Object[]{acceptedHost.getHost(), acceptedHost.getRole(), formatPriorityIndex(acceptedIndex)}));
    return bestHost;
  }

  @Override
  public void attemptConnectionUpgrade(List<HostSpec> currentTopology) {
    if (this.currentPriorityIndex <= 0) {
      return;
    }

    // Check if a previous upgrade attempt has completed.
    final Future<?> future = this.upgradeFuture;
    if (future != null) {
      if (!future.isDone()) {
        return;
      }

      // The task completed — check if a connection was stored in upgradeConnection.
      final Connection conn = this.upgradeConnection.get();
      final HostSpec connHost = this.upgradeConnectedHost;
      if (conn != null && connHost != null) {
        boolean isWriter = false;
        try {
          isWriter = this.topologyUtils.isWriterInstance(conn);
        } catch (SQLException ex) {
          this.upgradeConnection.set(null);
          this.upgradeConnectedHost = null;
          this.upgradeFuture = null;
          return;
        }

        // Determine the priority index using the host the async task actually connected to.
        final int newIndex = determinePriorityIndex(connHost, isWriter);
        if (newIndex >= 0 && newIndex < this.currentPriorityIndex) {
          // Upgrade successful — swap into monitoring connection.
          this.monitoringConnection.set(conn);
          this.upgradeConnection.set(null, false);
          this.currentPriorityIndex = newIndex;
          final int logIndex = newIndex;
          LOGGER.fine(() -> Messages.get(
              "ClusterTopologyMonitorImpl.upgradedMonitoringConnection",
              new Object[]{connHost.getHost(), this.priorities.get(logIndex), formatPriorityIndex(logIndex)}));
        } else {
          this.upgradeConnection.set(null);
        }
      } else if (conn != null) {
        this.upgradeConnection.set(null);
      }
      this.upgradeConnectedHost = null;
      this.upgradeFuture = null;
    }

    // Submit a new async upgrade attempt.
    if (this.upgradeFuture == null && currentTopology != null) {
      final List<List<HostSpec>> candidatesByPriority = findUpgradeCandidates(currentTopology);
      // Shuffle each priority bucket so different hosts are tried first within the same priority.
      // Priority order is preserved (higher priority buckets come before lower priority buckets).
      final List<HostSpec> candidates = new ArrayList<>();
      for (List<HostSpec> bucket : candidatesByPriority) {
        Collections.shuffle(bucket);
        candidates.addAll(bucket);
      }
      if (candidates.isEmpty()) {
        return;
      }

      final ExecutorService executor = ExecutorFactory.newSingleThreadExecutor("gatmu");
      final int candidateCount = candidates.size();
      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.upgradeTaskSubmitted",
          new Object[]{candidateCount}));
      this.upgradeFuture = executor.submit(() -> {
        try {
          for (HostSpec candidate : candidates) {
            if (Thread.currentThread().isInterrupted()) {
              return;
            }
            try {
              final Connection conn = this.pluginService.forceConnect(candidate, this.monitoringProperties);
              // Remember which host was actually connected to, then store the connection.
              this.upgradeConnectedHost = candidate;
              this.upgradeConnection.set(conn);
              // Wake up the main monitoring loop so it processes the upgrade immediately.
              if (this.upgradeReadyNotifier != null) {
                this.upgradeReadyNotifier.run();
              }
              return;
            } catch (SQLException ex) {
              LOGGER.finest(() -> Messages.get(
                  "ClusterTopologyMonitorImpl.upgradeAttemptFailed",
                  new Object[]{candidate.getHost()}));
            }
          }
        } catch (Exception ex) {
          LOGGER.log(Level.FINEST, ex, () -> Messages.get(
              "ClusterTopologyMonitorImpl.upgradeUnhandledException",
              new Object[]{ex.getMessage()}));
        } finally {
          executor.shutdown();
        }
      });
    }
  }

  @Override
  public void close() {
    final Future<?> future = this.upgradeFuture;
    if (future != null && !future.isDone()) {
      future.cancel(true);
    }
    this.upgradeFuture = null;
    this.upgradeConnection.clean();
    this.upgradeConnectedHost = null;
    this.currentPriorityIndex = -1;
  }

  protected @Nullable String getPrimaryRegion() {
    final HostSpec writer = this.writerHostSpec.get();
    if (writer != null) {
      return this.rdsUtils.getRdsRegion(writer.getHost());
    }
    return null;
  }

  protected int determinePriorityIndex(final @NonNull HostSpec hostSpec, boolean isWriter) {
    final String primaryRegion = getPrimaryRegion();
    final HostSpec effectiveHost = new HostSpec(hostSpec, isWriter ? HostRole.WRITER : HostRole.READER);

    for (int i = 0; i < this.priorities.size(); i++) {
      if (this.priorities.get(i).isSatisfiedBy(effectiveHost, primaryRegion, this.rdsUtils)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Returns candidates grouped by priority, with the highest priority bucket first. The caller is responsible
   * for shuffling each bucket before iterating, so that different hosts within the same priority are tried first
   * on different upgrade cycles.
   */
  protected List<List<HostSpec>> findUpgradeCandidates(List<HostSpec> hosts) {
    final String primaryRegion = getPrimaryRegion();
    List<List<HostSpec>> candidatesByPriority = new ArrayList<>();
    for (int i = 0; i < this.currentPriorityIndex && i < this.priorities.size(); i++) {
      final List<HostSpec> matching = this.priorities.get(i).findMatchingHosts(hosts, primaryRegion, this.rdsUtils);
      if (!matching.isEmpty()) {
        candidatesByPriority.add(matching);
      }
    }
    return candidatesByPriority;
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    List<Pair<String, Object>> state = new ArrayList<>();
    state.add(Pair.create("priorities", this.priorities.toString()));
    state.add(Pair.create("currentPriorityIndex", this.currentPriorityIndex));
    state.add(Pair.create("primaryRegion", this.getPrimaryRegion()));
    state.add(Pair.create("upgradeInProgress", this.upgradeFuture != null && !this.upgradeFuture.isDone()));
    return state;
  }
}
