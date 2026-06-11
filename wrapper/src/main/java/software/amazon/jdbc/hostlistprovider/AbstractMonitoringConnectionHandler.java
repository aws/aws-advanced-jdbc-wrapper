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
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;

/**
 * Base implementation of {@link MonitoringConnectionHandler} that handles the connection-priority lifecycle
 * (accept, batch-accept, async upgrade, close). Concrete subclasses provide the priority-evaluation hooks
 * specific to their priority model (Aurora vs. Global Aurora).
 *
 * @param <P> the concrete priority type (e.g., {@link MonitoringConnectionPriority} or
 *     {@link GdbMonitoringConnectionPriority}). The type's {@code toString()} should return a stable
 *     human-readable label for snapshot logging.
 */
public abstract class AbstractMonitoringConnectionHandler<P> implements MonitoringConnectionHandler {

  private static final Logger LOGGER = Logger.getLogger(AbstractMonitoringConnectionHandler.class.getName());

  protected final AtomicConnection monitoringConnection;
  protected final AtomicConnection upgradeConnection;
  protected final PluginService pluginService;
  protected final TopologyUtils topologyUtils;
  protected final Properties monitoringProperties;
  protected final List<P> priorities;
  protected final @Nullable Runnable upgradeReadyNotifier;

  protected int currentPriorityIndex = -1;
  protected volatile Future<?> upgradeFuture = null;
  protected volatile HostSpec upgradeConnectedHost = null;
  // A single long-lived executor for async upgrade attempts, created lazily on first use and shut down on close.
  protected ExecutorService upgradeExecutor = null;

  protected AbstractMonitoringConnectionHandler(
      final AtomicConnection monitoringConnection,
      final PluginService pluginService,
      final TopologyUtils topologyUtils,
      final Properties monitoringProperties,
      final List<P> priorities,
      final @Nullable Runnable upgradeReadyNotifier) {
    this.monitoringConnection = monitoringConnection;
    this.upgradeConnection = new AtomicConnection(this, false);
    this.pluginService = pluginService;
    this.topologyUtils = topologyUtils;
    this.monitoringProperties = monitoringProperties;
    this.priorities = priorities;
    this.upgradeReadyNotifier = upgradeReadyNotifier;
  }

  // ---- Subclass hooks --------------------------------------------------------------------------------------

  /**
   * Returns the index into {@link #priorities} that the given host satisfies, or {@code -1} if no priority
   * matches. The lowest-index match wins.
   *
   * @param host     the host being considered
   * @param isWriter whether the connection is to a writer
   */
  protected abstract int getPriorityIndex(HostSpec host, boolean isWriter);

  /**
   * Returns all hosts in {@code hosts} that match the priority at {@code priorityIndex} of {@link #priorities}.
   */
  protected abstract List<HostSpec> findHostsForPriority(int priorityIndex, List<HostSpec> hosts);

  /**
   * Returns the thread-name prefix used for the async upgrade executor (e.g., {@code "atmu"} or {@code "gatmu"}).
   */
  protected abstract String getUpgradeThreadName();

  /**
   * Hook for subclasses to publish additional snapshot state (e.g., GDB primary region). Returns null when none.
   */
  protected @Nullable List<Pair<String, Object>> getAdditionalSnapshotState() {
    return null;
  }

  // ---- Helpers ---------------------------------------------------------------------------------------------

  /**
   * Formats a priority index for logging. {@link Integer#MAX_VALUE} is shown as {@code <none>}
   * to avoid confusing users with a 10-digit number.
   */
  protected static String formatPriorityIndex(final int index) {
    return index == Integer.MAX_VALUE ? "<none>" : Integer.toString(index);
  }

  protected static int effectiveIndex(final int priorityIndex) {
    return priorityIndex >= 0 ? priorityIndex : Integer.MAX_VALUE;
  }

  // ---- MonitoringConnectionHandler implementation ----------------------------------------------------------

  @Override
  public boolean acceptConnection(Connection conn, boolean isWriter, HostSpec hostSpec) {
    final int priorityIndex = hostSpec == null ? -1 : getPriorityIndex(hostSpec, isWriter);
    // A connection that doesn't satisfy any priority is treated as worse than the lowest priority.
    final int effectiveIndex = effectiveIndex(priorityIndex);

    // No active monitoring connection — accept whatever is offered.
    if (this.monitoringConnection.get() == null || this.currentPriorityIndex < 0) {
      this.monitoringConnection.set(conn);
      this.currentPriorityIndex = effectiveIndex;
      LOGGER.fine(() -> Messages.get(
          "ClusterTopologyMonitorImpl.connectionAccepted",
          new Object[]{
              hostSpec != null ? hostSpec.getHost() : "unknown",
              isWriter ? "WRITER" : "READER",
              formatPriorityIndex(effectiveIndex)}));
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
              hostSpec != null ? hostSpec.getHost() : "unknown",
              isWriter ? "WRITER" : "READER",
              formatPriorityIndex(effectiveIndex)}));
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
      final int effectiveIndex = effectiveIndex(getPriorityIndex(host, isWriter));
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
      // Already at highest priority or no connection — nothing to upgrade.
      return;
    }

    // Check if a previous upgrade attempt has completed.
    final Future<?> future = this.upgradeFuture;
    if (future != null) {
      if (!future.isDone()) {
        // Still in progress — don't block.
        return;
      }

      // The task completed — check if a connection was stored in upgradeConnection.
      final Connection conn = this.upgradeConnection.get();
      final HostSpec connectedHost = this.upgradeConnectedHost;
      if (conn != null && connectedHost != null) {
        boolean isWriter = false;
        try {
          isWriter = this.topologyUtils.isWriterInstance(conn);
        } catch (SQLException ex) {
          this.upgradeConnection.set(null);
          this.upgradeConnectedHost = null;
          this.upgradeFuture = null;
          return;
        }

        final int newIndex = getPriorityIndex(connectedHost, isWriter);
        if (newIndex >= 0 && newIndex < this.currentPriorityIndex) {
          // Upgrade successful — swap into monitoring connection.
          // Take the connection out of upgradeConnection without closing it.
          this.upgradeConnection.set(null, false);
          this.monitoringConnection.set(conn);
          this.currentPriorityIndex = newIndex;
          final int logIndex = newIndex;
          LOGGER.fine(() -> Messages.get(
              "ClusterTopologyMonitorImpl.upgradedMonitoringConnection",
              new Object[]{
                  connectedHost.getHost(),
                  this.priorities.get(logIndex),
                  formatPriorityIndex(logIndex)}));
        } else {
          // Not a useful upgrade — close it.
          this.upgradeConnection.set(null);
        }
      } else if (conn != null) {
        // Connection without a known host (shouldn't normally happen) — discard.
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

      if (this.upgradeExecutor == null) {
        this.upgradeExecutor = ExecutorFactory.newSingleThreadExecutor(getUpgradeThreadName());
      }
      final int candidateCount = candidates.size();
      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.upgradeTaskSubmitted",
          new Object[]{candidateCount}));
      this.upgradeFuture = this.upgradeExecutor.submit(() -> {
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
                new Object[]{candidate.getHost(), ex.getMessage()}));
          }
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
    if (this.upgradeExecutor != null) {
      this.upgradeExecutor.shutdownNow();
      this.upgradeExecutor = null;
    }
    this.upgradeConnection.clean();
    this.upgradeConnectedHost = null;
    this.currentPriorityIndex = -1;
  }

  /**
   * Returns candidates grouped by priority, with the highest priority bucket first. The caller is responsible
   * for shuffling each bucket before iterating, so that different hosts within the same priority are tried first
   * on different upgrade cycles.
   */
  protected List<List<HostSpec>> findUpgradeCandidates(List<HostSpec> hosts) {
    List<List<HostSpec>> candidatesByPriority = new ArrayList<>();
    for (int i = 0; i < this.currentPriorityIndex && i < this.priorities.size(); i++) {
      List<HostSpec> matching = findHostsForPriority(i, hosts);
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
    state.add(Pair.create("upgradeInProgress", this.upgradeFuture != null && !this.upgradeFuture.isDone()));
    final List<Pair<String, Object>> extra = getAdditionalSnapshotState();
    if (extra != null) {
      state.addAll(extra);
    }
    return state;
  }
}
