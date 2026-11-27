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

package software.amazon.jdbc.plugin;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class OpenedConnectionTracker {

  static final Map<String, TrackedConnectionList> openedConnections = new ConcurrentHashMap<>();
  private static final String TELEMETRY_INVALIDATE_CONNECTIONS = "invalidate connections";
  private static final int PRUNE_INTERVAL_SEC = 30;
  private static final ResourceLock lock = new ResourceLock();
  private static volatile ScheduledExecutorService pruneConnectionsExecutorService;
  private static volatile ExecutorService invalidateConnectionsExecutorService;
  private static final Executor abortConnectionExecutor = new SynchronousExecutor();

  private static final Logger LOGGER = Logger.getLogger(OpenedConnectionTracker.class.getName());
  private static final RdsUtils rdsUtils = new RdsUtils();

  private static final Set<String> safeToCheckClosedClasses = new HashSet<>(Arrays.asList(
      "HikariProxyConnection",
      "org.postgresql.jdbc.PgConnection",
      "com.mysql.cj.jdbc.ConnectionImpl",
      "org.mariadb.jdbc.Connection"));

  private final PluginService pluginService;

  private static ScheduledExecutorService getOrCreatePruneExecutor() {
    ScheduledExecutorService executor = pruneConnectionsExecutorService;
    if (executor == null || executor.isShutdown()) {
      try (ResourceLock ignored = lock.obtain()) {
        executor = pruneConnectionsExecutorService;
        if (executor == null || executor.isShutdown()) {
          executor = ExecutorFactory.newSingleThreadScheduledThreadExecutor("pruneConnection");
          executor.scheduleAtFixedRate(
              OpenedConnectionTracker::pruneConnections, PRUNE_INTERVAL_SEC, PRUNE_INTERVAL_SEC, TimeUnit.SECONDS);
          pruneConnectionsExecutorService = executor;
        }
      }
    }
    return executor;
  }

  private static ExecutorService getOrCreateInvalidateExecutor() {
    ExecutorService executor = invalidateConnectionsExecutorService;
    if (executor == null || executor.isShutdown()) {
      try (ResourceLock ignored = lock.obtain()) {
        executor = invalidateConnectionsExecutorService;
        if (executor == null || executor.isShutdown()) {
          executor = ExecutorFactory.newCachedThreadPool("invalidateConnection");
          invalidateConnectionsExecutorService = executor;
        }
      }
    }
    return executor;
  }

  public OpenedConnectionTracker(final PluginService pluginService) {
    this.pluginService = pluginService;
  }

  public TrackedConnectionList.@Nullable Node populateOpenedConnectionQueue(
      final HostSpec hostSpec, final Connection conn) {
    if (hostSpec == null || conn == null) {
      return null;
    }

    // Share the reference so closing or invalidating a connection also invalidates its aliases.
    final WeakReference<Connection> connectionRef = new WeakReference<>(conn);
    final boolean instanceEndpoint = rdsUtils.isRdsInstance(hostSpec.getHost());
    final String hostId = hostSpec.getHostId();
    final String hostAndPort = hostSpec.getHostAndPort();
    TrackedConnectionList.Node node = null;
    // Publish all aliases together so a switchover snapshot cannot split a registration.
    try (ResourceLock ignored = lock.obtain()) {
      if (!instanceEndpoint && !StringUtils.isNullOrEmpty(hostId)) {
        node = trackConnection(hostId, connectionRef);
      }
      if (instanceEndpoint || !StringUtils.isNullOrEmpty(hostAndPort)) {
        node = trackConnection(hostAndPort, connectionRef);
      }
    }
    logOpenedConnections();
    return node;
  }

  /**
   * Invalidates all opened connections pointing to the same node in a daemon thread. 1
   *
   * @param hostSpec The {@link HostSpec} object containing the url of the node.
   */
  public void invalidateAllConnections(final HostSpec hostSpec) {
    if (hostSpec == null) {
      return;
    }
    invalidateAllConnections(hostSpec.getHostAndPort(), hostSpec.getHost(), hostSpec.getHostId());
  }

  public void invalidateAllConnections(final @Nullable String... keys) {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_INVALIDATE_CONNECTIONS, TelemetryTraceLevel.NESTED);

    try {
      for (String key : keys) {
        if (key == null) {
          continue;
        }
        try {
          final TrackedConnectionList connectionList = openedConnections.get(key);
          logConnectionList(key, connectionList);
          invalidateConnections(connectionList);
        } catch (Exception ex) {
          // ignore and continue
        }
      }
    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }
  }

  /**
   * Schedules invalidation of the connections currently tracked for these hosts. Endpoints can
   * point to the new deployment by the time the grace period expires.
   */
  public void scheduleInvalidation(final List<HostSpec> hosts, final long graceMs) {
    final Set<String> keys = new HashSet<>();
    for (HostSpec host : hosts) {
      keys.add(host.getHostAndPort());
      keys.add(host.getHost());
      final String hostId = host.getHostId();
      if (hostId != null) {
        keys.add(hostId);
      }
    }
    try (ResourceLock ignored = lock.obtain()) {
      final List<TrackedConnectionList.Node> snapshot = new ArrayList<>();
      for (String key : keys) {
        final TrackedConnectionList list = openedConnections.get(key);
        if (list != null) {
          list.collectNodes(snapshot);
        }
      }
      scheduleInvalidation(() -> invalidateSnapshot(snapshot), graceMs);
    }
  }

  void scheduleInvalidation(final Runnable task, final long graceMs) {
    final ExecutorService worker = getOrCreateInvalidateExecutor();
    getOrCreatePruneExecutor().schedule(() -> worker.execute(task), graceMs, TimeUnit.MILLISECONDS);
  }

  private static void invalidateSnapshot(final List<TrackedConnectionList.Node> snapshot) {
    final Set<Connection> connections = Collections.newSetFromMap(new IdentityHashMap<>());
    for (TrackedConnectionList.Node node : snapshot) {
      final Connection conn = node.ownerList.removeAndGet(node);
      if (conn != null) {
        connections.add(conn);
      }
    }
    LOGGER.finest(() -> Messages.get("OpenedConnectionTracker.invalidatingSnapshot",
        new Object[] {connections.size()}));
    for (Connection conn : connections) {
      try {
        conn.abort(abortConnectionExecutor);
      } catch (SQLException | RuntimeException ex) {
        // Continue aborting the remaining connections.
      }
    }
  }

  /**
   * Removes a tracked connection in O(1) using a direct node reference.
   *
   * @param node the node returned by {@link #populateOpenedConnectionQueue}
   */
  public void removeConnectionTracking(final TrackedConnectionList.Node node) {
    if (node == null) {
      return;
    }
    node.ownerList.remove(node);
  }

  /**
   * Removes a tracked connection by scanning the list for the given host.
   * This is the O(n) fallback used when no node reference is available.
   *
   * @param hostSpec the host specification identifying the connection's host
   * @param connection the connection to remove from tracking
   */
  public void removeConnectionTracking(final HostSpec hostSpec, final Connection connection) {
    final String hostAndPort = rdsUtils.isRdsInstance(hostSpec.getHost())
        ? hostSpec.getHostAndPort()
        : null;

    if (StringUtils.isNullOrEmpty(hostAndPort)) {
      return;
    }

    final TrackedConnectionList connectionList = openedConnections.get(hostAndPort);
    if (connectionList != null) {
      final Connection target = connection;
      connectionList.removeIf(
          connectionWeakReference -> {
            final Connection conn = connectionWeakReference.get();
            return conn == null || conn == target;
          });
    }
  }

  private TrackedConnectionList.Node trackConnection(
      final String instanceEndpoint, final WeakReference<Connection> connectionRef) {
    getOrCreatePruneExecutor();
    final TrackedConnectionList connectionList =
        openedConnections.computeIfAbsent(
            instanceEndpoint,
            (k) -> new TrackedConnectionList());
    return connectionList.addReference(connectionRef);
  }

  private void invalidateConnections(final @Nullable TrackedConnectionList connectionList) {
    if (connectionList == null || connectionList.isEmpty()) {
      return;
    }
    // connectionList is non-null past the guard; capture it so the refinement holds inside the lambda.
    final TrackedConnectionList list = connectionList;
    getOrCreateInvalidateExecutor().submit(() -> {
      final List<Connection> connections = list.drainAll();
      for (final Connection conn : connections) {
        try {
          conn.abort(abortConnectionExecutor);
        } catch (final SQLException e) {
          // swallow this exception, current connection should be useless anyway.
        }
      }
    });
  }

  public void logOpenedConnections() {
    LOGGER.finest(() -> {
      final StringBuilder builder = new StringBuilder();
      openedConnections.forEach((key, list) -> {
        if (!list.isEmpty()) {
          builder.append("\t");
          builder.append(key).append(" :");
          builder.append("\n\t{");
          list.appendTo(builder);
          builder.append("\n\t}\n");
        }
      });
      return String.format("Opened Connections Tracked: \n[\n%s\n]", builder);
    });
  }

  private void logConnectionList(final String host, final @Nullable TrackedConnectionList list) {
    if (list == null || list.isEmpty()) {
      return;
    }

    final StringBuilder builder = new StringBuilder();
    builder.append(host).append("\n[");
    list.appendTo(builder);
    builder.append("\n]");
    LOGGER.finest(() -> Messages.get("OpenedConnectionTracker.invalidatingConnections",
        new Object[] {builder.toString()}));
  }

  protected static void pruneConnections() {
    openedConnections.forEach((key, list) -> {
      list.removeIf(connectionWeakReference -> {
        final Connection conn = connectionWeakReference.get();
        if (conn == null) {
          return true;
        }
        // The following classes do not check connection validity by calling a DB server
        // so it's safe to check whether connection is already closed.
        if (safeToCheckClosedClasses.contains(conn.getClass().getSimpleName())
            || safeToCheckClosedClasses.contains(conn.getClass().getName())) {
          try {
            return conn.isClosed();
          } catch (SQLException ex) {
            return false;
          }
        }
        return false;
      });
    });
  }

  public static void clearCache() {
    try (ResourceLock ignored = lock.obtain()) {
      // Pending grace periods may still hold nodes from these lists.
      openedConnections.values().forEach(list -> list.removeIf(ref -> true));
      openedConnections.clear();
    }
  }

  public static void releaseResources() {
    try (ResourceLock ignored = lock.obtain()) {
      if (pruneConnectionsExecutorService != null) {
        pruneConnectionsExecutorService.shutdownNow();
      }
      if (invalidateConnectionsExecutorService != null) {
        invalidateConnectionsExecutorService.shutdownNow();
      }
      clearCache();
    }
  }
}
