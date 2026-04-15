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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class OpenedConnectionTracker {

  static final Map<String, TrackedConnectionList> openedConnections = new ConcurrentHashMap<>();
  private static final String TELEMETRY_INVALIDATE_CONNECTIONS = "invalidate connections";
  private static final int PRUNE_INTERVAL_SEC = 30;
  private static final ScheduledExecutorService pruneConnectionsExecutorService =
      ExecutorFactory.newSingleThreadScheduledThreadExecutor("pruneConnection");
  private static final ExecutorService invalidateConnectionsExecutorService =
      ExecutorFactory.newCachedThreadPool("invalidateConnection");
  private static final Executor abortConnectionExecutor = new SynchronousExecutor();

  private static final Logger LOGGER = Logger.getLogger(OpenedConnectionTracker.class.getName());
  private static final RdsUtils rdsUtils = new RdsUtils();

  private static final Set<String> safeToCheckClosedClasses = new HashSet<>(Arrays.asList(
      "HikariProxyConnection",
      "org.postgresql.jdbc.PgConnection",
      "com.mysql.cj.jdbc.ConnectionImpl",
      "org.mariadb.jdbc.Connection"));

  private final PluginService pluginService;

  static {
    pruneConnectionsExecutorService.scheduleAtFixedRate(
        OpenedConnectionTracker::pruneConnections, PRUNE_INTERVAL_SEC, PRUNE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  public OpenedConnectionTracker(final PluginService pluginService) {
    this.pluginService = pluginService;
  }

  public TrackedConnectionList.Node populateOpenedConnectionQueue(final HostSpec hostSpec, final Connection conn) {
    if (hostSpec == null || conn == null) {
      return null;
    }

    // Check if the connection was established using an instance endpoint
    if (rdsUtils.isRdsInstance(hostSpec.getHost())) {
      final TrackedConnectionList.Node node = trackConnection(hostSpec.getHostAndPort(), conn);
      logOpenedConnections();
      return node;
    }

    // It might be a custom domain name. Let's track by hostId and custom domain name
    TrackedConnectionList.Node lastNode = null;
    if (!StringUtils.isNullOrEmpty(hostSpec.getHostId())) {
      lastNode = trackConnection(hostSpec.getHostId(), conn);
    }
    if (!StringUtils.isNullOrEmpty(hostSpec.getHostAndPort())) {
      lastNode = trackConnection(hostSpec.getHostAndPort(), conn);
    }
    logOpenedConnections();
    return lastNode;
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

  public void invalidateAllConnections(final String... keys) {
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
      final String instanceEndpoint, final Connection connection) {
    if (connection == null) {
      return null;
    }
    final TrackedConnectionList connectionList =
        openedConnections.computeIfAbsent(
            instanceEndpoint,
            (k) -> new TrackedConnectionList());
    return connectionList.add(connection);
  }

  private void invalidateConnections(final TrackedConnectionList connectionList) {
    if (connectionList == null || connectionList.isEmpty()) {
      return;
    }
    invalidateConnectionsExecutorService.submit(() -> {
      final List<Connection> connections = connectionList.drainAll();
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

  private void logConnectionList(final String host, final TrackedConnectionList list) {
    if (list == null || list.isEmpty()) {
      return;
    }

    final StringBuilder builder = new StringBuilder();
    builder.append(host).append("\n[");
    list.appendTo(builder);
    builder.append("\n]");
    LOGGER.finest(Messages.get("OpenedConnectionTracker.invalidatingConnections",
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
    openedConnections.clear();
  }
}
