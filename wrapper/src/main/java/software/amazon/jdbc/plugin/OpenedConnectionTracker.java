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
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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

  static final Map<String, Queue<WeakReference<Connection>>> openedConnections = new ConcurrentHashMap<>();
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

  public void populateOpenedConnectionQueue(final HostSpec hostSpec, final Connection conn) {
    if (hostSpec == null || conn == null) {
      return;
    }

    // Check if the connection was established using an instance endpoint
    if (rdsUtils.isRdsInstance(hostSpec.getHost())) {
      trackConnection(hostSpec.getHostAndPort(), conn);
      logOpenedConnections();
      return;
    }

    // It might be a custom domain name. Let's track by hostId and custom domain name
    if (!StringUtils.isNullOrEmpty(hostSpec.getHostId())) {
      trackConnection(hostSpec.getHostId(), conn);
    }
    if (!StringUtils.isNullOrEmpty(hostSpec.getHostAndPort())) {
      trackConnection(hostSpec.getHostAndPort(), conn);
    }
    logOpenedConnections();
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
          final Queue<WeakReference<Connection>> connectionQueue = openedConnections.get(key);
          logConnectionQueue(key, connectionQueue);
          invalidateConnections(connectionQueue);
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

  public void removeConnectionTracking(final HostSpec hostSpec, final Connection connection) {
    final String hostAndPort = rdsUtils.isRdsInstance(hostSpec.getHost())
        ? hostSpec.getHostAndPort()
        : null;

    if (StringUtils.isNullOrEmpty(hostAndPort)) {
      return;
    }

    final Queue<WeakReference<Connection>> connectionQueue = openedConnections.get(hostAndPort);
    if (connectionQueue != null) {
      connectionQueue.removeIf(
          connectionWeakReference -> Objects.equals(connectionWeakReference.get(), connection));
    }
  }

  private void trackConnection(final String instanceEndpoint, final Connection connection) {
    if (connection == null) {
      return;
    }
    final Queue<WeakReference<Connection>> connectionQueue =
        openedConnections.computeIfAbsent(
            instanceEndpoint,
            (k) -> new ConcurrentLinkedQueue<>());
    connectionQueue.add(new WeakReference<>(connection));
  }

  private void invalidateConnections(final Queue<WeakReference<Connection>> connectionQueue) {
    if (connectionQueue == null || connectionQueue.isEmpty()) {
      return;
    }
    invalidateConnectionsExecutorService.submit(() -> {
      WeakReference<Connection> connReference;
      while ((connReference = connectionQueue.poll()) != null) {
        final Connection conn = connReference.get();
        if (conn == null) {
          continue;
        }

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
      openedConnections.forEach((key, queue) -> {
        if (!queue.isEmpty()) {
          builder.append("\t");
          builder.append(key).append(" :");
          builder.append("\n\t{");
          for (final WeakReference<Connection> connection : queue) {
            final Connection tmpConn = connection.get();
            if (tmpConn != null) {
              builder.append("\n\t\t").append(tmpConn);
            }
          }
          builder.append("\n\t}\n");
        }
      });
      return String.format("Opened Connections Tracked: \n[\n%s\n]", builder);
    });
  }

  private void logConnectionQueue(final String host, final Queue<WeakReference<Connection>> queue) {
    if (queue == null || queue.isEmpty()) {
      return;
    }

    final StringBuilder builder = new StringBuilder();
    builder.append(host).append("\n[");
    for (final WeakReference<Connection> connection : queue) {
      builder.append("\n\t").append(connection.get());
    }
    builder.append("\n]");
    LOGGER.finest(Messages.get("OpenedConnectionTracker.invalidatingConnections", new Object[] {builder.toString()}));
  }

  protected static void pruneConnections() {
    openedConnections.forEach((key, queue) -> {
      queue.removeIf(connectionWeakReference -> {
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
