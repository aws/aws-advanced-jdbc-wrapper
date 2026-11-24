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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.GlobalAuroraTopologyDialect;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.Utils;

public class AuroraConnectionTrackerPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(AuroraConnectionTrackerPlugin.class.getName());

  // Check topology changes 3 min after last failover
  private static final long TOPOLOGY_CHANGES_EXPECTED_TIME_NANO = TimeUnit.MINUTES.toNanos(3);

  private final Set<String> subscribedMethods;

  public static final AwsWrapperProperty INVALIDATE_ON_PRIMARY_REGION_CHANGE =
      new AwsWrapperProperty(
          "invalidateOnPrimaryRegionChange",
          "true",
          "Aurora Global Database only. When the primary region changes, close the tracked connections to the "
              + "other instances of the new primary region, and do not accept a new connection through an "
              + "active cluster writer endpoint that reached an instance other than the current writer.");

  static {
    PropertyDefinition.registerPluginProperties(AuroraConnectionTrackerPlugin.class);
  }

  private static final AtomicLong hostListRefreshEndTimeNano = new AtomicLong(0);

  // New writer (host:port) of each primary region change that has been handled, and when. A region change is
  // detected independently by every connection that sees it; only the first one invalidates connections.
  private static final Map<String, Long> handledPrimaryRegionChanges = new ConcurrentHashMap<>();

  private final PluginService pluginService;
  private final boolean invalidateOnPrimaryRegionChange;
  private final RdsUtils rdsHelper;
  private final OpenedConnectionTracker tracker;
  private @Nullable HostSpec currentWriter = null;
  private boolean needUpdateCurrentWriter = false;

  AuroraConnectionTrackerPlugin(final PluginService pluginService, final Properties props) {
    this(pluginService, props, new RdsUtils(), new OpenedConnectionTracker(pluginService));
  }

  AuroraConnectionTrackerPlugin(
      final PluginService pluginService,
      final Properties props,
      final RdsUtils rdsUtils,
      final OpenedConnectionTracker tracker) {
    this.pluginService = pluginService;
    this.rdsHelper = rdsUtils;
    this.tracker = tracker;
    this.invalidateOnPrimaryRegionChange = INVALIDATE_ON_PRIMARY_REGION_CHANGE.getBoolean(props);

    final HashSet<String> methods = new HashSet<>(
        this.pluginService.getTargetDriverDialect().getNetworkBoundMethodNames(props));
    methods.add(JdbcMethod.CONNECTION_CLOSE.methodName);
    methods.add(JdbcMethod.CONNECTION_ABORT.methodName);
    methods.add(JdbcMethod.CONNECT.methodName);
    methods.add(JdbcMethod.NOTIFYNODELISTCHANGED.methodName);
    this.subscribedMethods = Collections.unmodifiableSet(methods);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(final String driverProtocol, final HostSpec hostSpec, final Properties props,
      final boolean isInitialConnection, final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    final Connection conn = connectFunc.call();

    if (conn != null && !Boolean.TRUE.equals(this.pluginService.isPooledConnection())) {
      final HostSpec routedHostSpec = this.pluginService.getRoutedHostSpec();
      HostSpec connectionHostSpec = routedHostSpec == null ? hostSpec : routedHostSpec;
      final RdsUrlType type = this.rdsHelper.identifyRdsType(connectionHostSpec.getHost());
      if (type.isRdsCluster() || type == RdsUrlType.OTHER || type == RdsUrlType.IP_ADDRESS) {
        final HostSpec identifiedHostSpec = this.pluginService.identifyConnection(conn, connectionHostSpec);
        if (identifiedHostSpec != null) {
          connectionHostSpec = identifiedHostSpec;
          this.pluginService.setRoutedHostSpec(identifiedHostSpec);
        }
      }
      this.rejectIfActiveWriterEndpointMissedWriter(hostSpec, connectionHostSpec, conn, isInitialConnection);
      final TrackedConnectionList.Node node = tracker.populateOpenedConnectionQueue(connectionHostSpec, conn);
      this.pluginService.setTrackedConnectionNode(node);
    }

    return conn;
  }

  @Override
  public <T, E extends Exception> T execute(final Class<T> resultClass, final Class<E> exceptionClass,
      final Object methodInvokeOn, final String methodName, final JdbcCallable<T, E> jdbcMethodFunc,
      final @Nullable Object[] jdbcMethodArgs) throws E {

    final HostSpec currentHostSpec = this.pluginService.getCurrentHostSpec();
    this.rememberWriter();

    try {
      if (!methodName.equals(JdbcMethod.CONNECTION_CLOSE.methodName)
          && !methodName.equals(JdbcMethod.CONNECTION_ABORT.methodName)) {
        long localHostListRefreshEndTimeNano = hostListRefreshEndTimeNano.get();
        boolean needRefreshHostLists = false;
        if (localHostListRefreshEndTimeNano > 0) {
          if (localHostListRefreshEndTimeNano > System.nanoTime()) {
            // The time specified in hostListRefreshThresholdTimeNano isn't yet reached.
            // Need to continue to refresh host list.
            needRefreshHostLists = true;
          } else {
            // The time specified in hostListRefreshThresholdTimeNano is reached, and we can stop further refreshes
            // of host list. If hostListRefreshThresholdTimeNano has changed while this thread processes the code,
            // we can't override a new value in hostListRefreshThresholdTimeNano.
            hostListRefreshEndTimeNano.compareAndSet(localHostListRefreshEndTimeNano, 0);
          }
        }
        if (this.needUpdateCurrentWriter || needRefreshHostLists) {
          // Calling this method may effectively close/abort a current connection
          this.checkWriterChanged(needRefreshHostLists);
        }
      }
      final T result = jdbcMethodFunc.call();
      if ((methodName.equals(JdbcMethod.CONNECTION_CLOSE.methodName)
          || methodName.equals(JdbcMethod.CONNECTION_ABORT.methodName))) {
        final TrackedConnectionList.Node node = this.pluginService.getTrackedConnectionNode();
        if (node != null) {
          tracker.removeConnectionTracking(node);
          this.pluginService.setTrackedConnectionNode(null);
        } else {
          tracker.removeConnectionTracking(currentHostSpec, this.pluginService.getCurrentConnection());
        }
      }
      return result;

    } catch (final Exception e) {
      if (e instanceof FailoverSQLException) {
        hostListRefreshEndTimeNano.set(System.nanoTime() + TOPOLOGY_CHANGES_EXPECTED_TIME_NANO);
        // Calling this method may effectively close/abort a current connection
        this.checkWriterChanged(true);
      }
      throw e;
    }
  }

  private void checkWriterChanged(boolean needRefreshHostLists) {
    if (needRefreshHostLists) {
      try {
        this.pluginService.refreshHostList();
      } catch (SQLException ex) {
        // do nothing
      }
    }

    final HostSpec hostSpecAfterFailover = Utils.getWriter(this.pluginService.getAllHosts());
    if (hostSpecAfterFailover == null) {
      return;
    }

    final HostSpec previousWriter = this.currentWriter;
    if (previousWriter == null) {
      this.currentWriter = hostSpecAfterFailover;
      this.needUpdateCurrentWriter = false;
    } else if (!previousWriter.getHostAndPort().equals(hostSpecAfterFailover.getHostAndPort())) {
      // the writer's changed
      tracker.invalidateAllConnections(previousWriter);
      this.invalidateIfPrimaryRegionChanged(previousWriter, hostSpecAfterFailover);
      tracker.logOpenedConnections();
      this.currentWriter = hostSpecAfterFailover;
      this.needUpdateCurrentWriter = false;
      hostListRefreshEndTimeNano.set(0);
    }
  }

  // For tests.
  static void clearHandledPrimaryRegionChanges() {
    handledPrimaryRegionChanges.clear();
  }

  private boolean isGlobalDatabase() {
    return this.invalidateOnPrimaryRegionChange
        && this.pluginService.getDialect() instanceof GlobalAuroraTopologyDialect;
  }

  /**
   * Aurora Global Database: when the writer moves to another region, the connections this application holds to
   * the instances of the new primary region were opened while that region was secondary, so they are reader
   * connections. Close them all, except connections to the new writer itself.
   *
   * <p>Every connection detects the same change on its own; only the first detection within
   * {@link #TOPOLOGY_CHANGES_EXPECTED_TIME_NANO} acts on it.
   */
  private void invalidateIfPrimaryRegionChanged(final HostSpec previousWriter, final HostSpec newWriter) {
    if (!this.isGlobalDatabase()) {
      return;
    }
    final String previousRegion = this.rdsHelper.getRdsRegion(previousWriter.getHost());
    final String newRegion = this.rdsHelper.getRdsRegion(newWriter.getHost());
    if (previousRegion == null || newRegion == null || previousRegion.equalsIgnoreCase(newRegion)) {
      return;
    }

    final long now = System.nanoTime();
    final String newWriterHostAndPort = newWriter.getHostAndPort();
    final Long handledAt = handledPrimaryRegionChanges.get(newWriterHostAndPort);
    if (handledAt != null && now - handledAt < TOPOLOGY_CHANGES_EXPECTED_TIME_NANO) {
      return;
    }
    final boolean firstToHandle = handledAt == null
        ? handledPrimaryRegionChanges.putIfAbsent(newWriterHostAndPort, now) == null
        : handledPrimaryRegionChanges.replace(newWriterHostAndPort, handledAt, now);
    if (!firstToHandle) {
      return;
    }

    LOGGER.info(() -> Messages.get(
        "AuroraConnectionTrackerPlugin.primaryRegionChanged",
        new Object[] {previousRegion, newRegion, newWriterHostAndPort}));
    this.tracker.invalidateConnectionsInRegion(newRegion, newWriterHostAndPort);
  }

  /**
   * Aurora Global Database: a new connection through a cluster writer endpoint whose region is now the primary
   * region must reach the current writer. During a switchover a connection can be opened from an outdated
   * topology (writer still in the other region), which treats the endpoint as inactive and accepts whatever
   * instance its DNS points to, often a reader. Such a connection is closed here and the connect fails with a
   * network-class SQL state, so that the caller (for example the initialConnection plugin) opens it again.
   */
  private void rejectIfActiveWriterEndpointMissedWriter(
      final HostSpec requestedHostSpec,
      final HostSpec connectionHostSpec,
      final Connection conn,
      final boolean isInitialConnection) throws SQLException {

    if (!isInitialConnection
        || !this.isGlobalDatabase()
        || this.rdsHelper.identifyRdsType(requestedHostSpec.getHost()) != RdsUrlType.RDS_WRITER_CLUSTER
        || !this.rdsHelper.isRdsInstance(connectionHostSpec.getHost())) {
      return;
    }

    try {
      // The topology this connection was opened with may be outdated; use the latest shared one.
      this.pluginService.refreshHostList();
    } catch (final SQLException e) {
      return;
    }
    final HostSpec writer = Utils.getWriter(this.pluginService.getAllHosts());
    if (writer == null
        || !this.rdsHelper.isRdsInstance(writer.getHost())
        || !this.rdsHelper.isSameRegion(writer.getHost(), requestedHostSpec.getHost())
        || writer.getHostAndPort().equals(connectionHostSpec.getHostAndPort())) {
      // No known writer, the endpoint is inactive (writer in another region), or the writer was reached.
      return;
    }

    final String message = Messages.get(
        "AuroraConnectionTrackerPlugin.activeWriterEndpointMissedWriter",
        new Object[] {requestedHostSpec.getHost(), connectionHostSpec.getHostAndPort(), writer.getHostAndPort()});
    LOGGER.fine(message);
    try {
      conn.close();
    } catch (final SQLException e) {
      // ignore, the connection is discarded anyway
    }
    throw new SQLException(message, SqlState.CONNECTION_UNABLE_TO_CONNECT.getState());
  }

  private void rememberWriter() {
    if (this.currentWriter == null || this.needUpdateCurrentWriter) {
      this.currentWriter = Utils.getWriter(this.pluginService.getAllHosts());
      this.needUpdateCurrentWriter = false;
    }
  }

  @Override
  public void notifyNodeListChanged(final Map<String, EnumSet<NodeChangeOptions>> changes) {
    for (final String node : changes.keySet()) {
      final EnumSet<NodeChangeOptions> nodeChanges = changes.get(node);
      if (nodeChanges.contains(NodeChangeOptions.PROMOTED_TO_READER)) {
        tracker.invalidateAllConnections(node);
      }
      if (nodeChanges.contains(NodeChangeOptions.PROMOTED_TO_WRITER)) {
        this.needUpdateCurrentWriter = true;
      }
    }
  }
}
