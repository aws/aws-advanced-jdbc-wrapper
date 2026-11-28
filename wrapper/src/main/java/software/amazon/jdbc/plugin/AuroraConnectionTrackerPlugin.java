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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
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
          "Aurora Global Database only. When the primary region changes, close the tracked connections that were "
              + "opened for the writer but point to a reader in the new primary region.");

  static {
    PropertyDefinition.registerPluginProperties(AuroraConnectionTrackerPlugin.class);
  }

  private static final AtomicLong hostListRefreshEndTimeNano = new AtomicLong(0);

  /** A primary region change (previous writer to new writer, as host:port) and when it was handled. */
  private static final class PrimaryRegionChange {
    final String previousWriter;
    final String newWriter;
    final long handledAtNano;

    PrimaryRegionChange(final String previousWriter, final String newWriter, final long handledAtNano) {
      this.previousWriter = previousWriter;
      this.newWriter = newWriter;
      this.handledAtNano = handledAtNano;
    }

    boolean isSameChange(final String previousWriter, final String newWriter) {
      return this.previousWriter.equals(previousWriter) && this.newWriter.equals(newWriter);
    }
  }

  // The last primary region change that has been handled. A region change is detected independently by every
  // connection that sees it; only the first one invalidates connections. Keyed by the previous and the new writer
  // so that a switchback (A to B, then B to A) within the window is handled as a new change.
  private static final AtomicReference<@Nullable PrimaryRegionChange> lastHandledPrimaryRegionChange =
      new AtomicReference<>();

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
      final TrackedConnectionList.Node node = tracker.populateOpenedConnectionQueue(
          connectionHostSpec, conn, this.isOpenedForWriter(hostSpec));
      this.pluginService.setTrackedConnectionNode(node);
    }

    return conn;
  }

  /**
   * Whether a connection to the given host, as requested before any routing or instance identification, is meant
   * for the writer: through a cluster writer endpoint, or to an instance selected as the writer. Connections through
   * a reader or custom cluster endpoint, and to instances selected as readers, are not.
   */
  private boolean isOpenedForWriter(final HostSpec requestedHostSpec) {
    final RdsUrlType type = this.rdsHelper.identifyRdsType(requestedHostSpec.getHost());
    if (type == RdsUrlType.RDS_WRITER_CLUSTER || type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER) {
      return true;
    }
    if (type == RdsUrlType.RDS_READER_CLUSTER || type == RdsUrlType.RDS_CUSTOM_CLUSTER) {
      return false;
    }
    return requestedHostSpec.getRole() == HostRole.WRITER;
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
    } else {
      // Same writer as remembered: nothing to invalidate for this notification.
      this.needUpdateCurrentWriter = false;
    }
  }

  // For tests.
  static void clearHandledPrimaryRegionChanges() {
    lastHandledPrimaryRegionChange.set(null);
  }

  private boolean shouldInvalidateOnPrimaryRegionChange() {
    return this.invalidateOnPrimaryRegionChange
        && this.pluginService.getDialect() instanceof GlobalAuroraTopologyDialect;
  }

  /**
   * Aurora Global Database: when the writer moves to another region, the connections this application opened for
   * the writer (through the cluster writer endpoint, or to an instance selected as the writer) while the new primary
   * region was still secondary landed on reader instances. Close them, except connections to the new writer itself.
   * Connections opened for a reader are still valid and stay open.
   *
   * <p>Every connection detects the same change on its own; only the first detection within
   * {@link #TOPOLOGY_CHANGES_EXPECTED_TIME_NANO} acts on it.
   */
  private void invalidateIfPrimaryRegionChanged(final HostSpec previousWriter, final HostSpec newWriter) {
    if (!this.shouldInvalidateOnPrimaryRegionChange()) {
      return;
    }
    final String previousRegion = this.rdsHelper.getRdsRegion(previousWriter.getHost());
    final String newRegion = this.rdsHelper.getRdsRegion(newWriter.getHost());
    if (previousRegion == null || newRegion == null || previousRegion.equalsIgnoreCase(newRegion)) {
      return;
    }

    final long now = System.nanoTime();
    final String previousWriterHostAndPort = previousWriter.getHostAndPort();
    final String newWriterHostAndPort = newWriter.getHostAndPort();
    final PrimaryRegionChange lastHandled = lastHandledPrimaryRegionChange.get();
    if (lastHandled != null
        && lastHandled.isSameChange(previousWriterHostAndPort, newWriterHostAndPort)
        && now - lastHandled.handledAtNano < TOPOLOGY_CHANGES_EXPECTED_TIME_NANO) {
      return;
    }
    final PrimaryRegionChange change = new PrimaryRegionChange(previousWriterHostAndPort, newWriterHostAndPort, now);
    if (!lastHandledPrimaryRegionChange.compareAndSet(lastHandled, change)) {
      // Another connection handled a change meanwhile.
      return;
    }

    LOGGER.info(() -> Messages.get(
        "AuroraConnectionTrackerPlugin.primaryRegionChanged",
        new Object[] {previousRegion, newRegion, newWriterHostAndPort}));
    this.tracker.invalidateConnectionsOpenedForWriterInRegion(newRegion, newWriterHostAndPort);
  }

  /**
   * Records the writer the first time this connection sees a topology. A later writer change
   * (needUpdateCurrentWriter, set by {@link #notifyNodeListChanged}) is left to {@link #checkWriterChanged}, which
   * compares the previous and the new writer; overwriting the writer here would hide the change from it.
   */
  private void rememberWriter() {
    if (this.currentWriter == null) {
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
