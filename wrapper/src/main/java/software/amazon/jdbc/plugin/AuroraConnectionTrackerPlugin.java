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
import java.util.logging.Logger;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.Utils;

public class AuroraConnectionTrackerPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(AuroraConnectionTrackerPlugin.class.getName());

  // Check topology changes 3 min after last failover
  private static final long TOPOLOGY_CHANGES_EXPECTED_TIME_NANO = TimeUnit.MINUTES.toNanos(3);

  private final Set<String> subscribedMethods;

  private static final AtomicLong hostListRefreshEndTimeNano = new AtomicLong(0);

  private final PluginService pluginService;
  private final RdsUtils rdsHelper;
  private final OpenedConnectionTracker tracker;
  private HostSpec currentWriter = null;
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
      final RdsUrlType type = this.rdsHelper.identifyRdsType(hostSpec.getHost());
      if (type.isRdsCluster() || type == RdsUrlType.OTHER || type == RdsUrlType.IP_ADDRESS) {
        hostSpec.resetAliases();
        this.pluginService.fillAliases(conn, hostSpec);
      }
      tracker.populateOpenedConnectionQueue(hostSpec, conn);
    }

    return conn;
  }

  @Override
  public <T, E extends Exception> T execute(final Class<T> resultClass, final Class<E> exceptionClass,
      final Object methodInvokeOn, final String methodName, final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs) throws E {

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
        tracker.removeConnectionTracking(currentHostSpec, this.pluginService.getCurrentConnection());
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

    if (this.currentWriter == null) {
      this.currentWriter = hostSpecAfterFailover;
      this.needUpdateCurrentWriter = false;
    } else if (!this.currentWriter.getHostAndPort().equals(hostSpecAfterFailover.getHostAndPort())) {
      // the writer's changed
      tracker.invalidateAllConnections(this.currentWriter);
      tracker.logOpenedConnections();
      this.currentWriter = hostSpecAfterFailover;
      this.needUpdateCurrentWriter = false;
      hostListRefreshEndTimeNano.set(0);
    }
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
