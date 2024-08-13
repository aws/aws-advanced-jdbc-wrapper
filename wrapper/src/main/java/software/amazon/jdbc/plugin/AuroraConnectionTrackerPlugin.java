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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SubscribedMethodHelper;

public class AuroraConnectionTrackerPlugin extends AbstractConnectionPlugin implements
    CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(AuroraConnectionTrackerPlugin.class.getName());

  static final String METHOD_ABORT = "Connection.abort";
  static final String METHOD_CLOSE = "Connection.close";
  private final Set<String> subscribedMethods;
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

    final HashSet<String> methods = new HashSet<>();
    methods.add("connect");
    methods.add("forceConnect");
    methods.add("notifyNodeListChanged");
    methods.addAll(this.pluginService.getTargetDriverDialect().getNetworkBoundMethodNames());
    this.subscribedMethods = Collections.unmodifiableSet(methods);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(final String driverProtocol, final HostSpec hostSpec, final Properties props,
      final boolean isInitialConnection, final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    return connectInternal(hostSpec, connectFunc);
  }

  public Connection connectInternal(
      final HostSpec hostSpec, final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final Connection conn = connectFunc.call();

    if (conn != null) {
      final RdsUrlType type = this.rdsHelper.identifyRdsType(hostSpec.getHost());
      if (type.isRdsCluster()) {
        hostSpec.resetAliases();
        this.pluginService.fillAliases(conn, hostSpec);
      }
      tracker.populateOpenedConnectionQueue(hostSpec, conn);
    }

    return conn;
  }

  @Override
  public Connection forceConnect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> forceConnectFunc) throws SQLException {
    return connectInternal(hostSpec, forceConnectFunc);
  }

  @Override
  public <T, E extends Exception> T execute(final Class<T> resultClass, final Class<E> exceptionClass,
      final Object methodInvokeOn, final String methodName, final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs) throws E {

    final HostSpec currentHostSpec = this.pluginService.getCurrentHostSpec();
    this.rememberWriter();

    try {
      final T result = jdbcMethodFunc.call();
      if ((methodName.equals(METHOD_CLOSE) || methodName.equals(METHOD_ABORT))) {
        tracker.invalidateCurrentConnection(currentHostSpec, this.pluginService.getCurrentConnection());
      } else if (this.needUpdateCurrentWriter) {
        this.checkWriterChanged();
      }
      return result;

    } catch (final Exception e) {
      if (e instanceof FailoverSQLException) {
        this.checkWriterChanged();
      }
      throw e;
    }
  }

  private void checkWriterChanged() {
    final HostSpec hostSpecAfterFailover = this.getWriter(this.pluginService.getHosts());

    if (this.currentWriter == null) {
      this.currentWriter = hostSpecAfterFailover;
      this.needUpdateCurrentWriter = false;

    } else if (!this.currentWriter.equals(hostSpecAfterFailover)) {
      // the writer's changed
      tracker.invalidateAllConnections(this.currentWriter);
      tracker.logOpenedConnections();
      this.currentWriter = hostSpecAfterFailover;
      this.needUpdateCurrentWriter = false;
    }
  }

  private void rememberWriter() {
    if (this.currentWriter == null || this.needUpdateCurrentWriter) {
      this.currentWriter = this.getWriter(this.pluginService.getHosts());
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

  @Override
  public void releaseResources() {
    tracker.pruneNullConnections();
  }

  private HostSpec getWriter(final @NonNull List<HostSpec> hosts) {
    for (final HostSpec hostSpec : hosts) {
      if (hostSpec.getRole() == HostRole.WRITER) {
        return hostSpec;
      }
    }
    return null;
  }
}
