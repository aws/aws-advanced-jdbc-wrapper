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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlMethodAnalyzer;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

/**
 * This connection plugin will always be the last plugin in the connection plugin chain, and will
 * invoke the JDBC method passed down the chain.
 */
public final class DefaultConnectionPlugin implements ConnectionPlugin {

  private static final Logger LOGGER =  Logger.getLogger(DefaultConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Collections.singletonList("*")));
  private static final SqlMethodAnalyzer sqlMethodAnalyzer = new SqlMethodAnalyzer();

  private final @NonNull ConnectionProvider defaultConnProvider;
  private final @Nullable ConnectionProvider effectiveConnProvider;

  private final ConnectionProviderManager connProviderManager;
  private final PluginService pluginService;
  private final PluginManagerService pluginManagerService;

  public DefaultConnectionPlugin(
      final PluginService pluginService,
      final ConnectionProvider defaultConnProvider,
      final @Nullable ConnectionProvider effectiveConnProvider,
      final PluginManagerService pluginManagerService) {
    this(pluginService,
        defaultConnProvider,
        effectiveConnProvider,
        pluginManagerService,
        new ConnectionProviderManager(defaultConnProvider));
  }

  public DefaultConnectionPlugin(
      final PluginService pluginService,
      final ConnectionProvider defaultConnProvider,
      final @Nullable ConnectionProvider effectiveConnProvider,
      final PluginManagerService pluginManagerService,
      final ConnectionProviderManager connProviderManager) {
    if (pluginService == null) {
      throw new IllegalArgumentException("pluginService");
    }
    if (pluginManagerService == null) {
      throw new IllegalArgumentException("pluginManagerService");
    }
    if (defaultConnProvider == null) {
      throw new IllegalArgumentException("connectionProvider");
    }

    this.pluginService = pluginService;
    this.pluginManagerService = pluginManagerService;
    this.defaultConnProvider = defaultConnProvider;
    this.effectiveConnProvider = effectiveConnProvider;
    this.connProviderManager = connProviderManager;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    LOGGER.finest(
        () -> Messages.get("DefaultConnectionPlugin.executingMethod", new Object[] {methodName}));

    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        this.pluginService.getTargetName(), TelemetryTraceLevel.NESTED);

    T result;
    try {
      result = jdbcMethodFunc.call();
    } finally {
      telemetryContext.closeContext();
    }

    final Connection currentConn = this.pluginService.getCurrentConnection();
    final Connection boundConnection = WrapperUtils.getConnectionFromSqlObject(methodInvokeOn);
    if (boundConnection != null && boundConnection != currentConn) {
      // The method being invoked is using an old connection, so transaction/autocommit analysis should be skipped.
      // ConnectionPluginManager#execute blocks all methods invoked using old connections except for close/abort.
      return result;
    }

    if (sqlMethodAnalyzer.doesOpenTransaction(currentConn, methodName, jdbcMethodArgs)) {
      this.pluginManagerService.setInTransaction(true);
    } else if (
        sqlMethodAnalyzer.doesCloseTransaction(currentConn, methodName, jdbcMethodArgs)
            // According to the JDBC spec, transactions are committed if autocommit is switched from false to true.
            || sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(currentConn, methodName,
            jdbcMethodArgs)) {
      this.pluginManagerService.setInTransaction(false);
    }

    if (sqlMethodAnalyzer.isStatementSettingAutoCommit(methodName, jdbcMethodArgs)) {
      final Boolean autocommit = sqlMethodAnalyzer.getAutoCommitValueFromSqlStatement(jdbcMethodArgs);
      if (autocommit != null) {
        try {
          currentConn.setAutoCommit(autocommit);
        } catch (final SQLException e) {
          // do nothing
        }
      }
    }

    return result;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    ConnectionProvider connProvider = null;

    if (this.effectiveConnProvider != null) {
      if (this.effectiveConnProvider.acceptsUrl(driverProtocol, hostSpec, props)) {
        connProvider = this.effectiveConnProvider;
      }
    }

    if (connProvider == null) {
      connProvider =
          this.connProviderManager.getConnectionProvider(driverProtocol, hostSpec, props);
    }

    // It's guaranteed that this plugin is always the last in plugin chain so connectFunc can be
    // ignored.
    return connectInternal(driverProtocol, hostSpec, props, connProvider, isInitialConnection);
  }

  private Connection connectInternal(
      String driverProtocol,
      HostSpec hostSpec,
      Properties props,
      ConnectionProvider connProvider,
      final boolean isInitialConnection)
      throws SQLException {

    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        connProvider.getTargetName(), TelemetryTraceLevel.NESTED);

    Connection conn;
    try {
      conn = connProvider.connect(
          driverProtocol,
          this.pluginService.getDialect(),
          this.pluginService.getTargetDriverDialect(),
          hostSpec,
          props);
    } finally {
      telemetryContext.closeContext();
    }

    this.connProviderManager.initConnection(conn, driverProtocol, hostSpec, props);

    this.pluginService.setAvailability(hostSpec.asAliases(), HostAvailability.AVAILABLE);
    if (isInitialConnection) {
      this.pluginService.updateDialect(conn);
    }

    return conn;
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {

    // It's guaranteed that this plugin is always the last in plugin chain so forceConnectFunc can be
    // ignored.
    return connectInternal(driverProtocol, hostSpec, props, this.defaultConnProvider, isInitialConnection);
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) {
    if (HostRole.UNKNOWN.equals(role)) {
      // Users must request either a writer or a reader role.
      return false;
    }

    if (this.effectiveConnProvider != null) {
      return this.effectiveConnProvider.acceptsStrategy(role, strategy);
    }
    return this.connProviderManager.acceptsStrategy(role, strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy)
      throws SQLException {
    List<HostSpec> hosts = this.pluginService.getHosts();

    return this.getHostSpecByStrategy(hosts, role, strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(final List<HostSpec> hosts, final HostRole role, final String strategy)
      throws SQLException {
    if (HostRole.UNKNOWN.equals(role)) {
      // Users must request either a writer or a reader role.
      throw new SQLException("DefaultConnectionPlugin.unknownRoleRequested");
    }

    if (hosts.isEmpty()) {
      throw new SQLException(Messages.get("DefaultConnectionPlugin.noHostsAvailable"));
    }

    if (this.effectiveConnProvider != null) {
      return this.effectiveConnProvider.getHostSpecByStrategy(hosts,
          role, strategy, this.pluginService.getProperties());
    }
    return this.connProviderManager.getHostSpecByStrategy(hosts, role, strategy, this.pluginService.getProperties());
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {

    // do nothing
    // It's guaranteed that this plugin is always the last in plugin chain so initHostProviderFunc
    // can be omitted.
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(
      final EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(final Map<String, EnumSet<NodeChangeOptions>> changes) {
    // do nothing
  }

  List<String> parseMultiStatementQueries(String query) {
    if (query == null || query.isEmpty()) {
      return new ArrayList<>();
    }

    query = query.replaceAll("\\s+", " ");

    // Check to see if string only has blank spaces.
    if (query.trim().isEmpty()) {
      return new ArrayList<>();
    }

    return Arrays.stream(query.split(";")).collect(Collectors.toList());
  }
}
