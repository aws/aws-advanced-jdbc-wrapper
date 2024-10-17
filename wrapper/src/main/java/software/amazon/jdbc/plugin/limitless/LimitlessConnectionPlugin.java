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

package software.amazon.jdbc.plugin.limitless;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.dialect.AuroraLimitlessDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class LimitlessConnectionPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(LimitlessConnectionPlugin.class.getName());
  public static final AwsWrapperProperty WAIT_F0R_ROUTER_INFO = new AwsWrapperProperty(
      "limitlessWaitForTransactionRouterInfo",
      "true",
      "If the cache of transaction router info is empty and a new connection is made, this property toggles whether "
          + "the plugin will wait and synchronously fetch transaction router info before selecting a transaction "
          + "router to connect to, or to fall back to using the provided DB Shard Group endpoint URL.");
  public static final AwsWrapperProperty GET_ROUTER_RETRY_INTERVAL_MILLIS = new AwsWrapperProperty(
      "limitlessGetTransactionRouterInfoRetryIntervalMs",
      "300",
      "Interval in millis between retries fetching Limitless Transaction Router information.");
  public static final AwsWrapperProperty GET_ROUTER_MAX_RETRIES = new AwsWrapperProperty(
      "limitlessGetTransactionRouterInfoMaxRetries",
      "5",
      "Max number of connection retries fetching Limitless Transaction Router information.");
  public static final AwsWrapperProperty INTERVAL_MILLIS = new AwsWrapperProperty(
      "limitlessTransactionRouterMonitorIntervalMs",
      "15000",
      "Interval in millis between polling for Limitless Transaction Routers to the database.");
  public static final AwsWrapperProperty MAX_RETRIES = new AwsWrapperProperty(
      "limitlessConnectMaxRetries",
      "5",
      "Max number of connection retries the Limitless Connection Plugin will attempt.");

  protected final PluginService pluginService;
  protected final Properties properties;
  private final Supplier<LimitlessRouterService> limitlessRouterServiceSupplier;
  private LimitlessRouterService limitlessRouterService;
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
        }
      });

  static {
    PropertyDefinition.registerPluginProperties(LimitlessConnectionPlugin.class);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  public LimitlessConnectionPlugin(final PluginService pluginService, final @NonNull Properties properties) {
    this(pluginService,
        properties,
        () -> new LimitlessRouterServiceImpl(pluginService));
  }

  public LimitlessConnectionPlugin(
      final PluginService pluginService,
      final @NonNull Properties properties,
      final @NonNull Supplier<LimitlessRouterService> limitlessRouterServiceSupplier) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.limitlessRouterServiceSupplier = limitlessRouterServiceSupplier;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, isInitialConnection, connectFunc);
  }

  private Connection connectInternal(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    final Dialect dialect = this.pluginService.getDialect();
    if (dialect instanceof AuroraLimitlessDialect) {
      return connectInternalWithDialect(driverProtocol, hostSpec, props, isInitialConnection, connectFunc);
    } else {
      return connectInternalWithoutDialect(driverProtocol, hostSpec, props, isInitialConnection, connectFunc);
    }
  }

  private Connection connectInternalWithDialect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    initLimitlessRouterMonitorService();
    if (isInitialConnection) {
      this.limitlessRouterService
          .startMonitoring(hostSpec, properties, INTERVAL_MILLIS.getInteger(properties));
    }

    List<HostSpec> limitlessRouters = this.limitlessRouterService.getLimitlessRouters(
        this.pluginService.getHostListProvider().getClusterId(), props);

    Connection conn = null;
    if (Utils.isNullOrEmpty(limitlessRouters)) {
      conn = connectFunc.call();
      LOGGER.finest(Messages.get("LimitlessConnectionPlugin.limitlessRouterCacheEmpty"));
      final boolean waitForRouterInfo = WAIT_F0R_ROUTER_INFO.getBoolean(props);
      if (waitForRouterInfo) {
        limitlessRouters = synchronouslyGetLimitlessRoutersWithRetry(conn, hostSpec.getPort(), props);
      } else {
        LOGGER.finest(Messages.get("LimitlessConnectionPlugin.usingProvidedConnectUrl"));
        return conn;
      }
    }

    if (limitlessRouters.contains(hostSpec)) {
      LOGGER.finest(Messages.get("LimitlessConnectionPlugin.connectWithHost", new Object[] {hostSpec.getHost()}));
      if (conn == null  || conn.isClosed()) {
        try {
          conn = connectFunc.call();
        } catch (final SQLException e) {
          return retryConnectWithLeastLoadedRouters(limitlessRouters, props, conn, hostSpec);
        }
      }
      return conn;
    }

    RoundRobinHostSelector.setRoundRobinHostWeightPairsProperty(props, limitlessRouters);
    HostSpec selectedHostSpec;
    try {
      selectedHostSpec = this.pluginService.getHostSpecByStrategy(limitlessRouters,
          HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
      LOGGER.fine(Messages.get(
          "LimitlessConnectionPlugin.selectedHost",
          new Object[] {selectedHostSpec.getHost()}));
    } catch (SQLException e) {
      LOGGER.warning(Messages.get("LimitlessConnectionPlugin.errorSelectingRouter", new Object[] {e.getMessage()}));
      if (conn == null  || conn.isClosed()) {
        conn = connectFunc.call();
      }
      return retryConnectWithLeastLoadedRouters(limitlessRouters, props, conn, hostSpec);
    }

    try {
      return pluginService.connect(selectedHostSpec, props);
    } catch (SQLException e) {
      LOGGER.fine(Messages.get(
          "LimitlessConnectionPlugin.failedToConnectToHost",
          new Object[] {selectedHostSpec.getHost()}));
      selectedHostSpec.setAvailability(HostAvailability.NOT_AVAILABLE);
      if (conn == null  || conn.isClosed()) {
        conn = connectFunc.call();
      }
      // Retry connect prioritising healthiest router for best chance of connection over load-balancing with round-robin
      return retryConnectWithLeastLoadedRouters(limitlessRouters, props, conn, hostSpec);
    }
  }

  private Connection connectInternalWithoutDialect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    final Connection conn = connectFunc.call();

    final Dialect dialect = this.pluginService.getDialect();
    if (!(dialect instanceof AuroraLimitlessDialect)) {
      throw new UnsupportedOperationException(Messages.get("LimitlessConnectionPlugin.unsupportedDialectOrDatabase",
          new Object[] {dialect}));
    }

    initLimitlessRouterMonitorService();
    if (isInitialConnection) {
      this.limitlessRouterService
          .startMonitoring(hostSpec, properties, INTERVAL_MILLIS.getInteger(properties));
    }

    List<HostSpec> limitlessRouters = this.limitlessRouterService.getLimitlessRouters(
        this.pluginService.getHostListProvider().getClusterId(), props);
    if (Utils.isNullOrEmpty(limitlessRouters)) {
      LOGGER.finest(Messages.get("LimitlessConnectionPlugin.limitlessRouterCacheEmpty"));
      final boolean waitForRouterInfo = WAIT_F0R_ROUTER_INFO.getBoolean(props);
      if (waitForRouterInfo) {
        synchronouslyGetLimitlessRoutersWithRetry(conn, hostSpec.getPort(), props);
      }
    }

    return conn;
  }

  private void initLimitlessRouterMonitorService() {
    if (limitlessRouterService == null) {
      this.limitlessRouterService = this.limitlessRouterServiceSupplier.get();
    }
  }

  private Connection retryConnectWithLeastLoadedRouters(final List<HostSpec> limitlessRouters, final Properties props,
      final Connection conn, final HostSpec hostSpec) throws SQLException {

    List<HostSpec> currentRouters = limitlessRouters;
    int retryCount = 0;
    final int maxRetries = MAX_RETRIES.getInteger(props);

    while (retryCount++ < maxRetries) {
      if (currentRouters.stream().noneMatch(h -> h.getAvailability().equals(HostAvailability.AVAILABLE))) {
        currentRouters = synchronouslyGetLimitlessRoutersWithRetry(conn, hostSpec.getPort(), props);
        if (currentRouters == null
            || currentRouters.isEmpty()
            || currentRouters.stream().noneMatch(h -> h.getAvailability().equals(HostAvailability.AVAILABLE))) {
          LOGGER.warning(Messages.get("LimitlessConnectionPlugin.noRoutersAvailableForRetry"));
          return conn;
        }
      }

      HostSpec selectedHostSpec = hostSpec;
      try {
        // Select healthiest router for best chance of connection over load-balancing with round-robin
        selectedHostSpec = this.pluginService.getHostSpecByStrategy(limitlessRouters,
            HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
        LOGGER.finest(Messages.get(
            "LimitlessConnectionPlugin.selectedHostForRetry",
            new Object[] {selectedHostSpec.getHost()}));
      } catch (final UnsupportedOperationException e) {
        LOGGER.severe(Messages.get("LimitlessConnectionPlugin.incorrectConfiguration"));
        throw e;
      } catch (final SQLException e) {
        // error from host selector
        continue;
      }

      try {
        return pluginService.connect(selectedHostSpec, props);
      } catch (final SQLException e) {
        selectedHostSpec.setAvailability(HostAvailability.NOT_AVAILABLE);
        LOGGER.finest(Messages.get(
            "LimitlessConnectionPlugin.failedToConnectToHost",
            new Object[] {selectedHostSpec.getHost()}));
      }
    }
    LOGGER.warning(Messages.get("LimitlessConnectionPlugin.maxRetriesExceeded"));
    return conn;
  }

  private List<HostSpec> synchronouslyGetLimitlessRoutersWithRetry(
      final Connection conn, final int hostPort, final Properties props) throws SQLException {
    LOGGER.finest(Messages.get("LimitlessConnectionPlugin.synchronouslyGetLimitlessRouters"));
    int retryCount = -1; // start at -1 since the first try is not a retry.
    int maxRetries = GET_ROUTER_MAX_RETRIES.getInteger(props);
    int retryIntervalMs = GET_ROUTER_RETRY_INTERVAL_MILLIS.getInteger(props);
    do {
      try {
        List<HostSpec> newLimitlessRouters = this.limitlessRouterService.forceGetLimitlessRoutersWithConn(
            conn, hostPort, props);
        if (newLimitlessRouters != null && !newLimitlessRouters.isEmpty()) {
          return newLimitlessRouters;
        }
        Thread.sleep(retryIntervalMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        retryCount++;
      }
    } while (retryCount < maxRetries);
    throw new SQLException(Messages.get("LimitlessConnectionPlugin.noRoutersAvailable"));
  }
}
