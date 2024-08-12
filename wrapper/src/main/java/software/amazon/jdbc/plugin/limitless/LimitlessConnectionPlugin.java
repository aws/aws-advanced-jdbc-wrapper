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
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class LimitlessConnectionPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(LimitlessConnectionPlugin.class.getName());
  protected static final AwsWrapperProperty INTERVAL_MILLIS = new AwsWrapperProperty(
      "limitlessTransactionRouterMonitorIntervalMs",
      "15000",
      "Interval in millis between polling for Limitless Transaction Routers to the database.");
  protected static final AwsWrapperProperty MAX_RETRIES = new AwsWrapperProperty(
      "limitlessConnectMaxRetries",
      "5",
      "Max number of connection retries the Limitless Connection Plugin will attempt.");
  protected final PluginService pluginService;
  protected final @NonNull Properties properties;
  private final @NonNull Supplier<LimitlessRouterService> limitlessRouterServiceSupplier;
  private LimitlessRouterService limitlessRouterService;
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
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
        LimitlessRouterServiceImpl::new);
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

  @Override
  public Connection forceConnect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, isInitialConnection, forceConnectFunc);
  }

  private Connection connectInternal(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection,
          SQLException> connectFunc) throws SQLException {
    initLimitlessRouterMonitorService();
    if (isInitialConnection) {
      this.limitlessRouterService
          .startMonitoring(pluginService, hostSpec, properties, INTERVAL_MILLIS.getInteger(properties));
    }

    List<HostSpec> limitlessRouters = this.limitlessRouterService.getLimitlessRouters(
        this.pluginService.getHostListProvider().getClusterId(), props);

    if (limitlessRouters.isEmpty()) {
      LOGGER.info(Messages.get("LimitlessConnectionPlugin.limitlessRouterCacheEmpty"));
      return connectFunc.call();
    } else if (limitlessRouters.contains(hostSpec)) {
      return connectFunc.call();
    }

    RoundRobinHostSelector.setRoundRobinHostWeightPairsProperty(props, limitlessRouters);
    final HostSpec selectedHostSpec;
    try {
      selectedHostSpec = this.pluginService.getHostSpecByStrategy(limitlessRouters,
          HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
      LOGGER.fine(Messages.get(
          "LimitlessConnectionPlugin.selectedHost",
          new Object[] {selectedHostSpec.getHost()}));
    } catch (UnsupportedOperationException e) {
      LOGGER.severe(Messages.get("LimitlessConnectionPlugin.incorrectConfiguration"));
      throw e;
    }

    try {
      return pluginService.connect(selectedHostSpec, props);
    } catch (SQLException e) {
      LOGGER.warning(Messages.get(
          "LimitlessConnectionPlugin.failedToConnectToHost",
          new Object[] {selectedHostSpec.getHost()}));
      selectedHostSpec.setAvailability(HostAvailability.NOT_AVAILABLE);
      return retryConnection(limitlessRouters, props, 0, e);
    }
  }

  private void initLimitlessRouterMonitorService() {
    if (limitlessRouterService == null) {
      this.limitlessRouterService = this.limitlessRouterServiceSupplier.get();
    }
  }

  private Connection retryConnection(final List<HostSpec> limitlessRouters, final Properties props, int inputRetryCount,
      final SQLException originalException) throws SQLException {

    int retryCount = inputRetryCount;
    final int maxRetries = MAX_RETRIES.getInteger(props) >= 0
        ? MAX_RETRIES.getInteger(props)
        : Integer.valueOf(MAX_RETRIES.defaultValue);

    while (limitlessRouters.stream().anyMatch(h -> h.getAvailability().equals(HostAvailability.AVAILABLE))
        && retryCount < maxRetries) {
      final HostSpec selectedHostSpec;
      try {
        selectedHostSpec = this.pluginService.getHostSpecByStrategy(limitlessRouters,
            HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
        LOGGER.fine(Messages.get(
            "LimitlessConnectionPlugin.selectedHostForRetry",
            new Object[] {selectedHostSpec.getHost()}));
      } catch (UnsupportedOperationException e) {
        LOGGER.severe(Messages.get("LimitlessConnectionPlugin.incorrectConfiguration"));
        throw e;
      }
      try {
        return pluginService.connect(selectedHostSpec, props);
      } catch (SQLException e) {
        selectedHostSpec.setAvailability(HostAvailability.NOT_AVAILABLE);
        LOGGER.warning(Messages.get(
            "LimitlessConnectionPlugin.failedToConnectToHost",
            new Object[] {selectedHostSpec.getHost()}));
      }
      retryCount++;
    }

    if (retryCount >= maxRetries) {
      throw new SQLException(Messages.get("LimitlessConnectionPlugin.maxRetriesExceeded"), originalException);
    }

    limitlessRouterService.runMonitor(this.pluginService.getHostListProvider().getClusterId(), props);
    final List<HostSpec> newLimitlessRouters = this.limitlessRouterService.getLimitlessRouters(
        this.pluginService.getHostListProvider().getClusterId(), props);
    if (newLimitlessRouters == null || newLimitlessRouters.isEmpty()) {
      throw new SQLException(Messages.get("LimitlessConnectionPlugin.noRoutersAvailableForRetry"), originalException);
    }

    return retryConnection(newLimitlessRouters, props, retryCount, originalException);
  }
}
