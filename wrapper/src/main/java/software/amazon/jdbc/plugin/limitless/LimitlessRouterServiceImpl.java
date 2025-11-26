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

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HighestWeightHostSelector;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.WeightedRandomHostSelector;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.HostSelectorUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.monitoring.MonitorErrorResponse;

public class LimitlessRouterServiceImpl implements LimitlessRouterService {
  private static final Logger LOGGER =
      Logger.getLogger(LimitlessRouterServiceImpl.class.getName());
  public static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "limitlessTransactionRouterMonitorDisposalTimeMs",
          "600000", // 10min
          "Interval in milliseconds for an Limitless router monitor to be considered inactive and to be disposed.");
  protected static final Map<String, ResourceLock> forceGetLimitlessRoutersLockMap = new ConcurrentHashMap<>();
  protected static final Set<MonitorErrorResponse> monitorErrorResponses =
      new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE));
  protected final FullServicesContainer servicesContainer;
  protected final PluginService pluginService;
  protected final LimitlessQueryHelper queryHelper;

  static {
    PropertyDefinition.registerPluginProperties(LimitlessRouterServiceImpl.class);
  }

  public LimitlessRouterServiceImpl(
      final @NonNull FullServicesContainer servicesContainer,
      final @NonNull Properties props) {
    this(servicesContainer, new LimitlessQueryHelper(servicesContainer.getPluginService()), props);
  }

  public LimitlessRouterServiceImpl(
      final @NonNull FullServicesContainer servicesContainer,
      final @NonNull LimitlessQueryHelper queryHelper,
      final @NonNull Properties props) {
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();
    this.queryHelper = queryHelper;

    this.servicesContainer.getStorageService().registerItemClassIfAbsent(
        LimitlessRouters.class,
        true,
        TimeUnit.MILLISECONDS.toNanos(MONITOR_DISPOSAL_TIME_MS.getLong(props)),
        null,
        null
    );

    this.servicesContainer.getMonitorService().registerMonitorTypeIfAbsent(
        LimitlessRouterMonitor.class,
        TimeUnit.MILLISECONDS.toNanos(MONITOR_DISPOSAL_TIME_MS.getLong(props)),
        TimeUnit.MINUTES.toNanos(3),
        monitorErrorResponses,
        LimitlessRouters.class
    );
  }

  @Override
  public void establishConnection(final LimitlessConnectionContext context) throws SQLException {
    context.setLimitlessRouters(getLimitlessRouters(this.pluginService.getHostListProvider().getClusterId()));

    if (Utils.isNullOrEmpty(context.getLimitlessRouters())) {
      LOGGER.finest(Messages.get("LimitlessRouterServiceImpl.limitlessRouterCacheEmpty"));
      final boolean waitForRouterInfo = LimitlessConnectionPlugin.WAIT_FOR_ROUTER_INFO.getBoolean(context.getProps());
      if (waitForRouterInfo) {
        synchronouslyGetLimitlessRoutersWithRetry(context);
      } else {
        LOGGER.finest(Messages.get("LimitlessRouterServiceImpl.usingProvidedConnectUrl"));
        if (context.getConnection() == null || context.getConnection().isClosed()) {
          context.setConnection(context.getConnectFunc().call());
        }
        return;
      }
    }

    if (Utils.containsHostAndPort(context.getLimitlessRouters(), context.getHostSpec().getHostAndPort())) {
      LOGGER.finest(Messages.get(
          "LimitlessRouterServiceImpl.connectWithHost",
          new Object[] {context.getHostSpec().getHost()}));
      if (context.getConnection() == null  || context.getConnection().isClosed()) {
        try {
          context.setConnection(context.getConnectFunc().call());
        } catch (final SQLException e) {
          if (this.isLoginException(e)) {
            throw e;
          }
          retryConnectWithLeastLoadedRouters(context);
        }
      }
      return;
    }

    HostSelectorUtils.setHostWeightPairsProperty(WeightedRandomHostSelector.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS,
        context.getProps(),
        context.getLimitlessRouters());
    HostSpec selectedHostSpec;
    try {
      selectedHostSpec = this.pluginService.getHostSpecByStrategy(
          context.getLimitlessRouters(),
          HostRole.WRITER,
          WeightedRandomHostSelector.STRATEGY_WEIGHTED_RANDOM);
      LOGGER.fine(Messages.get(
          "LimitlessRouterServiceImpl.selectedHost",
          new Object[] {selectedHostSpec != null ? selectedHostSpec.getHost() : "null"}));
    } catch (SQLException e) {
      if (this.isLoginException(e)) {
        throw e;
      }
      retryConnectWithLeastLoadedRouters(context);
      return;
    }

    if (selectedHostSpec == null) {
      retryConnectWithLeastLoadedRouters(context);
      return;
    }

    try {
      context.setConnection(this.pluginService.connect(selectedHostSpec, context.getProps(), context.getPlugin()));
    } catch (SQLException e) {
      if (this.isLoginException(e)) {
        throw e;
      }
      if (selectedHostSpec != null) {
        LOGGER.fine(Messages.get(
            "LimitlessRouterServiceImpl.failedToConnectToHost",
            new Object[] {selectedHostSpec.getHost()}));
        selectedHostSpec.setAvailability(HostAvailability.NOT_AVAILABLE);
      }
      // Retry connect prioritising the healthiest router for best chance of
      // connection over load-balancing with round-robin.
      retryConnectWithLeastLoadedRouters(context);
    }
  }

  protected List<HostSpec> getLimitlessRouters(final String clusterId) {
    LimitlessRouters routers = this.servicesContainer.getStorageService().get(LimitlessRouters.class, clusterId);
    return routers == null ? null : routers.getHosts();
  }

  private void retryConnectWithLeastLoadedRouters(
      final LimitlessConnectionContext context) throws SQLException {

    int retryCount = 0;
    final int maxRetries = LimitlessConnectionPlugin.MAX_RETRIES.getInteger(context.getProps());

    while (retryCount++ < maxRetries) {
      if (Utils.isNullOrEmpty(context.getLimitlessRouters())
          || context.getLimitlessRouters()
          .stream()
          .noneMatch(h -> h.getAvailability().equals(HostAvailability.AVAILABLE))) {
        synchronouslyGetLimitlessRoutersWithRetry(context);

        if (Utils.isNullOrEmpty(context.getLimitlessRouters())
            || context.getLimitlessRouters()
              .stream()
              .noneMatch(h -> h.getAvailability().equals(HostAvailability.AVAILABLE))) {
          LOGGER.warning(Messages.get("LimitlessRouterServiceImpl.noRoutersAvailableForRetry"));
          if (context.getConnection() != null && !context.getConnection().isClosed()) {
            return;
          } else {
            try {
              context.setConnection(context.getConnectFunc().call());
              return;
            } catch (final SQLException e) {
              if (this.isLoginException(e)) {
                throw e;
              }
              throw new SQLException(Messages.get(
                  "LimitlessRouterServiceImpl.unableToConnectNoRoutersAvailable",
                  new Object[] {context.getHostSpec().getHost()}), e);
            }
          }
        }
      }

      final HostSpec selectedHostSpec;
      try {
        // Select healthiest router for best chance of connection over load-balancing with round-robin
        selectedHostSpec = this.pluginService.getHostSpecByStrategy(context.getLimitlessRouters(),
            HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
        LOGGER.finest(Messages.get(
            "LimitlessRouterServiceImpl.selectedHostForRetry",
            new Object[] {selectedHostSpec != null ? selectedHostSpec.getHost() : "null"}));
        if (selectedHostSpec == null) {
          continue;
        }
      } catch (final UnsupportedOperationException e) {
        LOGGER.severe(Messages.get("LimitlessRouterServiceImpl.incorrectConfiguration"));
        throw e;
      } catch (final SQLException e) {
        // error from host selector
        continue;
      }

      try {
        context.setConnection(pluginService.connect(selectedHostSpec, context.getProps(), context.getPlugin()));
        if (context.getConnection() != null) {
          return;
        }
      } catch (final SQLException e) {
        if (this.isLoginException(e)) {
          throw e;
        }
        selectedHostSpec.setAvailability(HostAvailability.NOT_AVAILABLE);
        LOGGER.finest(Messages.get(
            "LimitlessRouterServiceImpl.failedToConnectToHost",
            new Object[] {selectedHostSpec.getHost()}));
      }
    }
    throw new SQLException(Messages.get("LimitlessRouterServiceImpl.maxRetriesExceeded"));
  }

  protected void synchronouslyGetLimitlessRoutersWithRetry(final LimitlessConnectionContext context)
      throws SQLException {
    LOGGER.finest(Messages.get("LimitlessRouterServiceImpl.synchronouslyGetLimitlessRouters"));
    int retryCount = -1; // start at -1 since the first try is not a retry.
    int maxRetries = LimitlessConnectionPlugin.GET_ROUTER_MAX_RETRIES.getInteger(context.getProps());
    int retryIntervalMs = LimitlessConnectionPlugin.GET_ROUTER_RETRY_INTERVAL_MILLIS.getInteger(context.getProps());
    do {
      try {
        synchronouslyGetLimitlessRouters(context);
        if (!Utils.isNullOrEmpty(context.getLimitlessRouters())) {
          return;
        }
        Thread.sleep(retryIntervalMs);
      } catch (final SQLException e) {
        if (this.isLoginException(e)) {
          throw e;
        }
        LOGGER.finest(Messages.get("LimitlessRouterServiceImpl.getLimitlessRoutersException", new Object[] {e}));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException(Messages.get("LimitlessRouterServiceImpl.interruptedSynchronousGetRouter"), e);
      } finally {
        retryCount++;
      }
    } while (retryCount < maxRetries);
    throw new SQLException(Messages.get("LimitlessRouterServiceImpl.noRoutersAvailable"));
  }

  protected void synchronouslyGetLimitlessRouters(final LimitlessConnectionContext context)
      throws SQLException {
    final ResourceLock lock = forceGetLimitlessRoutersLockMap.computeIfAbsent(
        this.pluginService.getHostListProvider().getClusterId(),
        key -> new ResourceLock()
    );
    try (ResourceLock ignored = lock.obtain()) {
      final List<HostSpec> limitlessRouters =
          getLimitlessRouters(this.pluginService.getHostListProvider().getClusterId());
      if (!Utils.isNullOrEmpty(limitlessRouters)) {
        context.setLimitlessRouters(limitlessRouters);
        return;
      }

      if (context.getConnection() == null || context.getConnection().isClosed()) {
        context.setConnection(context.getConnectFunc().call());
      }
      final List<HostSpec> newRouterList =
          this.queryHelper.queryForLimitlessRouters(context.getConnection(), context.getHostSpec().getPort());

      if (!Utils.isNullOrEmpty(newRouterList)) {
        context.setLimitlessRouters(newRouterList);
        LimitlessRouters newRouters = new LimitlessRouters(newRouterList);
        this.servicesContainer.getStorageService().set(
            this.pluginService.getHostListProvider().getClusterId(),
            newRouters);
      } else {
        throw new SQLException(Messages.get("LimitlessRouterServiceImpl.fetchedEmptyRouterList"));
      }
    }
  }

  protected boolean isLoginException(Throwable throwable) {
    return this.pluginService.isLoginException(throwable, this.pluginService.getTargetDriverDialect());
  }

  @Override
  public void startMonitoring(final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final int intervalMs) {

    try {
      final String limitlessRouterMonitorKey = pluginService.getHostListProvider().getClusterId();
      this.servicesContainer.getMonitorService().runIfAbsent(
          LimitlessRouterMonitor.class,
          limitlessRouterMonitorKey,
          this.servicesContainer,
          props,
          (servicesContainer) -> new LimitlessRouterMonitor(
                  servicesContainer,
                  hostSpec,
                  limitlessRouterMonitorKey,
                  props,
                  intervalMs));
    } catch (SQLException e) {
      LOGGER.warning(Messages.get("LimitlessRouterServiceImpl.errorStartingMonitor", new Object[]{e}));
      throw new RuntimeException(e);
    }
  }

  public static void clearCache() {
    forceGetLimitlessRoutersLockMap.clear();
  }
}
