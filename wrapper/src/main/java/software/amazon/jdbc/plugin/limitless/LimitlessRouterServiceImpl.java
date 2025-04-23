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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class LimitlessRouterServiceImpl implements LimitlessRouterService {
  private static final Logger LOGGER =
      Logger.getLogger(LimitlessRouterServiceImpl.class.getName());
  public static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "limitlessTransactionRouterMonitorDisposalTimeMs",
          "600000", // 10min
          "Interval in milliseconds for an Limitless router monitor to be considered inactive and to be disposed.");
  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);
  protected static final Map<String, ReentrantLock> forceGetLimitlessRoutersLockMap = new ConcurrentHashMap<>();
  protected final PluginService pluginService;
  protected final LimitlessQueryHelper queryHelper;
  protected final LimitlessRouterMonitorInitializer limitlessRouterMonitorInitializer;

  protected static final SlidingExpirationCacheWithCleanupThread<String, LimitlessRouterMonitor>
      limitlessRouterMonitors = new SlidingExpirationCacheWithCleanupThread<>(
          limitlessRouterMonitor -> true,
          limitlessRouterMonitor -> {
            try {
              limitlessRouterMonitor.close();
            } catch (Exception e) {
              LOGGER.warning(Messages.get("LimitlessRouterServiceImpl.errorClosingMonitor",
                  new Object[]{e.getMessage()}));
            }
          },
          CACHE_CLEANUP_NANO
      );

  protected static final SlidingExpirationCacheWithCleanupThread<String, List<HostSpec>>
      limitlessRouterCache =
      new SlidingExpirationCacheWithCleanupThread<>(
          x  -> true,
          x -> {},
        CACHE_CLEANUP_NANO
      );

  public LimitlessRouterServiceImpl(final @NonNull PluginService pluginService) {
    this(
        pluginService,
        (hostSpec,
            routerCache,
            routerCacheKey,
            props,
            intervalMs) ->
            new LimitlessRouterMonitor(
                pluginService,
                hostSpec,
                routerCache,
                routerCacheKey,
                props,
                intervalMs),
        new LimitlessQueryHelper(pluginService));
  }

  public LimitlessRouterServiceImpl(
      final @NonNull PluginService pluginService,
      final LimitlessRouterMonitorInitializer limitlessRouterMonitorInitializer,
      final LimitlessQueryHelper queryHelper) {
    this.pluginService = pluginService;
    this.limitlessRouterMonitorInitializer = limitlessRouterMonitorInitializer;
    this.queryHelper = queryHelper;
  }

  @Override
  public void establishConnection(final LimitlessConnectionContext context) throws SQLException {
    context.setLimitlessRouters(getLimitlessRouters(
        this.pluginService.getHostListProvider().getClusterId(), context.getProps()));

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

    if (context.getLimitlessRouters().contains(context.getHostSpec())) {
      LOGGER.finest(Messages.get(
          "LimitlessRouterServiceImpl.connectWithHost",
          new Object[] {context.getHostSpec().getHost()}));
      if (context.getConnection() == null  || context.getConnection().isClosed()) {
        try {
          context.setConnection(context.getConnectFunc().call());
        } catch (final SQLException e) {
          retryConnectWithLeastLoadedRouters(context);
        }
      }
      return;
    }

    RoundRobinHostSelector.setRoundRobinHostWeightPairsProperty(
        context.getProps(),
        context.getLimitlessRouters());
    HostSpec selectedHostSpec;
    try {
      selectedHostSpec = this.pluginService.getHostSpecByStrategy(
          context.getLimitlessRouters(),
          HostRole.WRITER,
          RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
      LOGGER.fine(Messages.get(
          "LimitlessRouterServiceImpl.selectedHost",
          new Object[] {selectedHostSpec != null ? selectedHostSpec.getHost() : "null"}));
    } catch (SQLException e) {
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
      if (selectedHostSpec != null) {
        LOGGER.fine(Messages.get(
            "LimitlessRouterServiceImpl.failedToConnectToHost",
            new Object[] {selectedHostSpec.getHost()}));
        selectedHostSpec.setAvailability(HostAvailability.NOT_AVAILABLE);
      }
      // Retry connect prioritising healthiest router for best chance of connection over load-balancing with round-robin
      retryConnectWithLeastLoadedRouters(context);
    }
  }

  protected List<HostSpec> getLimitlessRouters(final String clusterId, final Properties props) {
    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        MONITOR_DISPOSAL_TIME_MS.getLong(props));
    return limitlessRouterCache.get(clusterId, cacheExpirationNano);
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
              throw new SQLException(Messages.get("LimitlessRouterServiceImpl.noRoutersAvailable"));
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
    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        MONITOR_DISPOSAL_TIME_MS.getLong(context.getProps()));

    final ReentrantLock lock = forceGetLimitlessRoutersLockMap.computeIfAbsent(
        this.pluginService.getHostListProvider().getClusterId(),
        key -> new ReentrantLock()
    );
    lock.lock();
    try {
      final List<HostSpec> limitlessRouters =
          limitlessRouterCache.get(this.pluginService.getHostListProvider().getClusterId(), cacheExpirationNano);
      if (!Utils.isNullOrEmpty(limitlessRouters)) {
        context.setLimitlessRouters(limitlessRouters);
        return;
      }

      if (context.getConnection() == null || context.getConnection().isClosed()) {
        context.setConnection(context.getConnectFunc().call());
      }
      final List<HostSpec> newLimitlessRouters =
          this.queryHelper.queryForLimitlessRouters(context.getConnection(), context.getHostSpec().getPort());

      if (!Utils.isNullOrEmpty(newLimitlessRouters)) {
        context.setLimitlessRouters(newLimitlessRouters);
        limitlessRouterCache.put(
            this.pluginService.getHostListProvider().getClusterId(),
            newLimitlessRouters,
            TimeUnit.MILLISECONDS.toNanos(
                LimitlessRouterServiceImpl.MONITOR_DISPOSAL_TIME_MS.getLong(context.getProps())));
      } else {
        throw new SQLException(Messages.get("LimitlessRouterServiceImpl.fetchedEmptyRouterList"));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void startMonitoring(final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final int intervalMs) {

    try {
      final String limitlessRouterMonitorKey = pluginService.getHostListProvider().getClusterId();
      final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(MONITOR_DISPOSAL_TIME_MS.getLong(props));

      limitlessRouterMonitors.computeIfAbsent(
          limitlessRouterMonitorKey,
          key -> this.limitlessRouterMonitorInitializer
              .createLimitlessRouterMonitor(
                  hostSpec,
                  limitlessRouterCache,
                  limitlessRouterMonitorKey,
                  props,
                  intervalMs),
          cacheExpirationNano);
    } catch (SQLException e) {
      LOGGER.warning(Messages.get("LimitlessRouterServiceImpl.errorStartingMonitor", new Object[]{e}));
      throw new RuntimeException(e);
    }
  }

  public static void clearCache() {
    forceGetLimitlessRoutersLockMap.clear();
    limitlessRouterCache.clear();
  }
}
