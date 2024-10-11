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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.Utils;

public class LimitlessRouterServiceImpl implements LimitlessRouterService {
  public static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "limitlessTransactionRouterMonitorDisposalTimeMs",
          "600000", // 10min
          "Interval in milliseconds for an Limitless router monitor to be considered inactive and to be disposed.");
  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);
  protected static final ReentrantLock forceGetLimitlessRoutersLock = new ReentrantLock();
  protected final PluginService pluginService;
  protected final LimitlessQueryHelper queryHelper;
  protected final LimitlessRouterMonitorInitializer limitlessRouterMonitorInitializer;

  protected static final SlidingExpirationCacheWithCleanupThread<String, LimitlessRouterMonitor> limitlessRouterMonitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          limitlessRouterMonitor -> true,
          limitlessRouterMonitor -> {
            try {
              limitlessRouterMonitor.close();
            } catch (Exception e) {
              // ignore
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
    this(pluginService, LimitlessRouterMonitor::new, new LimitlessQueryHelper(pluginService));
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
  public List<HostSpec> getLimitlessRouters(final String clusterId, final Properties props) throws SQLException {
    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        MONITOR_DISPOSAL_TIME_MS.getLong(props));
    final List<HostSpec> limitlessRouters = limitlessRouterCache.get(clusterId, cacheExpirationNano);
    if (limitlessRouters == null) {
      return Collections.emptyList();
    }
    return limitlessRouters;
  }

  @Override
  public List<HostSpec> forceGetLimitlessRoutersWithConn(
      final Connection connection, final int hostPort, final Properties props) throws SQLException {
    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        MONITOR_DISPOSAL_TIME_MS.getLong(props));

    forceGetLimitlessRoutersLock.lock();
    try {
      final List<HostSpec> limitlessRouters =
          limitlessRouterCache.get(this.pluginService.getHostListProvider().getClusterId(), cacheExpirationNano);
      if (!Utils.isNullOrEmpty(limitlessRouters)) {
        return limitlessRouters;
      }

      final List<HostSpec> newLimitlessRouters =
          this.queryHelper.queryForLimitlessRouters(connection, hostPort);

      limitlessRouterCache.put(
          this.pluginService.getHostListProvider().getClusterId(),
          newLimitlessRouters, LimitlessRouterServiceImpl.MONITOR_DISPOSAL_TIME_MS.getLong(props));
      return newLimitlessRouters;
    } finally {
      forceGetLimitlessRoutersLock.unlock();
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
                  pluginService,
                  hostSpec,
                  limitlessRouterCache,
                  this.pluginService.getHostListProvider().getClusterId(),
                  props,
                  intervalMs),
          cacheExpirationNano);
    } catch (UnsupportedOperationException e) {
      throw e;
    }
  }
}
