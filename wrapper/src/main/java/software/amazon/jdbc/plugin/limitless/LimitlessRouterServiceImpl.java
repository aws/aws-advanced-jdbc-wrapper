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

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;

public class LimitlessRouterServiceImpl implements LimitlessRouterService {

  public static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "limitlessTransactionRouterMonitorDisposalTimeMs",
          "600000", // 10min
          "Interval in milliseconds for an Limitless router monitor to be considered inactive and to be disposed.");
  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);
  private final LimitlessRouterMonitorInitializer limitlessRouterMonitorInitializer;

  private static final SlidingExpirationCacheWithCleanupThread<String, LimitlessRouterMonitor> limitlessRouterMonitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          (limitlessRouterMonitor) -> true,
          (limitlessRouterMonitor) -> {
            try {
              limitlessRouterMonitor.close();
            } catch (Exception e) {
              // ignore
            }
          },
          CACHE_CLEANUP_NANO
      );

  public LimitlessRouterServiceImpl() {
    this(LimitlessRouterMonitor::new);
  }

  public LimitlessRouterServiceImpl(final LimitlessRouterMonitorInitializer limitlessRouterMonitorInitializer) {
    this.limitlessRouterMonitorInitializer = limitlessRouterMonitorInitializer;
  }

  @Override
  public List<HostSpec> getLimitlessRouters(final String clusterId, final Properties props) {
    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        MONITOR_DISPOSAL_TIME_MS.getLong(props));

    final LimitlessRouterMonitor
        limitlessRouterMonitor = limitlessRouterMonitors.get(clusterId, cacheExpirationNano);
    if (limitlessRouterMonitor == null) {
      return Collections.EMPTY_LIST;
    }
    return limitlessRouterMonitor.getLimitlessRouters();
  }

  @Override
  public synchronized void startMonitoring(final @NonNull PluginService pluginService,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final int intervalMs) {

    try {
      final String limitlessRouterMonitorKey = pluginService.getHostListProvider().getClusterId();
      final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(MONITOR_DISPOSAL_TIME_MS.getLong(props));

      limitlessRouterMonitors.computeIfAbsent(
          limitlessRouterMonitorKey,
          (key) -> this.limitlessRouterMonitorInitializer
              .createLimitlessRouterMonitor(pluginService, hostSpec, props, intervalMs),
          cacheExpirationNano);
    } catch (UnsupportedOperationException e) {
      throw e;
    }
  }
}
