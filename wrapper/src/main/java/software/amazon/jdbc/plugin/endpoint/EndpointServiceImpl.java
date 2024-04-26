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

package software.amazon.jdbc.plugin.endpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;

public class EndpointServiceImpl implements EndpointService {

  public static final AwsWrapperProperty ENDPOINT_MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "endpointMonitorDisposalTime",
          "600000", // 10min
          "Interval in milliseconds for an endopint monitor to be considered inactive and to be disposed.");
  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);
  private final EndpointMonitorInitializer endpointMonitorInitializer;

  private static final SlidingExpirationCacheWithCleanupThread<String, EndpointMonitor> endpointMonitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          (endpointMonitor) -> true,
          (endpointMonitor) -> {
            try {
              endpointMonitor.close();
            } catch (Exception e) {
              // ignore
            }
          },
          CACHE_CLEANUP_NANO
      );

  public EndpointServiceImpl() {
    this(EndpointMonitor::new);
  }

  public EndpointServiceImpl(final EndpointMonitorInitializer endpointMonitorInitializer) {
    this.endpointMonitorInitializer = endpointMonitorInitializer;
  }

  @Override
  public List<HostSpec> getEndpoints(final String clusterId, final Properties props) {
    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        ENDPOINT_MONITOR_DISPOSAL_TIME_MS.getLong(props));

    final EndpointMonitor endpointMonitor = endpointMonitors.get(clusterId, cacheExpirationNano);
    if (endpointMonitor == null) {
      return Collections.EMPTY_LIST;
    }
    return new ArrayList<>(endpointMonitor.getEndpoints());
  }

  @Override
  public synchronized void startMonitoring(final @NonNull PluginService pluginService,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final int intervalMs) {
//     if (endpointMonitor == null) {
//       endpointMonitor =
//           this.endpointMonitorInitializer.createEndpointMonitor(pluginService, hostSpec, props, intervalMs);
//     }
          final String endpointMonitorKey = pluginService.getHostListProvider().getClusterId();
      final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
          ENDPOINT_MONITOR_DISPOSAL_TIME_MS.getLong(props));

      endpointMonitors.computeIfAbsent(
          endpointMonitorKey,
          (key) -> this.endpointMonitorInitializer.createEndpointMonitor(pluginService, hostSpec, props, intervalMs),
          cacheExpirationNano);
  }
}
