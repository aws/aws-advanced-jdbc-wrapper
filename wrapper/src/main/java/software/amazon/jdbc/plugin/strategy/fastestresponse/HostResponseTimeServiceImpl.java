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

package software.amazon.jdbc.plugin.strategy.fastestresponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class HostResponseTimeServiceImpl implements HostResponseTimeService {

  private static final Logger LOGGER =
      Logger.getLogger(HostResponseTimeServiceImpl.class.getName());

  protected static final long CACHE_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(10);
  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);

  protected static final SlidingExpirationCacheWithCleanupThread<String, NodeResponseTimeMonitor> monitoringNodes
      = new SlidingExpirationCacheWithCleanupThread<>(
          (monitor) -> true,
          (monitor) -> {
            try {
              monitor.close();
            } catch (Exception ex) {
              // ignore
            }
          },
          CACHE_CLEANUP_NANO);
  protected static final ReentrantLock cacheLock = new ReentrantLock();

  protected int intervalMs;

  protected List<HostSpec> hosts = new ArrayList<>();

  protected final @NonNull PluginService pluginService;

  protected final @NonNull Properties props;

  protected final TelemetryFactory telemetryFactory;
  private final TelemetryGauge nodeCountGauge;

  public HostResponseTimeServiceImpl(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      int intervalMs) {

    this.pluginService = pluginService;
    this.props = props;
    this.intervalMs = intervalMs;
    this.telemetryFactory = this.pluginService.getTelemetryFactory();
    this.nodeCountGauge = telemetryFactory.createGauge("frt.nodes.count",
        () -> (long) monitoringNodes.size());

    monitoringNodes.setCleanupIntervalNanos(CACHE_CLEANUP_NANO);
  }

  @Override
  public int getResponseTime(HostSpec hostSpec) {
    final NodeResponseTimeMonitor monitor = monitoringNodes.get(hostSpec.getUrl(), CACHE_EXPIRATION_NANO);
    if (monitor == null) {
      return Integer.MAX_VALUE;
    }

    return monitor.getResponseTime();
  }

  @Override
  public void setHosts(final @NonNull List<HostSpec> hosts) {
    Set<String> oldHosts = this.hosts.stream().map(HostSpec::getUrl).collect(Collectors.toSet());
    this.hosts = hosts;

    // Going through all hosts in the topology and trying to find new ones.
    this.hosts.stream()
        // hostSpec is not in the set of hosts that already being monitored
        .filter(hostSpec -> !oldHosts.contains(hostSpec.getUrl()))
        .forEach(hostSpec -> {
          cacheLock.lock();
          try {
            monitoringNodes.computeIfAbsent(
                hostSpec.getUrl(),
                (key) -> new NodeResponseTimeMonitor(this.pluginService, hostSpec, this.props, this.intervalMs),
                CACHE_EXPIRATION_NANO);
          } finally {
            cacheLock.unlock();
          }
        });
  }

  public static void closeAllMonitors() {
    monitoringNodes.getEntries().values().forEach(monitor -> {
      try {
        monitor.close();
      } catch (Exception ex) {
        // ignore
      }
    });
    monitoringNodes.clear();
  }
}
