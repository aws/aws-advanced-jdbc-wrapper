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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.storage.SlidingExpirationCacheWithCleanupThread;

public class HostResponseTimeServiceImpl implements HostResponseTimeService {

  private static final Logger LOGGER =
      Logger.getLogger(HostResponseTimeServiceImpl.class.getName());

  protected int intervalMs;
  protected List<HostSpec> hosts = new ArrayList<>();

  protected final @NonNull FullServicesContainer servicesContainer;
  protected final @NonNull PluginService pluginService;

  protected final @NonNull Properties props;

  public HostResponseTimeServiceImpl(
      final @NonNull FullServicesContainer servicesContainer,
      final @NonNull Properties props,
      int intervalMs) {
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();
    this.props = props;
    this.intervalMs = intervalMs;
  }

  @Override
  public int getResponseTime(HostSpec hostSpec) {
    final NodeResponseTimeMonitor monitor =
        this.servicesContainer.getMonitorService().get(NodeResponseTimeMonitor.class, hostSpec.getUrl());
    if (monitor == null) {
      return Integer.MAX_VALUE;
    }

    return monitor.getResponseTime();
  }

  @Override
  public void setHosts(final @NonNull List<HostSpec> hosts) {
    List<HostSpec> oldHosts = this.hosts;
    this.hosts = hosts;

    // Going through all hosts in the topology and trying to find new ones.
    this.hosts.stream()
        // hostSpec is not in the set of hosts that already being monitored
        .filter(hostSpec ->!Utils.containsHostAndPort(oldHosts, hostSpec.getHostAndPort()))
        .forEach(hostSpec -> {
          try {
            this.servicesContainer.getMonitorService().runIfAbsent(
                NodeResponseTimeMonitor.class,
                hostSpec.getUrl(),
                servicesContainer.getStorageService(),
                servicesContainer.getTelemetryFactory(),
                servicesContainer.getDefaultConnectionProvider(),
                this.pluginService.getOriginalUrl(),
                this.pluginService.getDriverProtocol(),
                this.pluginService.getTargetDriverDialect(),
                this.pluginService.getDialect(),
                this.props,
                (servicesContainer) ->
                    new NodeResponseTimeMonitor(pluginService, hostSpec, this.props,
                        this.intervalMs));
          } catch (SQLException e) {
            LOGGER.warning(
                Messages.get("HostResponseTimeServiceImpl.errorStartingMonitor", new Object[] {hostSpec.getUrl(), e}));
          }
        });
  }
}
