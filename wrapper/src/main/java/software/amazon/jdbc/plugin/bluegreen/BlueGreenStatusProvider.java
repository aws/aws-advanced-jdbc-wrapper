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

package software.amazon.jdbc.plugin.bluegreen;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.SupportBlueGreen;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.Utils;

public class BlueGreenStatusProvider {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusProvider.class.getName());
  public static final AwsWrapperProperty BG_INTERVAL_BASELINE = new AwsWrapperProperty(
      "bgBaseline", "60000",
      "Baseline Blue/Green Deployment status checking interval (in msec).");

  public static final AwsWrapperProperty BG_INTERVAL_INCREASED = new AwsWrapperProperty(
      "bgIncreased", "1000",
      "Increased Blue/Green Deployment status checking interval (in msec).");

  public static final AwsWrapperProperty BG_INTERVAL_HIGH = new AwsWrapperProperty(
      "bgHigh", "100",
      "High Blue/Green Deployment status checking interval (in msec).");

  private static final String MONITORING_PROPERTY_PREFIX = "blue-green-monitoring-";
  private static final String DEFAULT_CONNECT_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));
  private static final String DEFAULT_SOCKET_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));

  protected static BlueGreenStatusMonitor blueMonitor = null;
  protected static BlueGreenStatusMonitor greenMonitor = null;
  protected static final ReentrantLock monitorInitLock = new ReentrantLock();

  protected static final HashMap<BlueGreenPhases, IntervalType> checkIntervalTypeMap =
      new HashMap<BlueGreenPhases, IntervalType>() {
        {
          put(BlueGreenPhases.NOT_CREATED, IntervalType.BASELINE);
          put(BlueGreenPhases.CREATED, IntervalType.INCREASED);
          put(BlueGreenPhases.PREPARATION_TO_SWITCH_OVER, IntervalType.INCREASED);
          put(BlueGreenPhases.SWITCHING_OVER, IntervalType.HIGH);
          put(BlueGreenPhases.SWITCH_OVER_COMPLETED, IntervalType.INCREASED);
        }
      };

  protected final HashMap<IntervalType, Long> checkIntervalMap = new HashMap<>();

  protected final PluginService pluginService;
  protected final Properties props;

  public BlueGreenStatusProvider(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) {

    this.pluginService = pluginService;
    this.props = props;

    checkIntervalMap.put(IntervalType.BASELINE, BG_INTERVAL_BASELINE.getLong(props));
    checkIntervalMap.put(IntervalType.INCREASED, BG_INTERVAL_INCREASED.getLong(props));
    checkIntervalMap.put(IntervalType.HIGH, BG_INTERVAL_HIGH.getLong(props));

    final Dialect dialect = this.pluginService.getDialect();
    if (dialect instanceof SupportBlueGreen) {
      this.initMonitoring();
    } else {
      LOGGER.warning("Blue/Green Deployments isn't supported by this database engine.");
    }
  }

  protected void initMonitoring() {
    if (blueMonitor == null || greenMonitor == null) {
      monitorInitLock.lock();
      try {
        if (blueMonitor == null) {
          blueMonitor =
              new BlueGreenStatusMonitor(
                  BlueGreenRole.SOURCE,
                  this.pluginService.getCurrentHostSpec(),
                  this.pluginService,
                  this.getMonitoringProperties(),
                  checkIntervalMap,
                  this::prepareStatus);
        }
        if (greenMonitor == null) {
          greenMonitor =
              new BlueGreenStatusMonitor(
                  BlueGreenRole.TARGET,
                  this.pluginService.getCurrentHostSpec(),
                  this.pluginService,
                  this.getMonitoringProperties(),
                  checkIntervalMap,
                  this::prepareStatus);
        }
      } finally {
        monitorInitLock.unlock();
      }
    }
  }

  protected Properties getMonitoringProperties() {
    final Properties monitoringConnProperties = PropertyUtils.copyProperties(this.props);
    this.props.stringPropertyNames().stream()
        .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
        .forEach(
            p -> {
              monitoringConnProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  this.props.getProperty(p));
              monitoringConnProperties.remove(p);
            });

    if (!monitoringConnProperties.containsKey(PropertyDefinition.CONNECT_TIMEOUT)) {
      monitoringConnProperties.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, DEFAULT_CONNECT_TIMEOUT_MS);
    }
    if (!monitoringConnProperties.containsKey(PropertyDefinition.SOCKET_TIMEOUT)) {
      monitoringConnProperties.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, DEFAULT_SOCKET_TIMEOUT_MS);
    }

    return monitoringConnProperties;
  }

  protected void prepareStatus(
      final BlueGreenRole role,
      final BlueGreenPhases blueGreenPhase,
      final List<HostSpec> topology,
      final Map<String, String> ipAddressesByHostAndPortMap) {

    // TODO: implement

    String ipMap = String.join("\n   ", ipAddressesByHostAndPortMap.entrySet().stream()
        .map(x -> String.format("%s -> %s", x.getKey(), x.getValue()))
        .collect(Collectors.toList()));
    LOGGER.finest(String.format("role %s, phase %s, \n%s \nIP map:\n   %s", role, blueGreenPhase, Utils.logTopology(topology), ipMap));
  }
}
