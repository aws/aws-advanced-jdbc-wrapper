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

package software.amazon.jdbc.plugin.efm.v3;

import static software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin.FAILURE_DETECTION_COUNT;
import static software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin.FAILURE_DETECTION_INTERVAL;
import static software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin.FAILURE_DETECTION_TIME;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.monitoring.MonitorErrorResponse;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;

/**
 * Resolves the {@link HostMonitorV3Impl} for a host through the core {@link MonitorService} and registers connection
 * slots with it. One instance exists per plugin instance; the monitors themselves are shared per host.
 */
public class HostMonitorServiceV3Impl implements StateSnapshotProvider {

  private static final Logger LOGGER = Logger.getLogger(HostMonitorServiceV3Impl.class.getName());

  public static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "monitorDisposalTime",
          "600000", // 10min
          "Interval in milliseconds for a monitor to be considered inactive and to be disposed.");

  public static final AwsWrapperProperty ABORT_IDLE_CONNECTIONS_ON_FAILURE =
      new AwsWrapperProperty(
          "abortIdleConnectionsOnFailure",
          "false",
          "When a host is detected as unhealthy, also abort idle connections to that host, "
              + "not only the ones with a call in flight.");

  protected static final long HEARTBEAT_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(3);

  protected final FullServicesContainer servicesContainer;
  protected final PluginService pluginService;
  protected final MonitorService coreMonitorService;
  protected final Properties properties;
  protected final @Nullable TelemetryCounter abortedConnectionsCounter;
  protected final int failureDetectionTimeMillis;
  protected final int failureDetectionIntervalMillis;
  protected final int failureDetectionCount;
  protected final long monitorDisposalTimeMillis;
  protected final boolean abortIdleConnections;

  static {
    PropertyDefinition.registerPluginProperties(HostMonitorServiceV3Impl.class);
  }

  public HostMonitorServiceV3Impl(final @NonNull FullServicesContainer servicesContainer, final Properties props) {
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();
    this.coreMonitorService = servicesContainer.getMonitorService();
    this.properties = props;
    this.abortedConnectionsCounter =
        servicesContainer.getTelemetryFactory().createCounter("efm3.connections.aborted");

    this.failureDetectionTimeMillis = FAILURE_DETECTION_TIME.getInteger(props);
    this.failureDetectionIntervalMillis = FAILURE_DETECTION_INTERVAL.getInteger(props);
    this.failureDetectionCount = FAILURE_DETECTION_COUNT.getInteger(props);
    this.monitorDisposalTimeMillis = MONITOR_DISPOSAL_TIME_MS.getLong(props);
    this.abortIdleConnections = ABORT_IDLE_CONNECTIONS_ON_FAILURE.getBoolean(props);

    // No error response: a stopped or errored monitor is dropped from the cache, and the next registration creates a
    // fresh one on demand, so recreating it eagerly would only start a thread nobody may use.
    this.coreMonitorService.registerMonitorTypeIfAbsent(
        HostMonitorV3Impl.class,
        TimeUnit.MILLISECONDS.toNanos(this.monitorDisposalTimeMillis),
        HEARTBEAT_TIMEOUT_NANOS,
        EnumSet.noneOf(MonitorErrorResponse.class),
        null);
  }

  public static void closeAllMonitors() {
    CoreServicesContainer.getInstance().getMonitorService().stopAndRemoveMonitors(HostMonitorV3Impl.class);
  }

  /**
   * Registers the slot with the monitor of its host, creating the monitor if needed. A monitor that has stopped since
   * it was cached is removed and replaced, so registration never silently lands on a dead monitor.
   *
   * @param slot the slot to register.
   * @return the monitor the slot is now registered with.
   * @throws SQLException if the monitor cannot be created.
   */
  public HostMonitorV3Impl register(final @NonNull MonitoredConnectionSlot slot) throws SQLException {
    final HostSpec hostSpec = slot.getHostSpec();
    final String key = this.getMonitorKey(hostSpec);

    HostMonitorV3Impl monitor = this.getMonitor(hostSpec, key);
    if (!monitor.register(slot)) {
      LOGGER.fine(() -> Messages.get(
          "HostMonitorServiceV3Impl.replacingStoppedMonitor", new Object[] {hostSpec.getHost()}));
      this.coreMonitorService.stopAndRemove(HostMonitorV3Impl.class, key);
      monitor = this.getMonitor(hostSpec, key);
      if (!monitor.register(slot)) {
        throw new SQLException(
            Messages.get("HostMonitorServiceV3Impl.monitorStopped", new Object[] {hostSpec.getHost()}));
      }
    }
    return monitor;
  }

  public void unregister(final @NonNull MonitoredConnectionSlot slot) {
    final HostMonitorV3Impl monitor = slot.getMonitor();
    if (monitor != null) {
      monitor.unregister(slot);
    }
  }

  protected String getMonitorKey(final @NonNull HostSpec hostSpec) {
    return String.format("%d:%d:%d:%b:%s",
        this.failureDetectionTimeMillis,
        this.failureDetectionIntervalMillis,
        this.failureDetectionCount,
        this.abortIdleConnections,
        hostSpec.getUrl());
  }

  protected HostMonitorV3Impl getMonitor(final @NonNull HostSpec hostSpec, final @NonNull String key)
      throws SQLException {
    return this.coreMonitorService.runIfAbsent(
        HostMonitorV3Impl.class,
        key,
        this.servicesContainer,
        this.pluginService.getProperties(),
        (monitorServicesContainer) -> new HostMonitorV3Impl(
            monitorServicesContainer,
            hostSpec,
            this.properties,
            this.failureDetectionTimeMillis,
            this.failureDetectionIntervalMillis,
            this.failureDetectionCount,
            this.monitorDisposalTimeMillis,
            this.abortIdleConnections,
            this.abortedConnectionsCounter));
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    final List<Pair<String, Object>> state = new ArrayList<>();
    state.add(Pair.create("failureDetectionTimeMillis", this.failureDetectionTimeMillis));
    state.add(Pair.create("failureDetectionIntervalMillis", this.failureDetectionIntervalMillis));
    state.add(Pair.create("failureDetectionCount", this.failureDetectionCount));
    state.add(Pair.create("monitorDisposalTimeMillis", this.monitorDisposalTimeMillis));
    state.add(Pair.create("abortIdleConnections", this.abortIdleConnections));
    return state;
  }
}
