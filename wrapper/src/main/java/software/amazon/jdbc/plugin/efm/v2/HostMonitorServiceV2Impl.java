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

package software.amazon.jdbc.plugin.efm.v2;

import static software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin.FAILURE_DETECTION_COUNT;
import static software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin.FAILURE_DETECTION_INTERVAL;
import static software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin.FAILURE_DETECTION_TIME;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.efm.base.HostMonitor;
import software.amazon.jdbc.plugin.efm.base.HostMonitorConnectionContext;
import software.amazon.jdbc.plugin.efm.base.HostMonitorService;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * This class handles the creation and clean up of monitoring threads to servers with one or more
 * active connections.
 */
public class HostMonitorServiceV2Impl implements HostMonitorService, StateSnapshotProvider {

  private static final Logger LOGGER = Logger.getLogger(HostMonitorServiceV2Impl.class.getName());
  public static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "monitorDisposalTime",
          "600000", // 10min
          "Interval in milliseconds for a monitor to be considered inactive and to be disposed.");
  protected static final Executor ABORT_EXECUTOR =
      ExecutorFactory.newSingleThreadExecutor("abort");

  protected final FullServicesContainer serviceContainer;
  protected final PluginService pluginService;
  protected final MonitorService coreMonitorService;
  protected final TelemetryFactory telemetryFactory;
  protected final TelemetryCounter abortedConnectionsCounter;
  protected final int failureDetectionTimeMillis;
  protected final int failureDetectionIntervalMillis;
  protected final int failureDetectionCount;

  protected HostMonitorKey monitorKey;

  static {
    PropertyDefinition.registerPluginProperties(HostMonitorServiceV2Impl.class);
  }

  public HostMonitorServiceV2Impl(final @NonNull FullServicesContainer serviceContainer, final Properties props) {
    this.serviceContainer = serviceContainer;
    this.coreMonitorService = serviceContainer.getMonitorService();
    this.pluginService = serviceContainer.getPluginService();
    this.telemetryFactory = serviceContainer.getTelemetryFactory();
    this.abortedConnectionsCounter = telemetryFactory.createCounter("efm2.connections.aborted");

    this.failureDetectionTimeMillis = FAILURE_DETECTION_TIME.getInteger(props);
    this.failureDetectionIntervalMillis = FAILURE_DETECTION_INTERVAL.getInteger(props);
    this.failureDetectionCount = FAILURE_DETECTION_COUNT.getInteger(props);

    this.coreMonitorService.registerMonitorTypeIfAbsent(
        HostMonitorV2Impl.class,
        TimeUnit.MILLISECONDS.toNanos(MONITOR_DISPOSAL_TIME_MS.getLong(props)),
        TimeUnit.MINUTES.toNanos(3),
        null,
        null);
  }

  public static void closeAllMonitors() {
    CoreServicesContainer.getInstance().getMonitorService().stopAndRemoveMonitors(HostMonitorV2Impl.class);
  }

  @Override
  public HostMonitorConnectionContext startMonitoring(
      Connection connectionToAbort,
      HostSpec hostSpec,
      Properties properties) throws SQLException {

    final HostMonitor monitor = this.getMonitor(hostSpec, properties);
    final HostMonitorConnectionContextV2 context = new HostMonitorConnectionContextV2(connectionToAbort);
    monitor.startMonitoring(context);
    return context;
  }

  @Override
  public void stopMonitoring(@NonNull final HostMonitorConnectionContext context) {
    if (context.shouldAbort()) {
      final Connection connectionToAbort = context.getConnection();
      context.setInactive();
      if (connectionToAbort != null) {
        try {
          connectionToAbort.abort(ABORT_EXECUTOR);
          connectionToAbort.close();
          if (this.abortedConnectionsCounter != null) {
            this.abortedConnectionsCounter.inc();
          }
        } catch (final SQLException sqlEx) {
          // ignore
          LOGGER.finest(
              () -> Messages.get(
                  "HostMonitorConnectionContext.exceptionAbortingConnection",
                  new Object[]{sqlEx.getMessage()}));
        }
      }
    } else {
      context.setInactive();
    }
  }

  /**
   * Get or create a {@link HostMonitorV2Impl} for a server.
   *
   * @param hostSpec Information such as hostname of the server.
   * @param properties The user configuration for the current connection.
   * @return A {@link HostMonitorV2Impl} object associated with a specific server.
   * @throws SQLException if there's errors getting or creating a monitor
   */
  protected HostMonitor getMonitor(
      final HostSpec hostSpec,
      final Properties properties) throws SQLException {
    String hostUrl = hostSpec.getUrl();
    if (this.monitorKey == null || !hostUrl.equals(this.monitorKey.getUrl())) {
      // The URL being monitored has changed, so we need to recalculate the monitor key.
      this.monitorKey = new HostMonitorKey(
          hostUrl,
          String.format("%d:%d:%d:%s",
              this.failureDetectionTimeMillis,
              this.failureDetectionIntervalMillis,
              this.failureDetectionCount,
              hostUrl)
      );
    }

    return this.coreMonitorService.runIfAbsent(
        HostMonitorV2Impl.class,
        this.monitorKey.getKeyValue(),
        this.serviceContainer,
        this.pluginService.getProperties(),
        (servicesContainer) -> new HostMonitorV2Impl(
            servicesContainer,
            hostSpec,
            properties,
            this.failureDetectionTimeMillis,
            this.failureDetectionIntervalMillis,
            this.failureDetectionCount,
            this.abortedConnectionsCounter));
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    List<Pair<String, Object>> state = new ArrayList<>();
    state.add(Pair.create("failureDetectionTimeMillis", this.failureDetectionTimeMillis));
    state.add(Pair.create("failureDetectionIntervalMillis", this.failureDetectionIntervalMillis));
    state.add(Pair.create("failureDetectionCount", this.failureDetectionCount));
    state.add(Pair.create("monitorKey", this.monitorKey.toString()));
    return state;
  }

  protected static class HostMonitorKey {
    private final String url;
    private final String keyValue;

    public HostMonitorKey(String url, String keyValue) {
      this.url = url;
      this.keyValue = keyValue;
    }

    public String getUrl() {
      return url;
    }

    public String getKeyValue() {
      return keyValue;
    }

    @Override
    public String toString() {
      return "HostMonitorKey [url=" + this.url + ", keyValue=" + this.keyValue + "]";
    }
  }
}
