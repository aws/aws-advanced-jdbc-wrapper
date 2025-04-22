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

package software.amazon.jdbc.plugin.efm2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * This class handles the creation and clean up of monitoring threads to servers with one or more
 * active connections.
 */
public class MonitorServiceImpl implements MonitorService {

  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());
  public static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "monitorDisposalTime",
          "600000", // 10min
          "Interval in milliseconds for a monitor to be considered inactive and to be disposed.");

  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);

  protected static final Executor ABORT_EXECUTOR = Executors.newSingleThreadExecutor();

  protected static final SlidingExpirationCacheWithCleanupThread<String, Monitor> monitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          Monitor::canDispose,
          (monitor) -> {
            try {
              monitor.close();
            } catch (Exception ex) {
              // ignore
            }
          },
          CACHE_CLEANUP_NANO);

  protected final PluginService pluginService;
  protected final MonitorInitializer monitorInitializer;
  protected final TelemetryFactory telemetryFactory;
  protected final TelemetryCounter abortedConnectionsCounter;

  public MonitorServiceImpl(final @NonNull PluginService pluginService) {
    this(
        pluginService,
        (hostSpec,
            properties,
            failureDetectionTimeMillis,
            failureDetectionIntervalMillis,
            failureDetectionCount,
            abortedConnectionsCounter) ->
            new MonitorImpl(
                pluginService,
                hostSpec,
                properties,
                failureDetectionTimeMillis,
                failureDetectionIntervalMillis,
                failureDetectionCount,
                abortedConnectionsCounter));
  }

  MonitorServiceImpl(
      final @NonNull PluginService pluginService,
      final @NonNull MonitorInitializer monitorInitializer) {
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.abortedConnectionsCounter = telemetryFactory.createCounter("efm2.connections.aborted");
    this.monitorInitializer = monitorInitializer;
  }

  public static void closeAllMonitors() {
    monitors.getEntries().values().forEach(monitor -> {
      try {
        monitor.close();
      } catch (Exception ex) {
        // ignore
      }
    });
    monitors.clear();
  }

  @Override
  public MonitorConnectionContext startMonitoring(
      final Connection connectionToAbort,
      final HostSpec hostSpec,
      final Properties properties,
      final int failureDetectionTimeMillis,
      final int failureDetectionIntervalMillis,
      final int failureDetectionCount) {

    final Monitor monitor = this.getMonitor(
        hostSpec,
        properties,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);

    final MonitorConnectionContext context = new MonitorConnectionContext(connectionToAbort);
    monitor.startMonitoring(context);

    return context;
  }

  @Override
  public void stopMonitoring(
      @NonNull final MonitorConnectionContext context,
      @NonNull Connection connectionToAbort) {

    if (context.shouldAbort()) {
      context.setInactive();
      try {
        connectionToAbort.abort(ABORT_EXECUTOR);
        connectionToAbort.close();
        this.abortedConnectionsCounter.inc();
      } catch (final SQLException sqlEx) {
        // ignore
        LOGGER.finest(
            () -> Messages.get(
                "MonitorConnectionContext.exceptionAbortingConnection",
                new Object[] {sqlEx.getMessage()}));
      }
    } else {
      context.setInactive();
    }
  }

  @Override
  public void releaseResources() {
    // do nothing
  }

  /**
   * Get or create a {@link MonitorImpl} for a server.
   *
   * @param hostSpec Information such as hostname of the server.
   * @param properties The user configuration for the current connection.
   * @param failureDetectionTimeMillis A failure detection time in millis.
   * @param failureDetectionIntervalMillis A failure detection interval in millis.
   * @param failureDetectionCount A failure detection count.
   * @return A {@link MonitorImpl} object associated with a specific server.
   */
  protected Monitor getMonitor(
      final HostSpec hostSpec,
      final Properties properties,
      final int failureDetectionTimeMillis,
      final int failureDetectionIntervalMillis,
      final int failureDetectionCount) {

    final String monitorKey = String.format("%d:%d:%d:%s",
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount,
        hostSpec.getUrl());

    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        MONITOR_DISPOSAL_TIME_MS.getLong(properties));

    return monitors.computeIfAbsent(
        monitorKey,
        (key) -> monitorInitializer.createMonitor(
            hostSpec,
            properties,
            failureDetectionTimeMillis,
            failureDetectionIntervalMillis,
            failureDetectionCount,
            this.abortedConnectionsCounter),
        cacheExpirationNano);
  }
}
