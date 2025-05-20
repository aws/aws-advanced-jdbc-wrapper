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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class NodeResponseTimeMonitor implements AutoCloseable, Runnable {

  private static final Logger LOGGER =
      Logger.getLogger(NodeResponseTimeMonitor.class.getName());

  private static final String MONITORING_PROPERTY_PREFIX = "frt-";
  private static final int NUM_OF_MEASURES = 5;

  private final int intervalMs;
  private final @NonNull HostSpec hostSpec;

  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicInteger responseTime = new AtomicInteger(Integer.MAX_VALUE);
  private final AtomicLong checkTimestamp = new AtomicLong(this.getCurrentTime());

  private final @NonNull Properties props;
  private final @NonNull PluginService pluginService;

  private final TelemetryFactory telemetryFactory;
  private final TelemetryGauge responseTimeMsGauge;


  private Connection monitoringConn = null;

  // TODO: remove and submit monitors to MonitorService instead
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1, runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    return monitoringThread;
  });

  public NodeResponseTimeMonitor(
      final @NonNull PluginService pluginService,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      int intervalMs) {

    this.pluginService = pluginService;
    this.hostSpec = hostSpec;
    this.props = props;
    this.intervalMs = intervalMs;
    this.telemetryFactory = this.pluginService.getTelemetryFactory();

    final String nodeId = StringUtils.isNullOrEmpty(this.hostSpec.getHostId())
        ? this.hostSpec.getHost()
        : this.hostSpec.getHostId();

    // Report current response time (in milliseconds) to telemetry engine.
    // Report -1 if response time couldn't be measured.
    this.responseTimeMsGauge = telemetryFactory.createGauge(
        String.format("frt.response.time.%s", nodeId),
        () -> this.responseTime.get() == Integer.MAX_VALUE ? -1 : (long) this.responseTime.get());

    this.threadPool.submit(this);
    this.threadPool.shutdown(); // No more task are accepted by pool.
  }

  // Return node response time in milliseconds.
  public int getResponseTime() {
    return this.responseTime.get();
  }

  public long getCheckTimestamp() {
    return this.checkTimestamp.get();
  }

  public HostSpec getHostSpec() {
    return this.hostSpec;
  }

  @Override
  public void close() throws Exception {
    this.stopped.set(true);

    // Waiting for 5s gives a thread enough time to exit monitoring loop and close database connection.
    if (!this.threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
      this.threadPool.shutdownNow();
    }
    LOGGER.finest(() -> Messages.get(
        "NodeResponseTimeMonitor.stopped",
        new Object[] {this.hostSpec.getHost()}));
  }

  // The method is for testing purposes.
  protected long getCurrentTime() {
    return System.nanoTime();
  }

  @Override
  public void run() {
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        "node response time thread", TelemetryTraceLevel.TOP_LEVEL);
    telemetryContext.setAttribute("url", hostSpec.getUrl());

    try {
      while (!this.stopped.get()) {
        this.openConnection();

        if (this.monitoringConn != null) {

          long responseTimeSum = 0;
          int count = 0;
          for (int i = 0; i < NUM_OF_MEASURES; i++) {
            if (this.stopped.get()) {
              break;
            }
            long startTime = this.getCurrentTime();
            if (this.pluginService.getTargetDriverDialect().ping(this.monitoringConn)) {
              long responseTime = this.getCurrentTime() - startTime;
              responseTimeSum += responseTime;
              count++;
            }
          }

          if (count > 0) {
            this.responseTime.set((int) TimeUnit.NANOSECONDS.toMillis(responseTimeSum / count));
          } else {
            this.responseTime.set(Integer.MAX_VALUE);
          }
          this.checkTimestamp.set(this.getCurrentTime());

          LOGGER.finest(() -> Messages.get(
              "NodeResponseTimeMonitor.responseTime",
              new Object[] {this.hostSpec.getHost(), this.responseTime.get()}));
        }

        TimeUnit.MILLISECONDS.sleep(this.intervalMs);
      }
    } catch (final InterruptedException intEx) {
      // exit thread
      LOGGER.finest(
          () -> Messages.get(
              "NodeResponseTimeMonitor.interruptedExceptionDuringMonitoring",
              new Object[] {this.hostSpec.getHost()}));
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "NodeResponseTimeMonitor.exceptionDuringMonitoringStop",
                new Object[]{this.hostSpec.getHost()}),
            ex); // We want to print full trace stack of the exception.
      }
    } finally {
      this.stopped.set(true);
      if (this.monitoringConn != null) {
        try {
          this.monitoringConn.close();
        } catch (final SQLException ex) {
          // ignore
        }
      }
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }
  }

  private void openConnection() {
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
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

        LOGGER.finest(() -> Messages.get(
                "NodeResponseTimeMonitor.openingConnection",
                new Object[] {this.hostSpec.getUrl()}));
        // TODO: replace with ConnectionService#createAuxiliaryConnection
        this.monitoringConn = this.pluginService.forceConnect(this.hostSpec, monitoringConnProperties);
        LOGGER.finest(() -> Messages.get(
            "NodeResponseTimeMonitor.openedConnection",
            new Object[] {this.monitoringConn}));
      }
    } catch (SQLException ex) {
      if (this.monitoringConn != null) {
        try {
          this.monitoringConn.close();
        } catch (Exception e) {
          // ignore
        }
        this.monitoringConn = null;
      }
    }
  }
}
