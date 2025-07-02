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

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

/**
 * This class uses a background thread to monitor a particular server with one or more active {@link
 * Connection}.
 */
public class HostMonitorImpl implements HostMonitor {

  private static final Logger LOGGER = Logger.getLogger(HostMonitorImpl.class.getName());
  private static final long THREAD_SLEEP_NANO = TimeUnit.MILLISECONDS.toNanos(100);
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  protected static final Executor ABORT_EXECUTOR =
      ExecutorFactory.newSingleThreadExecutor("abort");

  private final Queue<WeakReference<HostMonitorConnectionContext>> activeContexts = new ConcurrentLinkedQueue<>();
  private final Map<Long, Queue<WeakReference<HostMonitorConnectionContext>>> newContexts =
      new ConcurrentHashMap<>();
  private final PluginService pluginService;
  private final TelemetryFactory telemetryFactory;
  private final Properties properties;
  private final HostSpec hostSpec;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private Connection monitoringConn = null;
  // TODO: remove and submit monitors to MonitorService instead
  private final ExecutorService threadPool =
      ExecutorFactory.newFixedThreadPool(2, "threadPool");

  private final long failureDetectionTimeNano;
  private final long failureDetectionIntervalNano;
  private final int failureDetectionCount;

  private long invalidNodeStartTimeNano;
  private long failureCount;
  private boolean nodeUnhealthy = false;

  private final TelemetryCounter abortedConnectionsCounter;

  /**
   * Store the monitoring configuration for a connection.
   *
   * @param pluginService             A service for creating new connections.
   * @param hostSpec                  The {@link HostSpec} of the server this {@link HostMonitorImpl}
   *                                  instance is monitoring.
   * @param properties                The {@link Properties} containing additional monitoring
   *                                  configuration.
   * @param failureDetectionTimeMillis A failure detection time in millis.
   * @param failureDetectionIntervalMillis A failure detection interval in millis.
   * @param failureDetectionCount A failure detection count.
   * @param abortedConnectionsCounter Aborted connection telemetry counter.
   */
  public HostMonitorImpl(
      final @NonNull PluginService pluginService,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties properties,
      final int failureDetectionTimeMillis,
      final int failureDetectionIntervalMillis,
      final int failureDetectionCount,
      final TelemetryCounter abortedConnectionsCounter) {

    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.hostSpec = hostSpec;
    this.properties = properties;
    this.failureDetectionTimeNano = TimeUnit.MILLISECONDS.toNanos(failureDetectionTimeMillis);
    this.failureDetectionIntervalNano = TimeUnit.MILLISECONDS.toNanos(failureDetectionIntervalMillis);
    this.failureDetectionCount = failureDetectionCount;
    this.abortedConnectionsCounter = abortedConnectionsCounter;

    this.threadPool.submit(this::newContextRun); // task to handle new contexts
    this.threadPool.submit(this); // task to handle active monitoring contexts
    this.threadPool.shutdown(); // No more tasks are accepted by pool.
  }

  @Override
  public boolean canDispose() {
    return this.activeContexts.isEmpty() && this.newContexts.isEmpty();
  }

  @Override
  public void close() throws Exception {
    this.stopped.set(true);

    // Waiting for 30s gives a thread enough time to exit monitoring loop and close database connection.
    if (!this.threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
      this.threadPool.shutdownNow();
    }
    LOGGER.finest(() -> Messages.get(
        "HostMonitorImpl.stopped",
        new Object[] {this.hostSpec.getHost()}));
  }

  @Override
  public void startMonitoring(final HostMonitorConnectionContext context) {
    if (this.stopped.get()) {
      LOGGER.warning(() -> Messages.get("HostMonitorImpl.monitorIsStopped", new Object[] {this.hostSpec.getHost()}));
    }

    final long currentTimeNano = this.getCurrentTimeNano();
    long startMonitoringTimeNano = this.truncateNanoToSeconds(
        currentTimeNano + this.failureDetectionTimeNano);

    Queue<WeakReference<HostMonitorConnectionContext>> queue =
        this.newContexts.computeIfAbsent(
            startMonitoringTimeNano,
            (key) -> new ConcurrentLinkedQueue<>());
    queue.add(new WeakReference<>(context));
  }

  private long truncateNanoToSeconds(final long timeNano) {
    return TimeUnit.SECONDS.toNanos(TimeUnit.NANOSECONDS.toSeconds(timeNano));
  }

  // This method helps to organize unit tests.
  long getCurrentTimeNano() {
    return System.nanoTime();
  }

  public void newContextRun() {

    LOGGER.finest(() -> Messages.get(
        "HostMonitorImpl.startMonitoringThreadNewContext",
        new Object[] {this.hostSpec.getHost()}));

    try {
      while (!this.stopped.get()) {

        final long currentTimeNano = this.getCurrentTimeNano();

        final ArrayList<Long> processedKeys = new ArrayList<>();
        this.newContexts.entrySet().stream()
            // Get entries with key (that is a time in nanos) less or equal than current time.
            .filter(entry -> entry.getKey() < currentTimeNano)
            .forEach(entry -> {
              final Queue<WeakReference<HostMonitorConnectionContext>> queue = entry.getValue();
              processedKeys.add(entry.getKey());
              // Each value of found entry is a queue of monitoring contexts awaiting active monitoring.
              // Add all contexts to an active monitoring contexts queue.
              // Ignore disposed contexts.
              WeakReference<HostMonitorConnectionContext> contextWeakRef;
              while ((contextWeakRef = queue.poll()) != null) {
                HostMonitorConnectionContext context = contextWeakRef.get();
                if (context != null && context.isActive()) {
                  this.activeContexts.add(contextWeakRef);
                }
              }
            });
        processedKeys.forEach(this.newContexts::remove);

        TimeUnit.SECONDS.sleep(1);
      }
    } catch (final InterruptedException intEx) {
      // do nothing; just exit the thread
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "HostMonitorImpl.exceptionDuringMonitoringStop",
                new Object[] {this.hostSpec.getHost()}),
            ex); // We want to print full trace stack of the exception.
      }
    }

    LOGGER.finest(() -> Messages.get(
        "HostMonitorImpl.stopMonitoringThreadNewContext",
        new Object[] {this.hostSpec.getHost()}));
  }

  @Override
  public void run() {

    LOGGER.finest(() -> Messages.get(
        "HostMonitorImpl.startMonitoringThread",
        new Object[] {this.hostSpec.getHost()}));

    try {
      while (!this.stopped.get()) {

        if (this.activeContexts.isEmpty() && !this.nodeUnhealthy) {
          TimeUnit.NANOSECONDS.sleep(THREAD_SLEEP_NANO);
          continue;
        }

        final long statusCheckStartTimeNano = this.getCurrentTimeNano();
        final boolean isValid = this.checkConnectionStatus();
        final long statusCheckEndTimeNano = this.getCurrentTimeNano();

        this.updateNodeHealthStatus(isValid, statusCheckStartTimeNano, statusCheckEndTimeNano);

        final List<WeakReference<HostMonitorConnectionContext>> tmpActiveContexts = new ArrayList<>();
        WeakReference<HostMonitorConnectionContext> monitorContextWeakRef;

        while ((monitorContextWeakRef = this.activeContexts.poll()) != null) {
          if (this.stopped.get()) {
            break;
          }

          HostMonitorConnectionContext monitorContext = monitorContextWeakRef.get();
          if (monitorContext == null) {
            continue;
          }

          if (this.nodeUnhealthy) {
            // Kill connection.
            monitorContext.setNodeUnhealthy(true);
            final Connection connectionToAbort = monitorContext.getConnection();
            monitorContext.setInactive();
            if (connectionToAbort != null) {
              this.abortConnection(connectionToAbort);
              if (this.abortedConnectionsCounter != null) {
                this.abortedConnectionsCounter.inc();
              }
            }
          } else if (monitorContext.isActive()) {
            tmpActiveContexts.add(monitorContextWeakRef);
          }
        }

        // activeContexts is empty now and tmpActiveContexts contains all yet active contexts
        // Add active contexts back to the queue.
        this.activeContexts.addAll(tmpActiveContexts);

        long delayNano = this.failureDetectionIntervalNano - (statusCheckEndTimeNano - statusCheckStartTimeNano);
        if (delayNano < THREAD_SLEEP_NANO) {
          delayNano = THREAD_SLEEP_NANO;
        }
        TimeUnit.NANOSECONDS.sleep(delayNano);
      }
    } catch (final InterruptedException intEx) {
      // do nothing
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "HostMonitorImpl.exceptionDuringMonitoringStop",
                new Object[] {this.hostSpec.getHost()}),
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
    }

    LOGGER.finest(() -> Messages.get(
        "HostMonitorImpl.stopMonitoringThread",
        new Object[] {this.hostSpec.getHost()}));
  }

  /**
   * Check the status of the monitored server by establishing a connection and sending a ping.
   *
   * @return True, if the server is still alive.
   */
  boolean checkConnectionStatus() {
    TelemetryContext connectContext = telemetryFactory.openTelemetryContext(
        "connection status check", TelemetryTraceLevel.FORCE_TOP_LEVEL);

    if (connectContext != null) {
      connectContext.setAttribute("url", this.hostSpec.getHost());
    }

    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
        final Properties monitoringConnProperties = PropertyUtils.copyProperties(this.properties);

        this.properties.stringPropertyNames().stream()
            .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
            .forEach(
                p -> {
                  monitoringConnProperties.put(
                      p.substring(MONITORING_PROPERTY_PREFIX.length()),
                      this.properties.getProperty(p));
                  monitoringConnProperties.remove(p);
                });

        LOGGER.finest(() -> "Opening a monitoring connection to " + this.hostSpec.getUrl());
        // TODO: replace with ConnectionService#open
        this.monitoringConn = this.pluginService.forceConnect(this.hostSpec, monitoringConnProperties);
        LOGGER.finest(() -> "Opened monitoring connection: " + this.monitoringConn);
        return true;
      }

      // Some drivers, like MySQL Connector/J, execute isValid() in a double of specified timeout time.
      final int validTimeout = (int) TimeUnit.NANOSECONDS.toSeconds(
          this.failureDetectionIntervalNano - THREAD_SLEEP_NANO) / 2;
      return this.monitoringConn.isValid(validTimeout);
    } catch (final SQLException sqlEx) {
      return false;
    } finally {
      if (connectContext != null) {
        connectContext.closeContext();
      }
    }
  }

  private void updateNodeHealthStatus(
      final boolean connectionValid,
      final long statusCheckStartNano,
      final long statusCheckEndNano) {

    if (!connectionValid) {
      this.failureCount++;

      if (this.invalidNodeStartTimeNano == 0) {
        this.invalidNodeStartTimeNano = statusCheckStartNano;
      }

      final long invalidNodeDurationNano = statusCheckEndNano - this.invalidNodeStartTimeNano;
      final long maxInvalidNodeDurationNano =
          this.failureDetectionIntervalNano * Math.max(0, this.failureDetectionCount - 1);

      if (invalidNodeDurationNano >= maxInvalidNodeDurationNano) {
        LOGGER.fine(() ->
            Messages.get("HostMonitorConnectionContext.hostDead", new Object[] {this.hostSpec.getHost()}));
        this.nodeUnhealthy = true;
        return;
      }

      LOGGER.finest(
          () -> Messages.get(
              "HostMonitorConnectionContext.hostNotResponding",
              new Object[] {this.hostSpec.getHost(), this.failureCount}));
      return;
    }

    if (this.failureCount > 0) {
      // Node is back alive
      LOGGER.finest(
          () -> Messages.get("HostMonitorConnectionContext.hostAlive",
              new Object[] {this.hostSpec.getHost()}));
    }

    this.failureCount = 0;
    this.invalidNodeStartTimeNano = 0;
    this.nodeUnhealthy = false;
  }

  private void abortConnection(final @NonNull Connection connectionToAbort) {
    try {
      connectionToAbort.abort(ABORT_EXECUTOR);
      connectionToAbort.close();
    } catch (final SQLException sqlEx) {
      // ignore
      LOGGER.finest(
          () -> Messages.get(
              "HostMonitorConnectionContext.exceptionAbortingConnection",
              new Object[] {sqlEx.getMessage()}));
    }
  }

}
