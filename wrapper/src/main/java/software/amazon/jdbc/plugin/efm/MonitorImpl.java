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

package software.amazon.jdbc.plugin.efm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

/**
 * This class uses a background thread to monitor a particular server with one or more active {@link
 * Connection}.
 */
public class MonitorImpl implements Monitor {

  static class ConnectionStatus {

    boolean isValid;
    long elapsedTimeNano;

    ConnectionStatus(final boolean isValid, final long elapsedTimeNano) {
      this.isValid = isValid;
      this.elapsedTimeNano = elapsedTimeNano;
    }
  }

  private static final Logger LOGGER = Logger.getLogger(MonitorImpl.class.getName());
  private static final long THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final long MIN_CONNECTION_CHECK_TIMEOUT_MILLIS = 3000;
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  final Queue<MonitorConnectionContext> activeContexts = new ConcurrentLinkedQueue<>();
  private final Queue<MonitorConnectionContext> newContexts = new ConcurrentLinkedQueue<>();
  private final PluginService pluginService;
  private final TelemetryFactory telemetryFactory;
  private final Properties properties;
  private final HostSpec hostSpec;
  private final MonitorThreadContainer threadContainer;
  private final long monitorDisposalTimeMillis;
  private volatile long contextLastUsedTimestampNano;
  private volatile boolean stopped = false;
  private Connection monitoringConn = null;
  private long nodeCheckTimeoutMillis = MIN_CONNECTION_CHECK_TIMEOUT_MILLIS;

  private final TelemetryGauge contextsSizeGauge;
  private final TelemetryCounter nodeInvalidCounter;

  /**
   * Store the monitoring configuration for a connection.
   *
   * @param pluginService             A service for creating new connections.
   * @param hostSpec                  The {@link HostSpec} of the server this {@link MonitorImpl}
   *                                  instance is monitoring.
   * @param properties                The {@link Properties} containing additional monitoring
   *                                  configuration.
   * @param monitorDisposalTimeMillis Time in milliseconds before stopping the monitoring thread
   *                                  where there are no active connection to the server this
   *                                  {@link MonitorImpl} instance is monitoring.
   * @param threadContainer           A reference to the {@link MonitorThreadContainer} implementation
   *                                  that initialized this class.
   */
  public MonitorImpl(
      final @NonNull PluginService pluginService,
      @NonNull final HostSpec hostSpec,
      @NonNull final Properties properties,
      final long monitorDisposalTimeMillis,
      @NonNull final MonitorThreadContainer threadContainer) {
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.hostSpec = hostSpec;
    this.properties = properties;
    this.monitorDisposalTimeMillis = monitorDisposalTimeMillis;
    this.threadContainer = threadContainer;

    this.contextLastUsedTimestampNano = this.getCurrentTimeNano();
    this.contextsSizeGauge = telemetryFactory.createGauge("efm.activeContexts.queue.size",
        () -> (long) activeContexts.size());

    final String nodeId = StringUtils.isNullOrEmpty(this.hostSpec.getHostId())
        ? this.hostSpec.getHost()
        : this.hostSpec.getHostId();
    this.nodeInvalidCounter = telemetryFactory.createCounter(String.format("efm.nodeUnhealthy.count.%s", nodeId));
  }

  @Override
  public void startMonitoring(final MonitorConnectionContext context) {
    if (this.stopped) {
      LOGGER.warning(() -> Messages.get("MonitorImpl.monitorIsStopped", new Object[] {this.hostSpec.getHost()}));
    }
    final long currentTimeNano = this.getCurrentTimeNano();
    context.setStartMonitorTimeNano(currentTimeNano);
    this.contextLastUsedTimestampNano = currentTimeNano;
    this.newContexts.add(context);
  }

  @Override
  public void stopMonitoring(final MonitorConnectionContext context) {
    if (context == null) {
      LOGGER.warning(() -> Messages.get("MonitorImpl.contextNullWarning"));
      return;
    }

    context.setInactive();
    this.contextLastUsedTimestampNano = this.getCurrentTimeNano();
  }

  public void clearContexts() {
    this.newContexts.clear();
    this.activeContexts.clear();
  }

  @Override
  public void run() {

    LOGGER.finest(() -> Messages.get(
        "MonitorImpl.startMonitoringThread",
        new Object[]{this.hostSpec.getHost()}));

    try {
      this.stopped = false;
      while (true) {
        try {

          // process new contexts
          MonitorConnectionContext newMonitorContext;
          MonitorConnectionContext firstAddedNewMonitorContext = null;
          final long currentTimeNano = this.getCurrentTimeNano();

          while ((newMonitorContext = this.newContexts.poll()) != null) {
            if (firstAddedNewMonitorContext == newMonitorContext) {
              // This context has already been processed.
              // Add it back to the queue and process it in the next round.
              this.newContexts.add(newMonitorContext);
              break;
            }
            if (newMonitorContext.isActiveContext()) {
              if (newMonitorContext.getExpectedActiveMonitoringStartTimeNano() > currentTimeNano) {
                // The context active monitoring time hasn't come.
                // Add the context to the queue and check it later.
                this.newContexts.add(newMonitorContext);
                if (firstAddedNewMonitorContext == null) {
                  firstAddedNewMonitorContext = newMonitorContext;
                }
              } else {
                // It's time to start actively monitor this context.
                this.activeContexts.add(newMonitorContext);
              }
            }
          }

          if (!this.activeContexts.isEmpty()) {

            final long statusCheckStartTimeNano = this.getCurrentTimeNano();
            this.contextLastUsedTimestampNano = statusCheckStartTimeNano;

            final ConnectionStatus status = checkConnectionStatus(this.nodeCheckTimeoutMillis);

            long delayMillis = -1;
            MonitorConnectionContext monitorContext;
            MonitorConnectionContext firstAddedMonitorContext = null;

            while ((monitorContext = this.activeContexts.poll()) != null) {

              synchronized (monitorContext) {
                // If context is already invalid, just skip it
                if (!monitorContext.isActiveContext()) {
                  continue;
                }

                if (firstAddedMonitorContext == monitorContext) {
                  // this context has already been processed by this loop
                  // add it to the queue and exit this loop
                  this.activeContexts.add(monitorContext);
                  break;
                }

                // otherwise, process this context
                monitorContext.updateConnectionStatus(
                    this.hostSpec.getUrl(),
                    statusCheckStartTimeNano,
                    statusCheckStartTimeNano + status.elapsedTimeNano,
                    status.isValid);

                // If context is still valid and node is still healthy, it needs to continue updating this context
                if (monitorContext.isActiveContext() && !monitorContext.isNodeUnhealthy()) {
                  this.activeContexts.add(monitorContext);
                  if (firstAddedMonitorContext == null) {
                    firstAddedMonitorContext = monitorContext;
                  }

                  if (delayMillis == -1 || delayMillis > monitorContext.getFailureDetectionIntervalMillis()) {
                    delayMillis = monitorContext.getFailureDetectionIntervalMillis();
                  }
                }
              }
            }

            if (delayMillis == -1) {
              // No active contexts
              delayMillis = THREAD_SLEEP_WHEN_INACTIVE_MILLIS;
            } else {
              delayMillis -= status.elapsedTimeNano;
              // Check for min delay between node health check
              if (delayMillis <= 0) {
                delayMillis = MIN_CONNECTION_CHECK_TIMEOUT_MILLIS;
              }
              // Use this delay as node checkout timeout since it corresponds to min interval for all active contexts
              this.nodeCheckTimeoutMillis = delayMillis;
            }

            this.sleep(delayMillis);

          } else {
            if ((this.getCurrentTimeNano() - this.contextLastUsedTimestampNano)
                >= TimeUnit.MILLISECONDS.toNanos(this.monitorDisposalTimeMillis)) {
              threadContainer.releaseResource(this);
              break;
            }
            this.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
          }

        } catch (final InterruptedException intEx) {
          throw intEx;
        } catch (final Exception ex) {
          // log and ignore
          if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(
                Level.FINEST,
                Messages.get(
                   "MonitorImpl.exceptionDuringMonitoringContinue",
                    new Object[]{this.hostSpec.getHost()}),
                ex); // We want to print full trace stack of the exception.
          }
        }
      }
    } catch (final InterruptedException intEx) {
      // exit thread
      LOGGER.warning(
          () -> Messages.get(
              "MonitorImpl.interruptedExceptionDuringMonitoring",
              new Object[] {this.hostSpec.getHost()}));
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "MonitorImpl.exceptionDuringMonitoringStop",
                new Object[]{this.hostSpec.getHost()}),
            ex); // We want to print full trace stack of the exception.
      }
    } finally {
      threadContainer.releaseResource(this);
      this.stopped = true;
      if (this.monitoringConn != null) {
        try {
          this.monitoringConn.close();
        } catch (final SQLException ex) {
          // ignore
        }
      }
    }

    LOGGER.finest(() -> Messages.get(
        "MonitorImpl.stopMonitoringThread",
        new Object[]{this.hostSpec.getHost()}));
  }

  /**
   * Check the status of the monitored server by sending a ping.
   *
   * @param shortestFailureDetectionIntervalMillis The shortest failure detection interval used by
   *                                               all the connections to this server. This value is
   *                                               used as the maximum time to wait for a response
   *                                               from the server.
   * @return whether the server is still alive and the elapsed time spent checking.
   */
  ConnectionStatus checkConnectionStatus(final long shortestFailureDetectionIntervalMillis) {
    TelemetryContext connectContext = telemetryFactory.openTelemetryContext(
        "connection status check", TelemetryTraceLevel.FORCE_TOP_LEVEL);
    connectContext.setAttribute("url", hostSpec.getHost());

    long startNano = this.getCurrentTimeNano();
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
        startNano = this.getCurrentTimeNano();
        this.monitoringConn = this.pluginService.forceConnect(this.hostSpec, monitoringConnProperties);
        LOGGER.finest(() -> "Opened monitoring connection: " + this.monitoringConn);
        return new ConnectionStatus(true, this.getCurrentTimeNano() - startNano);
      }

      startNano = this.getCurrentTimeNano();
      final boolean isValid = this.monitoringConn.isValid(
          (int) TimeUnit.MILLISECONDS.toSeconds(shortestFailureDetectionIntervalMillis));
      if (!isValid) {
        this.nodeInvalidCounter.inc();
      }
      return new ConnectionStatus(isValid, this.getCurrentTimeNano() - startNano);

    } catch (final SQLException sqlEx) {
      this.nodeInvalidCounter.inc();
      return new ConnectionStatus(false, this.getCurrentTimeNano() - startNano);

    } finally {
      connectContext.closeContext();
    }
  }

  // This method helps to organize unit tests.
  long getCurrentTimeNano() {
    return System.nanoTime();
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Used to help with testing.
   */
  protected void sleep(long duration) throws InterruptedException {
    TimeUnit.MILLISECONDS.sleep(duration);
  }
}
