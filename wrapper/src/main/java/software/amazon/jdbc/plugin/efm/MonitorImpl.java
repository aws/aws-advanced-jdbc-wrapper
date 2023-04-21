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
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

/**
 * This class uses a background thread to monitor a particular server with one or more active {@link
 * Connection}.
 */
public class MonitorImpl implements Monitor {

  static class ConnectionStatus {

    boolean isValid;
    long elapsedTimeNano;

    ConnectionStatus(boolean isValid, long elapsedTimeNano) {
      this.isValid = isValid;
      this.elapsedTimeNano = elapsedTimeNano;
    }
  }

  private static final Logger LOGGER = Logger.getLogger(MonitorImpl.class.getName());
  private static final long THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final long MIN_CONNECTION_CHECK_TIMEOUT_MILLIS = 3000;
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  private final Queue<MonitorConnectionContext> activeContexts = new ConcurrentLinkedQueue<>();
  private final Queue<MonitorConnectionContext> newContexts = new ConcurrentLinkedQueue<>();
  private final PluginService pluginService;
  private final Properties properties;
  private final HostSpec hostSpec;
  private final MonitorService monitorService;
  private final long monitorDisposalTimeMillis;
  private volatile long contextLastUsedTimestampNano;
  private volatile boolean stopped = false;
  private Connection monitoringConn = null;
  private long nodeCheckTimeoutMillis = MIN_CONNECTION_CHECK_TIMEOUT_MILLIS;

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
   * @param monitorService            A reference to the {@link MonitorServiceImpl} implementation
   *                                  that initialized this class.
   */
  public MonitorImpl(
      final @NonNull PluginService pluginService,
      @NonNull HostSpec hostSpec,
      @NonNull Properties properties,
      long monitorDisposalTimeMillis,
      @NonNull MonitorService monitorService) {
    this.pluginService = pluginService;
    this.hostSpec = hostSpec;
    this.properties = properties;
    this.monitorDisposalTimeMillis = monitorDisposalTimeMillis;
    this.monitorService = monitorService;

    this.contextLastUsedTimestampNano = this.getCurrentTimeNano();
  }

  @Override
  public void startMonitoring(MonitorConnectionContext context) {
    final long currentTimeNano = this.getCurrentTimeNano();
    context.setStartMonitorTimeNano(currentTimeNano);
    this.contextLastUsedTimestampNano = currentTimeNano;
    this.newContexts.add(context);
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
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
    try {
      this.stopped = false;
      while (true) {

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

          final ConnectionStatus status =
              checkConnectionStatus(this.nodeCheckTimeoutMillis);

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
            if (delayMillis < MIN_CONNECTION_CHECK_TIMEOUT_MILLIS) {
              delayMillis = MIN_CONNECTION_CHECK_TIMEOUT_MILLIS;
            }
            // Use this delay as node checkout timeout since it corresponds to min interval for all active contexts
            this.nodeCheckTimeoutMillis = delayMillis;
          }

          TimeUnit.MILLISECONDS.sleep(delayMillis);

        } else {
          if ((this.getCurrentTimeNano() - this.contextLastUsedTimestampNano)
              >= TimeUnit.MILLISECONDS.toNanos(this.monitorDisposalTimeMillis)) {
            monitorService.notifyUnused(this);
            break;
          }
          TimeUnit.MILLISECONDS.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
        }
      }
    } catch (InterruptedException intEx) {
      // do nothing; exit thread
    } finally {
      if (this.monitoringConn != null) {
        try {
          this.monitoringConn.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
      this.stopped = true;
    }
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
    long startNano = this.getCurrentTimeNano();
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
        Properties monitoringConnProperties = PropertyUtils.copyProperties(this.properties);

        this.properties.stringPropertyNames().stream()
            .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
            .forEach(
                p -> {
                  monitoringConnProperties.put(
                      p.substring(MONITORING_PROPERTY_PREFIX.length()),
                      this.properties.getProperty(p));
                  monitoringConnProperties.remove(p);
                });

        startNano = this.getCurrentTimeNano();
        this.monitoringConn = this.pluginService.forceConnect(this.hostSpec, monitoringConnProperties);
        return new ConnectionStatus(true, this.getCurrentTimeNano() - startNano);
      }

      startNano = this.getCurrentTimeNano();
      boolean isValid = this.monitoringConn.isValid(
          (int) TimeUnit.MILLISECONDS.toSeconds(shortestFailureDetectionIntervalMillis));
      return new ConnectionStatus(isValid, this.getCurrentTimeNano() - startNano);
    } catch (SQLException sqlEx) {
      return new ConnectionStatus(false, this.getCurrentTimeNano() - startNano);
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
}
