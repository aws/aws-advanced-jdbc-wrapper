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

package com.amazon.awslabs.jdbc.plugin.efm;

import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.PluginService;
import com.amazon.awslabs.jdbc.util.PropertyUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class uses a background thread to monitor a particular server with one or more active {@link
 * Connection}.
 */
public class MonitorImpl implements Monitor {

  static class ConnectionStatus {

    boolean isValid;
    long elapsedTime; // in nanos

    ConnectionStatus(boolean isValid, long elapsedTimeNano) {
      this.isValid = isValid;
      this.elapsedTime = elapsedTimeNano;
    }
  }

  private static final Logger LOGGER = Logger.getLogger(MonitorImpl.class.getName());
  private static final long THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  private final Queue<MonitorConnectionContext> contexts = new ConcurrentLinkedQueue<>();
  private final PluginService pluginService;
  private final Properties properties;
  private final HostSpec hostSpec;
  private Connection monitoringConn = null;
  private long connectionCheckIntervalMillis = Long.MAX_VALUE;
  private final AtomicLong lastContextUsedTimestamp = new AtomicLong(); // in nanos
  private final long monitorDisposalTimeMillis;
  private final MonitorService monitorService;
  private final AtomicBoolean stopped = new AtomicBoolean(true);

  /**
   * Store the monitoring configuration for a connection.
   *
   * @param pluginService A service for creating new connections.
   * @param hostSpec The {@link HostSpec} of the server this {@link MonitorImpl} instance is
   *     monitoring.
   * @param properties The {@link Properties} containing additional monitoring configuration.
   * @param monitorDisposalTimeMillis Time before stopping the monitoring thread where there are no active
   *     connection to the server this {@link MonitorImpl} instance is monitoring.
   * @param monitorService A reference to the {@link MonitorServiceImpl} implementation that
   *     initialized this class.
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

    this.lastContextUsedTimestamp.set(this.getCurrentTimeNano());
  }

  @Override
  public void startMonitoring(MonitorConnectionContext context) {
    this.connectionCheckIntervalMillis =
        Math.min(this.connectionCheckIntervalMillis, context.getFailureDetectionIntervalMillis());
    final long currentTime = this.getCurrentTimeNano();
    context.setStartMonitorTime(currentTime);
    this.lastContextUsedTimestamp.set(currentTime);
    this.contexts.add(context);
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    if (context == null) {
      LOGGER.log(Level.WARNING, "Parameter 'context' should not be null.");
      return;
    }

    context.invalidate();
    this.contexts.remove(context);

    this.connectionCheckIntervalMillis = findShortestIntervalMillis();
  }

  public synchronized void clearContexts() {
    this.contexts.clear();
    this.connectionCheckIntervalMillis = findShortestIntervalMillis();
  }

  @Override
  public void run() {
    try {
      this.stopped.set(false);
      while (true) {
        if (!this.contexts.isEmpty()) {
          final long statusCheckStartTime = this.getCurrentTimeNano();
          this.lastContextUsedTimestamp.set(statusCheckStartTime);

          final ConnectionStatus status =
              checkConnectionStatus(this.getConnectionCheckIntervalMillis());

          for (MonitorConnectionContext monitorContext : this.contexts) {
            monitorContext.updateConnectionStatus(
                statusCheckStartTime, statusCheckStartTime + status.elapsedTime, status.isValid);
          }

          TimeUnit.MILLISECONDS.sleep(
              Math.max(0, this.getConnectionCheckIntervalMillis() - TimeUnit.NANOSECONDS.toMillis(status.elapsedTime)));
        } else {
          if ((this.getCurrentTimeNano() - this.lastContextUsedTimestamp.get())
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
      this.stopped.set(true);
    }
  }

  /**
   * Check the status of the monitored server by sending a ping.
   *
   * @param shortestFailureDetectionIntervalMillis The shortest failure detection interval used by
   *     all the connections to this server. This value is used as the maximum time to wait for a
   *     response from the server.
   * @return whether the server is still alive and the elapsed time spent checking.
   */
  ConnectionStatus checkConnectionStatus(final long shortestFailureDetectionIntervalMillis) {
    long start = this.getCurrentTimeNano();
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

        start = this.getCurrentTimeNano();
        this.monitoringConn = this.pluginService.connect(this.hostSpec, monitoringConnProperties);
        return new ConnectionStatus(true, this.getCurrentTimeNano() - start);
      }

      start = this.getCurrentTimeNano();
      boolean isValid = this.monitoringConn.isValid(
          (int) TimeUnit.MILLISECONDS.toSeconds(shortestFailureDetectionIntervalMillis));
      return new ConnectionStatus(isValid, this.getCurrentTimeNano() - start);
    } catch (SQLException sqlEx) {
      // LOGGER.log(Level.FINEST, String.format("[Monitor] Error checking connection status: %s",
      // sqlEx.getMessage()));
      return new ConnectionStatus(false, this.getCurrentTimeNano() - start);
    }
  }

  // This method helps to organize unit tests.
  long getCurrentTimeNano() {
    return System.nanoTime();
  }

  long getConnectionCheckIntervalMillis() {
    if (this.connectionCheckIntervalMillis == Long.MAX_VALUE) {
      // connectionCheckIntervalMillis is at Long.MAX_VALUE because there are no contexts
      // available.
      return 0;
    }
    return this.connectionCheckIntervalMillis;
  }

  @Override
  public boolean isStopped() {
    return this.stopped.get();
  }

  private long findShortestIntervalMillis() {
    long currentMin = Long.MAX_VALUE;
    for (MonitorConnectionContext context : this.contexts) {
      currentMin = Math.min(currentMin, context.getFailureDetectionIntervalMillis());
    }
    return currentMin;
  }
}
