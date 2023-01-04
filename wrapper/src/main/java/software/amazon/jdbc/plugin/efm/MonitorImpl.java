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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

  static final long DEFAULT_CONNECTION_CHECK_INTERVAL_MILLIS = 100;
  static final long DEFAULT_CONNECTION_CHECK_TIMEOUT_MILLIS = 3000;
  private static final Logger LOGGER = Logger.getLogger(MonitorImpl.class.getName());
  private static final long THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  private final long monitorDisposalTimeMillis;
  private final Queue<MonitorConnectionContext> contexts = new ConcurrentLinkedQueue<>();
  private final PluginService pluginService;
  private final Properties properties;
  private final HostSpec hostSpec;
  private final AtomicLong contextLastUsedTimestampNano = new AtomicLong();
  private final MonitorService monitorService;
  private final AtomicBoolean stopped = new AtomicBoolean(true);
  private AtomicLong connectionCheckIntervalMillis = new AtomicLong(DEFAULT_CONNECTION_CHECK_INTERVAL_MILLIS);
  private AtomicBoolean isConnectionCheckIntervalInitialized = new AtomicBoolean(false);
  private Connection monitoringConn = null;

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

    this.contextLastUsedTimestampNano.set(this.getCurrentTimeNano());
  }

  @Override
  public void startMonitoring(MonitorConnectionContext context) {
    if (!this.isConnectionCheckIntervalInitialized.get()) {
      this.connectionCheckIntervalMillis.set(context.getFailureDetectionIntervalMillis());
      this.isConnectionCheckIntervalInitialized.set(true);
    } else {
      this.connectionCheckIntervalMillis.set(Math.min(
          this.connectionCheckIntervalMillis.get(),
          context.getFailureDetectionIntervalMillis()));
    }

    final long currentTimeNano = this.getCurrentTimeNano();
    context.setStartMonitorTimeNano(currentTimeNano);
    this.contextLastUsedTimestampNano.set(currentTimeNano);
    this.contexts.add(context);
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    if (context == null) {
      LOGGER.warning(() -> Messages.get("MonitorImpl.contextNullWarning"));
      return;
    }

    context.invalidate();
    this.contexts.remove(context);

    this.connectionCheckIntervalMillis.set(findShortestIntervalMillis());
    this.isConnectionCheckIntervalInitialized.set(true);
  }

  public synchronized void clearContexts() {
    this.contexts.clear();
    this.connectionCheckIntervalMillis.set(findShortestIntervalMillis());
    this.isConnectionCheckIntervalInitialized.set(true);
  }

  @Override
  public void run() {
    try {
      this.stopped.set(false);
      while (true) {
        if (!this.contexts.isEmpty()) {
          final long statusCheckStartTimeNano = this.getCurrentTimeNano();
          this.contextLastUsedTimestampNano.set(statusCheckStartTimeNano);

          final ConnectionStatus status =
              checkConnectionStatus(this.getConnectionCheckTimeoutMillis());

          for (MonitorConnectionContext monitorContext : this.contexts) {
            monitorContext.updateConnectionStatus(
                statusCheckStartTimeNano, statusCheckStartTimeNano + status.elapsedTimeNano, status.isValid);
          }

          TimeUnit.MILLISECONDS.sleep(
              Math.max(0,
                  this.getConnectionCheckIntervalMillis() - TimeUnit.NANOSECONDS.toMillis(status.elapsedTimeNano)));
        } else {
          if ((this.getCurrentTimeNano() - this.contextLastUsedTimestampNano.get())
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
        this.monitoringConn = this.pluginService.connect(this.hostSpec, monitoringConnProperties);
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

  long getConnectionCheckTimeoutMillis() {
    return this.connectionCheckIntervalMillis.get() == 0 ? DEFAULT_CONNECTION_CHECK_TIMEOUT_MILLIS
        : this.connectionCheckIntervalMillis.get();
  }

  long getConnectionCheckIntervalMillis() {
    return this.connectionCheckIntervalMillis.get() == 0 ? DEFAULT_CONNECTION_CHECK_INTERVAL_MILLIS
        : this.connectionCheckIntervalMillis.get();
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
    return currentMin == Long.MAX_VALUE ? DEFAULT_CONNECTION_CHECK_INTERVAL_MILLIS : currentMin;
  }
}
