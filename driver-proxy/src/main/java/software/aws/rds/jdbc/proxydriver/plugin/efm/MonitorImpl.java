/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

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
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.PluginService;
import software.aws.rds.jdbc.proxydriver.util.PropertyUtils;

/**
 * This class uses a background thread to monitor a particular server with one or more active {@link
 * Connection}.
 */
public class MonitorImpl implements Monitor {

  static class ConnectionStatus {

    boolean isValid;
    long elapsedTime;

    ConnectionStatus(boolean isValid, long elapsedTime) {
      this.isValid = isValid;
      this.elapsedTime = elapsedTime;
    }
  }

  private static final Logger LOGGER = Logger.getLogger(MonitorImpl.class.getName());
  private static final int THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  private final Queue<MonitorConnectionContext> contexts = new ConcurrentLinkedQueue<>();
  private final PluginService pluginService;
  private final Properties properties;
  private final HostSpec hostSpec;
  private Connection monitoringConn = null;
  private int connectionCheckIntervalMillis = Integer.MAX_VALUE;
  private final AtomicLong lastContextUsedTimestamp = new AtomicLong();
  private final long monitorDisposalTime;
  private final MonitorService monitorService;
  private final AtomicBoolean stopped = new AtomicBoolean(true);

  /**
   * Store the monitoring configuration for a connection.
   *
   * @param pluginService A service for creating new connections.
   * @param hostSpec The {@link HostSpec} of the server this {@link MonitorImpl} instance is
   *     monitoring.
   * @param properties The {@link Properties} containing additional monitoring configuration.
   * @param monitorDisposalTime Time before stopping the monitoring thread where there are no active
   *     connection to the server this {@link MonitorImpl} instance is monitoring.
   * @param monitorService A reference to the {@link MonitorServiceImpl} implementation that
   *     initialized this class.
   */
  public MonitorImpl(
      final @NonNull PluginService pluginService,
      @NonNull HostSpec hostSpec,
      @NonNull Properties properties,
      long monitorDisposalTime,
      @NonNull MonitorService monitorService) {
    this.pluginService = pluginService;
    this.hostSpec = hostSpec;
    this.properties = properties;
    this.monitorDisposalTime = monitorDisposalTime;
    this.monitorService = monitorService;
  }

  @Override
  public synchronized void startMonitoring(MonitorConnectionContext context) {
    this.connectionCheckIntervalMillis =
        Math.min(this.connectionCheckIntervalMillis, context.getFailureDetectionIntervalMillis());
    final long currentTime = this.getCurrentTimeMillis();
    context.setStartMonitorTime(currentTime);
    this.lastContextUsedTimestamp.set(currentTime);
    this.contexts.add(context);
  }

  @Override
  public synchronized void stopMonitoring(MonitorConnectionContext context) {
    if (context == null) {
      LOGGER.log(Level.WARNING, "Parameter 'context' should not be null.");
      return;
    }
    synchronized (context) {
      this.contexts.remove(context);
      context.invalidate();
    }
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
          final long statusCheckStartTime = this.getCurrentTimeMillis();
          this.lastContextUsedTimestamp.set(statusCheckStartTime);

          final ConnectionStatus status =
              checkConnectionStatus(this.getConnectionCheckIntervalMillis());

          for (MonitorConnectionContext monitorContext : this.contexts) {
            monitorContext.updateConnectionStatus(
                statusCheckStartTime, statusCheckStartTime + status.elapsedTime, status.isValid);
          }

          TimeUnit.MILLISECONDS.sleep(
              Math.max(0, this.getConnectionCheckIntervalMillis() - status.elapsedTime));
        } else {
          if ((this.getCurrentTimeMillis() - this.lastContextUsedTimestamp.get())
              >= this.monitorDisposalTime) {
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
  ConnectionStatus checkConnectionStatus(final int shortestFailureDetectionIntervalMillis) {
    long start = this.getCurrentTimeMillis();
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

        start = this.getCurrentTimeMillis();
        this.monitoringConn = this.pluginService.connect(this.hostSpec, monitoringConnProperties);
        return new ConnectionStatus(true, this.getCurrentTimeMillis() - start);
      }

      start = this.getCurrentTimeMillis();
      boolean isValid = this.monitoringConn.isValid(shortestFailureDetectionIntervalMillis / 1000);
      return new ConnectionStatus(isValid, this.getCurrentTimeMillis() - start);
    } catch (SQLException sqlEx) {
      // LOGGER.log(Level.FINEST, String.format("[Monitor] Error checking connection status: %s",
      // sqlEx.getMessage()));
      return new ConnectionStatus(false, this.getCurrentTimeMillis() - start);
    }
  }

  // This method helps to organize unit tests.
  long getCurrentTimeMillis() {
    return System.currentTimeMillis();
  }

  int getConnectionCheckIntervalMillis() {
    if (this.connectionCheckIntervalMillis == Integer.MAX_VALUE) {
      // connectionCheckIntervalMillis is at Integer.MAX_VALUE because there are no contexts
      // available.
      return 0;
    }
    return this.connectionCheckIntervalMillis;
  }

  @Override
  public boolean isStopped() {
    return this.stopped.get();
  }

  private int findShortestIntervalMillis() {
    if (this.contexts.isEmpty()) {
      return Integer.MAX_VALUE;
    }

    return this.contexts.stream()
        .min(Comparator.comparing(MonitorConnectionContext::getFailureDetectionIntervalMillis))
        .map(MonitorConnectionContext::getFailureDetectionIntervalMillis)
        .orElse(0);
  }
}
