/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a
 * server should be considered unhealthy.
 */
public class MonitorConnectionContext {

  private static final Logger LOGGER = Logger.getLogger(MonitorConnectionContext.class.getName());

  private final int failureDetectionIntervalMillis;
  private final int failureDetectionTimeMillis;
  private final int failureDetectionCount;

  private final Set<String> hostAliases;
  private final Connection connectionToAbort;

  private long startMonitorTime;
  private long invalidNodeStartTime;
  private int failureCount;
  private boolean nodeUnhealthy;
  private AtomicBoolean activeContext = new AtomicBoolean(true);

  /**
   * Constructor.
   *
   * @param connectionToAbort A reference to the connection associated with this context that will
   *     be aborted in case of server failure.
   * @param hostAliases All valid references to the server.
   * @param failureDetectionTimeMillis Grace period after which node monitoring starts.
   * @param failureDetectionIntervalMillis Interval between each failed connection check.
   * @param failureDetectionCount Number of failed connection checks before considering database
   *     node as unhealthy.
   */
  public MonitorConnectionContext(
      Connection connectionToAbort,
      Set<String> hostAliases,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {
    this.connectionToAbort = connectionToAbort;
    this.hostAliases = hostAliases;
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
  }

  void setStartMonitorTime(long startMonitorTime) {
    this.startMonitorTime = startMonitorTime;
  }

  Set<String> getHostAliases() {
    return this.hostAliases;
  }

  public int getFailureDetectionTimeMillis() {
    return failureDetectionTimeMillis;
  }

  public int getFailureDetectionIntervalMillis() {
    return failureDetectionIntervalMillis;
  }

  public int getFailureDetectionCount() {
    return failureDetectionCount;
  }

  public int getFailureCount() {
    return this.failureCount;
  }

  void setFailureCount(int failureCount) {
    this.failureCount = failureCount;
  }

  void setInvalidNodeStartTime(long invalidNodeStartTimeMillis) {
    this.invalidNodeStartTime = invalidNodeStartTimeMillis;
  }

  void resetInvalidNodeStartTime() {
    this.invalidNodeStartTime = 0;
  }

  boolean isInvalidNodeStartTimeDefined() {
    return this.invalidNodeStartTime > 0;
  }

  public long getInvalidNodeStartTime() {
    return this.invalidNodeStartTime;
  }

  public boolean isNodeUnhealthy() {
    return this.nodeUnhealthy;
  }

  void setNodeUnhealthy(boolean nodeUnhealthy) {
    this.nodeUnhealthy = nodeUnhealthy;
  }

  public boolean isActiveContext() {
    return this.activeContext.get();
  }

  public void invalidate() {
    this.activeContext.set(false);
  }

  synchronized void abortConnection() {
    if (this.connectionToAbort == null || !this.activeContext.get()) {
      return;
    }

    try {
      this.connectionToAbort.close();
    } catch (SQLException sqlEx) {
      // ignore
      LOGGER.log(
          Level.FINEST,
          String.format("Exception during aborting connection: %s", sqlEx.getMessage()));
    }
  }

  /**
   * Update whether the connection is still valid if the total elapsed time has passed the grace
   * period.
   *
   * @param statusCheckStartTime The time when connection status check started in milliseconds.
   * @param currentTime The time when connection status check ended in milliseconds.
   * @param isValid Whether the connection is valid.
   */
  public void updateConnectionStatus(long statusCheckStartTime, long currentTime, boolean isValid) {
    if (!this.activeContext.get()) {
      return;
    }

    final long totalElapsedTimeMillis = currentTime - this.startMonitorTime;

    if (totalElapsedTimeMillis > this.failureDetectionTimeMillis) {
      this.setConnectionValid(isValid, statusCheckStartTime, currentTime);
    }
  }

  /**
   * Set whether the connection to the server is still valid based on the monitoring settings set in
   * the {@link Connection}.
   *
   * <p>These monitoring settings include:
   *
   * <ul>
   *   <li>{@code failureDetectionInterval}
   *   <li>{@code failureDetectionTime}
   *   <li>{@code failureDetectionCount}
   * </ul>
   *
   * @param connectionValid Boolean indicating whether the server is still responsive.
   * @param statusCheckStartTime The time when connection status check started in milliseconds.
   * @param currentTime The time when connection status check ended in milliseconds.
   */
  void setConnectionValid(boolean connectionValid, long statusCheckStartTime, long currentTime) {

    if (!connectionValid) {
      this.failureCount++;

      if (!this.isInvalidNodeStartTimeDefined()) {
        this.setInvalidNodeStartTime(statusCheckStartTime);
      }

      final long invalidNodeDurationMillis = currentTime - this.getInvalidNodeStartTime();
      final long maxInvalidNodeDurationMillis =
          (long) this.getFailureDetectionIntervalMillis()
              * Math.max(0, this.getFailureDetectionCount());

      if (invalidNodeDurationMillis >= maxInvalidNodeDurationMillis) {
        LOGGER.log(Level.FINE, String.format("Host %s is *dead*.", hostAliases));
        this.setNodeUnhealthy(true);
        this.abortConnection();
        return;
      }

      LOGGER.log(
          Level.FINEST,
          String.format("Host %s is not *responding* (%d).", hostAliases, this.getFailureCount()));
      return;
    }

    this.setFailureCount(0);
    this.resetInvalidNodeStartTime();
    this.setNodeUnhealthy(false);

    LOGGER.log(Level.FINEST, String.format("Host %s is *alive*.", hostAliases));
  }
}
