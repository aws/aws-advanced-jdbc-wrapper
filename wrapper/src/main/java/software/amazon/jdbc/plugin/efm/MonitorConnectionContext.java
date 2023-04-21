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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.util.Messages;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a
 * server should be considered unhealthy. The context is shared between the main thread and the monitor thread.
 */
public class MonitorConnectionContext {

  private static final Logger LOGGER = Logger.getLogger(MonitorConnectionContext.class.getName());
  private static final Executor ABORT_EXECUTOR = Executors.newSingleThreadExecutor();

  private final long failureDetectionIntervalMillis;
  private final long failureDetectionTimeMillis;
  private final long failureDetectionCount;
  private final Connection connectionToAbort;
  private final Monitor monitor;

  private volatile boolean activeContext = true;
  private volatile boolean nodeUnhealthy = false;
  private long startMonitorTimeNano;
  private long expectedActiveMonitoringStartTimeNano;
  private long invalidNodeStartTimeNano; // Only accessed by monitor thread
  private long failureCount; // Only accessed by monitor thread

  /**
   * Constructor.
   *
   * @param monitor                        A reference to a monitor object.
   * @param connectionToAbort              A reference to the connection associated with this context that will
   *                                       be aborted in case of server failure.
   * @param failureDetectionTimeMillis     Grace period after which node monitoring starts.
   * @param failureDetectionIntervalMillis Interval between each failed connection check.
   * @param failureDetectionCount          Number of failed connection checks before considering database
   *                                       node as unhealthy.
   */
  public MonitorConnectionContext(
      Monitor monitor,
      Connection connectionToAbort,
      long failureDetectionTimeMillis,
      long failureDetectionIntervalMillis,
      long failureDetectionCount) {
    this.monitor = monitor;
    this.connectionToAbort = connectionToAbort;
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
  }

  void setStartMonitorTimeNano(long startMonitorTimeNano) {
    this.startMonitorTimeNano = startMonitorTimeNano;
    this.expectedActiveMonitoringStartTimeNano = startMonitorTimeNano
        + TimeUnit.MILLISECONDS.toNanos(this.failureDetectionTimeMillis);
  }

  public long getFailureDetectionIntervalMillis() {
    return failureDetectionIntervalMillis;
  }

  public long getFailureDetectionCount() {
    return failureDetectionCount;
  }

  public long getFailureCount() {
    return this.failureCount;
  }

  public long getExpectedActiveMonitoringStartTimeNano() {
    return this.expectedActiveMonitoringStartTimeNano;
  }

  public Monitor getMonitor() {
    return this.monitor;
  }

  void setFailureCount(long failureCount) {
    this.failureCount = failureCount;
  }

  void setInvalidNodeStartTimeNano(long invalidNodeStartTimeNano) {
    this.invalidNodeStartTimeNano = invalidNodeStartTimeNano;
  }

  void resetInvalidNodeStartTime() {
    this.invalidNodeStartTimeNano = 0;
  }

  boolean isInvalidNodeStartTimeDefined() {
    return this.invalidNodeStartTimeNano > 0;
  }

  long getInvalidNodeStartTimeNano() {
    return this.invalidNodeStartTimeNano;
  }

  public boolean isNodeUnhealthy() {
    return this.nodeUnhealthy;
  }

  void setNodeUnhealthy(boolean nodeUnhealthy) {
    this.nodeUnhealthy = nodeUnhealthy;
  }

  public boolean isActiveContext() {
    return this.activeContext;
  }

  public void setInactive() {
    this.activeContext = false;
  }

  void abortConnection() {
    if (this.connectionToAbort == null || !this.activeContext) {
      return;
    }

    try {
      this.connectionToAbort.abort(ABORT_EXECUTOR);
    } catch (SQLException sqlEx) {
      // ignore
      LOGGER.finest(
          () -> Messages.get(
              "MonitorConnectionContext.exceptionAbortingConnection",
              new Object[] {sqlEx.getMessage()}));
    }
  }

  /**
   * Update whether the connection is still valid if the total elapsed time has passed the grace
   * period.
   *
   * @param hostName                 A node name for logging purposes.
   * @param statusCheckStartTimeNano The time when connection status check started in nanos.
   * @param statusCheckEndTimeNano   The time when connection status check ended in nanos.
   * @param isValid                  Whether the connection is valid.
   */
  public void updateConnectionStatus(
      String hostName,
      long statusCheckStartTimeNano,
      long statusCheckEndTimeNano,
      boolean isValid) {

    if (!this.activeContext) {
      return;
    }

    final long totalElapsedTimeNano = statusCheckEndTimeNano - this.startMonitorTimeNano;

    if (totalElapsedTimeNano > TimeUnit.MILLISECONDS.toNanos(this.failureDetectionTimeMillis)) {
      this.setConnectionValid(hostName, isValid, statusCheckStartTimeNano, statusCheckEndTimeNano);
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
   * @param hostName             A node name for logging purposes.
   * @param connectionValid      Boolean indicating whether the server is still responsive.
   * @param statusCheckStartNano The time when connection status check started in nanos.
   * @param statusCheckEndNano   The time when connection status check ended in nanos.
   */
  void setConnectionValid(
      String hostName,
      boolean connectionValid,
      long statusCheckStartNano,
      long statusCheckEndNano) {

    if (!connectionValid) {
      this.failureCount++;

      if (!this.isInvalidNodeStartTimeDefined()) {
        this.setInvalidNodeStartTimeNano(statusCheckStartNano);
      }

      final long invalidNodeDurationNano = statusCheckEndNano - this.getInvalidNodeStartTimeNano();
      final long maxInvalidNodeDurationMillis =
          this.getFailureDetectionIntervalMillis()
              * Math.max(0, this.getFailureDetectionCount());

      if (invalidNodeDurationNano >= TimeUnit.MILLISECONDS.toNanos(maxInvalidNodeDurationMillis)) {
        LOGGER.fine(() -> Messages.get("MonitorConnectionContext.hostDead", new Object[] {hostName}));
        this.setNodeUnhealthy(true);
        this.abortConnection();
        return;
      }

      LOGGER.finest(
          () -> Messages.get(
              "MonitorConnectionContext.hostNotResponding",
              new Object[] {hostName, this.getFailureCount()}));
      return;
    }

    this.setFailureCount(0);
    this.resetInvalidNodeStartTime();
    this.setNodeUnhealthy(false);

    LOGGER.finest(
        () -> Messages.get("MonitorConnectionContext.hostAlive",
            new Object[] {hostName}));
  }
}
