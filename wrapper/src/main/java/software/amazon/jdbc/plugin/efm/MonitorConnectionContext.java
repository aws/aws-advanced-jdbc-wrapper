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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a
 * server should be considered unhealthy. The context is shared between the main thread and the monitor thread.
 */
public class MonitorConnectionContext {

  private static final Logger LOGGER = Logger.getLogger(MonitorConnectionContext.class.getName());

  private final TelemetryCounter abortedConnectionsCounter;

  private final long failureDetectionIntervalMillis;
  private final long failureDetectionTimeMillis;
  private final long failureDetectionCount;

  private final Set<String> hostAliases; // Variable is never written, so it does not need to be thread-safe
  private final Connection connectionToAbort;

  private final AtomicBoolean activeContext = new AtomicBoolean(true);
  private final AtomicBoolean nodeUnhealthy = new AtomicBoolean();
  private final AtomicLong startMonitorTimeNano = new AtomicLong();
  private long invalidNodeStartTimeNano; // Only accessed by monitor thread
  private long failureCount; // Only accessed by monitor thread

  /**
   * Constructor.
   *
   * @param connectionToAbort              A reference to the connection associated with this context that will
   *                                       be aborted in case of server failure.
   * @param hostAliases                    All valid references to the server.
   * @param failureDetectionTimeMillis     Grace period after which node monitoring starts.
   * @param failureDetectionIntervalMillis Interval between each failed connection check.
   * @param failureDetectionCount          Number of failed connection checks before considering database
   *                                       node as unhealthy.
   */
  public MonitorConnectionContext(
      Connection connectionToAbort,
      Set<String> hostAliases,
      long failureDetectionTimeMillis,
      long failureDetectionIntervalMillis,
      long failureDetectionCount,
      TelemetryCounter abortedConnectionsCounter) {
    this.connectionToAbort = connectionToAbort;
    // Variable is never written, so it does not need to be thread-safe
    this.hostAliases = new HashSet<>(hostAliases);
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
    this.abortedConnectionsCounter = abortedConnectionsCounter;
  }

  void setStartMonitorTimeNano(long startMonitorTimeNano) {
    this.startMonitorTimeNano.set(startMonitorTimeNano);
  }

  Set<String> getHostAliases() {
    return Collections.unmodifiableSet(this.hostAliases);
  }

  public long getFailureDetectionTimeMillis() {
    return failureDetectionTimeMillis;
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

  public long getInvalidNodeStartTimeNano() {
    return this.invalidNodeStartTimeNano;
  }

  public boolean isNodeUnhealthy() {
    return this.nodeUnhealthy.get();
  }

  void setNodeUnhealthy(boolean nodeUnhealthy) {
    this.nodeUnhealthy.set(nodeUnhealthy);
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
      abortedConnectionsCounter.inc();
      this.connectionToAbort.close();
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
   * @param statusCheckStartTimeNano The time when connection status check started in nanos.
   * @param statusCheckEndTimeNano   The time when connection status check ended in nanos.
   * @param isValid                  Whether the connection is valid.
   */
  public void updateConnectionStatus(long statusCheckStartTimeNano, long statusCheckEndTimeNano, boolean isValid) {
    if (!this.activeContext.get()) {
      return;
    }

    final long totalElapsedTimeNano = statusCheckEndTimeNano - this.startMonitorTimeNano.get();

    if (totalElapsedTimeNano > TimeUnit.MILLISECONDS.toNanos(this.failureDetectionTimeMillis)) {
      this.setConnectionValid(isValid, statusCheckStartTimeNano, statusCheckEndTimeNano);
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
   * @param connectionValid      Boolean indicating whether the server is still responsive.
   * @param statusCheckStartNano The time when connection status check started in nanos.
   * @param statusCheckEndNano   The time when connection status check ended in nanos.
   */
  void setConnectionValid(boolean connectionValid, long statusCheckStartNano, long statusCheckEndNano) {

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
        LOGGER.fine(() -> Messages.get("MonitorConnectionContext.hostDead", new Object[] {hostAliases}));
        this.setNodeUnhealthy(true);
        this.abortConnection();
        return;
      }

      LOGGER.finest(
          () -> Messages.get(
              "MonitorConnectionContext.hostNotResponding",
              new Object[] {hostAliases, this.getFailureCount()}));
      return;
    }

    this.setFailureCount(0);
    this.resetInvalidNodeStartTime();
    this.setNodeUnhealthy(false);

    LOGGER.finest(
        () -> Messages.get("MonitorConnectionContext.hostAlive",
            new Object[] {hostAliases}));
  }
}
