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

  private final long failureDetectionIntervalMillis;
  private final long failureDetectionTimeMillis;
  private final long failureDetectionCount;

  private final Set<String> hostAliases;
  private final Connection connectionToAbort;

  private long startMonitorTime; // in nanos
  private long invalidNodeStartTime; // in nanos
  private long failureCount;
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
      long failureDetectionTimeMillis,
      long failureDetectionIntervalMillis,
      long failureDetectionCount) {
    this.connectionToAbort = connectionToAbort;
    this.hostAliases = hostAliases;
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
  }

  void setStartMonitorTime(long startMonitorTimeNano) {
    this.startMonitorTime = startMonitorTimeNano;
  }

  Set<String> getHostAliases() {
    return this.hostAliases;
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

  void setInvalidNodeStartTime(long invalidNodeStartTimeNano) {
    this.invalidNodeStartTime = invalidNodeStartTimeNano;
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
   * @param statusCheckStartTime The time when connection status check started in nanos.
   * @param currentTime The time when connection status check ended in nanos.
   * @param isValid Whether the connection is valid.
   */
  public void updateConnectionStatus(long statusCheckStartTime, long currentTime, boolean isValid) {
    if (!this.activeContext.get()) {
      return;
    }

    final long totalElapsedTimeNano = currentTime - this.startMonitorTime;

    if (totalElapsedTimeNano > (this.failureDetectionTimeMillis * 1000000L)) {
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
   * @param statusCheckStartTime The time when connection status check started in nanos.
   * @param currentTime The time when connection status check ended in nanos.
   */
  void setConnectionValid(boolean connectionValid, long statusCheckStartTime, long currentTime) {

    if (!connectionValid) {
      this.failureCount++;

      if (!this.isInvalidNodeStartTimeDefined()) {
        this.setInvalidNodeStartTime(statusCheckStartTime);
      }

      final long invalidNodeDurationNano = currentTime - this.getInvalidNodeStartTime();
      final long maxInvalidNodeDurationMillis =
          (long) this.getFailureDetectionIntervalMillis()
              * Math.max(0, this.getFailureDetectionCount());

      if (invalidNodeDurationNano >= (maxInvalidNodeDurationMillis * 1000000L)) {
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
