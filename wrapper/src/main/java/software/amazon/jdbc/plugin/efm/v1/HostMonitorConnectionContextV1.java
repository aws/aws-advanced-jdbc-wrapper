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

package software.amazon.jdbc.plugin.efm.v1;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.plugin.efm.base.HostMonitorConnectionContext;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a
 * server should be considered unhealthy. The context is shared between the main thread and the monitor thread.
 */
public class HostMonitorConnectionContextV1 extends HostMonitorConnectionContext {

  private static final Logger LOGGER = Logger.getLogger(HostMonitorConnectionContextV1.class.getName());
  private static final Executor ABORT_EXECUTOR =
      ExecutorFactory.newSingleThreadExecutor("abort");

  private TelemetryCounter abortedConnectionsCounter;

  private long failureDetectionIntervalMillis;
  private long failureDetectionTimeMillis;
  private long failureDetectionCount;

  private volatile boolean activeContext = true;
  private long startMonitorTimeNano;
  private long expectedActiveMonitoringStartTimeNano;
  private long invalidNodeStartTimeNano; // Only accessed by monitor thread
  private long failureCount; // Only accessed by monitor thread

  private final ResourceLock lock = new ResourceLock();

  public HostMonitorConnectionContextV1() {
  }

  public HostMonitorConnectionContextV1(Connection connectionToAbort) {
    super(connectionToAbort);
  }

  /**
   * Constructor.
   *
   * @param connectionToAbort              A reference to the connection associated with this context that will
   *                                       be aborted in case of server failure.
   * @param failureDetectionTimeMillis     Grace period after which node monitoring starts.
   * @param failureDetectionIntervalMillis Interval between each failed connection check.
   * @param failureDetectionCount          Number of failed connection checks before considering database
   *                                       node as unhealthy.
   * @param abortedConnectionsCounter Aborted connection telemetry counter.
   */
  public HostMonitorConnectionContextV1(
      final Connection connectionToAbort,
      final long failureDetectionTimeMillis,
      final long failureDetectionIntervalMillis,
      final long failureDetectionCount,
      TelemetryCounter abortedConnectionsCounter) {
    super(connectionToAbort);
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
    this.abortedConnectionsCounter = abortedConnectionsCounter;
  }

  public void initContext(final Connection connectionToAbort,
      final long failureDetectionTimeMillis,
      final long failureDetectionIntervalMillis,
      final long failureDetectionCount,
      TelemetryCounter abortedConnectionsCounter) {
    this.connectionToAbortRef.set(new WeakReference<>(connectionToAbort));
    this.nodeUnhealthy.set(false);
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
    this.abortedConnectionsCounter = abortedConnectionsCounter;
  }

  void setStartMonitorTimeNano(final long startMonitorTimeNano) {
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

  public void setFailureCount(final long failureCount) {
    this.failureCount = failureCount;
  }

  void setInvalidNodeStartTimeNano(final long invalidNodeStartTimeNano) {
    this.invalidNodeStartTimeNano = invalidNodeStartTimeNano;
  }

  public void resetInvalidNodeStartTime() {
    this.invalidNodeStartTimeNano = 0;
  }

  public boolean isInvalidNodeStartTimeDefined() {
    return this.invalidNodeStartTimeNano > 0;
  }

  long getInvalidNodeStartTimeNano() {
    return this.invalidNodeStartTimeNano;
  }

  public boolean isActiveContext() {
    return this.activeContext;
  }

  public void setInactive() {
    this.activeContext = false;
  }

  public void abortConnection() {
    final WeakReference<Connection> connRef = this.connectionToAbortRef.get();
    if (connRef == null || !this.activeContext) {
      return;
    }

    final Connection connectionToAbort = connRef.get();
    if (connectionToAbort == null) {
      return;
    }

    try {
      connectionToAbort.abort(ABORT_EXECUTOR);
      connectionToAbort.close();
      if (this.abortedConnectionsCounter != null) {
        this.abortedConnectionsCounter.inc();
      }
    } catch (final SQLException sqlEx) {
      // ignore
      LOGGER.finest(
          () -> Messages.get(
              "HostMonitorConnectionContext.exceptionAbortingConnection",
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
      final String hostName,
      final long statusCheckStartTimeNano,
      final long statusCheckEndTimeNano,
      final boolean isValid) {

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
  public void setConnectionValid(
      final String hostName,
      final boolean connectionValid,
      final long statusCheckStartNano,
      final long statusCheckEndNano) {

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
        LOGGER.fine(() -> Messages.get("HostMonitorConnectionContext.hostDead", new Object[] {hostName}));
        this.setNodeUnhealthy(true);
        this.abortConnection();
        return;
      }

      LOGGER.finest(
          () -> Messages.get(
              "HostMonitorConnectionContext.hostNotResponding",
              new Object[] {hostName, this.getFailureCount()}));
      return;
    }

    this.setFailureCount(0);
    this.resetInvalidNodeStartTime();
    this.setNodeUnhealthy(false);

    LOGGER.finest(
        () -> Messages.get("HostMonitorConnectionContext.hostAlive",
            new Object[] {hostName}));
  }

  public ResourceLock getLock() {
    return this.lock;
  }
}
