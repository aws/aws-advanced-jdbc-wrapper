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

package software.amazon.jdbc.plugin.efm.v3;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;

/**
 * Per-connection monitoring state shared between the application thread that executes JDBC calls and the
 * {@link HostMonitorV3Impl} that watches the host.
 *
 * <p>The slot is created once per physical connection and registered with the host monitor once. Each JDBC call then
 * costs the application thread two atomic operations on {@link #inFlightSinceNanos}: a write of the start time on
 * entry and a reset on exit. The monitor only reads that value while the host is healthy and writes it exactly once,
 * through {@link #claim(long)}, when it decides to abort the connection. Because the claim is a compare-and-set against
 * the start time the application thread observed, the two sides can never disagree about whether the call was aborted:
 * either the call finished first (the claim fails and nothing is aborted) or the monitor claimed first (the application
 * thread sees {@link #ABORTED} on exit and marks the host unavailable).
 *
 * <p>The slot holds the connection weakly so that a wrapper that is dropped without being closed cannot pin its
 * physical connection through the monitor.
 */
public final class MonitoredConnectionSlot {

  /** Marker stored in {@link #inFlightSinceNanos} once the monitor has claimed the connection for abort. */
  public static final long ABORTED = -1L;

  /** Marker stored in {@link #inFlightSinceNanos} while no call is in flight. */
  public static final long IDLE = 0L;

  private final WeakReference<Connection> connectionRef;
  private final @NonNull HostSpec hostSpec;
  private final AtomicLong inFlightSinceNanos = new AtomicLong(IDLE);
  private volatile long lastActivityNanos;
  private volatile @Nullable HostMonitorV3Impl monitor;

  // Re-entrancy depth. The wrapper may re-enter the plugin chain for nested network-bound calls; only the outermost
  // call owns the in-flight timestamp. Atomic so that an application sharing a connection across threads (unsupported,
  // but seen in the wild) cannot leave the slot permanently in flight through a lost update.
  private final AtomicInteger depth = new AtomicInteger(0);

  public MonitoredConnectionSlot(final @NonNull Connection connection, final @NonNull HostSpec hostSpec,
      final long nowNanos) {
    this.connectionRef = new WeakReference<>(connection);
    this.hostSpec = hostSpec;
    this.lastActivityNanos = nowNanos;
  }

  public @NonNull HostSpec getHostSpec() {
    return this.hostSpec;
  }

  /**
   * Returns the monitored connection, or null once it has been garbage collected.
   *
   * @return the monitored connection, or null.
   */
  public @Nullable Connection getConnection() {
    return this.connectionRef.get();
  }

  /**
   * Marks the start of a network-bound call.
   *
   * @param nowNanos the current {@link System#nanoTime()}.
   * @return true if the monitor aborted this connection while it was idle, so the caller should mark the host as
   *     unavailable before running the call (which will fail on the aborted connection).
   */
  public boolean enter(final long nowNanos) {
    if (this.depth.getAndIncrement() != 0) {
      return false;
    }
    return this.inFlightSinceNanos.getAndSet(nowNanos) == ABORTED;
  }

  /**
   * Marks the end of a network-bound call.
   *
   * @param nowNanos the current {@link System#nanoTime()}.
   * @return true if the monitor claimed this connection for abort while the call was in flight, so the caller should
   *     mark the host as unavailable.
   */
  public boolean exit(final long nowNanos) {
    if (this.depth.decrementAndGet() != 0) {
      return false;
    }
    final long previous = this.inFlightSinceNanos.getAndSet(IDLE);
    this.lastActivityNanos = nowNanos;
    return previous == ABORTED;
  }

  /**
   * Returns the start time of the call currently in flight, {@link #IDLE} if none, or {@link #ABORTED} if the monitor
   * has claimed the connection and the application thread has not yet observed it.
   *
   * @return the in-flight marker.
   */
  public long getInFlightSinceNanos() {
    return this.inFlightSinceNanos.get();
  }

  /**
   * Claims the connection for abort. Succeeds only if the slot is still in the state the monitor observed, so a call
   * that completed in the meantime is never reported as aborted.
   *
   * @param expectedInFlightSinceNanos the value previously returned by {@link #getInFlightSinceNanos()}, either a call
   *                                   start time or {@link #IDLE}.
   * @return true if the claim succeeded and the caller now owns the abort.
   */
  public boolean claim(final long expectedInFlightSinceNanos) {
    return expectedInFlightSinceNanos != ABORTED
        && this.inFlightSinceNanos.compareAndSet(expectedInFlightSinceNanos, ABORTED);
  }

  public long getLastActivityNanos() {
    return this.lastActivityNanos;
  }

  public @Nullable HostMonitorV3Impl getMonitor() {
    return this.monitor;
  }

  void setMonitor(final @Nullable HostMonitorV3Impl monitor) {
    this.monitor = monitor;
  }

  @Override
  public String toString() {
    return "MonitoredConnectionSlot [host=" + this.hostSpec.getHost()
        + ", inFlightSinceNanos=" + this.inFlightSinceNanos.get()
        + ", connection=" + this.connectionRef.get() + "]";
  }
}
