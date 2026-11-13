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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.EventSubscriber;
import software.amazon.jdbc.util.events.MonitorResetEvent;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

/**
 * Watches one database host on behalf of every connection registered with it, on a single background thread.
 *
 * <p>Connections register a {@link MonitoredConnectionSlot} once; the monitor never allocates or synchronizes per JDBC
 * call. Each iteration it scans the registered slots, and only when some call has been in flight for longer than
 * {@code failureDetectionTime} does it probe the host over its own monitoring connection. After
 * {@code failureDetectionCount} consecutive failed probes the host is unhealthy and every call that has outlived the
 * grace period is aborted. Memory and CPU scale with the number of registered connections, not with call throughput.
 */
public class HostMonitorV3Impl extends AbstractMonitor implements EventSubscriber, StateSnapshotProvider {

  private static final Logger LOGGER = Logger.getLogger(HostMonitorV3Impl.class.getName());
  private static final String MONITORING_THREAD_NAME = "efm3-monitor";
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";
  private static final long MIN_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
  private static final long MAX_SCAN_TICK_NANOS = TimeUnit.SECONDS.toNanos(1);
  private static final int MIN_VALIDITY_CHECK_TIMEOUT_SEC = 1;
  private static final long MIN_TERMINATION_TIMEOUT_SEC = 1;

  // Aborting a connection may touch a dead socket. A shared cached pool keeps those calls off the monitor thread of
  // the host, and its idle threads expire on their own, so no thread outlives the monitors that used it.
  protected static final Executor ABORT_EXECUTOR = ExecutorFactory.newCachedThreadPool("efm3-abort");

  protected final FullServicesContainer servicesContainer;
  protected final TelemetryFactory telemetryFactory;
  protected final HostSpec hostSpec;
  protected final Properties monitoringProperties;
  protected final AtomicConnection monitoringConn;

  private final long failureDetectionTimeNanos;
  private final long failureDetectionIntervalNanos;
  private final int failureDetectionCount;
  private final long slotDisposalTimeNanos;
  private final long scanTickNanos;
  private final boolean abortIdleConnections;

  private final Set<MonitoredConnectionSlot> slots = ConcurrentHashMap.newKeySet();
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final AtomicBoolean hostUnhealthy = new AtomicBoolean(false);

  private final @Nullable TelemetryCounter abortedConnectionsCounter;
  private final @Nullable TelemetryCounter hostUnhealthyCounter;

  /**
   * Creates a monitor for one host.
   *
   * @param servicesContainer              the services container to open the monitoring connection with.
   * @param hostSpec                       the host to monitor.
   * @param properties                     the properties of the connection that created this monitor; the
   *                                       {@code monitoring-} prefixed ones configure the monitoring connection.
   * @param failureDetectionTimeMillis     grace period before a call in flight makes the monitor probe the host.
   * @param failureDetectionIntervalMillis interval between probes.
   * @param failureDetectionCount          consecutive failed probes before the host is unhealthy.
   * @param monitorDisposalTimeMillis      idle time after which a registered connection is forgotten.
   * @param abortIdleConnections           whether idle registered connections are aborted when the host is unhealthy.
   * @param abortedConnectionsCounter      telemetry counter for aborted connections, shared across hosts.
   */
  // AtomicConnection registers 'this' as a GC cleanup referent only; it does not access any
  // not-yet-initialized state during construction, so passing the under-initialization receiver
  // is safe. The checker cannot see this, hence the localized suppression.
  @SuppressWarnings({"argument", "assignment"})
  public HostMonitorV3Impl(
      final @NonNull FullServicesContainer servicesContainer,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties properties,
      final int failureDetectionTimeMillis,
      final int failureDetectionIntervalMillis,
      final int failureDetectionCount,
      final long monitorDisposalTimeMillis,
      final boolean abortIdleConnections,
      final @Nullable TelemetryCounter abortedConnectionsCounter) {
    // The probe is bounded by the monitoring timeouts below, so the monitor can always wind down within two intervals.
    super(
        Math.max(MIN_TERMINATION_TIMEOUT_SEC, TimeUnit.MILLISECONDS.toSeconds(2L * failureDetectionIntervalMillis)),
        ExecutorFactory.newSingleThreadExecutor(MONITORING_THREAD_NAME));
    this.servicesContainer = servicesContainer;
    this.telemetryFactory = servicesContainer.getTelemetryFactory();
    this.hostSpec = hostSpec;
    this.failureDetectionTimeNanos = TimeUnit.MILLISECONDS.toNanos(failureDetectionTimeMillis);
    this.failureDetectionIntervalNanos = TimeUnit.MILLISECONDS.toNanos(failureDetectionIntervalMillis);
    this.failureDetectionCount = Math.max(1, failureDetectionCount);
    this.slotDisposalTimeNanos = TimeUnit.MILLISECONDS.toNanos(monitorDisposalTimeMillis);
    this.scanTickNanos = Math.max(MIN_SLEEP_NANOS, Math.min(MAX_SCAN_TICK_NANOS, this.failureDetectionTimeNanos / 10));
    this.abortIdleConnections = abortIdleConnections;
    this.abortedConnectionsCounter = abortedConnectionsCounter;
    this.monitoringConn = new AtomicConnection(
        this, PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.getBoolean(properties));
    this.monitoringProperties = buildMonitoringProperties(
        properties,
        failureDetectionIntervalMillis,
        servicesContainer.getPluginService().getTargetDriverDialect(),
        hostSpec.getHost());

    final String nodeId = StringUtils.isNullOrEmpty(this.hostSpec.getHostId())
        ? this.hostSpec.getHost()
        : this.hostSpec.getHostId();
    this.hostUnhealthyCounter = this.telemetryFactory.createCounter(
        String.format("efm3.hostUnhealthy.count.%s", nodeId));
  }

  /**
   * Derives the monitoring connection properties: the connection's properties minus host-selection restrictions, with
   * every {@code monitoring-} prefixed property applied on top. The connect and socket timeouts are taken only from the
   * {@code monitoring-} prefixed properties; when absent they default to {@code failureDetectionInterval} so that a
   * probe against an unreachable host can never block the monitor thread.
   */
  protected static Properties buildMonitoringProperties(
      final @NonNull Properties properties,
      final int failureDetectionIntervalMillis,
      final @Nullable TargetDriverDialect targetDriverDialect,
      final String host) {
    final Properties monitoringProps = PropertyUtils.copyProperties(properties);

    // Drop inherited restrictions before the monitoring overrides are applied, so an explicit
    // 'monitoring-' value below still takes effect. This monitor watches one specific node, which may be
    // a reader, so a target driver setting that rejects read-only servers would stop it connecting.
    PropertyUtils.removeHostSelectionProperties(monitoringProps, targetDriverDialect);

    final String intervalMillis = String.valueOf(failureDetectionIntervalMillis);
    PropertyDefinition.CONNECT_TIMEOUT.set(monitoringProps, intervalMillis);
    PropertyDefinition.SOCKET_TIMEOUT.set(monitoringProps, intervalMillis);

    for (final String name : properties.stringPropertyNames()) {
      if (!name.startsWith(MONITORING_PROPERTY_PREFIX)) {
        continue;
      }
      final String value = properties.getProperty(name);
      monitoringProps.remove(name);
      if (value != null) {
        monitoringProps.put(name.substring(MONITORING_PROPERTY_PREFIX.length()), value);
      }
    }

    LOGGER.finest(() -> Messages.get(
        "HostMonitorV3Impl.monitoringTimeouts",
        new Object[] {
            host,
            PropertyDefinition.CONNECT_TIMEOUT.getString(monitoringProps),
            PropertyDefinition.SOCKET_TIMEOUT.getString(monitoringProps)}));
    return monitoringProps;
  }

  /**
   * Registers a connection slot with this monitor.
   *
   * @param slot the slot to register.
   * @return false if this monitor has already stopped and the caller must obtain a fresh one.
   */
  public boolean register(final @NonNull MonitoredConnectionSlot slot) {
    if (this.stop.get()) {
      return false;
    }
    this.slots.add(slot);
    slot.setMonitor(this);
    return true;
  }

  public void unregister(final @NonNull MonitoredConnectionSlot slot) {
    this.slots.remove(slot);
    if (slot.getMonitor() == this) {
      slot.setMonitor(null);
    }
  }

  public boolean isStopped() {
    return this.stop.get();
  }

  public boolean isHostUnhealthy() {
    return this.hostUnhealthy.get();
  }

  public int getRegisteredSlotCount() {
    return this.slots.size();
  }

  @Override
  public boolean canDispose() {
    return this.slots.isEmpty();
  }

  @Override
  public void monitor() {
    LOGGER.finest(() -> Messages.get("HostMonitorImpl.startMonitoringThread", new Object[] {this.hostSpec.getHost()}));

    try {
      this.servicesContainer.getEventPublisher().subscribe(this, Collections.singleton(MonitorResetEvent.class));

      while (!this.stop.get()) {
        try {
          this.monitorIteration();
        } catch (final InterruptedException intEx) {
          Thread.currentThread().interrupt();
          break;
        } catch (final Throwable throwable) {
          // A failed iteration must not terminate this thread; otherwise failure detection silently
          // stops for this host. Log the error and keep monitoring.
          LOGGER.log(
              Level.WARNING,
              Messages.get("HostMonitorImpl.exceptionDuringMonitoringContinue", new Object[] {this.hostSpec.getHost()}),
              throwable);
          this.sleepNanos(MIN_SLEEP_NANOS);
        }
      }
    } catch (final InterruptedException intEx) {
      Thread.currentThread().interrupt();
    } finally {
      this.stop.set(true);
      this.monitoringConn.clean();
      this.servicesContainer.getEventPublisher().unsubscribe(this, Collections.singleton(MonitorResetEvent.class));
    }

    LOGGER.finest(() -> Messages.get("HostMonitorImpl.stopMonitoringThread", new Object[] {this.hostSpec.getHost()}));
  }

  /**
   * One monitoring iteration: sweep stale slots, decide whether a probe is needed, probe, update host health, and abort
   * the connections that qualify.
   *
   * @throws InterruptedException if the thread is interrupted while sleeping.
   */
  protected void monitorIteration() throws InterruptedException {
    final long now = this.getCurrentTimeNano();
    this.lastActivityTimestampNanos.set(now);

    this.sweepSlots(now);

    if (!this.hostUnhealthy.get() && !this.hasDueSlot(now)) {
      // Nothing has been in flight long enough to justify a probe. Scan again shortly; a probe is not needed until the
      // grace period of some call has elapsed, so a coarse tick does not delay detection.
      this.sleepNanos(this.scanTickNanos);
      return;
    }

    final long probeStart = this.getCurrentTimeNano();
    final boolean valid = this.checkConnectionStatus();
    final long probeEnd = this.getCurrentTimeNano();
    this.lastActivityTimestampNanos.set(probeEnd);

    this.updateHostHealth(valid);

    if (this.hostUnhealthy.get()) {
      this.abortConnections(probeEnd);
    }

    this.sleepNanos(Math.max(MIN_SLEEP_NANOS, this.failureDetectionIntervalNanos - (probeEnd - probeStart)));
  }

  /**
   * Drops slots whose connection has been garbage collected or which have been idle longer than the disposal time.
   * A slot that starts a call concurrently with its eviction is put back: the application thread publishes its
   * in-flight timestamp before it checks its registration, and this method clears the registration before it
   * re-reads the timestamp, so at least one side notices the other.
   */
  protected void sweepSlots(final long now) {
    for (final MonitoredConnectionSlot slot : this.slots) {
      final Connection connection = slot.getConnection();
      final long inFlightSince = slot.getInFlightSinceNanos();
      final boolean idle = inFlightSince == MonitoredConnectionSlot.IDLE
          || inFlightSince == MonitoredConnectionSlot.ABORTED;
      if (connection == null
          || (idle && now - slot.getLastActivityNanos() >= this.slotDisposalTimeNanos)) {
        this.unregister(slot);
        if (connection != null && slot.getInFlightSinceNanos() > MonitoredConnectionSlot.IDLE) {
          this.register(slot);
        }
      }
    }
  }

  protected boolean hasDueSlot(final long now) {
    for (final MonitoredConnectionSlot slot : this.slots) {
      final long inFlightSince = slot.getInFlightSinceNanos();
      if (inFlightSince > MonitoredConnectionSlot.IDLE && now - inFlightSince >= this.failureDetectionTimeNanos) {
        return true;
      }
    }
    return false;
  }

  protected void updateHostHealth(final boolean valid) {
    if (!valid) {
      final int failures = this.consecutiveFailures.incrementAndGet();
      if (this.hostUnhealthyCounter != null) {
        this.hostUnhealthyCounter.inc();
      }
      if (failures >= this.failureDetectionCount) {
        if (this.hostUnhealthy.compareAndSet(false, true)) {
          LOGGER.fine(() -> Messages.get(
              "HostMonitorConnectionContext.hostDead", new Object[] {this.hostSpec.getHost()}));
        }
      } else {
        LOGGER.finest(() -> Messages.get(
            "HostMonitorConnectionContext.hostNotResponding", new Object[] {this.hostSpec.getHost(), failures}));
      }
      return;
    }

    if (this.consecutiveFailures.getAndSet(0) > 0) {
      LOGGER.finest(() -> Messages.get(
          "HostMonitorConnectionContext.hostAlive", new Object[] {this.hostSpec.getHost()}));
    }
    this.hostUnhealthy.set(false);
  }

  /**
   * Aborts every registered connection whose call has outlived the grace period, and, when configured, every idle one.
   * Each connection is claimed with a compare-and-set first, so a call that completed in the meantime is left alone and
   * no connection is aborted twice.
   */
  protected void abortConnections(final long now) {
    for (final MonitoredConnectionSlot slot : this.slots) {
      final long inFlightSince = slot.getInFlightSinceNanos();
      if (inFlightSince == MonitoredConnectionSlot.ABORTED) {
        continue;
      }
      if (inFlightSince == MonitoredConnectionSlot.IDLE) {
        if (!this.abortIdleConnections) {
          continue;
        }
      } else if (now - inFlightSince < this.failureDetectionTimeNanos) {
        continue;
      }

      if (slot.claim(inFlightSince)) {
        final Connection connection = slot.getConnection();
        if (connection != null) {
          this.abortConnection(connection);
        }
      }
    }
  }

  protected void abortConnection(final @NonNull Connection connectionToAbort) {
    try {
      this.servicesContainer.getPluginService().getTargetDriverDialect()
          .abortConnection(connectionToAbort, ABORT_EXECUTOR);
      if (this.abortedConnectionsCounter != null) {
        this.abortedConnectionsCounter.inc();
      }
    } catch (final SQLException sqlEx) {
      LOGGER.finest(() -> Messages.get(
          "HostMonitorConnectionContext.exceptionAbortingConnection", new Object[] {sqlEx.getMessage()}));
    }
  }

  /**
   * Probes the host over the monitoring connection, opening it first if needed. Opening the connection counts as a
   * successful probe. Both the connect and the validity check are bounded by the monitoring timeouts.
   *
   * @return true if the host answered.
   */
  protected boolean checkConnectionStatus() {
    final TelemetryContext connectContext = this.telemetryFactory.openTelemetryContext(
        "connection status check", TelemetryTraceLevel.FORCE_TOP_LEVEL);
    if (connectContext != null) {
      connectContext.setAttribute("url", this.hostSpec.getHost());
    }

    try {
      final Connection current = this.monitoringConn.get();
      if (current == null || current.isClosed()) {
        LOGGER.finest(() -> Messages.get(
            "HostMonitorV3Impl.openingMonitoringConnection", new Object[] {this.hostSpec.getUrl()}));
        this.monitoringConn.set(
            this.servicesContainer.getPluginService().forceConnect(this.hostSpec, this.monitoringProperties));
        return true;
      }

      // Some drivers, like MySQL Connector/J, take up to twice the requested isValid() timeout.
      final int validTimeoutSec = (int) TimeUnit.NANOSECONDS.toSeconds(this.failureDetectionIntervalNanos) / 2;
      return current.isValid(Math.max(MIN_VALIDITY_CHECK_TIMEOUT_SEC, validTimeoutSec));
    } catch (final SQLException sqlEx) {
      return false;
    } finally {
      if (connectContext != null) {
        connectContext.closeContext();
      }
    }
  }

  @Override
  public void close() {
    this.monitoringConn.clean();
  }

  protected void reset() {
    LOGGER.finest(() -> Messages.get("HostMonitorV3Impl.reset", new Object[] {this.hostSpec.getHost()}));
    this.monitoringConn.set(null);
    this.consecutiveFailures.set(0);
    this.hostUnhealthy.set(false);
  }

  @Override
  public void processEvent(final Event event) {
    if (event instanceof MonitorResetEvent) {
      final MonitorResetEvent resetEvent = (MonitorResetEvent) event;
      if (resetEvent.getEndpoints().contains(this.hostSpec.getHost())) {
        this.reset();
      }
    }
  }

  // These two methods exist so that unit tests can control time and skip real sleeps.
  protected long getCurrentTimeNano() {
    return System.nanoTime();
  }

  protected void sleepNanos(final long nanos) throws InterruptedException {
    TimeUnit.NANOSECONDS.sleep(nanos);
  }

  // Checker Framework: snapshot values are intentionally nullable, but the StateSnapshotProvider
  // contract types them as Pair<String, Object> (non-null Object). Widening that interface across
  // all implementers is out of scope, so the nullable value inference is suppressed locally.
  @Override
  @SuppressWarnings("type.arguments.not.inferred")
  public List<Pair<String, Object>> getSnapshotState() {
    final List<Pair<String, Object>> state = new ArrayList<>();
    state.add(Pair.create("hostSpec", this.hostSpec.toString()));
    PropertyUtils.addSnapshotState(state, "monitoringProperties", this.monitoringProperties);
    state.add(Pair.create("monitoringConn", this.monitoringConn.get()));
    state.add(Pair.create("failureDetectionTimeNanos", this.failureDetectionTimeNanos));
    state.add(Pair.create("failureDetectionIntervalNanos", this.failureDetectionIntervalNanos));
    state.add(Pair.create("failureDetectionCount", this.failureDetectionCount));
    state.add(Pair.create("abortIdleConnections", this.abortIdleConnections));
    state.add(Pair.create("consecutiveFailures", this.consecutiveFailures.get()));
    state.add(Pair.create("hostUnhealthy", this.hostUnhealthy.get()));
    state.add(Pair.create("registeredSlots", this.slots.size()));
    int inFlight = 0;
    for (final MonitoredConnectionSlot slot : this.slots) {
      if (slot.getInFlightSinceNanos() > MonitoredConnectionSlot.IDLE) {
        inFlight++;
      }
    }
    state.add(Pair.create("inFlightSlots", inFlight));
    return state;
  }

  @Override
  public String toString() {
    return "HostMonitorV3Impl [host=" + this.hostSpec.getHost() + ", slots=" + this.slots.size() + "]";
  }
}
