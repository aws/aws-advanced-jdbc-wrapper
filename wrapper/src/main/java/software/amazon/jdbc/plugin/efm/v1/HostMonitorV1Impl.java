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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.efm.base.HostMonitor;
import software.amazon.jdbc.plugin.efm.base.HostMonitorConnectionContext;
import software.amazon.jdbc.plugin.efm.v2.HostMonitorV2Impl;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.MonitorResetEvent;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class HostMonitorV1Impl extends AbstractMonitor implements HostMonitor, StateSnapshotProvider {

  private static final Logger LOGGER = Logger.getLogger(HostMonitorV1Impl.class.getName());
  private static final String MONITORING_THREAD_NAME_PREFIX = "efm-monitor";
  private static final long TERMINATION_TIMEOUT_SEC = 30;
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  private static final long THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final long MIN_CONNECTION_CHECK_TIMEOUT_MILLIS = 100;


  protected final FullServicesContainer servicesContainer;
  protected final TelemetryFactory telemetryFactory;
  protected final Properties properties;
  protected final Properties monitoringProperties;
  protected final HostSpec hostSpec;
  protected AtomicConnection monitoringConn;

  private final Queue<HostMonitorConnectionContextV1> newContexts = new ConcurrentLinkedQueue<>();
  private final Queue<HostMonitorConnectionContextV1> activeContexts = new ConcurrentLinkedQueue<>();
  private volatile long nodeCheckTimeoutMillis = MIN_CONNECTION_CHECK_TIMEOUT_MILLIS;
  private final TelemetryCounter nodeInvalidCounter;

  /**
   * Store the monitoring configuration for a connection.
   *
   * @param servicesContainer          The telemetry factory to use to create telemetry data.
   * @param hostSpec                  The {@link HostSpec} of the server this {@link HostMonitorV2Impl}
   *                                  instance is monitoring.
   * @param properties                The {@link Properties} containing additional monitoring
   *                                  configuration.
   */
  public HostMonitorV1Impl(
      final @NonNull FullServicesContainer servicesContainer,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties properties) {
    super(TERMINATION_TIMEOUT_SEC, ExecutorFactory.newFixedThreadPool(1, MONITORING_THREAD_NAME_PREFIX));
    this.servicesContainer = servicesContainer;
    this.telemetryFactory = servicesContainer.getTelemetryFactory();
    this.hostSpec = hostSpec;
    this.properties = properties;
    this.monitoringConn = new AtomicConnection(
        this, PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.getBoolean(properties));

    this.monitoringProperties = PropertyUtils.copyProperties(this.properties);
    this.properties.stringPropertyNames().stream()
        .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
        .forEach(
            p -> {
              this.monitoringProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  this.properties.getProperty(p));
              this.monitoringProperties.remove(p);
            });

    final String nodeId = StringUtils.isNullOrEmpty(this.hostSpec.getHostId())
        ? this.hostSpec.getHost()
        : this.hostSpec.getHostId();
    this.nodeInvalidCounter = telemetryFactory.createCounter(String.format("efm.nodeUnhealthy.count.%s", nodeId));
  }

  @Override
  public void start() {
    this.monitorExecutor.submit(this);
    this.monitorExecutor.shutdown(); // No more tasks are accepted by pool.
  }

  @Override
  public void startMonitoring(final HostMonitorConnectionContext context) {
    if (this.stop.get()) {
      LOGGER.warning(() -> Messages.get("HostMonitorImpl.monitorIsStopped", new Object[] {this.hostSpec.getHost()}));
    }

    assert context instanceof HostMonitorConnectionContextV1;

    final long currentTimeNano = this.getCurrentTimeNano();
    this.lastActivityTimestampNanos.set(currentTimeNano);
    ((HostMonitorConnectionContextV1) context).setStartMonitorTimeNano(currentTimeNano);
    this.newContexts.add((HostMonitorConnectionContextV1) context);
  }

  @Override
  public void monitor() {

    LOGGER.finest(() -> Messages.get(
        "HostMonitorImpl.startMonitoringThread",
        new Object[] {this.hostSpec.getHost()}));

    try {

      this.servicesContainer.getEventPublisher().subscribe(
          this, Collections.singleton(MonitorResetEvent.class));

      while (!this.stop.get()) {

        // process new contexts
        HostMonitorConnectionContextV1 newMonitorContext;
        HostMonitorConnectionContextV1 firstAddedNewMonitorContext = null;
        final long currentTimeNano = this.getCurrentTimeNano();

        while ((newMonitorContext = this.newContexts.poll()) != null) {
          if (this.stop.get()) {
            break;
          }
          if (firstAddedNewMonitorContext == newMonitorContext) {
            // This context has already been processed.
            // Add it back to the queue and process it in the next round.
            this.newContexts.add(newMonitorContext);
            break;
          }
          if (newMonitorContext.isActiveContext()) {
            if (newMonitorContext.getExpectedActiveMonitoringStartTimeNano() > currentTimeNano) {
              // The context active monitoring time hasn't come.
              // Add the context to the queue and check it later.
              this.newContexts.add(newMonitorContext);
              if (firstAddedNewMonitorContext == null) {
                firstAddedNewMonitorContext = newMonitorContext;
              }
            } else {
              // It's time to start actively monitor this context.
              this.activeContexts.add(newMonitorContext);
            }
          }
        }

        final Connection copyConnection = this.monitoringConn.get();
        if (!this.activeContexts.isEmpty()
            || copyConnection == null
            || copyConnection.isClosed()) {

          final long statusCheckStartTimeNano = this.getCurrentTimeNano();
          final boolean isValid = this.checkConnectionStatus(this.nodeCheckTimeoutMillis);
          final long statusCheckEndTimeNano = this.getCurrentTimeNano();
          final long elapsedTimeNano = statusCheckEndTimeNano - statusCheckStartTimeNano;
          this.lastActivityTimestampNanos.set(statusCheckEndTimeNano);

          long delayMillis = -1;
          HostMonitorConnectionContextV1 monitorContext;
          HostMonitorConnectionContextV1 firstAddedMonitorContext = null;

          while ((monitorContext = this.activeContexts.poll()) != null) {
            if (this.stop.get()) {
              break;
            }
            monitorContext.getLock().lock();
            try {
              // If context is already invalid, just skip it
              if (!monitorContext.isActiveContext()) {
                continue;
              }

              if (firstAddedMonitorContext == monitorContext) {
                // this context has already been processed by this loop
                // add it to the queue and exit this loop
                this.activeContexts.add(monitorContext);
                break;
              }

              // otherwise, process this context
              monitorContext.updateConnectionStatus(
                  this.hostSpec.getUrl(),
                  statusCheckStartTimeNano,
                  statusCheckEndTimeNano,
                  isValid);

              // If context is still valid and node is still healthy, it needs to continue updating this context
              if (monitorContext.isActiveContext() && !monitorContext.isNodeUnhealthy()) {
                this.activeContexts.add(monitorContext);
                if (firstAddedMonitorContext == null) {
                  firstAddedMonitorContext = monitorContext;
                }

                if (delayMillis == -1 || delayMillis > monitorContext.getFailureDetectionIntervalMillis()) {
                  delayMillis = monitorContext.getFailureDetectionIntervalMillis();
                }
              }
            } finally {
              monitorContext.getLock().unlock();
            }
          }

          if (delayMillis == -1) {
            // No active contexts
            delayMillis = THREAD_SLEEP_WHEN_INACTIVE_MILLIS;
          } else {
            delayMillis -= TimeUnit.NANOSECONDS.toMillis(elapsedTimeNano);
            // Check for min delay between node health check
            if (delayMillis <= MIN_CONNECTION_CHECK_TIMEOUT_MILLIS) {
              delayMillis = MIN_CONNECTION_CHECK_TIMEOUT_MILLIS;
            }
            // Use this delay as node checkout timeout since it corresponds to min interval for all active contexts
            this.nodeCheckTimeoutMillis = delayMillis;
          }

          TimeUnit.MILLISECONDS.sleep(delayMillis);

        } else {
          TimeUnit.MILLISECONDS.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
        }
      }
    } catch (final InterruptedException intEx) {
      Thread.currentThread().interrupt();
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "HostMonitorImpl.exceptionDuringMonitoringStop",
                new Object[] {this.hostSpec.getHost()}),
            ex); // We want to print full trace stack of the exception.
      }
    } finally {
      this.stop.set(true);
      this.monitoringConn.clean();
      this.servicesContainer.getEventPublisher().unsubscribe(
          this, Collections.singleton(MonitorResetEvent.class));
    }

    LOGGER.finest(() -> Messages.get(
        "HostMonitorImpl.stopMonitoringThread",
        new Object[] {this.hostSpec.getHost()}));
  }

  /**
   * Check the status of the monitored server by sending a ping.
   *
   * @param shortestFailureDetectionIntervalMillis The shortest failure detection interval used by
   *                                               all the connections to this server. This value is
   *                                               used as the maximum time to wait for a response
   *                                               from the server.
   * @return whether the server is still alive.
   */
  boolean checkConnectionStatus(final long shortestFailureDetectionIntervalMillis) {
    TelemetryContext connectContext = telemetryFactory.openTelemetryContext(
        "connection status check", TelemetryTraceLevel.FORCE_TOP_LEVEL);

    if (connectContext != null) {
      connectContext.setAttribute("url", hostSpec.getHost());
    }

    try {
      final Connection copyConnection = this.monitoringConn.get();
      if (copyConnection == null || copyConnection.isClosed()) {
        // open a new connection
        LOGGER.finest(() -> "Opening a monitoring connection to " + this.hostSpec.getUrl());
        this.monitoringConn.set(
            this.servicesContainer.getPluginService().forceConnect(this.hostSpec, this.monitoringProperties));
        LOGGER.finest(() -> "Opened monitoring connection: " + this.monitoringConn.get());
        return true;
      }

      final Connection copyConnection2 = this.monitoringConn.get();
      // Some drivers, like MySQL Connector/J, execute isValid() in a double of specified timeout time.
      // TODO: fix me. Need to find a better solution to double timeout issue.
      final boolean isValid = copyConnection2 != null && copyConnection2.isValid(
          (int) TimeUnit.MILLISECONDS.toSeconds(shortestFailureDetectionIntervalMillis) / 2);
      if (!isValid) {
        if (this.nodeInvalidCounter != null) {
          this.nodeInvalidCounter.inc();
        }
      }
      return isValid;

    } catch (final SQLException sqlEx) {
      if (this.nodeInvalidCounter != null) {
        this.nodeInvalidCounter.inc();
      }
      return false;

    } finally {
      if (connectContext != null) {
        connectContext.closeContext();
      }
    }
  }

  // This method helps to organize unit tests.
  protected long getCurrentTimeNano() {
    return System.nanoTime();
  }

  protected void reset() {
    LOGGER.finest("Reset: " + this.hostSpec.getHost());
    this.monitoringConn.set(null);
  }

  @Override
  public void processEvent(Event event) {
    if (event instanceof MonitorResetEvent) {
      LOGGER.finest("MonitorResetEvent received");
      final MonitorResetEvent resetEvent = (MonitorResetEvent) event;
      if (resetEvent.getEndpoints().contains(this.hostSpec.getHost())) {
        this.reset();
      }
    }
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    List<Pair<String, Object>> state = new ArrayList<>();
    state.add(Pair.create("hostSpec", this.hostSpec != null ? this.hostSpec.toString() : null));
    PropertyUtils.addSnapshotState(state, "monitoringProperties", this.monitoringProperties);
    state.add(Pair.create("monitoringConn", this.monitoringConn != null ? this.monitoringConn.get() : null));
    state.add(Pair.create("nodeCheckTimeoutMillis", this.nodeCheckTimeoutMillis));
    state.add(Pair.create("activeContexts (size)", this.activeContexts.size()));
    state.add(Pair.create("newContexts (size)", this.newContexts.size()));
    return state;
  }
}
