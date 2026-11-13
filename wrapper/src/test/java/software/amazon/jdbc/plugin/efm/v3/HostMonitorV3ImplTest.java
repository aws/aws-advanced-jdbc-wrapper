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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.events.MonitorResetEvent;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class HostMonitorV3ImplTest {

  private static final int FAILURE_DETECTION_TIME_MILLIS = 30000;
  private static final int FAILURE_DETECTION_INTERVAL_MILLIS = 5000;
  private static final int FAILURE_DETECTION_COUNT = 3;
  private static final long DISPOSAL_TIME_MILLIS = 600000;

  private static final long GRACE_NANOS = TimeUnit.MILLISECONDS.toNanos(FAILURE_DETECTION_TIME_MILLIS);
  private static final long T0 = 1_000_000_000_000L;

  @Mock PluginService pluginService;
  @Mock FullServicesContainer servicesContainer;
  @Mock TargetDriverDialect targetDriverDialect;
  @Mock Connection monitoringConnection;
  @Mock Connection appConnection;
  @Mock Connection otherAppConnection;
  @Mock HostSpec hostSpec;
  @Mock TelemetryFactory telemetryFactory;
  @Mock TelemetryContext telemetryContext;
  @Mock TelemetryCounter telemetryCounter;
  @Mock TelemetryCounter abortedConnectionsCounter;
  @Mock EventPublisher eventPublisher;

  private AutoCloseable closeable;
  private final AtomicLong clock = new AtomicLong(T0);
  private final Properties properties = new Properties();
  private HostMonitorV3Impl monitor;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(pluginService.forceConnect(any(HostSpec.class), any(Properties.class))).thenReturn(monitoringConnection);
    when(pluginService.getTargetDriverDialect()).thenReturn(targetDriverDialect);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(servicesContainer.getEventPublisher()).thenReturn(eventPublisher);
    when(telemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(telemetryContext);
    when(telemetryFactory.createCounter(anyString())).thenReturn(telemetryCounter);
    when(hostSpec.getHost()).thenReturn("test-host");
    when(hostSpec.getUrl()).thenReturn("test-host:5432/");
  }

  @AfterEach
  void cleanUp() throws Exception {
    if (monitor != null) {
      monitor.stop();
    }
    closeable.close();
  }

  private HostMonitorV3Impl newMonitor(final boolean abortIdleConnections) throws Exception {
    final HostMonitorV3Impl created = spy(new HostMonitorV3Impl(
        servicesContainer,
        hostSpec,
        properties,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT,
        DISPOSAL_TIME_MILLIS,
        abortIdleConnections,
        abortedConnectionsCounter));
    doAnswer(inv -> clock.get()).when(created).getCurrentTimeNano();
    doNothing().when(created).sleepNanos(anyLong());
    this.monitor = created;
    return created;
  }

  private MonitoredConnectionSlot registeredSlot(final Connection connection) {
    final MonitoredConnectionSlot slot = new MonitoredConnectionSlot(connection, hostSpec, clock.get());
    assertTrue(monitor.register(slot));
    return slot;
  }

  private void advance(final long nanos) {
    clock.addAndGet(nanos);
  }

  @Test
  void noProbe_whileNoCallHasOutlivedTheGracePeriod() throws Exception {
    newMonitor(false);
    final MonitoredConnectionSlot slot = registeredSlot(appConnection);
    slot.enter(clock.get());

    advance(GRACE_NANOS - 1);
    monitor.monitorIteration();

    verify(monitor, never()).checkConnectionStatus();
    assertFalse(monitor.isHostUnhealthy());
  }

  @Test
  void probe_onceACallOutlivesTheGracePeriod() throws Exception {
    newMonitor(false);
    doReturn(true).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot slot = registeredSlot(appConnection);
    slot.enter(clock.get());

    advance(GRACE_NANOS);
    monitor.monitorIteration();

    verify(monitor, times(1)).checkConnectionStatus();
    assertFalse(monitor.isHostUnhealthy());
    assertEquals(T0, slot.getInFlightSinceNanos(), "a healthy probe must leave the call untouched");
  }

  @Test
  void hostUnhealthyAfterCountFailures_abortsOnlyTheInFlightCall() throws Exception {
    newMonitor(false);
    doReturn(false).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot inFlight = registeredSlot(appConnection);
    final MonitoredConnectionSlot idle = registeredSlot(otherAppConnection);
    inFlight.enter(clock.get());
    advance(GRACE_NANOS);

    for (int i = 0; i < FAILURE_DETECTION_COUNT - 1; i++) {
      monitor.monitorIteration();
      assertFalse(monitor.isHostUnhealthy(), "host must stay healthy before failureDetectionCount failures");
      verify(targetDriverDialect, never()).abortConnection(any(Connection.class), any(Executor.class));
    }

    monitor.monitorIteration();

    assertTrue(monitor.isHostUnhealthy());
    verify(targetDriverDialect, times(1)).abortConnection(eq(appConnection), any(Executor.class));
    verify(targetDriverDialect, never()).abortConnection(eq(otherAppConnection), any(Executor.class));
    verify(abortedConnectionsCounter, times(1)).inc();
    assertEquals(MonitoredConnectionSlot.ABORTED, inFlight.getInFlightSinceNanos());
    assertEquals(MonitoredConnectionSlot.IDLE, idle.getInFlightSinceNanos());
    assertTrue(inFlight.exit(clock.get()), "the application thread must observe the abort");

    // A further unhealthy iteration must not abort the same connection again.
    monitor.monitorIteration();
    verify(targetDriverDialect, times(1)).abortConnection(any(Connection.class), any(Executor.class));
  }

  @Test
  void callThatCompletedBeforeTheVerdict_isNotAborted() throws Exception {
    newMonitor(false);
    doReturn(false).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot completing = registeredSlot(appConnection);
    final MonitoredConnectionSlot stillRunning = registeredSlot(otherAppConnection);
    completing.enter(clock.get());
    stillRunning.enter(clock.get());
    advance(GRACE_NANOS);

    monitor.monitorIteration();
    monitor.monitorIteration();
    assertFalse(completing.exit(clock.get()), "the call completed before any verdict");

    monitor.monitorIteration();

    assertTrue(monitor.isHostUnhealthy());
    verify(targetDriverDialect, never()).abortConnection(eq(appConnection), any(Executor.class));
    verify(targetDriverDialect, times(1)).abortConnection(eq(otherAppConnection), any(Executor.class));
    assertEquals(MonitoredConnectionSlot.IDLE, completing.getInFlightSinceNanos());
  }

  @Test
  void youngInFlightCall_isNotAbortedUntilItsGracePeriodElapses() throws Exception {
    newMonitor(false);
    doReturn(false).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot oldCall = registeredSlot(appConnection);
    oldCall.enter(clock.get());
    advance(GRACE_NANOS);
    for (int i = 0; i < FAILURE_DETECTION_COUNT; i++) {
      monitor.monitorIteration();
    }
    assertTrue(monitor.isHostUnhealthy());

    final MonitoredConnectionSlot youngCall = registeredSlot(otherAppConnection);
    youngCall.enter(clock.get());
    monitor.monitorIteration();
    verify(targetDriverDialect, never()).abortConnection(eq(otherAppConnection), any(Executor.class));

    advance(GRACE_NANOS);
    monitor.monitorIteration();
    verify(targetDriverDialect, times(1)).abortConnection(eq(otherAppConnection), any(Executor.class));
  }

  @Test
  void idleConnections_areAbortedOnlyWhenConfigured() throws Exception {
    newMonitor(true);
    doReturn(false).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot inFlight = registeredSlot(appConnection);
    final MonitoredConnectionSlot idle = registeredSlot(otherAppConnection);
    inFlight.enter(clock.get());
    advance(GRACE_NANOS);

    for (int i = 0; i < FAILURE_DETECTION_COUNT; i++) {
      monitor.monitorIteration();
    }

    verify(targetDriverDialect, times(1)).abortConnection(eq(appConnection), any(Executor.class));
    verify(targetDriverDialect, times(1)).abortConnection(eq(otherAppConnection), any(Executor.class));
    assertTrue(idle.enter(clock.get()), "the idle connection's next call must learn about the abort");
  }

  @Test
  void successfulProbe_resetsFailuresAndHealth() throws Exception {
    newMonitor(false);
    doReturn(false, false, true, false, false, false).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot slot = registeredSlot(appConnection);
    slot.enter(clock.get());
    advance(GRACE_NANOS);

    // two failures, one success: the failure streak restarts
    monitor.monitorIteration();
    monitor.monitorIteration();
    monitor.monitorIteration();
    assertFalse(monitor.isHostUnhealthy());

    monitor.monitorIteration();
    monitor.monitorIteration();
    assertFalse(monitor.isHostUnhealthy(), "the streak must have restarted after the successful probe");
    monitor.monitorIteration();
    assertTrue(monitor.isHostUnhealthy());
  }

  @Test
  void sweep_forgetsSlotsIdleLongerThanDisposalTime() throws Exception {
    newMonitor(false);
    final MonitoredConnectionSlot slot = registeredSlot(appConnection);
    slot.enter(clock.get());
    slot.exit(clock.get());
    assertFalse(monitor.canDispose());

    advance(TimeUnit.MILLISECONDS.toNanos(DISPOSAL_TIME_MILLIS) - 1);
    monitor.monitorIteration();
    assertEquals(1, monitor.getRegisteredSlotCount());

    advance(1);
    monitor.monitorIteration();
    assertEquals(0, monitor.getRegisteredSlotCount());
    assertNull(slot.getMonitor(), "a swept slot must re-register lazily on its next call");
    assertTrue(monitor.canDispose());
  }

  @Test
  void sweep_keepsSlotWithACallInFlight() throws Exception {
    newMonitor(false);
    doReturn(true).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot slot = registeredSlot(appConnection);
    slot.enter(clock.get());

    advance(TimeUnit.MILLISECONDS.toNanos(DISPOSAL_TIME_MILLIS) * 2);
    monitor.monitorIteration();

    assertEquals(1, monitor.getRegisteredSlotCount());
    assertSame(monitor, slot.getMonitor());
  }

  @Test
  void monitorIteration_updatesTheHeartbeat() throws Exception {
    newMonitor(false);
    advance(TimeUnit.MINUTES.toNanos(5));
    monitor.monitorIteration();
    assertEquals(clock.get(), monitor.getLastActivityTimestampNanos());
  }

  @Test
  void register_isRefusedOnceStopped() throws Exception {
    newMonitor(false);
    monitor.stop();
    assertTrue(monitor.isStopped());
    assertFalse(monitor.register(new MonitoredConnectionSlot(appConnection, hostSpec, clock.get())));
  }

  @Test
  void monitoringProperties_defaultTimeoutsToInterval_prefixedValuesWin() throws Exception {
    properties.setProperty("connectTimeout", "30000");
    properties.setProperty("socketTimeout", "30000");
    properties.setProperty("monitoring-socketTimeout", "1234");
    properties.setProperty("monitoring-user", "monitor");
    newMonitor(false);

    final Properties monitoringProps = monitor.monitoringProperties;
    assertEquals(String.valueOf(FAILURE_DETECTION_INTERVAL_MILLIS), monitoringProps.getProperty("connectTimeout"),
        "the application's connectTimeout must not be inherited");
    assertEquals("1234", monitoringProps.getProperty("socketTimeout"));
    assertEquals("monitor", monitoringProps.getProperty("user"));
    assertNull(monitoringProps.getProperty("monitoring-socketTimeout"));
    assertNull(monitoringProps.getProperty("monitoring-user"));
  }

  @Test
  void checkConnectionStatus_opensThenValidatesTheMonitoringConnection() throws Exception {
    newMonitor(false);

    assertTrue(monitor.checkConnectionStatus(), "opening the monitoring connection counts as a successful probe");
    verify(pluginService, times(1)).forceConnect(eq(hostSpec), any(Properties.class));

    when(monitoringConnection.isClosed()).thenReturn(false);
    when(monitoringConnection.isValid(anyInt())).thenReturn(true, false);
    assertTrue(monitor.checkConnectionStatus());
    assertFalse(monitor.checkConnectionStatus());
    verify(monitoringConnection, times(2)).isValid(FAILURE_DETECTION_INTERVAL_MILLIS / 1000 / 2);

    when(monitoringConnection.isValid(anyInt())).thenThrow(new SQLException("boom"));
    assertFalse(monitor.checkConnectionStatus());
  }

  @Test
  void checkConnectionStatus_reportsFailureWhenConnectFails() throws Exception {
    newMonitor(false);
    when(pluginService.forceConnect(any(HostSpec.class), any(Properties.class)))
        .thenThrow(new SQLException("unreachable"));
    assertFalse(monitor.checkConnectionStatus());
  }

  @Test
  void resetEvent_forThisHost_clearsHealthState() throws Exception {
    newMonitor(false);
    doReturn(false).when(monitor).checkConnectionStatus();
    final MonitoredConnectionSlot slot = registeredSlot(appConnection);
    slot.enter(clock.get());
    advance(GRACE_NANOS);
    for (int i = 0; i < FAILURE_DETECTION_COUNT; i++) {
      monitor.monitorIteration();
    }
    assertTrue(monitor.isHostUnhealthy());

    monitor.processEvent(new MonitorResetEvent("cluster", Collections.singleton("other-host")));
    assertTrue(monitor.isHostUnhealthy(), "a reset for another host must be ignored");

    monitor.processEvent(new MonitorResetEvent("cluster", Collections.singleton("test-host")));
    assertFalse(monitor.isHostUnhealthy());
  }

  @Test
  void abortFailure_isSwallowed() throws Exception {
    newMonitor(false);
    doReturn(false).when(monitor).checkConnectionStatus();
    final SQLException abortError = new SQLException("abort failed");
    doThrow(abortError)
        .when(targetDriverDialect).abortConnection(any(Connection.class), any(Executor.class));
    final MonitoredConnectionSlot slot = registeredSlot(appConnection);
    slot.enter(clock.get());
    advance(GRACE_NANOS);

    for (int i = 0; i < FAILURE_DETECTION_COUNT; i++) {
      monitor.monitorIteration();
    }

    assertEquals(MonitoredConnectionSlot.ABORTED, slot.getInFlightSinceNanos());
    verify(abortedConnectionsCounter, never()).inc();
  }
}
