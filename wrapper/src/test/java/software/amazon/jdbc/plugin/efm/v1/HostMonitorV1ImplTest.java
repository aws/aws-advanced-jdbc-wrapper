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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.events.MonitorResetEvent;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class HostMonitorV1ImplTest {

  @Mock PluginService pluginService;
  @Mock FullServicesContainer servicesContainer;
  @Mock Connection connection;
  @Mock HostSpec hostSpec;
  @Mock HostMonitorConnectionContextV1 contextWithShortInterval;
  @Mock HostMonitorConnectionContextV1 contextWithLongInterval;
  @Mock TelemetryFactory telemetryFactory;
  @Mock TelemetryContext telemetryContext;
  @Mock TelemetryCounter telemetryCounter;
  @Mock ResourceLock mockResourceLock;
  @Mock EventPublisher eventPublisher;

  private static final long SHORT_INTERVAL_MILLIS = 30;
  private static final long SHORT_INTERVAL_SECONDS = TimeUnit.MILLISECONDS.toSeconds(SHORT_INTERVAL_MILLIS);
  private static final long LONG_INTERVAL_MILLIS = 300;

  private AutoCloseable closeable;
  private HostMonitorV1Impl monitor;
  private Properties properties;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    properties = new Properties();

    when(contextWithShortInterval.getFailureDetectionIntervalMillis()).thenReturn(SHORT_INTERVAL_MILLIS);
    when(contextWithLongInterval.getFailureDetectionIntervalMillis()).thenReturn(LONG_INTERVAL_MILLIS);
    when(contextWithShortInterval.getLock()).thenReturn(mockResourceLock);
    when(contextWithShortInterval.isActiveContext()).thenReturn(true);
    when(contextWithLongInterval.isActiveContext()).thenReturn(true);
    when(pluginService.forceConnect(any(HostSpec.class), any(Properties.class))).thenReturn(connection);
    when(pluginService.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(telemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(telemetryContext);
    when(telemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(telemetryContext);
    when(telemetryFactory.createCounter(anyString())).thenReturn(telemetryCounter);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(servicesContainer.getEventPublisher()).thenReturn(eventPublisher);
    when(hostSpec.getHost()).thenReturn("test-host");
    when(hostSpec.getUrl()).thenReturn("test-url");

    monitor = spy(new HostMonitorV1Impl(servicesContainer, hostSpec, properties));
  }

  @AfterEach
  void cleanUp() throws Exception {
    if (monitor != null) {
      monitor.stop();
    }
    closeable.close();
  }

  @Test
  void test_5_isConnectionHealthyWithNoExistingConnection() throws SQLException {
    final boolean isValid = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    verify(pluginService).forceConnect(any(HostSpec.class), any(Properties.class));
    assertTrue(isValid);
  }

  @Test
  void test_6_isConnectionHealthyWithExistingConnection() throws SQLException {
    when(connection.isValid(eq((int) SHORT_INTERVAL_SECONDS))).thenReturn(Boolean.TRUE, Boolean.FALSE);
    when(connection.isClosed()).thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    final boolean isValid1 =
        monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    assertTrue(isValid1);

    final boolean isValid2 =
        monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    assertFalse(isValid2);

    verify(connection, times(2)).isValid(anyInt());
  }

  @Test
  void test_7_isConnectionHealthyWithSQLException() throws SQLException {
    when(connection.isValid(anyInt())).thenThrow(new SQLException());
    when(connection.isClosed()).thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    assertDoesNotThrow(
        () -> {
          final boolean isValid =
              monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
          assertFalse(isValid);
        });
  }

  @Test
  void test_constructor_withMonitoringProperties() {
    Properties props = new Properties();
    props.setProperty("monitoring-user", "monitor_user");
    props.setProperty("monitoring-password", "monitor_pass");

    HostMonitorV1Impl mon = new HostMonitorV1Impl(servicesContainer, hostSpec, props);
    assertNotNull(mon);
  }

  @Test
  void test_constructor_withHostId() {
    when(hostSpec.getHostId()).thenReturn("host-123");
    HostMonitorV1Impl mon = new HostMonitorV1Impl(servicesContainer, hostSpec, properties);
    assertNotNull(mon);
    verify(telemetryFactory).createCounter(eq("efm.nodeUnhealthy.count.host-123"));
  }

  @Test
  void test_startMonitoring() {
    monitor.startMonitoring(contextWithShortInterval);
    verify(contextWithShortInterval).setStartMonitorTimeNano(anyLong());
  }

  @Test
  void test_startMonitoring_whenStopped() {
    monitor.stop();
    monitor.startMonitoring(contextWithShortInterval);
    verify(contextWithShortInterval).setStartMonitorTimeNano(anyLong());
  }

  @Test
  void test_reset() {
    monitor.reset();
    assertNull(monitor.monitoringConn.get());
  }

  @Test
  void test_processEvent_monitorResetEvent() {
    when(hostSpec.getHost()).thenReturn("test-host");
    MonitorResetEvent event = mock(MonitorResetEvent.class);
    when(event.getEndpoints()).thenReturn(Collections.singleton("test-host"));

    monitor.processEvent(event);
    assertNull(monitor.monitoringConn.get());
  }

  @Test
  void test_processEvent_differentHost() throws SQLException {
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    Connection conn = monitor.monitoringConn.get();

    MonitorResetEvent event = mock(MonitorResetEvent.class);
    when(event.getEndpoints()).thenReturn(Collections.singleton("other-host"));

    monitor.processEvent(event);
    assertEquals(conn, monitor.monitoringConn.get());
  }

  @Test
  void test_getSnapshotState() {
    List<Pair<String, Object>> state = monitor.getSnapshotState();
    assertNotNull(state);
    assertFalse(state.isEmpty());
  }

  @Test
  void test_checkConnectionStatus_closedConnection() throws SQLException {
    when(connection.isClosed()).thenReturn(true);
    monitor.monitoringConn.set(connection);

    boolean isValid = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    assertTrue(isValid);
    verify(pluginService).forceConnect(any(HostSpec.class), any(Properties.class));
  }

  @Test
  void test_checkConnectionStatus_incrementsCounter() throws SQLException {
    when(connection.isValid(anyInt())).thenReturn(false);
    when(connection.isClosed()).thenReturn(false);
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    verify(telemetryCounter, atLeastOnce()).inc();
  }

  @Test
  void test_monitor_processesContexts() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);
    when(contextWithShortInterval.getExpectedActiveMonitoringStartTimeNano()).thenReturn(0L);

    doAnswer(inv -> {
      if (callCount.incrementAndGet() > 2) {
        monitor.stop();
      }
      return System.nanoTime();
    }).when(monitor).getCurrentTimeNano();

    monitor.startMonitoring(contextWithShortInterval);

    Thread monitorThread = new Thread(() -> monitor.monitor());
    monitorThread.start();
    monitorThread.join(2000);

    verify(eventPublisher).subscribe(eq(monitor), anySet());
  }
}
