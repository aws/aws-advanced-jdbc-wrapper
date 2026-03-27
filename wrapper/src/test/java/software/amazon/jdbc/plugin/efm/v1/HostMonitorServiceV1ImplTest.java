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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.jdbc.JdbcConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.efm.base.ConnectionContextService;
import software.amazon.jdbc.plugin.efm.base.HostMonitor;
import software.amazon.jdbc.plugin.efm.base.HostMonitorConnectionContext;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class HostMonitorServiceV1ImplTest {

  @Mock private HostMonitor monitorA;
  @Mock private HostMonitor monitorB;
  @Mock private ExecutorService executorService;
  @Mock private Future<?> task;
  @Mock private HostSpec hostSpec;
  @Mock private HostSpec hostSpecA;
  @Mock private HostSpec hostSpecB;
  @Mock private JdbcConnection connection;
  @Mock private PluginService pluginService;
  @Mock private TelemetryFactory telemetryFactory;
  @Mock private TelemetryCounter telemetryCounter;
  @Mock private FullServicesContainer servicesContainer;
  @Mock private MonitorService monitorService;
  @Mock private ConnectionContextService connectionContextService;

  private Properties properties;
  private AutoCloseable closeable;
  private HostMonitorServiceV1Impl hostMonitorService;
  private ArgumentCaptor<HostMonitorConnectionContextV1> contextCaptor;

  @BeforeEach
  void init() throws SQLException {
    properties = new Properties();
    closeable = MockitoAnnotations.openMocks(this);
    contextCaptor = ArgumentCaptor.forClass(HostMonitorConnectionContextV1.class);

    when(pluginService.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(telemetryFactory.createCounter(anyString())).thenReturn(telemetryCounter);
    doReturn(task).when(executorService).submit(any(HostMonitor.class));

    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(servicesContainer.getMonitorService()).thenReturn(monitorService);
    when(servicesContainer.getConnectionContextService()).thenReturn(connectionContextService);
    when(connectionContextService.acquire(any(), any())).thenAnswer(invocation -> {
      HostMonitorConnectionContextV1 ctx = mock(HostMonitorConnectionContextV1.class);
      when(ctx.getConnection()).thenReturn(connection);
      return ctx;
    });
    when(monitorService.runIfAbsent(any(), eq("hostA"), any(), any(), any())).thenReturn(monitorA);
    when(monitorService.runIfAbsent(any(), eq("hostB"), any(), any(), any())).thenReturn(monitorB);
    when(monitorService.runIfAbsent(any(), eq("host"), any(), any(), any())).thenReturn(monitorA);
    this.hostMonitorService = new HostMonitorServiceV1Impl(servicesContainer, properties);

    when(hostSpec.getUrl()).thenReturn("host");
    when(hostSpecA.getUrl()).thenReturn("hostA");
    when(hostSpecB.getUrl()).thenReturn("hostB");
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_startMonitoringWithNoExecutor() throws SQLException {
    doNothing().when(monitorA).startMonitoring(contextCaptor.capture());

    hostMonitorService.startMonitoring(
        connection,
        hostSpec,
        properties);

    assertNotNull(contextCaptor.getValue());
  }

  @Test
  void test_startMonitoringCalledMultipleTimes() throws SQLException {
    doNothing().when(monitorA).startMonitoring(contextCaptor.capture());

    final int runs = 5;

    for (int i = 0; i < runs; i++) {
      hostMonitorService.startMonitoring(
          connection,
          hostSpec,
          properties);
    }

    assertNotNull(contextCaptor.getValue());
  }

  @Test
  void test_getMonitorCalledWithMultipleNodesInKeys() throws SQLException {
    when(monitorService.runIfAbsent(any(), eq("hostA"), any(), any(), any())).thenReturn(monitorA);
    when(monitorService.runIfAbsent(any(), eq("hostB"), any(), any(), any())).thenReturn(monitorA);
    
    final HostMonitor monitorOne = hostMonitorService.getMonitor(hostSpecA, properties);
    assertNotNull(monitorOne);

    // Should get the same monitor as before as contain the same key "nodeTwo.domain"
    final HostMonitor monitorOneSame = hostMonitorService.getMonitor(hostSpecB, properties);
    assertNotNull(monitorOneSame);
    assertEquals(monitorOne, monitorOneSame);
  }

  @Test
  void test_getMonitorCalledWithDifferentNodeKeys() throws SQLException {
    final HostMonitor monitorOne = hostMonitorService.getMonitor(hostSpecA, properties);
    assertNotNull(monitorOne);

    // Ensuring monitor is the same one and not creating a new one
    final HostMonitor monitorOneDupe = hostMonitorService.getMonitor(hostSpecA, properties);
    assertEquals(monitorOne, monitorOneDupe);

    final HostMonitor monitorTwo = hostMonitorService.getMonitor(hostSpecB, properties);
    assertNotNull(monitorTwo);
    assertNotEquals(monitorOne, monitorTwo);
  }

  @Test
  void test_stopMonitoring_shouldAbort() throws SQLException {
    HostMonitorConnectionContext context = mock(HostMonitorConnectionContext.class);
    Connection conn = mock(Connection.class);
    when(context.shouldAbort()).thenReturn(true);
    when(context.getConnection()).thenReturn(conn);
    
    hostMonitorService.stopMonitoring(context);

    verify(connectionContextService).release(context);
    verify(conn).abort(any());
    verify(conn).close();
    verify(telemetryCounter).inc();
  }

  @Test
  void test_stopMonitoring_shouldNotAbort() {
    HostMonitorConnectionContext context = mock(HostMonitorConnectionContext.class);
    when(context.shouldAbort()).thenReturn(false);
    
    hostMonitorService.stopMonitoring(context);
    
    verify(connectionContextService).release(context);
    verify(context, never()).getConnection();
  }

  @Test
  void test_stopMonitoring_nullConnection() {
    HostMonitorConnectionContext context = mock(HostMonitorConnectionContext.class);
    when(context.shouldAbort()).thenReturn(true);
    when(context.getConnection()).thenReturn(null);
    
    hostMonitorService.stopMonitoring(context);
    verify(connectionContextService).release(context);
  }

  @Test
  void test_stopMonitoring_sqlException() throws SQLException {
    HostMonitorConnectionContext context = mock(HostMonitorConnectionContext.class);
    Connection conn = mock(Connection.class);
    when(context.shouldAbort()).thenReturn(true);
    when(context.getConnection()).thenReturn(conn);
    doThrow(new SQLException()).when(conn).abort(any());
    
    hostMonitorService.stopMonitoring(context);
    
    verify(connectionContextService).release(context);
  }

  @Test
  void test_getSnapshotState() {
    List<Pair<String, Object>> state = hostMonitorService.getSnapshotState();
    
    assertNotNull(state);
    assertEquals(3, state.size());
    
    boolean hasTimeMillis = false;
    boolean hasIntervalMillis = false;
    boolean hasCount = false;
    
    for (Pair<String, Object> pair : state) {
      if ("failureDetectionTimeMillis".equals(pair.getValue1())) {
        hasTimeMillis = true;
      }
      if ("failureDetectionIntervalMillis".equals(pair.getValue1())) {
        hasIntervalMillis = true;
      }
      if ("failureDetectionCount".equals(pair.getValue1())) {
        hasCount = true;
      }
    }
    
    assertTrue(hasTimeMillis);
    assertTrue(hasIntervalMillis);
    assertTrue(hasCount);
  }

  @Test
  void test_constructor_initializesProperties() {
    assertNotNull(hostMonitorService);
  }

  @Test
  void test_startMonitoring_createsContextWithCorrectParameters() throws SQLException {
    doNothing().when(monitorA).startMonitoring(contextCaptor.capture());
    
    hostMonitorService.startMonitoring(connection, hostSpec, properties);
    
    HostMonitorConnectionContextV1 ctx = contextCaptor.getValue();
    assertNotNull(ctx);
    assertNotNull(ctx.getConnection());
  }
}
