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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.monitoring.MonitorErrorResponse;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class HostMonitorServiceV3ImplTest {

  private static final String EXPECTED_KEY = "30000:5000:3:false:test-host:5432/";

  @Mock FullServicesContainer servicesContainer;
  @Mock PluginService pluginService;
  @Mock MonitorService coreMonitorService;
  @Mock TelemetryFactory telemetryFactory;
  @Mock TelemetryCounter telemetryCounter;
  @Mock HostSpec hostSpec;
  @Mock Connection connection;
  @Mock HostMonitorV3Impl stoppedMonitor;
  @Mock HostMonitorV3Impl healthyMonitor;

  private final Properties properties = new Properties();
  private AutoCloseable closeable;
  private HostMonitorServiceV3Impl service;
  private MonitoredConnectionSlot slot;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getMonitorService()).thenReturn(coreMonitorService);
    when(servicesContainer.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(telemetryFactory.createCounter(anyString())).thenReturn(telemetryCounter);
    when(pluginService.getProperties()).thenReturn(properties);
    when(hostSpec.getHost()).thenReturn("test-host");
    when(hostSpec.getUrl()).thenReturn("test-host:5432/");
    when(stoppedMonitor.register(any())).thenReturn(false);
    when(healthyMonitor.register(any())).thenReturn(true);
    service = new HostMonitorServiceV3Impl(servicesContainer, properties);
    slot = new MonitoredConnectionSlot(connection, hostSpec, System.nanoTime());
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void constructor_registersTheMonitorTypeWithoutAutomaticRecreation() {
    verify(coreMonitorService).registerMonitorTypeIfAbsent(
        eq(HostMonitorV3Impl.class),
        eq(TimeUnit.MINUTES.toNanos(10)),
        eq(TimeUnit.MINUTES.toNanos(3)),
        eq(EnumSet.noneOf(MonitorErrorResponse.class)),
        isNull());
  }

  @Test
  void register_usesTheCachedMonitorKeyedBySettingsAndUrl() throws SQLException {
    when(coreMonitorService.runIfAbsent(eq(HostMonitorV3Impl.class), eq(EXPECTED_KEY), any(), any(), any()))
        .thenReturn(healthyMonitor);

    assertSame(healthyMonitor, service.register(slot));

    verify(healthyMonitor).register(slot);
    verify(coreMonitorService, never()).stopAndRemove(any(), any());
  }

  @Test
  void register_replacesAStoppedMonitor() throws SQLException {
    when(coreMonitorService.runIfAbsent(eq(HostMonitorV3Impl.class), eq(EXPECTED_KEY), any(), any(), any()))
        .thenReturn(stoppedMonitor, healthyMonitor);

    assertSame(healthyMonitor, service.register(slot));

    verify(coreMonitorService).stopAndRemove(HostMonitorV3Impl.class, EXPECTED_KEY);
    verify(coreMonitorService, times(2))
        .runIfAbsent(eq(HostMonitorV3Impl.class), eq(EXPECTED_KEY), any(), any(), any());
    verify(healthyMonitor).register(slot);
  }

  @Test
  void register_failsWhenTheReplacementIsAlsoStopped() throws SQLException {
    when(coreMonitorService.runIfAbsent(eq(HostMonitorV3Impl.class), eq(EXPECTED_KEY), any(), any(), any()))
        .thenReturn(stoppedMonitor);

    assertThrows(SQLException.class, () -> service.register(slot));
    verify(coreMonitorService, times(1)).stopAndRemove(HostMonitorV3Impl.class, EXPECTED_KEY);
  }

  @Test
  void monitorKey_reflectsTheDetectionSettings() {
    properties.setProperty("failureDetectionTime", "1000");
    properties.setProperty("failureDetectionInterval", "200");
    properties.setProperty("failureDetectionCount", "7");
    properties.setProperty("abortIdleConnectionsOnFailure", "true");
    service = new HostMonitorServiceV3Impl(servicesContainer, properties);

    assertEquals("1000:200:7:true:test-host:5432/", service.getMonitorKey(hostSpec));
  }

  @Test
  void unregister_delegatesToTheSlotMonitor() {
    slot.setMonitor(healthyMonitor);
    service.unregister(slot);
    verify(healthyMonitor).unregister(slot);

    service.unregister(new MonitoredConnectionSlot(connection, hostSpec, System.nanoTime()));
    verify(healthyMonitor, times(1)).unregister(any());
  }
}
