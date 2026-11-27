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

package software.amazon.jdbc.plugin.strategy.fastestresponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class NodeResponseTimeMonitorTest {

  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private PluginService mockPluginService;
  @Mock private TelemetryFactory mockTelemetryFactory;

  private AutoCloseable closeable;
  private final HostSpec hostSpec =
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("reader-1").port(3306).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  /**
   * The monitoring connection is not watched by any failure-detection plugin, so it must not be
   * opened without connect and socket timeouts: a node that stops answering would otherwise block
   * the monitor for the OS TCP timeout.
   */
  @Test
  void monitoringConnectionGetsDefaultTimeouts() {
    final NodeResponseTimeMonitor monitor =
        new NodeResponseTimeMonitor(mockServicesContainer, hostSpec, new Properties(), 1000);

    assertEquals(String.valueOf(NodeResponseTimeMonitor.DEFAULT_SOCKET_TIMEOUT_MS),
        PropertyDefinition.SOCKET_TIMEOUT.getString(monitor.getMonitoringConnProperties()));
    assertEquals(String.valueOf(NodeResponseTimeMonitor.DEFAULT_CONNECT_TIMEOUT_MS),
        PropertyDefinition.CONNECT_TIMEOUT.getString(monitor.getMonitoringConnProperties()));
  }

  @Test
  void explicitTimeoutsTakePrecedenceOverTheDefaults() {
    final Properties props = new Properties();
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "1234");
    props.setProperty("frt-" + PropertyDefinition.CONNECT_TIMEOUT.name, "4321");

    final NodeResponseTimeMonitor monitor =
        new NodeResponseTimeMonitor(mockServicesContainer, hostSpec, props, 1000);

    assertEquals("1234", PropertyDefinition.SOCKET_TIMEOUT.getString(monitor.getMonitoringConnProperties()));
    assertEquals("4321", PropertyDefinition.CONNECT_TIMEOUT.getString(monitor.getMonitoringConnProperties()));
  }
}
