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

package software.amazon.jdbc.plugin.customendpoint;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.jdbc.plugin.customendpoint.CustomEndpointPlugin.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.function.BiFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.connection.ConnectionInfo;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class CustomEndpointPluginTest {
  private final String writerClusterUrl = "writer.cluster-XYZ.us-east-1.rds.amazonaws.com";
  private final String customEndpointUrl = "custom.cluster-custom-XYZ.us-east-1.rds.amazonaws.com";

  private AutoCloseable closeable;
  private final Properties props = new Properties();
  private final HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();
  private final HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
  private final HostSpec writerClusterHost = hostSpecBuilder.host(writerClusterUrl).build();
  private final HostSpec host = hostSpecBuilder.host(customEndpointUrl).build();

  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private ConnectionInfo mockConnectionInfo;
  @Mock private PluginService mockPluginService;
  @Mock private MonitorService mockMonitorService;
  @Mock private BiFunction<HostSpec, Region, RdsClient> mockRdsClientFunc;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock private TelemetryCounter mockTelemetryCounter;
  @Mock private JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock private JdbcCallable<Statement, SQLException> mockJdbcMethodFunc;
  @Mock private Connection mockConnection;
  @Mock private CustomEndpointMonitor mockMonitor;
  @Mock TargetDriverDialect mockTargetDriverDialect;


  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getMonitorService()).thenReturn(mockMonitorService);
    when(mockServicesContainer.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(any(String.class))).thenReturn(mockTelemetryCounter);
    when(mockMonitor.hasCustomEndpointInfo()).thenReturn(true);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    props.clear();
  }

  private CustomEndpointPlugin getSpyPlugin() throws SQLException {
    CustomEndpointPlugin plugin = new CustomEndpointPlugin(mockServicesContainer, props, mockRdsClientFunc);
    CustomEndpointPlugin spyPlugin = spy(plugin);
    doReturn(mockMonitor).when(spyPlugin).createMonitorIfAbsent(any(Properties.class));
    return spyPlugin;
  }

  @Test
  public void testConnect_monitorNotCreatedIfNotCustomEndpointHost() throws SQLException {
    CustomEndpointPlugin spyPlugin = getSpyPlugin();

    spyPlugin.connect(mockConnectionInfo, writerClusterHost, true, mockConnectFunc);

    verify(mockConnectFunc, times(1)).call();
    verify(spyPlugin, never()).createMonitorIfAbsent(any(Properties.class));
  }

  @Test
  public void testConnect_monitorCreated() throws SQLException {
    CustomEndpointPlugin spyPlugin = getSpyPlugin();

    spyPlugin.connect(mockConnectionInfo, host, true, mockConnectFunc);

    verify(spyPlugin, times(1)).createMonitorIfAbsent(eq(props));
    verify(mockConnectFunc, times(1)).call();
  }

  @Test
  public void testConnect_timeoutWaitingForInfo() throws SQLException {
    WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS.set(props, "1");
    CustomEndpointPlugin spyPlugin = getSpyPlugin();
    when(mockMonitor.hasCustomEndpointInfo()).thenReturn(false);

    assertThrows(SQLException.class, () -> spyPlugin.connect(mockConnectionInfo, host, true, mockConnectFunc));

    verify(spyPlugin, times(1)).createMonitorIfAbsent(eq(props));
    verify(mockConnectFunc, never()).call();
  }

  @Test
  public void testExecute_monitorNotCreatedIfNotCustomEndpointHost() throws SQLException {
    CustomEndpointPlugin spyPlugin = getSpyPlugin();

    spyPlugin.execute(
        Statement.class, SQLException.class, mockConnection, "Connection.createStatement", mockJdbcMethodFunc, null);

    verify(mockJdbcMethodFunc, times(1)).call();
    verify(spyPlugin, never()).createMonitorIfAbsent(any(Properties.class));
  }

  @Test
  public void testExecute_monitorCreated() throws SQLException {
    CustomEndpointPlugin spyPlugin = getSpyPlugin();
    spyPlugin.customEndpointHostSpec = host;

    spyPlugin.execute(
        Statement.class, SQLException.class, mockConnection, "Connection.createStatement", mockJdbcMethodFunc, null);

    verify(spyPlugin, times(1)).createMonitorIfAbsent(eq(props));
    verify(mockJdbcMethodFunc, times(1)).call();
  }
}
