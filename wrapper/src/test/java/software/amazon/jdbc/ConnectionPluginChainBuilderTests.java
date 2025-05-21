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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.plugin.ConnectTimeConnectionPlugin;
import software.amazon.jdbc.plugin.DefaultConnectionPlugin;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPlugin;
import software.amazon.jdbc.plugin.dev.DeveloperConnectionPlugin;
import software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.util.CompleteServicesContainer;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class ConnectionPluginChainBuilderTests {

  @Mock ConnectionProvider mockConnectionProvider;
  @Mock CompleteServicesContainer mockServicesContainer;
  @Mock PluginService mockPluginService;
  @Mock PluginManagerService mockPluginManagerService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;

  private AutoCloseable closeable;

  @AfterEach
  void afterEach() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void beforeEach() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
  }

  @Test
  public void testSortPlugins() throws SQLException {
    ConnectionPluginChainBuilder builder = new ConnectionPluginChainBuilder();
    Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "iam,efm2,failover");

    List<ConnectionPlugin> result = builder.getPlugins(
        mockServicesContainer,
        mockConnectionProvider,
        null,
        mockPluginManagerService,
        props,
        null);

    assertNotNull(result);
    assertEquals(4, result.size());
    assertInstanceOf(FailoverConnectionPlugin.class, result.get(0));
    assertInstanceOf(HostMonitoringConnectionPlugin.class, result.get(1));
    assertInstanceOf(IamAuthConnectionPlugin.class, result.get(2));
    assertInstanceOf(DefaultConnectionPlugin.class, result.get(3));
  }

  @Test
  public void testPreservePluginOrder() throws SQLException {
    ConnectionPluginChainBuilder builder = new ConnectionPluginChainBuilder();
    Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "iam,efm2,failover");
    props.put(PropertyDefinition.AUTO_SORT_PLUGIN_ORDER.name, "false");

    List<ConnectionPlugin> result = builder.getPlugins(
        mockServicesContainer,
        mockConnectionProvider,
        null,
        mockPluginManagerService,
        props,
        null);

    assertNotNull(result);
    assertEquals(4, result.size());
    assertInstanceOf(IamAuthConnectionPlugin.class, result.get(0));
    assertInstanceOf(HostMonitoringConnectionPlugin.class, result.get(1));
    assertInstanceOf(FailoverConnectionPlugin.class, result.get(2));
    assertInstanceOf(DefaultConnectionPlugin.class, result.get(3));
  }

  @Test
  public void testSortPluginsWithStickToPrior() throws SQLException {
    ConnectionPluginChainBuilder builder = new ConnectionPluginChainBuilder();
    Properties props = new Properties();
    props.put(PropertyDefinition.PLUGINS.name, "dev,iam,executionTime,connectTime,efm2,failover");

    List<ConnectionPlugin> result = builder.getPlugins(
        mockServicesContainer,
        mockConnectionProvider,
        null,
        mockPluginManagerService,
        props,
        null);

    assertNotNull(result);
    assertEquals(7, result.size());
    assertInstanceOf(DeveloperConnectionPlugin.class, result.get(0));
    assertInstanceOf(FailoverConnectionPlugin.class, result.get(1));
    assertInstanceOf(HostMonitoringConnectionPlugin.class, result.get(2));
    assertInstanceOf(IamAuthConnectionPlugin.class, result.get(3));
    assertInstanceOf(ExecutionTimeConnectionPlugin.class, result.get(4));
    assertInstanceOf(ConnectTimeConnectionPlugin.class, result.get(5));
    assertInstanceOf(DefaultConnectionPlugin.class, result.get(6));
  }
}
