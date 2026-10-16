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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin;
import software.amazon.jdbc.plugin.ConnectTimeConnectionPlugin;
import software.amazon.jdbc.plugin.DefaultConnectionPlugin;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPlugin;
import software.amazon.jdbc.plugin.cache.CacheConnection;
import software.amazon.jdbc.plugin.dev.DeveloperConnectionPlugin;
import software.amazon.jdbc.plugin.efm.v2.HostMonitoringConnectionPluginV2;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.plugin.readwritesplitting.SimpleReadWriteSplittingPlugin;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class ConnectionPluginChainBuilderTests {

  @Mock ConnectionProvider mockConnectionProvider;
  @Mock FullServicesContainer mockServicesContainer;
  @Mock PluginService mockPluginService;
  @Mock StorageService mockStorageService;
  @Mock PluginManagerService mockPluginManagerService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock MonitorService mockMonitorService;

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
    when(mockServicesContainer.getPluginManagerService()).thenReturn(mockPluginManagerService);
    when(mockServicesContainer.getStorageService()).thenReturn(mockStorageService);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());
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
        props,
        null);

    assertNotNull(result);
    assertEquals(4, result.size());
    assertInstanceOf(FailoverConnectionPlugin.class, result.get(0));
    assertInstanceOf(HostMonitoringConnectionPluginV2.class, result.get(1));
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
        props,
        null);

    assertNotNull(result);
    assertEquals(4, result.size());
    assertInstanceOf(IamAuthConnectionPlugin.class, result.get(0));
    assertInstanceOf(HostMonitoringConnectionPluginV2.class, result.get(1));
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
        props,
        null);

    assertNotNull(result);
    assertEquals(7, result.size());
    assertInstanceOf(DeveloperConnectionPlugin.class, result.get(0));
    assertInstanceOf(FailoverConnectionPlugin.class, result.get(1));
    assertInstanceOf(HostMonitoringConnectionPluginV2.class, result.get(2));
    assertInstanceOf(IamAuthConnectionPlugin.class, result.get(3));
    assertInstanceOf(ExecutionTimeConnectionPlugin.class, result.get(4));
    assertInstanceOf(ConnectTimeConnectionPlugin.class, result.get(5));
    assertInstanceOf(DefaultConnectionPlugin.class, result.get(6));
  }

  @Test
  public void testPluginCodesAreUnique() {
    final Map<String, List<String>> classesByCode = new TreeMap<>();
    for (Map.Entry<String, String> entry : ConnectionPluginChainBuilder.pluginCodeByPlugin.entrySet()) {
      classesByCode.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(entry.getKey());
    }
    final Map<String, List<String>> duplicates = classesByCode.entrySet().stream()
        .filter(e -> e.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));

    assertTrue(duplicates.isEmpty(),
        "pluginCodeByPlugin contains duplicate plugin codes (code=[plugin classes]): " + duplicates);
  }

  @Test
  public void testPluginCodesExistForAllPlugins() {
    final Map<String, String> pluginClassByFactoryCode = getPluginClassByFactoryCode();
    final Map<String, String> unmapped = pluginClassByFactoryCode.entrySet().stream()
        .filter(e -> !ConnectionPluginChainBuilder.pluginCodeByPlugin.containsKey(e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));

    assertTrue(unmapped.isEmpty(),
        "pluginCodeByPlugin has no entry for these plugin classes (factory code=plugin class): " + unmapped);
  }

  @Test
  public void testPluginCodesHaveNoOrphanEntries() {
    final Set<String> pluginClasses = new HashSet<>(getPluginClassByFactoryCode().values());
    final Set<String> orphans = ConnectionPluginChainBuilder.pluginCodeByPlugin.keySet().stream()
        .filter(clazz -> !pluginClasses.contains(clazz))
        .collect(Collectors.toCollection(TreeSet::new));

    assertTrue(orphans.isEmpty(),
        "pluginCodeByPlugin has entries that match no plugin created by pluginFactoriesByCode: " + orphans);
  }

  /**
   * Creates a plugin with every factory registered in
   * {@link ConnectionPluginChainBuilder#pluginFactoriesByCode} and returns the class name of each
   * created plugin, keyed by the factory code. Fails the test if a factory can't create its plugin,
   * since the plugin class then can't be checked.
   */
  private Map<String, String> getPluginClassByFactoryCode() {
    when(mockServicesContainer.getMonitorService()).thenReturn(mockMonitorService);

    // Some plugins refuse to start without these parameters.
    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.example.com");
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.example.com");
    props.setProperty(AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY.name, "test-secret");
    props.setProperty(AwsSecretsManagerConnectionPlugin.REGION_PROPERTY.name, "us-east-1");
    props.setProperty(CacheConnection.CACHE_RW_ENDPOINT_ADDR.name, "cache.example.com:6379");

    final Map<String, String> pluginClassByFactoryCode = new TreeMap<>();
    final Map<String, String> failures = new TreeMap<>();
    for (Map.Entry<String, ConnectionPluginFactory> entry
        : ConnectionPluginChainBuilder.pluginFactoriesByCode.entrySet()) {
      try {
        final ConnectionPlugin plugin = entry.getValue().getInstance(mockServicesContainer, props);
        pluginClassByFactoryCode.put(entry.getKey(), plugin.getClass().getName());
      } catch (Exception ex) {
        failures.put(entry.getKey(), ex.toString());
      }
    }

    assertTrue(failures.isEmpty(), "Unable to create plugins for these factory codes: " + failures);
    return pluginClassByFactoryCode;
  }
}
