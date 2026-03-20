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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.efm.base.HostMonitorService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.ResourceLock;

class HostMonitoringConnectionPluginV1Test {

  static final Class<Connection> MONITOR_METHOD_INVOKE_ON = Connection.class;
  static final String MONITOR_METHOD_NAME = JdbcMethod.STATEMENT_EXECUTEQUERY.methodName;
  static final String NO_MONITOR_METHOD_NAME = JdbcMethod.CONNECTION_ABORT.methodName;
  static final int FAILURE_DETECTION_TIME = 10;
  static final int FAILURE_DETECTION_INTERVAL = 100;
  static final int FAILURE_DETECTION_COUNT = 5;
  private static final Object[] EMPTY_ARGS = {};
  @Mock PluginService pluginService;
  @Mock FullServicesContainer servicesContainer;
  @Mock Dialect mockDialect;
  @Mock Connection connection;
  @Mock Statement statement;
  @Mock ResultSet resultSet;
  @Captor ArgumentCaptor<String> stringArgumentCaptor;
  Properties properties = new Properties();
  @Mock HostSpec hostSpec;
  @Mock HostSpec hostSpec2;
  @Mock Supplier<HostMonitorService> supplier;
  @Mock RdsUtils rdsUtils;
  @Mock HostMonitorConnectionContextV1 context;
  @Mock ResourceLock mockResourceLock;
  @Mock HostMonitorService monitorService;
  @Mock JdbcCallable<ResultSet, SQLException> sqlFunction;
  @Mock TargetDriverDialect targetDriverDialect;

  private HostMonitoringConnectionPluginV1 plugin;
  private AutoCloseable closeable;

  /**
   * Generate different sets of method arguments where one argument is null to ensure {@link
   * HostMonitoringConnectionPlugin#HostMonitoringConnectionPlugin(PluginService,
   * Properties)} can handle null arguments correctly.
   *
   * @return different sets of arguments.
   */
  private static Stream<Arguments> generateNullArguments() {
    final PluginService pluginService = mock(PluginService.class);
    final Properties properties = new Properties();

    return Stream.of(
        Arguments.of(null, null),
        Arguments.of(pluginService, null),
        Arguments.of(null, properties));
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    initDefaultMockReturns();
    properties.clear();
  }

  void initDefaultMockReturns() throws Exception {
    when(supplier.get()).thenReturn(monitorService);
    when(monitorService.startMonitoring(
            any(Connection.class),
            any(HostSpec.class),
            any(Properties.class)))
        .thenReturn(context);
    when(context.getLock()).thenReturn(mockResourceLock);

    when(pluginService.getCurrentConnection()).thenReturn(connection);
    when(pluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(pluginService.getDialect()).thenReturn(mockDialect);
    when(pluginService.getTargetDriverDialect()).thenReturn(targetDriverDialect);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(targetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(
        new HashSet<>(Collections.singletonList(MONITOR_METHOD_NAME)));
    when(mockDialect.getHostAliasQuery()).thenReturn("any");
    when(hostSpec.getHost()).thenReturn("host");
    when(hostSpec.getHost()).thenReturn("port");
    when(hostSpec2.getHost()).thenReturn("host");
    when(hostSpec2.getHost()).thenReturn("port");
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(any())).thenReturn(resultSet);
    when(rdsUtils.identifyRdsType(any())).thenReturn(RdsUrlType.RDS_INSTANCE);

    properties.put("failureDetectionEnabled", Boolean.TRUE.toString());
    properties.put("failureDetectionTime", String.valueOf(FAILURE_DETECTION_TIME));
    properties.put("failureDetectionInterval", String.valueOf(FAILURE_DETECTION_INTERVAL));
    properties.put("failureDetectionCount", String.valueOf(FAILURE_DETECTION_COUNT));
  }

  private void initializePlugin() {
    plugin = new HostMonitoringConnectionPluginV1(servicesContainer, properties, supplier, rdsUtils);
  }

  @Test
  void test_executeWithMonitoringDisabled() throws Exception {
    properties.put("failureDetectionEnabled", Boolean.FALSE.toString());

    initializePlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    verify(supplier, never()).get();
    verify(monitorService, never())
        .startMonitoring(any(), any(), any());
    verify(monitorService, never()).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  @Test
  void test_executeWithNoNeedToMonitor() throws Exception {

    initializePlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        NO_MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    verify(supplier, atMostOnce()).get();
    verify(monitorService, never())
        .startMonitoring(any(), any(), any());
    verify(monitorService, never()).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  @Test
  void test_executeMonitoringEnabled() throws Exception {

    initializePlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    verify(supplier, times(1)).get();
    verify(monitorService, times(1))
        .startMonitoring(any(), any(), any());
    verify(monitorService, times(1)).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  @Test
  void test_connect_exceptionRaisedDuringGenerateHostAliases() throws SQLException {
    initializePlugin();

    doThrow(new SQLException()).when(connection).createStatement();

    // Ensure SQLException raised in `generateHostAliases` are ignored.
    final Connection conn = plugin.connect("protocol", hostSpec, properties, true, () -> connection);
    assertNotNull(conn);
  }

  @Test
  void test_releaseResources() throws SQLException {
    initializePlugin();

    // Test releaseResources when the monitor service has not been initialized.
    plugin.releaseResources();

    // Test releaseResources when the monitor service has been initialized.
    plugin.execute(
        ResultSet.class,
        SQLException.class,
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);
    plugin.releaseResources();
  }
}
