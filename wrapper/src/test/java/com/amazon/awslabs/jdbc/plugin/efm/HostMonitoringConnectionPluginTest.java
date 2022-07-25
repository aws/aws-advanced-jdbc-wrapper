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

package com.amazon.awslabs.jdbc.plugin.efm;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.JdbcCallable;
import com.amazon.awslabs.jdbc.PluginService;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class HostMonitoringConnectionPluginTest {

  static final Class<Connection> MONITOR_METHOD_INVOKE_ON = Connection.class;
  static final String MONITOR_METHOD_NAME = "Connection.executeQuery";
  static final String NO_MONITOR_METHOD_NAME = "Connection.abort";
  static final int FAILURE_DETECTION_TIME = 10;
  static final int FAILURE_DETECTION_INTERVAL = 100;
  static final int FAILURE_DETECTION_COUNT = 5;
  private static final Object[] EMPTY_ARGS = {};
  @Mock PluginService pluginService;
  @Mock Connection connection;
  Properties properties = new Properties();
  @Mock HostSpec hostSpec;
  @Mock Supplier<MonitorService> supplier;
  @Mock MonitorConnectionContext context;
  @Mock MonitorService monitorService;
  @Mock JdbcCallable<ResultSet, SQLException> sqlFunction;
  private HostMonitoringConnectionPlugin plugin;
  private AutoCloseable closeable;

  /**
   * Generate different sets of method arguments where one argument is null to ensure {@link
   * com.amazon.awslabs.jdbc.plugin.efm.HostMonitoringConnectionPlugin#HostMonitoringConnectionPlugin(PluginService,
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
            anySet(),
            any(HostSpec.class),
            any(Properties.class),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(context);

    when(pluginService.getCurrentConnection()).thenReturn(connection);
    when(pluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(hostSpec.getHost()).thenReturn("host");
    when(hostSpec.getHost()).thenReturn("port");
    when(hostSpec.getAliases()).thenReturn(new HashSet<>(Arrays.asList("host:port")));

    properties.put("failureDetectionEnabled", Boolean.TRUE.toString());
    properties.put("failureDetectionTime", String.valueOf(FAILURE_DETECTION_TIME));
    properties.put("failureDetectionInterval", String.valueOf(FAILURE_DETECTION_INTERVAL));
    properties.put("failureDetectionCount", String.valueOf(FAILURE_DETECTION_COUNT));
  }

  private void initializePlugin() {
    plugin = new HostMonitoringConnectionPlugin(pluginService, properties, supplier);
  }

  @ParameterizedTest
  @MethodSource("generateNullArguments")
  @Disabled // TODO: Need to enforce @NonNull and @Nullable checks for entire project to pass this
  // test
  void test_1_initWithNullArguments(
      final PluginService pluginService, final Properties properties) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HostMonitoringConnectionPlugin(pluginService, properties));
  }

  @Test
  void test_2_executeWithMonitoringDisabled() throws Exception {
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
        .startMonitoring(any(), any(), any(), any(), anyInt(), anyInt(), anyInt());
    verify(monitorService, never()).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  @Test
  void test_3_executeWithNoNeedToMonitor() throws Exception {

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
        .startMonitoring(any(), any(), any(), any(), anyInt(), anyInt(), anyInt());
    verify(monitorService, never()).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }

  @Test
  void test_4_executeMonitoringEnabled() throws Exception {

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
        .startMonitoring(any(), any(), any(), any(), anyInt(), anyInt(), anyInt());
    verify(monitorService, times(1)).stopMonitoring(context);
    verify(sqlFunction, times(1)).call();
  }
}
