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

package software.amazon.jdbc.plugin.limitless;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.dialect.AuroraPgDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class LimitlessConnectionPluginTest {

  private static final String DRIVER_PROTOCOL = "jdbc:postgresql:";
  private static final HostSpec INPUT_HOST_SPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("pg.testdb.us-east-2.rds.amazonaws.com").build();
  private static final String CLUSTER_ID = "someClusterId";

  private static final HostSpec expectedSelectedHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("expected-selected-instance").role(HostRole.WRITER).weight(Long.MAX_VALUE).build();
  private static final Dialect expectedDialect = new AuroraPgDialect();
  @Mock JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock private Connection mockConnection;
  @Mock private PluginService mockPluginService;
  @Mock private HostListProvider mockHostListProvider;
  @Mock private LimitlessRouterService mockLimitlessRouterService;
  private static Properties props;

  private static LimitlessConnectionPlugin plugin;

  private AutoCloseable closeable;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    plugin = new LimitlessConnectionPlugin(mockPluginService, props, () -> mockLimitlessRouterService);

    when(mockPluginService.getHostListProvider()).thenReturn(mockHostListProvider);
    when(mockPluginService.getDialect()).thenReturn(expectedDialect);
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
    when(mockConnectFunc.call()).thenReturn(mockConnection);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testConnect() throws SQLException {
    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build(),
        expectedSelectedHostSpec
    );
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(endpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(expectedSelectedHostSpec);

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true, mockConnectFunc);

    verify(mockLimitlessRouterService, times(1)).startMonitoring(INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(1)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1)).connect(expectedSelectedHostSpec, props);
  }

  @Test
  void testConnect_givenNotInitialConnection() throws SQLException {
    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build(),
        expectedSelectedHostSpec
    );
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(endpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(expectedSelectedHostSpec);

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, false, mockConnectFunc);

    verify(mockLimitlessRouterService, times(0)).startMonitoring(INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(1)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1)).connect(expectedSelectedHostSpec, props);
  }

  @Test
  void testConnect_givenEmptyLimitlessRouterCache() throws SQLException {
    final List<HostSpec> emptyEndpointHostSpecList = Collections.emptyList();
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(emptyEndpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(expectedSelectedHostSpec);

    final Properties propsWaitForRouterInfoSetTrue = new Properties();
    propsWaitForRouterInfoSetTrue.setProperty(LimitlessConnectionPlugin.WAIT_F0R_ROUTER_INFO.name, "true");
  }

  @Test
  void testConnect_givenEmptyLimitlessRouterCacheAndNoWaitForRouterInfo() throws SQLException {
    final List<HostSpec> emptyEndpointHostSpecList = Collections.emptyList();
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(emptyEndpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(expectedSelectedHostSpec);

    final Properties propsWaitForRouterInfoSetFalse = new Properties();
    propsWaitForRouterInfoSetFalse.setProperty(LimitlessConnectionPlugin.WAIT_F0R_ROUTER_INFO.name, "false");

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, propsWaitForRouterInfoSetFalse, false, mockConnectFunc);

    verify(mockLimitlessRouterService, times(0)).startMonitoring(INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, propsWaitForRouterInfoSetFalse);
    verify(mockPluginService, times(0)).getHostSpecByStrategy(emptyEndpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(0)).connect(expectedSelectedHostSpec, propsWaitForRouterInfoSetFalse);
    verify(mockConnectFunc, times(1)).call();
  }

  @Test
  void testConnect_givenHostSpecInLimitlessRouterCache() throws SQLException {
    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build(),
        INPUT_HOST_SPEC
    );
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(endpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(expectedSelectedHostSpec);

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, false, mockConnectFunc);

    verify(mockLimitlessRouterService, times(0)).startMonitoring(INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(0)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(0)).connect(expectedSelectedHostSpec, props);
    verify(mockConnectFunc, times(1)).call();
  }

  @Test
  void testConnect_givenSuccessfulRetry() throws SQLException {
    final HostSpec mockExpectedRetryHostSpec = mock(HostSpec.class);
    when(mockExpectedRetryHostSpec.getHost()).thenReturn("-expected-retry-instance");

    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build(),
        expectedSelectedHostSpec,
        mockExpectedRetryHostSpec
    );

    final Connection expectedConnection = mock(Connection.class);

    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(endpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), eq(RoundRobinHostSelector.STRATEGY_ROUND_ROBIN)))
        .thenReturn(expectedSelectedHostSpec);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), eq(HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT)))
        .thenReturn(mockExpectedRetryHostSpec);
    when(mockPluginService.connect(eq(expectedSelectedHostSpec), any())).thenThrow(SQLException.class);
    when(mockPluginService.connect(eq(mockExpectedRetryHostSpec), any())).thenReturn(expectedConnection);

    final Connection actualConnection =
        plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true, mockConnectFunc);

    assertEquals(expectedConnection, actualConnection);

    verify(mockLimitlessRouterService, times(1)).startMonitoring(INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(1)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(
        mockPluginService,
        times(1)
    ).getHostSpecByStrategy(
        endpointHostSpecList,
        HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
    verify(mockPluginService, times(1)).connect(expectedSelectedHostSpec, props);
    verify(mockPluginService, times(1))
        .connect(mockExpectedRetryHostSpec, props);
    verify(mockExpectedRetryHostSpec, times(0)).setAvailability(HostAvailability.NOT_AVAILABLE);
  }

  @Test
  void testConnect_givenNoAvailableRoutersForRetry() throws SQLException {

    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .availability(HostAvailability.NOT_AVAILABLE).build()
    );

    when(mockLimitlessRouterService.getLimitlessRouters(any(), any()))
        .thenReturn(endpointHostSpecList, Collections.emptyList());
    when(mockPluginService.getHostSpecByStrategy(any(), any(), eq(RoundRobinHostSelector.STRATEGY_ROUND_ROBIN)))
        .thenReturn(expectedSelectedHostSpec);
    when(mockPluginService.connect(eq(expectedSelectedHostSpec), any())).thenThrow(SQLException.class);

    assertThrows(
        SQLException.class,
        () -> plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true, mockConnectFunc));

    verify(mockLimitlessRouterService, times(1)).startMonitoring(INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(1)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(
        mockPluginService,
        times(0)
    ).getHostSpecByStrategy(
        endpointHostSpecList,
        HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
    verify(mockPluginService, times(1)).connect(expectedSelectedHostSpec, props);
  }

  @Test
  void testConnect_givenMaxRetries_throwsSqlException() throws SQLException {
    final HostSpec mockExpectedRetryHostSpec = mock(HostSpec.class);
    when(mockExpectedRetryHostSpec.getHost()).thenReturn("-expected-retry-instance");

    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build(),
        expectedSelectedHostSpec,
        mockExpectedRetryHostSpec
    );

    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(endpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), eq(RoundRobinHostSelector.STRATEGY_ROUND_ROBIN)))
        .thenReturn(expectedSelectedHostSpec);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), eq(HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT)))
        .thenReturn(mockExpectedRetryHostSpec);
    when(mockPluginService.connect(any(), any())).thenThrow(SQLException.class);

    final Properties propsWithMaxRetries = new Properties();
    final int maxRetries = 7;
    propsWithMaxRetries.setProperty(LimitlessConnectionPlugin.MAX_RETRIES.name, String.valueOf(maxRetries));


    final Connection actualConn = plugin
        .connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, propsWithMaxRetries, true, mockConnectFunc);
    assertEquals(mockConnection, actualConn);

    verify(mockLimitlessRouterService, times(1)).startMonitoring(INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, propsWithMaxRetries);
    verify(mockPluginService, times(1)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(
        mockPluginService,
        times(maxRetries)
    ).getHostSpecByStrategy(
        endpointHostSpecList,
        HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
    verify(mockPluginService, times(1)).connect(expectedSelectedHostSpec, propsWithMaxRetries);
    verify(mockPluginService, times(maxRetries))
        .connect(mockExpectedRetryHostSpec, propsWithMaxRetries);
    verify(mockExpectedRetryHostSpec, times(maxRetries)).setAvailability(HostAvailability.NOT_AVAILABLE);
  }
}
