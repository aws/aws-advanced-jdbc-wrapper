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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
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
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class LimitlessConnectionPluginTest {

  private static final String DRIVER_PROTOCOL = "jdbc:postgresql:";
  private static final HostSpec INPUT_HOST_SPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("pg.testdb.us-east-2.rds.amazonaws.com").build();
  private static final String CLUSTER_ID = "someClusterId";

  private static final HostSpec highestWeightHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("expected-selected-instance").role(HostRole.WRITER).weight(Long.MAX_VALUE).build();
  @Mock private static Connection connection;
  @Mock JdbcCallable<Connection, SQLException> mockConnectFuncLambda;
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
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
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
        highestWeightHostSpec
    );
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(endpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(highestWeightHostSpec);

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true, mockConnectFuncLambda);

    verify(mockLimitlessRouterService, times(1)).startMonitoring(mockPluginService, INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(1)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1)).connect(highestWeightHostSpec, props);
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
        highestWeightHostSpec
    );
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(endpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(highestWeightHostSpec);

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, false, mockConnectFuncLambda);

    verify(mockLimitlessRouterService, times(0)).startMonitoring(mockPluginService, INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(1)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1)).connect(highestWeightHostSpec, props);
  }

  @Test
  void testConnect_givenEmptyLimitlessRouterCache() throws SQLException {
    final List<HostSpec> emptyEndpointHostSpecList = Collections.emptyList();
    when(mockLimitlessRouterService.getLimitlessRouters(any(), any())).thenReturn(emptyEndpointHostSpecList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(highestWeightHostSpec);

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, false, mockConnectFuncLambda);

    verify(mockLimitlessRouterService, times(0)).startMonitoring(mockPluginService, INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(0)).getHostSpecByStrategy(emptyEndpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(0)).connect(highestWeightHostSpec, props);
    verify(mockConnectFuncLambda, times(1)).call();
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
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(highestWeightHostSpec);

    plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, false, mockConnectFuncLambda);

    verify(mockLimitlessRouterService, times(0)).startMonitoring(mockPluginService, INPUT_HOST_SPEC,
        props, Integer.parseInt(LimitlessConnectionPlugin.INTERVAL_MILLIS.defaultValue));
    verify(mockLimitlessRouterService, times(1)).getLimitlessRouters(CLUSTER_ID, props);
    verify(mockPluginService, times(0)).getHostSpecByStrategy(endpointHostSpecList,
        HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(0)).connect(highestWeightHostSpec, props);
    verify(mockConnectFuncLambda, times(1)).call();
  }
}
