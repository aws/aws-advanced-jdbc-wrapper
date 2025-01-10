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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Ignore;
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
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class LimitlessRouterServiceImplTest {

  private static final String CLUSTER_ID = "someClusterId";
  @Mock private PluginService mockPluginService;
  @Mock private HostListProvider mockHostListProvider;
  @Mock private LimitlessRouterMonitor mockLimitlessRouterMonitor;
  @Mock private LimitlessQueryHelper mockQueryHelper;
  @Mock JdbcCallable<Connection, SQLException> mockConnectFuncLambda;
  @Mock private Connection mockConnection;
  private static final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("some-instance").role(HostRole.WRITER).build();
  private static final long someExpirationNano = TimeUnit.MILLISECONDS.toNanos(60000);
  private static Properties props;
  private AutoCloseable closeable;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    when(mockConnectFuncLambda.call()).thenReturn(mockConnection);
    when(mockPluginService.getHostListProvider()).thenReturn(mockHostListProvider);
    when(mockPluginService.getProperties()).thenReturn(props);
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
  }

  @AfterEach
  public void cleanup() throws Exception {
    closeable.close();
    LimitlessRouterServiceImpl.limitlessRouterCache.clear();
  }

  @Test
  void testEstablishConnection_GivenGetEmptyRouterListAndWaitForRouterInfo_ThenThrow() throws SQLException {
    when(mockQueryHelper.queryForLimitlessRouters(any(Connection.class), anyInt())).thenReturn(Collections.emptyList());

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        hostSpec,
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    assertThrows(SQLException.class, () -> limitlessRouterService.establishConnection(inputContext));
  }

  @Test
  void testEstablishConnection_GivenGetEmptyRouterListAndNoWaitForRouterInfo_ThenCallConnectFunc() throws SQLException {
    props.setProperty(LimitlessConnectionPlugin.WAIT_FOR_ROUTER_INFO.name, "false");
    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        hostSpec,
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    verify(mockConnectFuncLambda, times(1)).call();
  }

  @Test
  void testEstablishConnection_GivenHostSpecInRouterCache_ThenCallConnectFunc() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build());
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, routerList, someExpirationNano);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        routerList.get(1),
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    verify(mockConnectFuncLambda, times(1)).call();
  }

  @Test
  void testEstablishConnection_GivenFetchRouterListAndHostSpecInRouterList_ThenCallConnectFunc()
      throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-1").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-2").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-3").role(HostRole.WRITER)
            .build());

    when(mockQueryHelper.queryForLimitlessRouters(any(Connection.class), anyInt())).thenReturn(routerList);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        routerList.get(1),
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    assertEquals(routerList, LimitlessRouterServiceImpl.limitlessRouterCache.get(CLUSTER_ID, someExpirationNano));
    verify(mockQueryHelper, times(1))
        .queryForLimitlessRouters(inputContext.getConnection(), inputContext.getHostSpec().getPort());
    verify(mockConnectFuncLambda, times(1)).call();
  }

  @Test
  void testEstablishConnection_GivenRouterCache_ThenSelectsHost() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build());
    final HostSpec selectedRouter = routerList.get(2);
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, routerList, someExpirationNano);

    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(selectedRouter);
    when(mockPluginService.connect(any(), any(), any())).thenReturn(mockConnection);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        hostSpec,
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1)).connect(selectedRouter, inputContext.getProps(), null);
    verify(mockConnectFuncLambda, times(0)).call();
  }

  @Test
  void testEstablishConnection_GivenFetchRouterList_ThenSelectsHost() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-1").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-2").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-3").role(HostRole.WRITER)
            .build());
    final HostSpec selectedRouter = routerList.get(2);
    when(mockQueryHelper.queryForLimitlessRouters(any(Connection.class), anyInt())).thenReturn(routerList);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(selectedRouter);
    when(mockPluginService.connect(any(), any(), any())).thenReturn(mockConnection);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        hostSpec,
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    assertEquals(routerList, LimitlessRouterServiceImpl.limitlessRouterCache.get(CLUSTER_ID, someExpirationNano));
    verify(mockQueryHelper, times(1))
        .queryForLimitlessRouters(inputContext.getConnection(), inputContext.getHostSpec().getPort());
    verify(mockConnectFuncLambda, times(1)).call();
    verify(mockPluginService, times(1)).connect(eq(selectedRouter), eq(inputContext.getProps()), eq(null));
  }

  @Test
  void testEstablishConnection_GivenHostSpecInRouterCacheAndCallConnectFuncThrows_ThenRetry() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .availability(HostAvailability.AVAILABLE).build());
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, routerList, someExpirationNano);
    final HostSpec selectedRouter = routerList.get(2);
    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        routerList.get(1),
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );

    when(mockConnectFuncLambda.call()).thenThrow(new SQLException());
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(selectedRouter);
    when(mockPluginService.connect(any(), any(), any())).thenReturn(mockConnection);

    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    assertEquals(routerList, LimitlessRouterServiceImpl.limitlessRouterCache.get(CLUSTER_ID, someExpirationNano));
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
    verify(mockPluginService, times(1)).connect(selectedRouter, inputContext.getProps(), null);
    verify(mockConnectFuncLambda, times(1)).call();
  }

  @Ignore
  @Test
  void testEstablishConnection_GivenSelectsHostThrows_ThenRetry() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-1").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-2").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-3").role(HostRole.WRITER)
            .build());
    final HostSpec selectedRouter = routerList.get(2);
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, routerList, someExpirationNano);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any()))
        .thenThrow(new SQLException())
        .thenReturn(selectedRouter);
    when(mockPluginService.connect(any(), any(), any())).thenReturn(mockConnection);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        hostSpec,
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    assertEquals(routerList, LimitlessRouterServiceImpl.limitlessRouterCache.get(CLUSTER_ID, someExpirationNano));
    verify(mockPluginService, times(2)).getHostSpecByStrategy(any(), any(), any());
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
    verify(mockPluginService, times(1)).connect(selectedRouter, inputContext.getProps(), null);
  }

  @Test
  void testEstablishConnection_GivenSelectsHostNull_ThenRetry() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-1").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-2").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-3").role(HostRole.WRITER)
            .build());
    final HostSpec selectedRouter = routerList.get(2);
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, routerList, someExpirationNano);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any()))
        .thenReturn(null, selectedRouter);
    when(mockPluginService.connect(any(), any(), any())).thenReturn(mockConnection);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        hostSpec,
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    assertEquals(routerList, LimitlessRouterServiceImpl.limitlessRouterCache.get(CLUSTER_ID, someExpirationNano));
    verify(mockPluginService, times(2)).getHostSpecByStrategy(any(), any(), any());
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
    verify(mockPluginService, times(1)).connect(selectedRouter, inputContext.getProps(), null);
  }

  @Test
  void testEstablishConnection_GivenPluginServiceConnectThrows_ThenRetry() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-1").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-2").role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("some-instance-3").role(HostRole.WRITER)
            .build());
    final HostSpec selectedRouter = routerList.get(1);
    final HostSpec selectedRouterForRetry = routerList.get(2);
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, routerList, someExpirationNano);
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any()))
        .thenReturn(selectedRouter, selectedRouterForRetry);
    when(mockPluginService.connect(any(), any(), any()))
        .thenThrow(new SQLException())
        .thenReturn(mockConnection);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        hostSpec,
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );
    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    limitlessRouterService.establishConnection(inputContext);

    assertEquals(mockConnection, inputContext.getConnection());
    assertEquals(routerList, LimitlessRouterServiceImpl.limitlessRouterCache.get(CLUSTER_ID, someExpirationNano));
    verify(mockPluginService, times(2)).getHostSpecByStrategy(any(), any(), any());
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    verify(mockPluginService, times(1))
        .getHostSpecByStrategy(routerList, HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);
    verify(mockPluginService, times(2)).connect(any(), any(), any());
    verify(mockPluginService).connect(selectedRouter, inputContext.getProps(), null);
    verify(mockPluginService).connect(selectedRouterForRetry, inputContext.getProps(), null);
  }

  @Test
  void testEstablishConnection_GivenRetryAndMaxRetriesExceeded_thenThrowSqlException() throws SQLException {
    final List<HostSpec> routerList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .availability(HostAvailability.AVAILABLE).build());
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, routerList, someExpirationNano);

    final LimitlessConnectionContext inputContext = new LimitlessConnectionContext(
        routerList.get(0),
        props,
        null,
        mockConnectFuncLambda,
        null,
        null
    );

    when(mockConnectFuncLambda.call()).thenThrow(new SQLException());
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any())).thenReturn(routerList.get(0));
    when(mockPluginService.connect(any(), any(), any())).thenThrow(new SQLException());

    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockQueryHelper);

    assertThrows(SQLException.class, () -> limitlessRouterService.establishConnection(inputContext));

    verify(mockPluginService, times(LimitlessConnectionPlugin.MAX_RETRIES.getInteger(props)))
        .connect(any(), any(), any());
    verify(mockPluginService, times(LimitlessConnectionPlugin.MAX_RETRIES.getInteger(props)))
        .getHostSpecByStrategy(any(), any(), any());
  }
}
