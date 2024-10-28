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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class LimitlessRouterServiceImplTest {

  private static final String CLUSTER_ID = "someClusterId";
  private static final String OTHER_CLUSTER_ID = "someOtherClusterId";
  @Mock private PluginService mockPluginService;
  @Mock private HostListProvider mockHostListProvider;
  @Mock private LimitlessRouterMonitor mockLimitlessRouterMonitor;
  @Mock private Connection mockConnection;
  @Mock private JdbcCallable<Connection, SQLException> mockConnectFunc;
  private static final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("some-instance").role(HostRole.WRITER).build();
  private static final int intervalMs = 1000;
  private static Properties props;
  private AutoCloseable closeable;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    when(mockConnectFunc.call()).thenReturn(mockConnection);
  }

  @AfterEach
  public void cleanup() throws Exception {
    closeable.close();
    LimitlessRouterServiceImpl.limitlessRouterCache.clear();
  }

  @Test
  void testGetLimitlessRouters() throws SQLException {
    when(mockPluginService.getHostListProvider()).thenReturn(mockHostListProvider);
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build()
    );
    LimitlessRouterServiceImpl.limitlessRouterCache.put(CLUSTER_ID, endpointHostSpecList, 600000);
    final LimitlessQueryHelper mockLimitlessQueryHelper = Mockito.mock(LimitlessQueryHelper.class);

    final LimitlessRouterService limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockLimitlessQueryHelper);
    limitlessRouterService.startMonitoring(hostSpec, props, intervalMs);
    final List<HostSpec> actualEndpointHostSpecList = limitlessRouterService.getLimitlessRouters(CLUSTER_ID, props);

    assertEquals(endpointHostSpecList, actualEndpointHostSpecList);
  }

  @Test
  void testForceGetLimitlessRouters() throws SQLException {
    when(mockPluginService.getHostListProvider()).thenReturn(mockHostListProvider);
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
    final List<HostSpec> expectedHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build()
    );
    final LimitlessQueryHelper mockLimitlessQueryHelper = Mockito.mock(LimitlessQueryHelper.class);
    when(mockLimitlessQueryHelper.queryForLimitlessRouters(any(Connection.class), anyInt()))
        .thenReturn(expectedHostSpecList);

    final Connection expectedConn = mockConnection;
    final int expectedHostPort = 39;

    final LimitlessRouterServiceImpl limitlessRouterService = new LimitlessRouterServiceImpl(
            mockPluginService,
            (a, b, c, d, e) -> mockLimitlessRouterMonitor,
            mockLimitlessQueryHelper);

    final List<HostSpec> actualHostSpecList = limitlessRouterService
        .forceGetLimitlessRouters(expectedConn, mockConnectFunc, expectedHostPort, props);

    verify(mockLimitlessQueryHelper, times(1))
        .queryForLimitlessRouters(expectedConn, expectedHostPort);
    assertEquals(expectedHostSpecList, actualHostSpecList);
  }

  @Test
  void testForceGetLimitlessRoutersNullConnection() throws SQLException {
    when(mockPluginService.getHostListProvider()).thenReturn(mockHostListProvider);
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
    final List<HostSpec> expectedHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build()
    );
    final LimitlessQueryHelper mockLimitlessQueryHelper = Mockito.mock(LimitlessQueryHelper.class);
    when(mockLimitlessQueryHelper.queryForLimitlessRouters(any(Connection.class), anyInt()))
        .thenReturn(expectedHostSpecList);

    final Connection expectedConn = mockConnection;
    final int expectedHostPort = 39;

    final LimitlessRouterServiceImpl limitlessRouterService = new LimitlessRouterServiceImpl(
        mockPluginService,
        (a, b, c, d, e) -> mockLimitlessRouterMonitor,
        mockLimitlessQueryHelper);

    final List<HostSpec> actualHostSpecList = limitlessRouterService
        .forceGetLimitlessRouters(null, mockConnectFunc, expectedHostPort, props);

    verify(mockLimitlessQueryHelper, times(1))
        .queryForLimitlessRouters(expectedConn, expectedHostPort);
    assertEquals(expectedHostSpecList, actualHostSpecList);
  }
}
