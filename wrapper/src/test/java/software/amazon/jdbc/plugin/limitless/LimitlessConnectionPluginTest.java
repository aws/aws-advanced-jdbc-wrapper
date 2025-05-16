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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.jdbc.plugin.limitless.LimitlessConnectionPlugin.INTERVAL_MILLIS;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.AuroraPgDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.PgDialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class LimitlessConnectionPluginTest {

  private static final String DRIVER_PROTOCOL = "jdbc:postgresql:";
  private static final HostSpec INPUT_HOST_SPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("pg.testdb.us-east-2.rds.amazonaws.com").build();
  private static final String CLUSTER_ID = "someClusterId";

  private static final HostSpec expectedSelectedHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("expected-selected-instance").role(HostRole.WRITER).weight(Long.MAX_VALUE).build();
  private static final Dialect supportedDialect = new AuroraPgDialect();
  @Mock JdbcCallable<Connection, SQLException> mockConnectFuncLambda;
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
    when(mockPluginService.getDialect()).thenReturn(supportedDialect);
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
    when(mockConnectFuncLambda.call()).thenReturn(mockConnection);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testConnect() throws SQLException {
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        LimitlessConnectionContext context = (LimitlessConnectionContext) invocation.getArguments()[0];
        context.setConnection(mockConnection);
        return null;
      }
    }).when(mockLimitlessRouterService).establishConnection(any());

    final Connection expectedConnection = mockConnection;
    final Connection actualConnection = plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true,
        mockConnectFuncLambda);

    assertEquals(expectedConnection, actualConnection);
    verify(mockPluginService, times(1)).getDialect();
    verify(mockConnectFuncLambda, times(0)).call();
    verify(mockLimitlessRouterService, times(1))
        .startMonitoring(INPUT_HOST_SPEC, props, INTERVAL_MILLIS.getInteger(props));
    verify(mockLimitlessRouterService, times(1)).establishConnection(any());
  }

  @Test
  void testConnectGivenNullConnection() throws SQLException {
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        LimitlessConnectionContext context = (LimitlessConnectionContext) invocation.getArguments()[0];
        context.setConnection(null);
        return null;
      }
    }).when(mockLimitlessRouterService).establishConnection(any());

    assertThrows(
        SQLException.class,
        () -> plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true, mockConnectFuncLambda));

    verify(mockPluginService, times(1)).getDialect();
    verify(mockConnectFuncLambda, times(0)).call();
    verify(mockLimitlessRouterService, times(1))
        .startMonitoring(INPUT_HOST_SPEC, props, INTERVAL_MILLIS.getInteger(props));
    verify(mockLimitlessRouterService, times(1)).establishConnection(any());
  }

  @Test
  void testConnectGivenUnsupportedDialect() throws SQLException {
    final Dialect unsupportedDialect = new PgDialect();
    when(mockPluginService.getDialect()).thenReturn(unsupportedDialect, unsupportedDialect);

    assertThrows(
        UnsupportedOperationException.class,
        () -> plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true, mockConnectFuncLambda));

    verify(mockPluginService, times(2)).getDialect();
    verify(mockConnectFuncLambda, times(1)).call();
    verify(mockLimitlessRouterService, times(0))
        .startMonitoring(INPUT_HOST_SPEC, props, INTERVAL_MILLIS.getInteger(props));
    verify(mockLimitlessRouterService, times(0)).establishConnection(any());
  }

  @Test
  void testConnectGivenSupportedDialectAfterRefresh() throws SQLException {
    final Dialect unsupportedDialect = new PgDialect();
    when(mockPluginService.getDialect()).thenReturn(unsupportedDialect, supportedDialect);

    final Connection expectedConnection = mockConnection;
    final Connection actualConnection = plugin.connect(DRIVER_PROTOCOL, INPUT_HOST_SPEC, props, true,
        mockConnectFuncLambda);

    assertEquals(expectedConnection, actualConnection);
    verify(mockPluginService, times(2)).getDialect();
    verify(mockConnectFuncLambda, times(1)).call();
    verify(mockLimitlessRouterService, times(1))
        .startMonitoring(INPUT_HOST_SPEC, props, INTERVAL_MILLIS.getInteger(props));
    verify(mockLimitlessRouterService, times(1)).establishConnection(any());
  }
}
