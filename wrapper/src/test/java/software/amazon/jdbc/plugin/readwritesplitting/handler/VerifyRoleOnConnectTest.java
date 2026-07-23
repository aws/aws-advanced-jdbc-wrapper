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

package software.amazon.jdbc.plugin.readwritesplitting.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/** Unit tests for {@link VerifyRoleOnConnect}. */
public class VerifyRoleOnConnectTest {

  private static final String STRATEGY = "random";
  private static final String PROTOCOL = "jdbc:postgresql:";

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private HostListProviderService hostListProviderService;
  @Mock private JdbcCallable<Connection, SQLException> connectFunc;
  @Mock private Connection conn;

  private final Properties props = new Properties();
  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance").port(5432).role(HostRole.WRITER).build();

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.pluginService()).thenReturn(pluginService);
    when(connectFunc.call()).thenReturn(conn);
    when(pluginService.acceptsStrategy(any(), eq(STRATEGY))).thenReturn(true);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void unsupportedStrategy_throws() throws SQLException {
    when(pluginService.acceptsStrategy(any(), eq(STRATEGY))).thenReturn(false);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    assertThrows(UnsupportedOperationException.class,
        () -> handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc));
  }

  @Test
  void nonInitialConnection_passesThrough() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, false, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  void staticHostListProvider_passesThrough() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.isStaticHostListProvider()).thenReturn(true);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  void verifyDisabled_passesThrough() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.isStaticHostListProvider()).thenReturn(false);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, false);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  void roleMismatch_correctsInitialHostSpecRole() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.isStaticHostListProvider()).thenReturn(false);
    // The topology labels the initial host a writer, but the opened connection is a reader.
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.READER);
    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  void roleMatches_noUpdate() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.isStaticHostListProvider()).thenReturn(false);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.WRITER);
    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  void unknownRole_throws() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.isStaticHostListProvider()).thenReturn(false);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.UNKNOWN);
    doThrow(new SQLException("cannot verify role")).when(ctx).logAndThrow(anyString());
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    assertThrows(SQLException.class,
        () -> handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc));
  }
}
