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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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
import software.amazon.jdbc.hostlistprovider.StaticHostListProvider;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/** Unit tests for {@link VerifyRoleOnConnect}. */
public class VerifyRoleOnConnectTest {

  private static final String STRATEGY = "random";
  private static final String PROTOCOL = "jdbc:postgresql:";

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private HostListProviderService hostListProviderService;
  @Mock private StaticHostListProvider staticHostListProvider;
  @Mock private JdbcCallable<Connection, SQLException> connectFunc;
  @Mock private Connection conn;

  private final Properties props = new Properties();
  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance").port(5432).role(HostRole.WRITER).build();
  private final HostSpec otherWriterHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("other-instance").port(5432).role(HostRole.WRITER).build();

  private final List<LogRecord> warnings = new ArrayList<>();
  private Logger handlerLogger;
  private Handler warningCollector;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.pluginService()).thenReturn(pluginService);
    when(connectFunc.call()).thenReturn(conn);
    when(pluginService.acceptsStrategy(any(), eq(STRATEGY))).thenReturn(true);

    // The record of already-reported host list conditions is static and lives for the JVM, so clear
    // it to keep tests independent of each other and of execution order.
    VerifyRoleOnConnect.clearReportedStaticHostListRoles();

    warnings.clear();
    warningCollector = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
          warnings.add(record);
        }
      }

      @Override
      public void flush() {
        // nothing to flush
      }

      @Override
      public void close() {
        // nothing to close
      }
    };
    handlerLogger = Logger.getLogger(VerifyRoleOnConnect.class.getName());
    handlerLogger.addHandler(warningCollector);
  }

  @AfterEach
  void tearDown() throws Exception {
    handlerLogger.removeHandler(warningCollector);
    VerifyRoleOnConnect.clearReportedStaticHostListRoles();
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
  void verifyDisabled_passesThrough() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, false);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  void roleMismatch_correctsInitialHostSpecRole() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
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
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.WRITER);
    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  /**
   * A static host list derives roles from the connection string, so the measured role has to be
   * pushed into the host list itself: correcting only the initial host spec leaves reader and writer
   * selection reading the stale role.
   */
  @Test
  void staticHostListProvider_roleMismatch_correctsHostListRole() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(staticHostListProvider.updateHostRole(writerHost.getHostAndPort(), HostRole.READER)).thenReturn(true);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.READER);
    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService).setInitialConnectionHostSpec(any(HostSpec.class));
    verify(staticHostListProvider).updateHostRole(writerHost.getHostAndPort(), HostRole.READER);
    verify(pluginService).refreshHostList();
    assertEquals(1, warnings.size(),
        "A connection string that names the wrong role must be reported: writes would go to a reader.");
  }

  @Test
  void staticHostListProvider_roleMatches_leavesHostListAlone() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.WRITER);
    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(staticHostListProvider, never()).updateHostRole(anyString(), any(HostRole.class));
    verify(pluginService, never()).refreshHostList();
  }

  @Test
  void staticHostListProvider_hostNotUpdated_doesNotRefreshHostList() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(staticHostListProvider.updateHostRole(anyString(), any(HostRole.class))).thenReturn(false);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.READER);
    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    verify(pluginService, never()).refreshHostList();
    assertTrue(warnings.isEmpty(),
        "No role was replaced in the host list, so nothing may claim a correction was made.");
  }

  /**
   * The handler runs on the initial connection, and a fresh plugin service, host list provider and
   * plugin chain are built for every wrapper connection - so this path is reached once per
   * application connection. The connection string it complains about does not change between them.
   */
  @Test
  void staticHostListProvider_roleCorrected_reportedOncePerCondition() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(staticHostListProvider.updateHostRole(anyString(), any(HostRole.class))).thenReturn(true);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.READER);
    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);

    connectThreeTimes();

    assertEquals(1, warnings.size(), "Later connections must not restate the same unchanged fact.");
    verify(pluginService, times(3)).refreshHostList();
  }

  @Test
  void staticHostListProvider_roleCorrected_reportedAgainForAnotherHost() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(staticHostListProvider.updateHostRole(anyString(), any(HostRole.class))).thenReturn(true);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.READER);

    when(pluginService.getInitialConnectionHostSpec()).thenReturn(writerHost);
    new VerifyRoleOnConnect(STRATEGY, true).onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    when(pluginService.getInitialConnectionHostSpec()).thenReturn(otherWriterHost);
    new VerifyRoleOnConnect(STRATEGY, true)
        .onConnect(ctx, PROTOCOL, otherWriterHost, props, true, connectFunc);

    assertEquals(2, warnings.size(), "A second mislabelled host is a separate problem to fix.");
  }

  @Test
  void staticHostListProvider_roleQueryFails_reportedOncePerCondition() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(pluginService.getHostRole(conn)).thenThrow(new UnsupportedOperationException("no role query"));

    connectThreeTimes();

    assertEquals(1, warnings.size(),
        "A database that cannot report a role cannot report it on any connection; warn once.");
  }

  @Test
  void staticHostListProvider_unknownRole_reportedOncePerCondition() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.UNKNOWN);

    connectThreeTimes();

    assertEquals(1, warnings.size());
  }

  /** Each call stands for one application connection: a new plugin chain, hence a new handler. */
  private void connectThreeTimes() throws SQLException {
    for (int i = 0; i < 3; i++) {
      new VerifyRoleOnConnect(STRATEGY, true).onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);
    }
  }

  /**
   * A static host list already carries a declared role, so a database that cannot report its role
   * must keep working rather than have its connections refused.
   */
  @Test
  void staticHostListProvider_roleQueryFails_keepsDeclaredRole() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(pluginService.getHostRole(conn)).thenThrow(new UnsupportedOperationException("no role query"));
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  void staticHostListProvider_unknownRole_keepsDeclaredRole() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getHostListProvider()).thenReturn(staticHostListProvider);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.UNKNOWN);
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    final Connection result = handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc);

    assertEquals(conn, result);
    verify(hostListProviderService, never()).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  /** A topology-backed provider supplies its own roles, so an unreadable role still fails. */
  @Test
  void topologyProvider_roleQueryFails_propagates() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(pluginService.getHostRole(conn)).thenThrow(new UnsupportedOperationException("no role query"));
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    assertThrows(UnsupportedOperationException.class,
        () -> handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc));
  }

  @Test
  void unknownRole_throws() throws SQLException {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.UNKNOWN);
    doThrow(new SQLException("cannot verify role")).when(ctx).logAndThrow(anyString());
    final VerifyRoleOnConnect handler = new VerifyRoleOnConnect(STRATEGY, true);

    assertThrows(SQLException.class,
        () -> handler.onConnect(ctx, PROTOCOL, writerHost, props, true, connectFunc));
  }
}
