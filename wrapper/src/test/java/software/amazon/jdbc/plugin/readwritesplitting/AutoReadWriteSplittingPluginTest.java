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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.states.SessionStateService;

public class AutoReadWriteSplittingPluginTest {

  private AutoCloseable closeable;
  private AutoReadWriteSplittingPlugin plugin;
  private PluginCallContext callContext;

  @Mock PluginService mockPluginService;
  @Mock SessionStateService mockSessionStateService;
  @Mock Connection mockWriterConn;
  @Mock Connection mockReaderConn1;
  @Mock Connection mockReaderConn2;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer").port(5432).role(HostRole.WRITER).build();
  private final HostSpec readerHost1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader1").port(5432).role(HostRole.READER).build();
  private final HostSpec readerHost2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader2").port(5432).role(HostRole.READER).build();

  private AutoReadWriteSplittingPlugin balancingPlugin(final boolean includeWriter) {
    final Properties props = new Properties();
    props.setProperty(AutoReadWriteSplittingPlugin.QUERY_LEVEL_LOAD_BALANCING.name, "true");
    if (includeWriter) {
      props.setProperty(AutoReadWriteSplittingPlugin.LOAD_BALANCING_INCLUDE_WRITER.name, "true");
    }
    return new AutoReadWriteSplittingPlugin(mockPluginService, props);
  }

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    when(mockPluginService.getCallContext()).thenReturn(callContext);
    when(mockPluginService.getSessionStateService()).thenReturn(mockSessionStateService);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.empty());
    plugin = new AutoReadWriteSplittingPlugin(mockPluginService, new Properties());
  }

  @AfterEach
  void cleanUp() throws Exception {
    callContext.reset();
    closeable.close();
  }

  private void setContextQueryType(QueryType queryType) {
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, queryType);
  }

  private void setContextRoutingHint(RoutingHint hint) {
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, hint);
  }

  @Test
  void test_subscribedMethods_includesExecuteAndParentMethods() {
    Set<String> methods = plugin.getSubscribedMethods();
    assertTrue(methods.contains("PreparedStatement.executeQuery"));
    assertTrue(methods.contains("PreparedStatement.executeUpdate"));
    assertTrue(methods.contains("PreparedStatement.execute"));
    assertTrue(methods.contains("PreparedStatement.executeBatch"));
    assertTrue(methods.contains("Statement.executeQuery"));
    assertTrue(methods.contains("Statement.executeUpdate"));
    assertTrue(methods.contains("Statement.execute"));
    assertTrue(methods.contains("Statement.executeBatch"));
    assertTrue(methods.contains("CallableStatement.execute"));
    assertTrue(methods.contains("Connection.setReadOnly"));
    assertTrue(methods.contains("Connection.setAutoCommit"));
  }

  @Test
  void test_selectQuery_routesToReader() {
    setContextQueryType(QueryType.SELECT);

    assertTrue(plugin.shouldRouteToReader());
  }

  @Test
  void test_insertQuery_routesToWriter() {
    setContextQueryType(QueryType.INSERT);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_updateQuery_routesToWriter() {
    setContextQueryType(QueryType.UPDATE);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_deleteQuery_routesToWriter() {
    setContextQueryType(QueryType.DELETE);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_selectForUpdate_routesToWriter() {
    setContextQueryType(QueryType.SELECT);
    callContext.setAttribute(SqlContextKeys.FOR_UPDATE, true);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_writerHint_overridesSelectToWriter() {
    setContextQueryType(QueryType.SELECT);
    setContextRoutingHint(RoutingHint.WRITER);

    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_readerHint_overridesToReader() {
    setContextQueryType(QueryType.INSERT);
    setContextRoutingHint(RoutingHint.READER);

    assertTrue(plugin.shouldRouteToReader());
  }

  @Test
  void test_noContext_fallsBackToWriter() {
    // No context attributes set — falls back to writer to be safe
    assertFalse(plugin.shouldRouteToReader());
  }

  @Test
  void test_shouldKeepCurrentConnection_falseWhenAutoCommitOnAndNoTransaction() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.of(true));

    assertFalse(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_shouldKeepCurrentConnection_falseWhenAutoCommitUnset() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.empty());

    assertFalse(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_shouldKeepCurrentConnection_trueWhenAutoCommitOff() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.of(false));

    assertTrue(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_shouldKeepCurrentConnection_trueWhenInTransaction() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(true);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.of(true));

    assertTrue(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_shouldKeepCurrentConnection_trueWhenKeepHint() throws Exception {
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockSessionStateService.getAutoCommit()).thenReturn(Optional.of(true));
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.KEEP);

    assertTrue(plugin.shouldKeepCurrentConnection());
  }

  @Test
  void test_queryLevelLoadBalancing_disabledByDefault() {
    assertFalse(plugin.isQueryLevelLoadBalancingEnabled());
  }

  @Test
  void test_queryLevelLoadBalancing_enabledByProperty() {
    assertTrue(balancingPlugin(false).isQueryLevelLoadBalancingEnabled());
  }

  @Test
  void test_loadBalancingCandidateRole_readerByDefault() {
    assertEquals(HostRole.READER, balancingPlugin(false).getLoadBalancingCandidateRole());
  }

  @Test
  void test_loadBalancingCandidateRole_nullWhenIncludeWriter() {
    assertNull(balancingPlugin(true).getLoadBalancingCandidateRole());
  }

  @Test
  void test_switchToBalancedReader_switchesFromWriterToReader() throws Exception {
    final AutoReadWriteSplittingPlugin p = balancingPlugin(false);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockWriterConn.isClosed()).thenReturn(false);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHost);
    when(mockPluginService.getHosts()).thenReturn(Arrays.asList(writerHost, readerHost1, readerHost2));
    when(mockPluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), anyString()))
        .thenReturn(readerHost1);
    when(mockPluginService.connect(eq(readerHost1), any(), any())).thenReturn(mockReaderConn1);
    when(mockPluginService.isPooledConnection()).thenReturn(false);

    p.switchToBalancedReader();

    verify(mockPluginService).setCurrentConnection(mockReaderConn1, readerHost1);
    // The writer connection is preserved for later write routing and never closed here.
    assertSame(mockWriterConn, p.getWriterConnection());
    verify(mockWriterConn, never()).close();
  }

  @Test
  void test_switchToBalancedReader_closesPreviousReaderWhenBalancingReaderToReader() throws Exception {
    final AutoReadWriteSplittingPlugin p = balancingPlugin(false);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(mockReaderConn1.isClosed()).thenReturn(false);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHost1);
    when(mockPluginService.getHosts()).thenReturn(Arrays.asList(writerHost, readerHost1, readerHost2));
    when(mockPluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), anyString()))
        .thenReturn(readerHost2);
    when(mockPluginService.connect(eq(readerHost2), any(), any())).thenReturn(mockReaderConn2);
    when(mockPluginService.isPooledConnection()).thenReturn(false);

    p.switchToBalancedReader();

    verify(mockPluginService).setCurrentConnection(mockReaderConn2, readerHost2);
    // The reader we balanced away from is returned to the pool / closed.
    verify(mockReaderConn1).close();
  }

  @Test
  void test_switchToBalancedReader_noSwitchWhenAlreadyOnSelectedHost() throws Exception {
    final AutoReadWriteSplittingPlugin p = balancingPlugin(false);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(mockReaderConn1.isClosed()).thenReturn(false);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHost1);
    when(mockPluginService.getHosts()).thenReturn(Arrays.asList(writerHost, readerHost1, readerHost2));
    when(mockPluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), anyString()))
        .thenReturn(readerHost1);

    p.switchToBalancedReader();

    verify(mockPluginService, never()).setCurrentConnection(any(), any());
    verify(mockPluginService, never()).connect(any(), any(), any());
  }

  @Test
  void test_switchToBalancedReader_includeWriterUsesNullRole() throws Exception {
    final AutoReadWriteSplittingPlugin p = balancingPlugin(true);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(mockReaderConn1.isClosed()).thenReturn(false);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHost1);
    when(mockPluginService.getHosts()).thenReturn(Arrays.asList(writerHost, readerHost1, readerHost2));
    when(mockPluginService.getHostSpecByStrategy(anyList(), isNull(), anyString()))
        .thenReturn(writerHost);
    when(mockPluginService.connect(eq(writerHost), any(), any())).thenReturn(mockWriterConn);
    when(mockPluginService.isPooledConnection()).thenReturn(false);

    p.switchToBalancedReader();

    // With loadBalancingIncludeWriter=true the selector is queried with a null role so the
    // writer instance is eligible as a balancing candidate.
    verify(mockPluginService).getHostSpecByStrategy(anyList(), isNull(), anyString());
    verify(mockPluginService).setCurrentConnection(mockWriterConn, writerHost);
  }
}
