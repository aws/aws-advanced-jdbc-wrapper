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

package software.amazon.jdbc.wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Tests which method the prepared and callable statement wrappers report for each
 * {@code addBatch} overload: {@code addBatch(String)} adds the SQL given as its argument rather than
 * the statement's prepared SQL, so it is reported as {@code Statement.addBatch}, while the
 * no-argument {@code addBatch()} run for every row of a batch keeps its own method and skips the
 * pipeline when no plugin subscribes to it.
 */
public class StatementAddBatchRoutingTest {

  @Mock private ConnectionWrapper connectionWrapper;
  @Mock private FullServicesContainer servicesContainer;
  @Mock private ConnectionPluginManager pluginManager;
  @Mock private PluginService pluginService;
  @Mock private PluginManagerService pluginManagerService;
  @Mock private TelemetryFactory telemetryFactory;

  private final List<JdbcMethod> pipelinedMethods = new ArrayList<>();
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);

    final Connection connection = mock(Connection.class);
    when(connectionWrapper.getCurrentConnection()).thenReturn(connection);
    when(connectionWrapper.getServicesContainer()).thenReturn(servicesContainer);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getPluginManagerService()).thenReturn(pluginManagerService);
    when(servicesContainer.getConnectionPluginManager()).thenReturn(pluginManager);
    when(pluginService.getCallContext()).thenReturn(new PluginCallContext());
    when(pluginManager.getTelemetryFactory()).thenReturn(telemetryFactory);

    // Only Statement.addBatch has a subscriber, as with the sqlParser and kmsEncryption plugins.
    when(pluginManager.mustUsePipeline(any()))
        .thenAnswer(i -> i.getArgument(0) == JdbcMethod.STATEMENT_ADDBATCH);
    when(pluginManager.execute(any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> {
          pipelinedMethods.add(i.getArgument(3));
          return ((JdbcCallable<?, ?>) i.getArgument(4)).call();
        });
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void preparedStatementAddBatchWithSql_isReportedAsStatementAddBatch() throws Exception {
    final PreparedStatement target = mock(PreparedStatement.class);
    final PreparedStatement statement =
        (PreparedStatement) WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class, target, connectionWrapper, pluginManager);

    statement.addBatch("UPDATE users SET ssn = 'x'");

    assertEquals(1, pipelinedMethods.size());
    assertEquals(JdbcMethod.STATEMENT_ADDBATCH, pipelinedMethods.get(0));
    verify(target).addBatch("UPDATE users SET ssn = 'x'");
  }

  @Test
  void callableStatementAddBatchWithSql_isReportedAsStatementAddBatch() throws Exception {
    final CallableStatement target = mock(CallableStatement.class);
    final CallableStatement statement =
        (CallableStatement) WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class, target, connectionWrapper, pluginManager);

    statement.addBatch("UPDATE users SET ssn = 'x'");

    assertEquals(1, pipelinedMethods.size());
    assertEquals(JdbcMethod.STATEMENT_ADDBATCH, pipelinedMethods.get(0));
    verify(target).addBatch("UPDATE users SET ssn = 'x'");
  }

  @Test
  void noArgumentAddBatch_skipsThePipeline() throws Exception {
    final PreparedStatement preparedTarget = mock(PreparedStatement.class);
    final CallableStatement callableTarget = mock(CallableStatement.class);
    final PreparedStatement prepared =
        (PreparedStatement) WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class, preparedTarget, connectionWrapper, pluginManager);
    final CallableStatement callable =
        (CallableStatement) WrapperUtils.wrapWithProxyIfNeeded(
            Statement.class, callableTarget, connectionWrapper, pluginManager);

    prepared.addBatch();
    callable.addBatch();

    assertTrue(pipelinedMethods.isEmpty(), pipelinedMethods.toString());
    verify(preparedTarget).addBatch();
    verify(callableTarget).addBatch();
  }
}
