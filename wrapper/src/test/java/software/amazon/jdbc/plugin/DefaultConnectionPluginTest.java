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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

class DefaultConnectionPluginTest {

  private DefaultConnectionPlugin plugin;

  @Mock PluginService pluginService;
  @Mock ConnectionProvider connectionProvider;
  @Mock PluginManagerService pluginManagerService;
  @Mock JdbcCallable<Void, SQLException> mockSqlFunction;
  @Mock Connection conn;
  @Mock Connection oldConn;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;


  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);

    when(pluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    // noinspection unchecked
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);

    plugin = new DefaultConnectionPlugin(pluginService, connectionProvider, pluginManagerService);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @ParameterizedTest
  @MethodSource("multiStatementQueries")
  void testParseMultiStatementQueries(final String sql, final List<String> expected) {
    final List<String> actual = plugin.parseMultiStatementQueries(sql);
    assertEquals(expected, actual);
  }

  @Test
  void testExecute_closeCurrentConnection() throws SQLException {
    when(this.pluginService.getCurrentConnection()).thenReturn(conn);
    plugin.execute(Void.class, SQLException.class, conn, "Connection.close", mockSqlFunction, new Object[]{});
    verify(pluginManagerService, times(1)).setInTransaction(false);
  }

  @Test
  void testExecute_closeOldConnection() throws SQLException {
    when(this.pluginService.getCurrentConnection()).thenReturn(conn);
    plugin.execute(Void.class, SQLException.class, oldConn, "Connection.close", mockSqlFunction, new Object[]{});
    verify(pluginManagerService, never()).setInTransaction(anyBoolean());
  }

  private static Stream<Arguments> multiStatementQueries() {
    return Stream.of(
        Arguments.of("", new ArrayList<String>()),
        Arguments.of(null, new ArrayList<String>()),
        Arguments.of("  ", new ArrayList<String>()),
        Arguments.of("some  \t  \r  \n   query;", Collections.singletonList("some query")),
        Arguments.of("some\t\t\r\n query;query2", Arrays.asList("some query", "query2"))
    );
  }
}
