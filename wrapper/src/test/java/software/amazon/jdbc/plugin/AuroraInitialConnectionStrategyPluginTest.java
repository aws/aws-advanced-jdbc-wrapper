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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;

public class AuroraInitialConnectionStrategyPluginTest {

  private static final String PROTOCOL = "jdbc:aws-wrapper:postgresql://";
  private static final String INSTANCE_HOST = "instance-1.xyz.us-east-1.rds.amazonaws.com";
  private static final String CLUSTER_HOST = "mydb.cluster-xyz.us-east-1.rds.amazonaws.com";
  private static final int RETRY_TIMEOUT_MS = 500;

  @Mock FullServicesContainer mockServicesContainer;
  @Mock PluginService mockPluginService;
  @Mock Dialect mockDialect;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock Connection mockConnection;

  private AutoCloseable closeable;
  private Properties props;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.filterAvailableHosts(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

    props = new Properties();
    AuroraInitialConnectionStrategyPlugin.OPEN_CONNECTION_RETRY_TIMEOUT_MS.set(props, String.valueOf(RETRY_TIMEOUT_MS));
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testConnect_nonInitialConnectionToInstance_passesThrough() throws SQLException {
    final HostSpec instanceHost = buildHost(INSTANCE_HOST, HostRole.READER);
    when(mockConnectFunc.call()).thenReturn(mockConnection);

    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(mockServicesContainer, props);
    final Connection conn = plugin.connect(PROTOCOL, instanceHost, props, false, mockConnectFunc);

    assertSame(mockConnection, conn);
    verify(mockConnectFunc, times(1)).call();
    verify(mockPluginService).setRoutedHostSpec(instanceHost);
    verify(mockPluginService, never()).connect(any(), any(), any());
  }

  @Test
  public void testConnect_nonInitialConnectionToInstance_networkExceptionPropagatesWithoutRetry()
      throws SQLException {
    final HostSpec instanceHost = buildHost(INSTANCE_HOST, HostRole.READER);
    final SQLException networkException = new SQLException("connection refused", "08001");
    when(mockConnectFunc.call()).thenThrow(networkException);
    when(mockPluginService.isNetworkException(any(Throwable.class), any())).thenReturn(true);
    when(mockPluginService.isLoginException(any(Throwable.class), any())).thenReturn(false);

    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(mockServicesContainer, props);

    final long startNano = System.nanoTime();
    final SQLException thrown = assertThrows(
        SQLException.class,
        () -> plugin.connect(PROTOCOL, instanceHost, props, false, mockConnectFunc));
    final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);

    assertSame(networkException, thrown);
    verify(mockConnectFunc, times(1)).call();
    verify(mockPluginService, never()).setRoutedHostSpec(any());
    assertTrue(elapsedMs < RETRY_TIMEOUT_MS,
        "expected an immediate failure but connect took " + elapsedMs + "ms");
  }

  @Test
  public void testConnect_initialConnectionToInstance_networkExceptionRetriesUntilTimeout() throws SQLException {
    final HostSpec instanceHost = buildHost(INSTANCE_HOST, HostRole.WRITER);
    when(mockConnectFunc.call()).thenThrow(new SQLException("connection refused", "08001"));
    when(mockPluginService.isNetworkException(any(Throwable.class), any())).thenReturn(true);
    when(mockPluginService.isLoginException(any(Throwable.class), any())).thenReturn(false);

    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(mockServicesContainer, props);

    final SQLException thrown = assertThrows(
        SQLException.class,
        () -> plugin.connect(PROTOCOL, instanceHost, props, true, mockConnectFunc));

    assertEquals(
        Messages.get(
            "AuroraInitialConnectionStrategyPlugin.timeout",
            new Object[] {
                (long) RETRY_TIMEOUT_MS,
                AuroraInitialConnectionStrategyPlugin.VERIFY_OPENED_CONNECTION_ROLE.name}),
        thrown.getMessage());
    verify(mockConnectFunc, atLeast(2)).call();
    verify(mockPluginService, never()).setRoutedHostSpec(any());
  }

  @Test
  public void testConnect_nonInitialConnectionToClusterEndpoint_substitutesWriter() throws SQLException {
    final HostSpec clusterHost = buildHost(CLUSTER_HOST, HostRole.WRITER);
    final HostSpec writerInstance = buildHost(INSTANCE_HOST, HostRole.WRITER);
    when(mockPluginService.getAllHosts()).thenReturn(Collections.singletonList(writerInstance));
    when(mockPluginService.connect(eq(writerInstance), any(), any())).thenReturn(mockConnection);

    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(mockServicesContainer, props);
    final Connection conn = plugin.connect(PROTOCOL, clusterHost, props, false, mockConnectFunc);

    assertSame(mockConnection, conn);
    verify(mockPluginService).connect(eq(writerInstance), any(), eq(plugin));
    verify(mockPluginService).setRoutedHostSpec(writerInstance);
    verify(mockConnectFunc, never()).call();
  }

  private static HostSpec buildHost(final String host, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(host).port(5432).role(role).build();
  }
}
