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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.ImportantEventService;

class AuroraInitialConnectionStrategyPluginTest {

  private static final String PROTOCOL = "jdbc:postgresql://";
  private static final String WRITER_CLUSTER_URL =
      "test-postgres.cluster-123456789012.us-east-2.rds.amazonaws.com";
  private static final String READER_CLUSTER_URL =
      "test-postgres.cluster-ro-123456789012.us-east-2.rds.amazonaws.com";
  private static final String INSTANCE_A_URL =
      "test-postgres-instance-a.123456789012.us-east-2.rds.amazonaws.com";
  private static final String INSTANCE_B_URL =
      "test-postgres-instance-b.123456789012.us-east-2.rds.amazonaws.com";
  private static final String INSTANCE_C_URL =
      "test-postgres-instance-c.123456789012.us-east-2.rds.amazonaws.com";

  @Mock FullServicesContainer mockContainer;
  @Mock PluginService mockPluginService;
  @Mock Dialect mockDialect;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock Connection mockConnectionA;
  @Mock Connection mockConnectionB;

  private final Properties properties = new Properties();
  private final AtomicReference<List<HostSpec>> allHostsRef =
      new AtomicReference<>(Collections.emptyList());
  private AutoCloseable closeable;

  private HostSpec writerClusterHost;
  private HostSpec readerClusterHost;
  private HostSpec writerA;
  private HostSpec readerA;
  private HostSpec writerB;
  private HostSpec readerB;
  private HostSpec readerC;

  @BeforeEach
  void init() throws SQLException {
    this.closeable = MockitoAnnotations.openMocks(this);
    this.properties.clear();
    this.properties.setProperty(
        AuroraInitialConnectionStrategyPlugin.OPEN_CONNECTION_RETRY_INTERVAL_MS.name, "10");
    this.properties.setProperty(
        AuroraInitialConnectionStrategyPlugin.OPEN_CONNECTION_RETRY_TIMEOUT_MS.name, "3000");

    this.writerClusterHost = buildHost(WRITER_CLUSTER_URL, HostRole.WRITER);
    this.readerClusterHost = buildHost(READER_CLUSTER_URL, HostRole.READER);
    this.writerA = buildHost(INSTANCE_A_URL, HostRole.WRITER);
    this.readerA = buildHost(INSTANCE_A_URL, HostRole.READER);
    this.writerB = buildHost(INSTANCE_B_URL, HostRole.WRITER);
    this.readerB = buildHost(INSTANCE_B_URL, HostRole.READER);
    this.readerC = buildHost(INSTANCE_C_URL, HostRole.READER);

    this.allHostsRef.set(Arrays.asList(this.writerA, this.readerB));

    when(this.mockContainer.getPluginService()).thenReturn(this.mockPluginService);
    when(this.mockContainer.getImportantEventService()).thenReturn(new ImportantEventService());
    when(this.mockPluginService.getDialect()).thenReturn(this.mockDialect);
    when(this.mockPluginService.getTargetDriverDialect()).thenReturn(this.mockTargetDriverDialect);
    when(this.mockDialect.filterAvailableHosts(anyList(), any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(this.mockPluginService.getAllHosts()).thenAnswer(invocation -> this.allHostsRef.get());
    when(this.mockPluginService.getHosts()).thenAnswer(invocation -> this.allHostsRef.get());

    doAnswer(invocation -> {
      final HostSpec target = invocation.getArgument(0);
      final HostAvailability availability = invocation.getArgument(1);
      for (final HostSpec host : this.allHostsRef.get()) {
        if (host.getHost().equalsIgnoreCase(target.getHost())) {
          host.setAvailability(availability);
        }
      }
      return null;
    }).when(this.mockPluginService).setAvailability(any(HostSpec.class), any(HostAvailability.class));
  }

  @AfterEach
  void cleanUp() throws Exception {
    this.closeable.close();
  }

  private static HostSpec buildHost(final String host, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(host)
        .port(5432)
        .role(role)
        .availability(HostAvailability.AVAILABLE)
        .build();
  }

  @Test
  void testConnect_demotedWriterNetworkError_usesUpdatedWriterAfterRefresh() throws SQLException {
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException networkEx = new SQLException("Connection refused", "08001");

    when(this.mockPluginService.isNetworkException(networkEx, this.mockTargetDriverDialect))
        .thenReturn(true);
    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenThrow(networkEx);
    when(this.mockPluginService.forceRefreshHostList(false, 0))
        .thenAnswer(invocation -> {
          this.allHostsRef.set(Arrays.asList(this.readerA, this.writerB));
          return true;
        });
    when(this.mockPluginService.connect(eq(this.writerB), eq(this.properties), eq(plugin)))
        .thenReturn(this.mockConnectionB);
    when(this.mockPluginService.getHostRole(this.mockConnectionB)).thenReturn(HostRole.WRITER);

    final Connection actual = plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc);

    assertSame(this.mockConnectionB, actual);
    verify(this.mockPluginService, times(1)).connect(eq(this.writerA), eq(this.properties), eq(plugin));
    verify(this.mockPluginService, times(1)).connect(eq(this.writerB), eq(this.properties), eq(plugin));
    verify(this.mockPluginService).setAvailability(this.writerA, HostAvailability.NOT_AVAILABLE);
    verify(this.mockPluginService, atLeastOnce()).forceRefreshHostList(false, 0);
    verify(this.mockPluginService, never()).forceRefreshHostList();
    verify(this.mockPluginService).setRoutedHostSpec(this.writerB);
    verify(this.mockConnectFunc, never()).call();
  }

  @Test
  void testConnect_demotedWriterNetworkError_topologyUpdatesDuringRetryDelay() throws SQLException {
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException networkEx = new SQLException("Connection refused", "08001");
    final AtomicInteger refreshCalls = new AtomicInteger();

    when(this.mockPluginService.isNetworkException(networkEx, this.mockTargetDriverDialect))
        .thenReturn(true);
    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenThrow(networkEx);
    when(this.mockPluginService.forceRefreshHostList(false, 0))
        .thenAnswer(invocation -> {
          // 1st call wakes monitor while storage still holds old writer;
          // 2nd call (after delay) picks up new topology published during the sleep.
          if (refreshCalls.incrementAndGet() >= 2) {
            this.allHostsRef.set(Arrays.asList(this.readerA, this.writerB));
          }
          return true;
        });
    when(this.mockPluginService.connect(eq(this.writerB), eq(this.properties), eq(plugin)))
        .thenReturn(this.mockConnectionB);
    when(this.mockPluginService.getHostRole(this.mockConnectionB)).thenReturn(HostRole.WRITER);

    final Connection actual = plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc);

    assertSame(this.mockConnectionB, actual);
    verify(this.mockPluginService, times(1)).connect(eq(this.writerA), eq(this.properties), eq(plugin));
    verify(this.mockPluginService, times(2)).forceRefreshHostList(false, 0);
    verify(this.mockPluginService).setRoutedHostSpec(this.writerB);
  }

  @Test
  void testConnect_networkErrorTightSpinPrevented_honorsRetryInterval() throws SQLException {
    this.properties.setProperty(
        AuroraInitialConnectionStrategyPlugin.OPEN_CONNECTION_RETRY_INTERVAL_MS.name, "80");
    this.properties.setProperty(
        AuroraInitialConnectionStrategyPlugin.OPEN_CONNECTION_RETRY_TIMEOUT_MS.name, "250");
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException networkEx = new SQLException("Connection refused", "08001");
    final AtomicInteger attempts = new AtomicInteger();

    when(this.mockPluginService.isNetworkException(networkEx, this.mockTargetDriverDialect))
        .thenReturn(true);
    when(this.mockPluginService.forceRefreshHostList(false, 0)).thenReturn(true);
    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenAnswer(invocation -> {
          attempts.incrementAndGet();
          throw networkEx;
        });

    assertThrows(SQLException.class, () -> plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc));

    // With an 80ms retry delay inside a 250ms window, attempts stay bounded (~3-4) instead of spinning.
    assertTrue(attempts.get() >= 1 && attempts.get() <= 5,
        "Expected bounded retry attempts with backoff, got: " + attempts.get());
  }

  @Test
  void testConnect_demotedWriterReadOnlyError_refreshesAndSwitchesToNewWriter() throws SQLException {
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException readOnlyEx = new SQLException("read-only transaction", "25006");

    when(this.mockPluginService.isNetworkException(readOnlyEx, this.mockTargetDriverDialect))
        .thenReturn(false);
    when(this.mockPluginService.isReadOnlyConnectionException(readOnlyEx, this.mockTargetDriverDialect))
        .thenReturn(true);
    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenThrow(readOnlyEx);
    when(this.mockPluginService.forceRefreshHostList(false, 0))
        .thenAnswer(invocation -> {
          this.allHostsRef.set(Arrays.asList(this.readerA, this.writerB));
          return true;
        });
    when(this.mockPluginService.connect(eq(this.writerB), eq(this.properties), eq(plugin)))
        .thenReturn(this.mockConnectionB);
    when(this.mockPluginService.getHostRole(this.mockConnectionB)).thenReturn(HostRole.WRITER);

    final Connection actual = plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc);

    assertSame(this.mockConnectionB, actual);
    verify(this.mockPluginService, times(1)).connect(eq(this.writerA), eq(this.properties), eq(plugin));
    verify(this.mockPluginService, never()).setAvailability(any(), eq(HostAvailability.NOT_AVAILABLE));
    verify(this.mockPluginService, atLeastOnce()).forceRefreshHostList(false, 0);
    verify(this.mockPluginService).setRoutedHostSpec(this.writerB);
  }

  @Test
  void testConnect_readerSubstitutionNetworkError_marksFailedReaderNotAvailable() throws SQLException {
    this.allHostsRef.set(Arrays.asList(this.writerA, this.readerB, this.readerC));
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException networkEx = new SQLException("Connection refused", "08001");

    when(this.mockPluginService.acceptsStrategy(HostRole.READER, "random")).thenReturn(true);
    when(this.mockPluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), anyString()))
        .thenAnswer(invocation -> {
          final List<HostSpec> hosts = invocation.getArgument(0);
          return hosts.stream()
              .filter(h -> h.getRole() == HostRole.READER
                  && h.getAvailability() == HostAvailability.AVAILABLE)
              .findFirst()
              .orElse(null);
        });
    when(this.mockPluginService.isNetworkException(networkEx, this.mockTargetDriverDialect))
        .thenReturn(true);
    when(this.mockPluginService.forceRefreshHostList(false, 0)).thenReturn(true);
    when(this.mockPluginService.connect(eq(this.readerB), eq(this.properties), eq(plugin)))
        .thenThrow(networkEx);
    when(this.mockPluginService.connect(eq(this.readerC), eq(this.properties), eq(plugin)))
        .thenReturn(this.mockConnectionB);
    when(this.mockPluginService.getHostRole(this.mockConnectionB)).thenReturn(HostRole.READER);

    final Connection actual = plugin.connect(
        PROTOCOL, this.readerClusterHost, this.properties, true, this.mockConnectFunc);

    assertSame(this.mockConnectionB, actual);
    verify(this.mockPluginService).setAvailability(this.readerB, HostAvailability.NOT_AVAILABLE);
    assertEquals(HostAvailability.NOT_AVAILABLE, this.readerB.getAvailability());
    verify(this.mockPluginService, times(1)).connect(eq(this.readerB), eq(this.properties), eq(plugin));
    verify(this.mockPluginService, times(1)).connect(eq(this.readerC), eq(this.properties), eq(plugin));
    verify(this.mockPluginService).setRoutedHostSpec(this.readerC);
  }

  @Test
  void testConnect_loginException_throwsImmediatelyWithoutMarkingUnavailableOrRetrying()
      throws SQLException {
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException loginEx = new SQLException("Invalid password", "28P01");

    when(this.mockPluginService.isLoginException(loginEx, this.mockTargetDriverDialect))
        .thenReturn(true);
    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenThrow(loginEx);

    final SQLException thrown = assertThrows(SQLException.class, () -> plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc));

    assertSame(loginEx, thrown);
    verify(this.mockPluginService, times(1)).connect(eq(this.writerA), eq(this.properties), eq(plugin));
    verify(this.mockPluginService, never()).setAvailability(any(), any());
    verify(this.mockPluginService, never()).forceRefreshHostList(anyBoolean(), anyLong());
  }

  @Test
  void testConnect_nonRetryableSqlException_marksHostNotAvailableAndThrows() throws SQLException {
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException sqlEx = new SQLException("Unsupported query", "XX000");

    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenThrow(sqlEx);

    final SQLException thrown = assertThrows(SQLException.class, () -> plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc));

    assertSame(sqlEx, thrown);
    verify(this.mockPluginService, times(1)).connect(eq(this.writerA), eq(this.properties), eq(plugin));
    verify(this.mockPluginService).setAvailability(this.writerA, HostAvailability.NOT_AVAILABLE);
    verify(this.mockPluginService, never()).forceRefreshHostList(anyBoolean(), anyLong());
  }

  @Test
  void testConnect_refreshThrowsSqlExceptionDuringNetworkRetry_continuesAndSucceeds()
      throws SQLException {
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);
    final SQLException networkEx = new SQLException("Connection refused", "08001");
    final AtomicInteger refreshCalls = new AtomicInteger();

    when(this.mockPluginService.isNetworkException(networkEx, this.mockTargetDriverDialect))
        .thenReturn(true);
    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenThrow(networkEx);
    when(this.mockPluginService.forceRefreshHostList(false, 0))
        .thenAnswer(invocation -> {
          if (refreshCalls.incrementAndGet() == 1) {
            throw new SQLException("Transient topology refresh failure");
          }
          this.allHostsRef.set(Arrays.asList(this.readerA, this.writerB));
          return true;
        });
    when(this.mockPluginService.connect(eq(this.writerB), eq(this.properties), eq(plugin)))
        .thenReturn(this.mockConnectionB);
    when(this.mockPluginService.getHostRole(this.mockConnectionB)).thenReturn(HostRole.WRITER);

    final Connection actual = plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc);

    assertSame(this.mockConnectionB, actual);
    verify(this.mockPluginService, times(2)).forceRefreshHostList(false, 0);
    verify(this.mockPluginService).setRoutedHostSpec(this.writerB);
  }

  @Test
  void testConnect_wrongRoleOnConnectedInstance_forcesRefreshAndSwitchesToNewWriter()
      throws SQLException {
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.mockContainer, this.properties);

    when(this.mockPluginService.connect(eq(this.writerA), eq(this.properties), eq(plugin)))
        .thenReturn(this.mockConnectionA);
    when(this.mockPluginService.getHostRole(this.mockConnectionA)).thenReturn(HostRole.READER);
    doAnswer(invocation -> {
      this.allHostsRef.set(Arrays.asList(this.readerA, this.writerB));
      return null;
    }).when(this.mockPluginService).forceRefreshHostList();
    when(this.mockPluginService.connect(eq(this.writerB), eq(this.properties), eq(plugin)))
        .thenReturn(this.mockConnectionB);
    when(this.mockPluginService.getHostRole(this.mockConnectionB)).thenReturn(HostRole.WRITER);

    final Connection actual = plugin.connect(
        PROTOCOL, this.writerClusterHost, this.properties, true, this.mockConnectFunc);

    assertSame(this.mockConnectionB, actual);
    verify(this.mockConnectionA).close();
    verify(this.mockPluginService).forceRefreshHostList();
    verify(this.mockPluginService).setRoutedHostSpec(this.writerB);
  }
}
