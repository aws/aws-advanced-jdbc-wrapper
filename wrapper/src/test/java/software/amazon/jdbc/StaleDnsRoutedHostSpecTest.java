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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPluginFactory;
import software.amazon.jdbc.plugin.OpenedConnectionTracker;
import software.amazon.jdbc.plugin.efm.v2.HostMonitoringConnectionPluginV2;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Right after a writer failover the cluster writer endpoint may still resolve to the old writer,
 * which is now a reader. The stale DNS helper reconnects to the real writer. These tests verify
 * that the host recorded for the first connection is not carried over to the writer connection.
 */
public class StaleDnsRoutedHostSpecTest {

  private static final String PROTOCOL = "jdbc:aws-wrapper:mysql://";
  private static final String CLUSTER_DNS = "mydb.cluster-abc123.eu-central-1.rds.amazonaws.com";
  private static final String OLD_WRITER_DNS = "mydb-instance-old.abc123.eu-central-1.rds.amazonaws.com";
  private static final String NEW_WRITER_DNS = "mydb-instance-new.abc123.eu-central-1.rds.amazonaws.com";

  @Mock FullServicesContainer mockServicesContainer;
  @Mock PluginService mockPluginService;
  @Mock PluginManagerService mockPluginManagerService;
  @Mock ConnectionProvider mockConnectionProvider;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock HostListProviderService mockHostListProviderService;
  @Mock Connection readerConn;
  @Mock Connection writerConn;

  private AutoCloseable closeable;

  private static HostSpec hostSpec(final String host, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(host).port(3306).role(role).build();
  }

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getPluginManagerService()).thenReturn(mockPluginManagerService);
    when(mockServicesContainer.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  private final HostSpec clusterHost = hostSpec(CLUSTER_DNS, HostRole.WRITER);
  private final HostSpec oldWriter = hostSpec(OLD_WRITER_DNS, HostRole.READER);
  private final HostSpec newWriter = hostSpec(NEW_WRITER_DNS, HostRole.WRITER);
  private final Properties props = new Properties();
  // PluginServiceImpl keeps the routed host in a plain field.
  private final AtomicReference<HostSpec> routedHostSpec = new AtomicReference<>();
  private HostMonitoringConnectionPluginV2 efmPlugin;
  private boolean writerInstanceIsDown;

  private ConnectionPluginManager createManager(final boolean withTracker, final boolean isInitialConnection)
      throws SQLException {
    final List<HostSpec> topology = Arrays.asList(newWriter, oldWriter);
    when(mockPluginService.getRoutedHostSpec()).thenAnswer(inv -> routedHostSpec.get());
    doAnswer(inv -> {
      routedHostSpec.set(inv.getArgument(0));
      return null;
    }).when(mockPluginService).setRoutedHostSpec(any());

    when(mockPluginService.getAllHosts()).thenReturn(topology);
    when(mockPluginService.getHosts()).thenReturn(topology);
    when(mockPluginService.getHostRole(readerConn)).thenReturn(HostRole.READER);
    when(mockPluginService.getHostRole(writerConn)).thenReturn(HostRole.WRITER);
    when(mockPluginService.identifyConnection(eq(readerConn), any(HostSpec.class))).thenReturn(oldWriter);
    when(mockPluginService.identifyConnection(eq(writerConn), any(HostSpec.class))).thenReturn(newWriter);
    when(mockPluginService.identifyConnection(readerConn)).thenReturn(oldWriter);
    when(mockPluginService.identifyConnection(writerConn)).thenReturn(newWriter);
    when(mockHostListProviderService.getInitialConnectionHostSpec()).thenReturn(clusterHost);

    // Stands in for the driver: the cluster endpoint still resolves to the old writer.
    final ConnectionPlugin driverPlugin = new AbstractConnectionPlugin() {
      @Override
      public Set<String> getSubscribedMethods() {
        return Collections.singleton(JdbcMethod.CONNECT.methodName);
      }

      @Override
      public Connection connect(final String driverProtocol, final HostSpec hostSpec, final Properties props,
          final boolean isInitialConnection, final JdbcCallable<Connection, SQLException> connectFunc)
          throws SQLException {
        if (CLUSTER_DNS.equals(hostSpec.getHost())) {
          return readerConn;
        }
        if (writerInstanceIsDown) {
          throw new SQLException("writer instance is down");
        }
        return writerConn;
      }
    };

    final FailoverConnectionPlugin failoverPlugin = new FailoverConnectionPlugin(mockServicesContainer, props);
    failoverPlugin.initHostProvider(PROTOCOL, PROTOCOL + CLUSTER_DNS, props, mockHostListProviderService, () -> null);
    efmPlugin = new HostMonitoringConnectionPluginV2(mockServicesContainer, props);

    final List<ConnectionPlugin> plugins = new ArrayList<>();
    if (withTracker) {
      plugins.add(new AuroraConnectionTrackerPluginFactory().getInstance(mockServicesContainer, props));
    }
    plugins.addAll(Arrays.asList(failoverPlugin, efmPlugin, driverPlugin));
    final ConnectionPluginManager manager =
        new ConnectionPluginManager(mockConnectionProvider, null, props, plugins, mockTelemetryFactory);

    // PluginServiceImpl.connect re-enters the plugin chain.
    when(mockPluginService.connect(any(HostSpec.class), any(Properties.class))).thenAnswer(
        inv -> manager.connect(PROTOCOL, inv.getArgument(0), inv.getArgument(1), isInitialConnection, null));
    return manager;
  }

  @Test
  void newConnectionOpenedThroughStaleClusterDnsIsRecordedAsNewWriter() throws SQLException {
    try (MockedConstruction<OpenedConnectionTracker> trackers = mockConstruction(OpenedConnectionTracker.class)) {
      final ConnectionPluginManager manager = createManager(true, true);

      final Connection conn = manager.connect(PROTOCOL, clusterHost, props, true, null);

      // The stale DNS correction did run.
      assertSame(writerConn, conn);
      verify(readerConn).close();

      // ConnectionWrapper: the routed host becomes the current host of the new connection.
      final HostSpec routedAfterConnect = routedHostSpec.get();
      final HostSpec currentHostSpec = routedAfterConnect != null ? routedAfterConnect : clusterHost;
      routedHostSpec.set(null);
      when(mockPluginService.getCurrentConnection()).thenReturn(conn);
      when(mockPluginService.getCurrentHostSpec()).thenReturn(currentHostSpec);

      final ArgumentCaptor<HostSpec> trackedHosts = ArgumentCaptor.forClass(HostSpec.class);
      verify(trackers.constructed().get(0), atLeastOnce())
          .populateOpenedConnectionQueue(trackedHosts.capture(), eq(writerConn));

      assertAll(
          () -> assertEquals(NEW_WRITER_DNS, routedAfterConnect == null ? null : routedAfterConnect.getHost(),
              "routed host after reconnecting to the writer"),
          () -> assertEquals(NEW_WRITER_DNS, efmPlugin.getMonitoringHostSpec().getHost(),
              "EFM monitoring host of the writer connection"),
          () -> assertEquals(Collections.singleton(NEW_WRITER_DNS),
              trackedHosts.getAllValues().stream().map(HostSpec::getHost).collect(Collectors.toSet()),
              "hosts the writer connection is tracked under"));
    }
  }

  @Test
  void newConnectionWithoutTrackerPluginIsRecordedAsNewWriter() throws SQLException {
    final ConnectionPluginManager manager = createManager(false, true);

    final Connection conn = manager.connect(PROTOCOL, clusterHost, props, true, null);

    assertSame(writerConn, conn);
    assertEquals(NEW_WRITER_DNS, routedHostSpec.get() == null ? null : routedHostSpec.get().getHost());
  }

  @Test
  void failedWriterReconnectDoesNotKeepStaleRoutedHost() throws SQLException {
    writerInstanceIsDown = true;
    final ConnectionPluginManager manager = createManager(true, true);

    assertThrows(SQLException.class, () -> manager.connect(PROTOCOL, clusterHost, props, true, null));

    assertNull(routedHostSpec.get(), "routed host after a failed reconnect to the writer");
  }

  @Test
  void nonInitialReconnectWithoutTrackerPluginDoesNotLeaveRoutedHost() throws SQLException {
    final ConnectionPluginManager manager = createManager(false, false);

    final Connection conn = manager.connect(PROTOCOL, clusterHost, props, false, null);

    assertSame(writerConn, conn);
    assertNull(routedHostSpec.get(), "routed host left on an already opened connection");
  }
}
