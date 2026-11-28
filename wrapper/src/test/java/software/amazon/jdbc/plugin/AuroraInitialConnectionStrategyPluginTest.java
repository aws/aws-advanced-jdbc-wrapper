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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.ImportantEventService;

class AuroraInitialConnectionStrategyPluginTest {

  private static final HostSpec OHIO_ENDPOINT =
      host("global-ohio-mysql.cluster-abc.us-east-2.rds.amazonaws.com", HostRole.WRITER);
  private static final HostSpec OHIO_1_READER = host("global-ohio-mysql-instance-1.abc.us-east-2.rds.amazonaws.com",
      HostRole.READER);
  private static final HostSpec OHIO_1_WRITER = host(OHIO_1_READER.getHost(), HostRole.WRITER);
  private static final HostSpec OHIO_2 = host("global-ohio-mysql-instance-2.abc.us-east-2.rds.amazonaws.com",
      HostRole.READER);
  private static final HostSpec OREGON_1 = host("global-oregon-mysql-instance-1.xyz.us-west-2.rds.amazonaws.com",
      HostRole.WRITER);

  private final AtomicReference<List<HostSpec>> topology = new AtomicReference<>();
  private final AtomicReference<List<HostSpec>> topologyAfterRefresh = new AtomicReference<>();
  private PluginService pluginService;
  private FullServicesContainer servicesContainer;
  private Connection readerConnection;
  private Connection writerConnection;
  private JdbcCallable<Connection, SQLException> connectThroughEndpoint;

  private static HostSpec host(final String host, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(host).port(3306).role(role).build();
  }

  /** The client's Global Database settings for a cluster writer endpoint in a region that may be secondary. */
  private static Properties inactiveEndpointNoVerification() {
    final Properties props = new Properties();
    props.setProperty(AuroraInitialConnectionStrategyPlugin.INACTIVE_CLUSTER_WRITER_SUBSTITUTION_ROLE.name, "none");
    props.setProperty(
        AuroraInitialConnectionStrategyPlugin.VERIFY_INACTIVE_CLUSTER_WRITER_CONNECTION_ROLE.name, "none");
    props.setProperty(AuroraInitialConnectionStrategyPlugin.OPEN_CONNECTION_RETRY_INTERVAL_MS.name, "1");
    return props;
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws SQLException {
    this.pluginService = mock(PluginService.class);
    this.servicesContainer = mock(FullServicesContainer.class);
    this.readerConnection = mock(Connection.class);
    this.writerConnection = mock(Connection.class);
    this.connectThroughEndpoint = mock(JdbcCallable.class);
    final Dialect dialect = mock(Dialect.class);

    when(this.servicesContainer.getPluginService()).thenReturn(this.pluginService);
    when(this.servicesContainer.getImportantEventService()).thenReturn(mock(ImportantEventService.class));
    when(this.pluginService.getDialect()).thenReturn(dialect);
    when(dialect.filterAvailableHosts(anyList(), any())).thenAnswer(i -> i.getArgument(0));
    when(this.pluginService.getAllHosts()).thenAnswer(i -> this.topology.get());
    when(this.pluginService.getHosts()).thenAnswer(i -> this.topology.get());
    doAnswer(i -> {
      if (this.topologyAfterRefresh.get() != null) {
        this.topology.set(this.topologyAfterRefresh.get());
      }
      return null;
    }).when(this.pluginService).refreshHostList();
    // The cluster writer endpoint's DNS still points to the instance that is now a reader.
    when(this.connectThroughEndpoint.call()).thenReturn(this.readerConnection);
    when(this.pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(this.writerConnection);
    when(this.pluginService.getHostRole(this.readerConnection)).thenReturn(HostRole.READER);
    when(this.pluginService.getHostRole(this.writerConnection)).thenReturn(HostRole.WRITER);
  }

  @Test
  void testEndpointThatBecameActiveDuringConnectMustReachWriter() throws SQLException {
    // The connection is opened from an outdated topology (writer in the other region), so the endpoint is treated
    // as inactive. By the time it is open, the topology places the writer in the endpoint's region.
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1_READER, OHIO_2));
    this.topologyAfterRefresh.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.servicesContainer, inactiveEndpointNoVerification());

    final Connection result = plugin.connect(
        "jdbc:aws-wrapper:mysql://", OHIO_ENDPOINT, inactiveEndpointNoVerification(), true,
        this.connectThroughEndpoint);

    assertSame(this.writerConnection, result);
    verify(this.readerConnection).close();
    verify(this.pluginService).connect(any(HostSpec.class), any(Properties.class), any());
  }

  @Test
  void testFirstConnectionWithoutTopologyGetsNoRecheck() throws SQLException {
    // No topology yet is not a secondary region: the recheck is for an endpoint whose topology placed the writer
    // elsewhere. The connection is accepted as the settings ask, without an extra refresh.
    this.topology.set(Arrays.asList());
    this.topologyAfterRefresh.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.servicesContainer, inactiveEndpointNoVerification());

    final Connection result = plugin.connect(
        "jdbc:aws-wrapper:mysql://", OHIO_ENDPOINT, inactiveEndpointNoVerification(), true,
        this.connectThroughEndpoint);

    assertSame(this.readerConnection, result);
    verify(this.pluginService, never()).refreshHostList();
    verify(this.readerConnection, never()).close();
  }

  @Test
  void testFailedRecheckRefreshKeepsTheConnection() throws SQLException {
    // The recheck is best effort: a failed topology refresh must not turn a successful connection into an error.
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1_READER, OHIO_2));
    doThrow(new SQLException("topology refresh failed")).when(this.pluginService).refreshHostList();
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.servicesContainer, inactiveEndpointNoVerification());

    final Connection result = plugin.connect(
        "jdbc:aws-wrapper:mysql://", OHIO_ENDPOINT, inactiveEndpointNoVerification(), true,
        this.connectThroughEndpoint);

    assertSame(this.readerConnection, result);
    verify(this.readerConnection, never()).close();
  }

  @Test
  void testInactiveEndpointStillAcceptsReader() throws SQLException {
    // The writer stays in the other region: the endpoint is inactive and a reader is what the settings ask for.
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1_READER, OHIO_2));
    final AuroraInitialConnectionStrategyPlugin plugin =
        new AuroraInitialConnectionStrategyPlugin(this.servicesContainer, inactiveEndpointNoVerification());

    final Connection result = plugin.connect(
        "jdbc:aws-wrapper:mysql://", OHIO_ENDPOINT, inactiveEndpointNoVerification(), true,
        this.connectThroughEndpoint);

    assertSame(this.readerConnection, result);
    verify(this.readerConnection, never()).close();
    verify(this.pluginService, never()).connect(any(HostSpec.class), any(Properties.class), any());
  }
}
