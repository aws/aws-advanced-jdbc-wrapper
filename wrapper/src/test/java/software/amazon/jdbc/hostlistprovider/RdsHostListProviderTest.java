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

package software.amazon.jdbc.hostlistprovider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.TopologyDialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider.FetchTopologyResult;
import software.amazon.jdbc.hostlistprovider.monitoring.ClusterTopologyMonitorImpl;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.storage.TestStorageServiceImpl;

class RdsHostListProviderTest {
  private StorageService storageService;
  private RdsHostListProvider rdsHostListProvider;

  @Mock private Connection mockConnection;
  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private MonitorService mockMonitorService;
  @Mock private PluginService mockPluginService;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private HostSpecBuilder mockHostSpecBuilder;
  @Mock private EventPublisher mockEventPublisher;
  @Mock private ClusterTopologyMonitorImpl mockMonitor;
  @Mock private TopologyUtils mockTopologyUtils;
  @Mock private TopologyDialect mockDialect;

  private AutoCloseable closeable;
  private final HostSpec currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("foo").port(1234).build();
  private final List<HostSpec> hosts = Arrays.asList(
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host1").build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host2").build());

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    storageService = new TestStorageServiceImpl(mockEventPublisher);
    when(mockServicesContainer.getHostListProviderService()).thenReturn(mockHostListProviderService);
    when(mockServicesContainer.getStorageService()).thenReturn(storageService);
    when(mockServicesContainer.getMonitorService()).thenReturn(mockMonitorService);
    when(mockMonitorService.get(eq(ClusterTopologyMonitorImpl.class), anyString())).thenReturn(mockMonitor);
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.connect(any(HostSpec.class), any(Properties.class))).thenReturn(mockConnection);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(currentHostSpec);
    when(mockPluginService.getHostSpecBuilder()).thenReturn(mockHostSpecBuilder);
    when(mockHostListProviderService.getDialect()).thenReturn(mockDialect);
    when(mockHostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
    when(mockHostListProviderService.getCurrentConnection()).thenReturn(mockConnection);
  }

  @AfterEach
  void tearDown() throws Exception {
    storageService.clearAll();
    closeable.close();
  }

  private RdsHostListProvider getRdsHostListProvider(String originalUrl) throws SQLException {
    RdsHostListProvider provider = new RdsHostListProvider(
        mockTopologyUtils, new Properties(), originalUrl, mockServicesContainer);
    return provider;
  }

  @Test
  void testGetTopology_returnCachedTopology() throws SQLException {
    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("protocol://url/"));

    final List<HostSpec> expected = hosts;
    storageService.set(rdsHostListProvider.clusterId, new Topology(expected));

    final FetchTopologyResult result = rdsHostListProvider.getTopology(false);
    assertEquals(expected, result.hosts);
    assertEquals(2, result.hosts.size());
    verify(rdsHostListProvider, never()).queryForTopology();
  }

  @Test
  void testGetTopology_withForceUpdate_returnsUpdatedTopology() throws SQLException, TimeoutException {
    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("jdbc:someprotocol://url"));
    storageService.set(rdsHostListProvider.clusterId, new Topology(hosts));

    final List<HostSpec> newHosts = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("newHost").build());
    doReturn(newHosts).when(mockMonitor).forceRefresh(anyBoolean(), anyLong());

    final FetchTopologyResult result = rdsHostListProvider.getTopology(true);
    verify(rdsHostListProvider, atMostOnce()).queryForTopology();
    assertEquals(1, result.hosts.size());
    assertEquals(newHosts, result.hosts);
  }

  @Test
  void testGetTopology_noForceUpdate_queryReturnsEmptyHostList() throws SQLException {
    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("jdbc:someprotocol://url"));
    // rdsHostListProvider.clusterId = "cluster-id";

    final List<HostSpec> expected = hosts;
    storageService.set(rdsHostListProvider.clusterId, new Topology(expected));

    doReturn(new ArrayList<>()).when(rdsHostListProvider).queryForTopology();

    final FetchTopologyResult result = rdsHostListProvider.getTopology(false);
    verify(rdsHostListProvider, atMostOnce()).queryForTopology();
    assertEquals(2, result.hosts.size());
    assertEquals(expected, result.hosts);
  }

  @Test
  void testGetTopology_withForceUpdate_returnsInitialHostList() throws SQLException {
    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("jdbc:someprotocol://url"));
    rdsHostListProvider.clear();

    doReturn(new ArrayList<>()).when(rdsHostListProvider).queryForTopology();

    final FetchTopologyResult result = rdsHostListProvider.getTopology(true);
    verify(rdsHostListProvider, atMostOnce()).queryForTopology();
    assertNotNull(result.hosts);
    assertEquals(
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("url").build()),
        result.hosts);
  }

  @Test
  void testQueryForTopology_withDifferentDriverProtocol() throws SQLException, TimeoutException {
    final List<HostSpec> expectedMySQL = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("mysql").port(HostSpec.NO_PORT)
            .role(HostRole.WRITER).availability(HostAvailability.AVAILABLE).weight(0).build());
    final List<HostSpec> expectedPostgres = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("postgresql").port(HostSpec.NO_PORT)
            .role(HostRole.WRITER).availability(HostAvailability.AVAILABLE).weight(0).build());
    when(mockMonitor.forceRefresh(anyBoolean(), anyLong()))
        .thenReturn(expectedMySQL).thenReturn(expectedPostgres);


    rdsHostListProvider = getRdsHostListProvider("mysql://url/");

    List<HostSpec> hosts = rdsHostListProvider.queryForTopology();
    assertEquals(expectedMySQL, hosts);

    rdsHostListProvider = getRdsHostListProvider("postgresql://url/");
    hosts = rdsHostListProvider.queryForTopology();
    assertEquals(expectedPostgres, hosts);
  }

  @Test
  void testGetCachedTopology_returnStoredTopology() throws SQLException {
    rdsHostListProvider = getRdsHostListProvider("jdbc:someprotocol://url");

    final List<HostSpec> expected = hosts;
    storageService.set(rdsHostListProvider.clusterId, new Topology(expected));

    final List<HostSpec> result = rdsHostListProvider.getStoredTopology();
    assertEquals(expected, result);
  }

  @Test
  void testIdentifyConnectionWithInvalidNodeIdQuery() throws SQLException {
    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("jdbc:someprotocol://url"));

    assertThrows(SQLException.class, () -> rdsHostListProvider.identifyConnection(mockConnection));

    when(mockConnection.createStatement()).thenThrow(new SQLException("exception"));
    assertThrows(SQLException.class, () -> rdsHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionNullTopology() throws SQLException, TimeoutException {
    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("jdbc:someprotocol://url"));
    // rdsHostListProvider.instanceTemplate =
    //     new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("?.pattern").build();

    when(mockTopologyUtils.getInstanceId(mockConnection)).thenReturn(Pair.create("instance-1", "instance-1"));
    doReturn(null).when(rdsHostListProvider).refresh();
    doReturn(null).when(rdsHostListProvider).forceRefresh();

    assertNull(rdsHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionHostNotInTopology() throws SQLException, TimeoutException {
    final List<HostSpec> cachedTopology = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build());

    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("jdbc:someprotocol://url"));
    when(mockTopologyUtils.getInstanceId(mockConnection)).thenReturn(Pair.create("instance-1", "instance-1"));
    doReturn(cachedTopology).when(rdsHostListProvider).refresh();
    doReturn(cachedTopology).when(rdsHostListProvider).forceRefresh();

    assertNull(rdsHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionHostInTopology() throws SQLException, TimeoutException {
    final HostSpec expectedHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
        .port(HostSpec.NO_PORT)
        .role(HostRole.WRITER)
        .build();
    expectedHost.setHostId("instance-a-1");
    final List<HostSpec> cachedTopology = Collections.singletonList(expectedHost);

    rdsHostListProvider = Mockito.spy(getRdsHostListProvider("jdbc:someprotocol://url"));
    when(mockTopologyUtils.getInstanceId(mockConnection)).thenReturn(Pair.create("instance-a-1", "instance-a-1"));
    doReturn(cachedTopology).when(rdsHostListProvider).refresh();
    doReturn(cachedTopology).when(rdsHostListProvider).forceRefresh();

    final HostSpec actual = rdsHostListProvider.identifyConnection(mockConnection);
    assertEquals("instance-a-1.xyz.us-east-2.rds.amazonaws.com", actual.getHost());
    assertEquals("instance-a-1", actual.getHostId());
  }
}
