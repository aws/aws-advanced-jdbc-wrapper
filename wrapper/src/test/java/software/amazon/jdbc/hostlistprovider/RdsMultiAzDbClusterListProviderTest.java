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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider.FetchTopologyResult;
import software.amazon.jdbc.util.ServiceContainer;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.storage.TestStorageServiceImpl;
import software.amazon.jdbc.util.storage.Topology;

class RdsMultiAzDbClusterListProviderTest {
  private StorageService storageService;
  private RdsMultiAzDbClusterListProvider rdsMazDbClusterHostListProvider;

  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private ServiceContainer mockServiceContainer;
  @Mock private PluginService mockPluginService;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private EventPublisher mockEventPublisher;
  @Mock Dialect mockTopologyAwareDialect;
  @Captor private ArgumentCaptor<String> queryCaptor;

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
    when(mockServiceContainer.getHostListProviderService()).thenReturn(mockHostListProviderService);
    when(mockServiceContainer.getStorageService()).thenReturn(storageService);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.connect(any(HostSpec.class), any(Properties.class))).thenReturn(mockConnection);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(currentHostSpec);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(queryCaptor.capture())).thenReturn(mockResultSet);
    when(mockHostListProviderService.getDialect()).thenReturn(mockTopologyAwareDialect);
    when(mockHostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
  }

  @AfterEach
  void tearDown() throws Exception {
    RdsMultiAzDbClusterListProvider.clearAll();
    storageService.clearAll();
    closeable.close();
  }

  private RdsMultiAzDbClusterListProvider getRdsMazDbClusterHostListProvider(String originalUrl) throws SQLException {
    RdsMultiAzDbClusterListProvider provider = new RdsMultiAzDbClusterListProvider(
        new Properties(),
        originalUrl,
        mockServiceContainer,
        "foo",
        "bar",
        "baz",
        "fang",
        "li");
    provider.init();
    // provider.clusterId = "cluster-id";
    return provider;
  }

  @Test
  void testGetTopology_returnCachedTopology() throws SQLException {
    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("protocol://url/"));
    final List<HostSpec> expected = hosts;
    storageService.set(rdsMazDbClusterHostListProvider.clusterId, new Topology(expected));

    final FetchTopologyResult result = rdsMazDbClusterHostListProvider.getTopology(mockConnection, false);
    assertEquals(expected, result.hosts);
    assertEquals(2, result.hosts.size());
    verify(rdsMazDbClusterHostListProvider, never()).queryForTopology(mockConnection);
  }

  @Test
  void testGetTopology_withForceUpdate_returnsUpdatedTopology() throws SQLException {
    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url"));
    rdsMazDbClusterHostListProvider.isInitialized = true;

    storageService.set(rdsMazDbClusterHostListProvider.clusterId, new Topology(hosts));

    final List<HostSpec> newHosts = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("newHost").build());
    doReturn(newHosts).when(rdsMazDbClusterHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = rdsMazDbClusterHostListProvider.getTopology(mockConnection, true);
    verify(rdsMazDbClusterHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(1, result.hosts.size());
    assertEquals(newHosts, result.hosts);
  }

  @Test
  void testGetTopology_noForceUpdate_queryReturnsEmptyHostList() throws SQLException {
    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url"));
    rdsMazDbClusterHostListProvider.clusterId = "cluster-id";
    rdsMazDbClusterHostListProvider.isInitialized = true;

    final List<HostSpec> expected = hosts;
    storageService.set(rdsMazDbClusterHostListProvider.clusterId, new Topology(expected));

    doReturn(new ArrayList<>()).when(rdsMazDbClusterHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = rdsMazDbClusterHostListProvider.getTopology(mockConnection, false);
    verify(rdsMazDbClusterHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(2, result.hosts.size());
    assertEquals(expected, result.hosts);
  }

  @Test
  void testGetTopology_withForceUpdate_returnsInitialHostList() throws SQLException {
    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url"));
    rdsMazDbClusterHostListProvider.clear();

    doReturn(new ArrayList<>()).when(rdsMazDbClusterHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = rdsMazDbClusterHostListProvider.getTopology(mockConnection, true);
    verify(rdsMazDbClusterHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertNotNull(result.hosts);
    assertEquals(
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("url").build()),
        result.hosts);
  }

  @Test
  void testQueryForTopology_queryResultsInException() throws SQLException {
    rdsMazDbClusterHostListProvider = getRdsMazDbClusterHostListProvider("protocol://url/");
    when(mockStatement.executeQuery(queryCaptor.capture())).thenThrow(new SQLSyntaxErrorException());

    assertThrows(
        SQLException.class,
        () -> rdsMazDbClusterHostListProvider.queryForTopology(mockConnection));
  }

  @Test
  void testGetCachedTopology_returnCachedTopology() throws SQLException {
    rdsMazDbClusterHostListProvider = getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url");

    final List<HostSpec> expected = hosts;
    storageService.set(rdsMazDbClusterHostListProvider.clusterId, new Topology(expected));

    final List<HostSpec> result = rdsMazDbClusterHostListProvider.getStoredTopology();
    assertEquals(expected, result);
  }

  @Test
  void testTopologyCache_NoSuggestedClusterId() throws SQLException {
    RdsMultiAzDbClusterListProvider.clearAll();

    RdsMultiAzDbClusterListProvider provider1 =
        Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:something://cluster-a.domain.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.domain.com").port(HostSpec.NO_PORT).role(HostRole.WRITER).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-2.domain.com").port(HostSpec.NO_PORT).role(HostRole.READER).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-3.domain.com").port(HostSpec.NO_PORT).role(HostRole.READER).build());

    doReturn(topologyClusterA)
        .when(provider1).queryForTopology(any(Connection.class));

    assertEquals(0, storageService.size(Topology.class));

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    RdsMultiAzDbClusterListProvider provider2 =
        Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:something://cluster-b.domain.com/"));
    provider2.init();
    assertNull(provider2.getStoredTopology());

    final List<HostSpec> topologyClusterB = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-b-1.domain.com").port(HostSpec.NO_PORT).role(HostRole.WRITER).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-b-2.domain.com").port(HostSpec.NO_PORT).role(HostRole.READER).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-b-3.domain.com").port(HostSpec.NO_PORT).role(HostRole.READER).build());
    doReturn(topologyClusterB).when(provider2).queryForTopology(any(Connection.class));

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterB, topologyProvider2);

    assertEquals(2, storageService.size(Topology.class));
  }

  @Test
  void testTopologyCache_SuggestedClusterIdForRds() throws SQLException {
    RdsMultiAzDbClusterListProvider.clearAll();

    RdsMultiAzDbClusterListProvider provider1 =
        Mockito.spy(getRdsMazDbClusterHostListProvider(
            "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-2.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.READER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-3.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.READER)
            .build());

    doReturn(topologyClusterA).when(provider1).queryForTopology(any(Connection.class));

    assertEquals(0, storageService.size(Topology.class));

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    RdsMultiAzDbClusterListProvider provider2 =
        Mockito.spy(getRdsMazDbClusterHostListProvider(
            "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider2.init();

    assertEquals(provider1.clusterId, provider2.clusterId);
    assertTrue(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider2);

    assertEquals(1, storageService.size(Topology.class));
  }

  @Test
  void testTopologyCache_SuggestedClusterIdForInstance() throws SQLException {
    RdsMultiAzDbClusterListProvider.clearAll();

    RdsMultiAzDbClusterListProvider provider1 =
        Mockito.spy(getRdsMazDbClusterHostListProvider(
            "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-2.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.READER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-3.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.READER)
            .build());

    doReturn(topologyClusterA).when(provider1).queryForTopology(any(Connection.class));

    assertEquals(0, storageService.size(Topology.class));

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    RdsMultiAzDbClusterListProvider provider2 =
        Mockito.spy(getRdsMazDbClusterHostListProvider(
            "jdbc:something://instance-a-3.xyz.us-east-2.rds.amazonaws.com/"));
    provider2.init();

    assertEquals(provider1.clusterId, provider2.clusterId);
    assertTrue(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider2);

    assertEquals(1, storageService.size(Topology.class));
  }

  @Test
  void testTopologyCache_AcceptSuggestion() throws SQLException {
    RdsMultiAzDbClusterListProvider.clearAll();

    RdsMultiAzDbClusterListProvider provider1 =
        Mockito.spy(getRdsMazDbClusterHostListProvider(
            "jdbc:something://instance-a-2.xyz.us-east-2.rds.amazonaws.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-2.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.READER)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-3.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.READER)
            .build());

    doAnswer(a -> topologyClusterA).when(provider1).queryForTopology(any(Connection.class));

    assertEquals(0, storageService.size(Topology.class));

    List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    // RdsMultiAzDbClusterListProvider.logCache();

    RdsMultiAzDbClusterListProvider provider2 =
        Mockito.spy(getRdsMazDbClusterHostListProvider(
            "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider2.init();

    doAnswer(a -> topologyClusterA).when(provider2).queryForTopology(any(Connection.class));

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider2);

    assertNotEquals(provider1.clusterId, provider2.clusterId);
    assertFalse(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);
    assertEquals(2, storageService.size(Topology.class));
    assertEquals("cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com",
        RdsMultiAzDbClusterListProvider.suggestedPrimaryClusterIdCache.get(provider1.clusterId));

    // RdsMultiAzDbClusterListProvider.logCache();

    topologyProvider1 = provider1.forceRefresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);
    assertEquals(provider1.clusterId, provider2.clusterId);
    assertTrue(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);

    // RdsMultiAzDbClusterListProvider.logCache();
  }

  @Test
  void testIdentifyConnectionWithInvalidNodeIdQuery() throws SQLException {
    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url"));

    when(mockResultSet.next()).thenReturn(false);
    assertThrows(SQLException.class, () -> rdsMazDbClusterHostListProvider.identifyConnection(mockConnection));

    when(mockConnection.createStatement()).thenThrow(new SQLException("exception"));
    assertThrows(SQLException.class, () -> rdsMazDbClusterHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionNullTopology() throws SQLException {
    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url"));
    rdsMazDbClusterHostListProvider.clusterInstanceTemplate = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("?.pattern").build();

    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn("instance-1");
    doReturn(null).when(rdsMazDbClusterHostListProvider).refresh(mockConnection);
    doReturn(null).when(rdsMazDbClusterHostListProvider).forceRefresh(mockConnection);

    assertNull(rdsMazDbClusterHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionHostNotInTopology() throws SQLException {
    final List<HostSpec> cachedTopology = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build());

    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url"));
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn("instance-1");
    doReturn(cachedTopology).when(rdsMazDbClusterHostListProvider).refresh(mockConnection);
    doReturn(cachedTopology).when(rdsMazDbClusterHostListProvider).forceRefresh(mockConnection);

    assertNull(rdsMazDbClusterHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionHostInTopology() throws SQLException {
    final HostSpec expectedHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
        .hostId("instance-a-1")
        .port(HostSpec.NO_PORT)
        .role(HostRole.WRITER)
        .build();
    final List<HostSpec> cachedTopology = Collections.singletonList(expectedHost);

    rdsMazDbClusterHostListProvider = Mockito.spy(getRdsMazDbClusterHostListProvider("jdbc:someprotocol://url"));
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn("instance-a-1");
    doReturn(cachedTopology).when(rdsMazDbClusterHostListProvider).refresh(mockConnection);
    doReturn(cachedTopology).when(rdsMazDbClusterHostListProvider).forceRefresh(mockConnection);

    final HostSpec actual = rdsMazDbClusterHostListProvider.identifyConnection(mockConnection);
    assertEquals("instance-a-1.xyz.us-east-2.rds.amazonaws.com", actual.getHost());
    assertEquals("instance-a-1", actual.getHostId());
  }

}
