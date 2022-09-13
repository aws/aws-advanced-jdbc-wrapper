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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider.ClusterTopologyInfo;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider.FetchTopologyResult;

class AuroraHostListProviderTest {

  private AuroraHostListProvider auroraHostListProvider;

  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private PluginService mockPluginService;
  @Mock private HostListProviderService mockHostListProviderService;
  @Captor private ArgumentCaptor<String> queryCaptor;

  private AutoCloseable closeable;
  private final HostSpec currentHostSpec = new HostSpec("foo", 1234);
  private final List<HostSpec> hosts = Arrays.asList(new HostSpec("host1"), new HostSpec("host2"));

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.connect(any(HostSpec.class), any(Properties.class))).thenReturn(mockConnection);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(currentHostSpec);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(queryCaptor.capture())).thenReturn(mockResultSet);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (auroraHostListProvider != null) {
      auroraHostListProvider.clearAll();
    }
    closeable.close();
  }

  @Test
  void testGetTopology_returnCachedTopology() throws SQLException {
    auroraHostListProvider = Mockito.spy(new AuroraHostListProvider(
        "protocol", mockHostListProviderService, new Properties(), "protocol://url/"));

    final Instant lastUpdated = Instant.now();
    final List<HostSpec> expected = hosts;
    final ClusterTopologyInfo info = new ClusterTopologyInfo(
        "any", expected, lastUpdated, false, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, info);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, false);
    assertEquals(expected, result.hosts);
    assertEquals(2, result.hosts.size());
    verify(auroraHostListProvider, never()).queryForTopology(mockConnection);
  }

  @Test
  void testGetTopology_withForceUpdate_returnsUpdatedTopology() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        new AuroraHostListProvider("", mockHostListProviderService, new Properties(), "url"));

    final Instant lastUpdated = Instant.now();
    final ClusterTopologyInfo oldTopology = new ClusterTopologyInfo(
        "any", hosts, lastUpdated, false, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, oldTopology);

    final List<HostSpec> newHosts = Collections.singletonList(new HostSpec("newHost"));
    final ClusterTopologyInfo newTopology = new ClusterTopologyInfo(
        "any", newHosts, lastUpdated, false, false);
    doReturn(newTopology).when(auroraHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, true);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(1, result.hosts.size());
    assertEquals(newTopology.hosts, result.hosts);
  }

  @Test
  void testGetTopology_withoutForceUpdate_returnsEmptyHostList() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        new AuroraHostListProvider("", mockHostListProviderService, new Properties(), "url"));

    final Instant lastUpdated = Instant.now();
    final List<HostSpec> expected = hosts;
    final ClusterTopologyInfo oldTopology = new ClusterTopologyInfo(
        "any", expected, lastUpdated, false, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, oldTopology);

    final ClusterTopologyInfo newTopology =
        new ClusterTopologyInfo("any", new ArrayList<>(), lastUpdated, false, false);
    doReturn(newTopology).when(auroraHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, false);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(2, result.hosts.size());
    assertEquals(expected, result.hosts);
  }

  @Test
  void testGetTopology_withForceUpdate_returnsEmptyHostList() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        new AuroraHostListProvider("", mockHostListProviderService, new Properties(), "url"));
    auroraHostListProvider.clear();

    final Instant lastUpdated = Instant.now();
    final ClusterTopologyInfo newTopology = new ClusterTopologyInfo(
        "any", new ArrayList<>(), lastUpdated, false, false);
    doReturn(newTopology).when(auroraHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, true);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(0, result.hosts.size());
    assertEquals(new ArrayList<>(), result.hosts);
  }

  @Test
  void testQueryForTopology_withDifferentDriverProtocol() throws SQLException {
    final List<HostSpec> expectedMySQL = Collections.singletonList(new HostSpec("mysql"));
    final List<HostSpec> expectedPostgres = Collections.singletonList(new HostSpec("postgresql"));
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(eq(AuroraHostListProvider.FIELD_SESSION_ID))).thenReturn(
        AuroraHostListProvider.WRITER_SESSION_ID);

    when(mockResultSet.getString(eq(AuroraHostListProvider.FIELD_SERVER_ID))).thenReturn("mysql");
    auroraHostListProvider = new AuroraHostListProvider(
        "mysql", mockHostListProviderService, new Properties(), "mysql://url/");

    ClusterTopologyInfo result = auroraHostListProvider.queryForTopology(mockConnection);
    String query = queryCaptor.getValue();
    assertEquals(expectedMySQL, result.hosts);
    assertEquals(AuroraHostListProvider.MYSQL_RETRIEVE_TOPOLOGY_SQL, query);

    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(eq(AuroraHostListProvider.FIELD_SERVER_ID))).thenReturn("postgresql");
    auroraHostListProvider = new AuroraHostListProvider(
        "postgresql", mockHostListProviderService, new Properties(), "postgresql://url/");
    result = auroraHostListProvider.queryForTopology(mockConnection);
    query = queryCaptor.getValue();
    assertEquals(expectedPostgres, result.hosts);
    assertEquals(AuroraHostListProvider.PG_RETRIEVE_TOPOLOGY_SQL, query);
  }

  @Test
  void testQueryForTopology_queryResultsInException() throws SQLException {
    auroraHostListProvider = new AuroraHostListProvider(
        "protocol", mockHostListProviderService, new Properties(), "protocol://url/");
    when(mockStatement.executeQuery(anyString())).thenThrow(new SQLSyntaxErrorException());
    assertDoesNotThrow(() -> {
      ClusterTopologyInfo result = auroraHostListProvider.queryForTopology(mockConnection);
      assertEquals(new ArrayList<>(), result.hosts);
    });
  }

  @Test
  void testGetCachedTopology_returnCachedTopology() {
    auroraHostListProvider = new AuroraHostListProvider(
        "", mockHostListProviderService, new Properties(), "url");

    final Instant lastUpdated = Instant.now();
    final List<HostSpec> expected = hosts;
    final ClusterTopologyInfo info = new ClusterTopologyInfo(
        "any", expected, lastUpdated, false, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, info);

    final List<HostSpec> result = auroraHostListProvider.getCachedTopology();
    assertEquals(expected, result);
  }

  @Test
  void testGetCachedTopology_returnNull() {
    auroraHostListProvider = new AuroraHostListProvider(
        "", mockHostListProviderService, new Properties(), "url");
    // Test getCachedTopology with empty topology.
    assertNull(auroraHostListProvider.getCachedTopology());
    auroraHostListProvider.clear();

    auroraHostListProvider = new AuroraHostListProvider(
        "", mockHostListProviderService, new Properties(), "url");
    final Instant lastUpdated = Instant.now().minus(1, ChronoUnit.DAYS);
    final ClusterTopologyInfo info = new ClusterTopologyInfo("any", hosts, lastUpdated, false, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, info);
    // Test getCachedTopology with expired cache.
    assertNull(auroraHostListProvider.getCachedTopology());
  }

  @Test
  void testTopologyCache_NoSuggestedClusterId() throws SQLException {
    AuroraHostListProvider.topologyCache.clear();

    AuroraHostListProvider provider1 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://cluster-a.domain.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpec("instance-a-1.domain.com", HostSpec.NO_PORT, HostRole.WRITER),
        new HostSpec("instance-a-2.domain.com", HostSpec.NO_PORT, HostRole.READER),
        new HostSpec("instance-a-3.domain.com", HostSpec.NO_PORT, HostRole.READER));

    doReturn(new ClusterTopologyInfo(
        provider1.clusterId,
        topologyClusterA,
        Instant.now(),
        false,
        provider1.isPrimaryClusterId)).when(provider1).queryForTopology(any(Connection.class));

    assertTrue(AuroraHostListProvider.topologyCache.isEmpty());

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    AuroraHostListProvider provider2 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://cluster-b.domain.com/"));
    provider2.init();
    assertNull(provider2.getCachedTopology());

    final List<HostSpec> topologyClusterB = Arrays.asList(
        new HostSpec("instance-b-1.domain.com", HostSpec.NO_PORT, HostRole.WRITER),
        new HostSpec("instance-b-2.domain.com", HostSpec.NO_PORT, HostRole.READER),
        new HostSpec("instance-b-3.domain.com", HostSpec.NO_PORT, HostRole.READER));
    doReturn(new ClusterTopologyInfo(
        provider2.clusterId,
        topologyClusterB,
        Instant.now(),
        false,
        provider2.isPrimaryClusterId)).when(provider2).queryForTopology(any(Connection.class));

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterB, topologyProvider2);

    assertEquals(2, AuroraHostListProvider.topologyCache.size());
  }

  @Test
  void testTopologyCache_SuggestedClusterIdForRds() throws SQLException {
    AuroraHostListProvider.topologyCache.clear();

    AuroraHostListProvider provider1 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpec("instance-a-1.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.WRITER),
        new HostSpec("instance-a-2.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.READER),
        new HostSpec("instance-a-3.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.READER));

    doReturn(new ClusterTopologyInfo(
        provider1.clusterId,
        topologyClusterA,
        Instant.now(),
        false,
        provider1.isPrimaryClusterId)).when(provider1).queryForTopology(any(Connection.class));

    assertTrue(AuroraHostListProvider.topologyCache.isEmpty());

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    AuroraHostListProvider provider2 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider2.init();

    assertEquals(provider1.clusterId, provider2.clusterId);
    assertTrue(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider2);

    assertEquals(1, AuroraHostListProvider.topologyCache.size());
  }

  @Test
  void testTopologyCache_SuggestedClusterIdForInstance() throws SQLException {
    AuroraHostListProvider.topologyCache.clear();

    AuroraHostListProvider provider1 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpec("instance-a-1.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.WRITER),
        new HostSpec("instance-a-2.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.READER),
        new HostSpec("instance-a-3.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.READER));

    doReturn(new ClusterTopologyInfo(
        provider1.clusterId,
        topologyClusterA,
        Instant.now(),
        false,
        provider1.isPrimaryClusterId)).when(provider1).queryForTopology(any(Connection.class));

    assertTrue(AuroraHostListProvider.topologyCache.isEmpty());

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    AuroraHostListProvider provider2 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://instance-a-3.xyz.us-east-2.rds.amazonaws.com/"));
    provider2.init();

    assertEquals(provider1.clusterId, provider2.clusterId);
    assertTrue(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider2);

    assertEquals(1, AuroraHostListProvider.topologyCache.size());
  }

  @Test
  void testTopologyCache_AcceptSuggestion() throws SQLException, InterruptedException {
    AuroraHostListProvider.topologyCache.clear();

    AuroraHostListProvider provider1 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://instance-a-2.xyz.us-east-2.rds.amazonaws.com/"));
    provider1.init();
    final List<HostSpec> topologyClusterA = Arrays.asList(
        new HostSpec("instance-a-1.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.WRITER),
        new HostSpec("instance-a-2.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.READER),
        new HostSpec("instance-a-3.xyz.us-east-2.rds.amazonaws.com", HostSpec.NO_PORT, HostRole.READER));

    doAnswer(a -> {
      return new ClusterTopologyInfo(
          provider1.clusterId,
          topologyClusterA,
          Instant.now(),
          false,
          provider1.isPrimaryClusterId);
    }).when(provider1).queryForTopology(any(Connection.class));

    assertTrue(AuroraHostListProvider.topologyCache.isEmpty());

    List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    //AuroraHostListProvider.logCache();

    AuroraHostListProvider provider2 = Mockito.spy(new AuroraHostListProvider(
        "jdbc:something://",
        Mockito.spy(HostListProviderService.class),
        new Properties(),
        "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider2.init();

    doAnswer(a -> {
      return new ClusterTopologyInfo(
          provider2.clusterId,
          topologyClusterA,
          Instant.now(),
          false,
          provider2.isPrimaryClusterId);
    }).when(provider2).queryForTopology(any(Connection.class));

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider2);

    assertNotEquals(provider1.clusterId, provider2.clusterId);
    assertFalse(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);
    assertEquals(2, AuroraHostListProvider.topologyCache.size());
    assertEquals("cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/",
        AuroraHostListProvider.topologyCache.get(provider1.clusterId).suggestedPrimaryClusterId);

    //AuroraHostListProvider.logCache();

    topologyProvider1 = provider1.forceRefresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);
    assertEquals(provider1.clusterId, provider2.clusterId);
    assertTrue(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);

    //AuroraHostListProvider.logCache();
  }
}
