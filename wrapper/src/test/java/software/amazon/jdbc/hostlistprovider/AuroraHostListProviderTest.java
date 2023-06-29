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

import com.mysql.cj.exceptions.WrongArgumentException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider.FetchTopologyResult;

class AuroraHostListProviderTest {

  private final long defaultRefreshRateNano = TimeUnit.SECONDS.toNanos(5);
  private AuroraHostListProvider auroraHostListProvider;

  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private PluginService mockPluginService;
  @Mock private HostListProviderService mockHostListProviderService;
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
    AuroraHostListProvider.clearAll();
    closeable.close();
  }

  private AuroraHostListProvider getAuroraHostListProvider(
      HostListProviderService mockHostListProviderService,
      String originalUrl) throws SQLException {
    AuroraHostListProvider provider = new AuroraHostListProvider(
        new Properties(),
        originalUrl,
        mockHostListProviderService,
        "foo", "bar", "baz");
    provider.init();
    // provider.clusterId = "cluster-id";
    return provider;
  }

  @Test
  void testGetTopology_returnCachedTopology() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService, "protocol://url/"));

    final Instant lastUpdated = Instant.now();
    final List<HostSpec> expected = hosts;
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, expected, defaultRefreshRateNano);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, false);
    assertEquals(expected, result.hosts);
    assertEquals(2, result.hosts.size());
    verify(auroraHostListProvider, never()).queryForTopology(mockConnection);
  }

  @Test
  void testGetTopology_withForceUpdate_returnsUpdatedTopology() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url"));
    auroraHostListProvider.isInitialized = true;

    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, hosts, defaultRefreshRateNano);

    final List<HostSpec> newHosts = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("newHost").build());
    doReturn(newHosts).when(auroraHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, true);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(1, result.hosts.size());
    assertEquals(newHosts, result.hosts);
  }

  @Test
  void testGetTopology_noForceUpdate_queryReturnsEmptyHostList() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url"));
    auroraHostListProvider.clusterId = "cluster-id";
    auroraHostListProvider.isInitialized = true;

    final List<HostSpec> expected = hosts;
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, expected, defaultRefreshRateNano);

    doReturn(new ArrayList<>()).when(auroraHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, false);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(2, result.hosts.size());
    assertEquals(expected, result.hosts);
  }

  @Test
  void testGetTopology_withForceUpdate_returnsInitialHostList() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url"));
    auroraHostListProvider.clear();

    doReturn(new ArrayList<>()).when(auroraHostListProvider).queryForTopology(mockConnection);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, true);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertNotNull(result.hosts);
    assertEquals(
        Arrays.asList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("url").build()),
        result.hosts);
  }

  @Test
  void testQueryForTopology_withDifferentDriverProtocol() throws SQLException {
    final List<HostSpec> expectedMySQL = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("mysql").port(HostSpec.NO_PORT)
            .role(HostRole.WRITER).availability(HostAvailability.AVAILABLE).weight(0).build());
    final List<HostSpec> expectedPostgres = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("postgresql").port(HostSpec.NO_PORT)
            .role(HostRole.WRITER).availability(HostAvailability.AVAILABLE).weight(0).build());
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getBoolean(eq(2))).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn("mysql");


    auroraHostListProvider =
        getAuroraHostListProvider(mockHostListProviderService, "mysql://url/");

    List<HostSpec> hosts = auroraHostListProvider.queryForTopology(mockConnection);
    assertEquals(expectedMySQL, hosts);

    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(eq(1))).thenReturn("postgresql");

    auroraHostListProvider =
        getAuroraHostListProvider(mockHostListProviderService, "postgresql://url/");
    hosts = auroraHostListProvider.queryForTopology(mockConnection);
    assertEquals(expectedPostgres, hosts);
  }

  @Test
  void testQueryForTopology_queryResultsInException() throws SQLException {
    auroraHostListProvider =
        getAuroraHostListProvider(mockHostListProviderService, "protocol://url/");
    when(mockStatement.executeQuery(queryCaptor.capture())).thenThrow(new SQLSyntaxErrorException());

    assertThrows(
        SQLException.class,
        () -> auroraHostListProvider.queryForTopology(mockConnection));
  }

  @Test
  void testGetCachedTopology_returnCachedTopology() throws SQLException {
    auroraHostListProvider = getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url");

    final List<HostSpec> expected = hosts;
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, expected, defaultRefreshRateNano);

    final List<HostSpec> result = auroraHostListProvider.getCachedTopology();
    assertEquals(expected, result);
  }

  @Test
  void testGetCachedTopology_returnNull() throws InterruptedException, SQLException {
    auroraHostListProvider = getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url");
    // Test getCachedTopology with empty topology.
    assertNull(auroraHostListProvider.getCachedTopology());
    auroraHostListProvider.clear();

    auroraHostListProvider = getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url");
    final long refreshRateOneNanosecond = 1;
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, hosts, refreshRateOneNanosecond);
    TimeUnit.NANOSECONDS.sleep(1);

    // Test getCachedTopology with expired cache.
    assertNull(auroraHostListProvider.getCachedTopology());
  }

  @Test
  void testTopologyCache_NoSuggestedClusterId() throws SQLException {
    AuroraHostListProvider.clearAll();

    AuroraHostListProvider provider1 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
            "jdbc:something://cluster-a.domain.com/"));
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

    assertEquals(0, AuroraHostListProvider.topologyCache.size());

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    AuroraHostListProvider provider2 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
            "jdbc:something://cluster-b.domain.com/"));
    provider2.init();
    assertNull(provider2.getCachedTopology());

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

    assertEquals(2, AuroraHostListProvider.topologyCache.size());
  }

  @Test
  void testTopologyCache_SuggestedClusterIdForRds() throws SQLException {
    AuroraHostListProvider.clearAll();

    AuroraHostListProvider provider1 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
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

    assertEquals(0, AuroraHostListProvider.topologyCache.size());

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    AuroraHostListProvider provider2 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
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
    AuroraHostListProvider.clearAll();

    AuroraHostListProvider provider1 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
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

    assertEquals(0, AuroraHostListProvider.topologyCache.size());

    final List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    AuroraHostListProvider provider2 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
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
  void testTopologyCache_AcceptSuggestion() throws SQLException {
    AuroraHostListProvider.clearAll();

    AuroraHostListProvider provider1 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
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

    assertEquals(0, AuroraHostListProvider.topologyCache.size());

    List<HostSpec> topologyProvider1 = provider1.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);

    // AuroraHostListProvider.logCache();

    AuroraHostListProvider provider2 = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService,
            "jdbc:something://cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/"));
    provider2.init();

    doAnswer(a -> topologyClusterA).when(provider2).queryForTopology(any(Connection.class));

    final List<HostSpec> topologyProvider2 = provider2.refresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider2);

    assertNotEquals(provider1.clusterId, provider2.clusterId);
    assertFalse(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);
    assertEquals(2, AuroraHostListProvider.topologyCache.size());
    assertEquals("cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com/",
        AuroraHostListProvider.suggestedPrimaryClusterIdCache.get(provider1.clusterId));

    // AuroraHostListProvider.logCache();

    topologyProvider1 = provider1.forceRefresh(Mockito.mock(Connection.class));
    assertEquals(topologyClusterA, topologyProvider1);
    assertEquals(provider1.clusterId, provider2.clusterId);
    assertTrue(provider1.isPrimaryClusterId);
    assertTrue(provider2.isPrimaryClusterId);

    // AuroraHostListProvider.logCache();
  }

  @Test
  void testIdentifyConnectionWithInvalidNodeIdQuery() throws SQLException {
    auroraHostListProvider = Mockito.spy(getAuroraHostListProvider(
        mockHostListProviderService,
        "jdbc:someprotocol://url"));

    when(mockResultSet.next()).thenReturn(false);
    assertThrows(SQLException.class, () -> auroraHostListProvider.identifyConnection(mockConnection));

    when(mockConnection.createStatement()).thenThrow(new SQLException("exception"));
    assertThrows(SQLException.class, () -> auroraHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionNullTopology() throws SQLException {
    auroraHostListProvider = Mockito.spy(getAuroraHostListProvider(
        mockHostListProviderService,
        "jdbc:someprotocol://url"));
    auroraHostListProvider.clusterInstanceTemplate = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("?.pattern").build();

    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn("instance-1");
    when(auroraHostListProvider.refresh(eq(mockConnection))).thenReturn(null);

    assertNull(auroraHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionHostNotInTopology() throws SQLException {
    final List<HostSpec> cachedTopology = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build());

    auroraHostListProvider = Mockito.spy(getAuroraHostListProvider(
        mockHostListProviderService,
        "jdbc:someprotocol://url"));
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn("instance-1");
    when(auroraHostListProvider.refresh(eq(mockConnection))).thenReturn(cachedTopology);

    assertNull(auroraHostListProvider.identifyConnection(mockConnection));
  }

  @Test
  void testIdentifyConnectionHostInTopology() throws SQLException {
    final HostSpec expectedHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
        .port(HostSpec.NO_PORT)
        .role(HostRole.WRITER)
        .build();
    expectedHost.setHostId("instance-a-1");
    final List<HostSpec> cachedTopology = Collections.singletonList(expectedHost);

    auroraHostListProvider = Mockito.spy(getAuroraHostListProvider(
        mockHostListProviderService,
        "jdbc:someprotocol://url"));
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(eq(1))).thenReturn("instance-a-1");
    when(auroraHostListProvider.refresh()).thenReturn(cachedTopology);

    final HostSpec actual = auroraHostListProvider.identifyConnection(mockConnection);
    assertEquals("instance-a-1.xyz.us-east-2.rds.amazonaws.com", actual.getHost());
    assertEquals("instance-a-1", actual.getHostId());
  }

  @Test
  void testGetTopology_StaleRecord() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url"));
    auroraHostListProvider.isInitialized = true;

    final String hostName1 = "hostName1";
    final String hostName2 = "hostName2";
    final Float cpuUtilization = 11.1F;
    final Float nodeLag = 0.123F;
    final Timestamp firstTimestamp = Timestamp.from(Instant.now());
    final Timestamp secondTimestamp = new Timestamp(firstTimestamp.getTime() + 100);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(1)).thenReturn(hostName1).thenReturn(hostName2);
    when(mockResultSet.getBoolean(2)).thenReturn(true).thenReturn(true);
    when(mockResultSet.getFloat(3)).thenReturn(cpuUtilization).thenReturn(cpuUtilization);
    when(mockResultSet.getFloat(4)).thenReturn(nodeLag).thenReturn(nodeLag);
    when(mockResultSet.getTimestamp(5)).thenReturn(firstTimestamp).thenReturn(secondTimestamp);
    long weight = Math.round(nodeLag) * 100L + Math.round(cpuUtilization);
    final HostSpec expectedWriter = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(hostName2)
        .port(-1)
        .role(HostRole.WRITER)
        .availability(HostAvailability.AVAILABLE)
        .weight(weight)
        .lastUpdateTime(secondTimestamp)
        .build();

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, true);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);
    assertEquals(1, result.hosts.size());
    assertEquals(expectedWriter, result.hosts.get(0));
  }

  @Test
  void testGetTopology_InvalidLastUpdatedTimestamp() throws SQLException {
    auroraHostListProvider = Mockito.spy(
        getAuroraHostListProvider(mockHostListProviderService, "jdbc:someprotocol://url"));
    auroraHostListProvider.isInitialized = true;

    final String hostName = "hostName";
    final Float cpuUtilization = 11.1F;
    final Float nodeLag = 0.123F;
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(1)).thenReturn(hostName);
    when(mockResultSet.getBoolean(2)).thenReturn(true);
    when(mockResultSet.getFloat(3)).thenReturn(cpuUtilization);
    when(mockResultSet.getFloat(4)).thenReturn(nodeLag);
    when(mockResultSet.getTimestamp(5)).thenThrow(WrongArgumentException.class);

    final FetchTopologyResult result = auroraHostListProvider.getTopology(mockConnection, true);
    verify(auroraHostListProvider, atMostOnce()).queryForTopology(mockConnection);

    final String expectedLastUpdatedTimeStampRounded = Timestamp.from(Instant.now()).toString().substring(0, 16);
    assertEquals(1, result.hosts.size());
    assertEquals(
        expectedLastUpdatedTimeStampRounded,
        result.hosts.get(0).getLastUpdateTime().toString().substring(0, 16));
  }
}
