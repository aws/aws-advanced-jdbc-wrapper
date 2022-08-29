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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
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
    auroraHostListProvider.clearAll();
    closeable.close();
  }

  @Test
  void testGetTopology_returnCachedTopology() throws SQLException {
    auroraHostListProvider = Mockito.spy(new AuroraHostListProvider(
        "protocol", mockHostListProviderService, new Properties(), "protocol://url/"));

    final Instant lastUpdated = Instant.now();
    final List<HostSpec> expected = hosts;
    final ClusterTopologyInfo info = new ClusterTopologyInfo(expected, lastUpdated, false);
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
    final ClusterTopologyInfo oldTopology = new ClusterTopologyInfo(hosts, lastUpdated, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, oldTopology);

    final List<HostSpec> newHosts = Collections.singletonList(new HostSpec("newHost"));
    final ClusterTopologyInfo newTopology = new ClusterTopologyInfo(newHosts, lastUpdated, false);
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
    final ClusterTopologyInfo oldTopology = new ClusterTopologyInfo(expected, lastUpdated, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, oldTopology);

    final ClusterTopologyInfo newTopology =
        new ClusterTopologyInfo(new ArrayList<>(), lastUpdated, false);
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
    final ClusterTopologyInfo newTopology = new ClusterTopologyInfo(new ArrayList<>(), lastUpdated,
        false);
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
    final ClusterTopologyInfo info = new ClusterTopologyInfo(expected, lastUpdated, false);
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
    final ClusterTopologyInfo info = new ClusterTopologyInfo(hosts, lastUpdated, false);
    AuroraHostListProvider.topologyCache.put(auroraHostListProvider.clusterId, info);
    // Test getCachedTopology with expired cache.
    assertNull(auroraHostListProvider.getCachedTopology());
  }
}
