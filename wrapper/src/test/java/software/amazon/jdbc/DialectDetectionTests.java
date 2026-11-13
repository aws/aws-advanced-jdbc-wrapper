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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.AuroraMysqlDialect;
import software.amazon.jdbc.dialect.AuroraPgDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.dialect.GlobalAuroraMysqlDialect;
import software.amazon.jdbc.dialect.GlobalAuroraPgDialect;
import software.amazon.jdbc.dialect.MariaDbDialect;
import software.amazon.jdbc.dialect.MultiAzClusterMysqlDialect;
import software.amazon.jdbc.dialect.MultiAzClusterPgDialect;
import software.amazon.jdbc.dialect.MysqlDialect;
import software.amazon.jdbc.dialect.PgDialect;
import software.amazon.jdbc.dialect.RdsMysqlDialect;
import software.amazon.jdbc.dialect.RdsPgDialect;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.storage.StorageService;

/**
 * End-to-end dialect detection through {@link PluginServiceImpl}: the URL-based provisional
 * dialect, then the fingerprint-based confirmation on the first connection, then the endpoint cache.
 * The statements the detector runs are stubbed by their text, so these tests also pin the number of
 * round trips each deployment costs.
 */
public class DialectDetectionTests {
  private static final String LOCALHOST = "localhost";
  private static final String RDS_DATABASE = "database-1.xyz.us-east-2.rds.amazonaws.com";
  private static final String RDS_AURORA_DATABASE = "database-2.cluster-xyz.us-east-2.rds.amazonaws.com";
  private static final String MYSQL_PROTOCOL = "jdbc:mysql://";
  private static final String PG_PROTOCOL = "jdbc:postgresql://";
  private static final String MARIA_PROTOCOL = "jdbc:mariadb://";
  private static final String UNKNOWN_PROTOCOL = "jdbc:something://";
  private final Properties props = new Properties();
  private AutoCloseable closeable;
  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private StorageService mockStorageService;
  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private DatabaseMetaData mockMetaData;
  @Mock private HostSpec mockHost;
  @Mock private ConnectionPluginManager mockPluginManager;
  @Mock private TargetDriverDialect mockTargetDriverDialect;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(this.mockServicesContainer.getHostListProviderService()).thenReturn(mockHostListProviderService);
    when(this.mockServicesContainer.getConnectionPluginManager()).thenReturn(mockPluginManager);
    when(this.mockServicesContainer.getStorageService()).thenReturn(mockStorageService);
    when(this.mockConnection.createStatement()).thenReturn(this.mockStatement);
    when(this.mockConnection.getMetaData()).thenReturn(this.mockMetaData);
    mockPluginManager.plugins = new ArrayList<>();
    DialectManager.resetEndpointCache();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    DialectManager.resetEndpointCache();
  }

  PluginServiceImpl getPluginService(String host, String protocol) throws SQLException {
    return getPluginService(host, protocol, "");
  }

  PluginServiceImpl getPluginService(String host, String protocol, String urlSuffix) throws SQLException {
    PluginServiceImpl pluginService = spy(
        new PluginServiceImpl(
            mockServicesContainer,
            new ExceptionManager(),
            props,
            protocol + host + urlSuffix,
            protocol,
            null,
            mockTargetDriverDialect,
            null,
            null));

    when(this.mockServicesContainer.getHostListProviderService()).thenReturn(pluginService);
    when(this.mockServicesContainer.getPluginService()).thenReturn(pluginService);
    doNothing().when(pluginService).updateHostListProvider();
    when(this.mockHost.getHostAndPort()).thenReturn(host);
    pluginService.setInitialConnectionHostSpec(mockHost);
    return pluginService;
  }

  // --- Initial (provisional) dialect from protocol + URL ---

  @ParameterizedTest
  @MethodSource("getInitialDialectArguments")
  public void testInitialDialectDetection(String protocol, String host, Object expectedDialect) throws SQLException {
    final DialectManager dialectManager = new DialectManager(this.getPluginService(host, protocol));
    final Dialect dialect = dialectManager.getDialect(protocol, host, new Properties());
    assertEquals(expectedDialect, dialect.getClass());
  }

  static Stream<Arguments> getInitialDialectArguments() {
    return Stream.of(
        Arguments.of(MYSQL_PROTOCOL, LOCALHOST, MysqlDialect.class),
        Arguments.of(MYSQL_PROTOCOL, RDS_DATABASE, RdsMysqlDialect.class),
        Arguments.of(MYSQL_PROTOCOL, RDS_AURORA_DATABASE, AuroraMysqlDialect.class),
        Arguments.of(PG_PROTOCOL, LOCALHOST, PgDialect.class),
        Arguments.of(PG_PROTOCOL, RDS_DATABASE, RdsPgDialect.class),
        Arguments.of(PG_PROTOCOL, RDS_AURORA_DATABASE, AuroraPgDialect.class),
        Arguments.of(MARIA_PROTOCOL, LOCALHOST, MariaDbDialect.class),
        Arguments.of(MARIA_PROTOCOL, RDS_DATABASE, MariaDbDialect.class),
        Arguments.of(MARIA_PROTOCOL, RDS_AURORA_DATABASE, MariaDbDialect.class)
    );
  }

  // --- MySQL family: the resolved dialect follows the server, not the URL or the protocol ---

  @ParameterizedTest(name = "{0} + {1}")
  @MethodSource("getMysqlFamilyArguments")
  void testMysqlFamilyDetection(
      String protocol, String host, MysqlServer server, Class<?> expectedDialect, int expectedRoundTrips)
      throws SQLException {
    stubMysqlServer(server);

    final PluginServiceImpl target = getPluginService(host, protocol);
    target.updateDialect(mockConnection);

    assertEquals(expectedDialect, target.dialect.getClass());
    verify(mockStatement, times(expectedRoundTrips)).executeQuery(anyString());
  }

  static Stream<Arguments> getMysqlFamilyArguments() {
    final Stream.Builder<Arguments> b = Stream.builder();
    for (final String protocol : new String[] {MYSQL_PROTOCOL, MARIA_PROTOCOL}) {
      b.add(Arguments.of(protocol, LOCALHOST, MysqlServer.COMMUNITY, MysqlDialect.class, 2));
      b.add(Arguments.of(protocol, LOCALHOST, MysqlServer.MARIADB, MariaDbDialect.class, 2));
      b.add(Arguments.of(protocol, LOCALHOST, MysqlServer.RDS, RdsMysqlDialect.class, 2));
      b.add(Arguments.of(protocol, LOCALHOST, MysqlServer.MULTI_AZ_CLUSTER, MultiAzClusterMysqlDialect.class, 2));
      b.add(Arguments.of(protocol, LOCALHOST, MysqlServer.AURORA, AuroraMysqlDialect.class, 3));
      b.add(Arguments.of(protocol, LOCALHOST, MysqlServer.GLOBAL_AURORA, GlobalAuroraMysqlDialect.class, 3));
      // Same cluster, reached through an instance endpoint: the result must not change.
      b.add(Arguments.of(protocol, RDS_DATABASE, MysqlServer.GLOBAL_AURORA, GlobalAuroraMysqlDialect.class, 3));
      b.add(Arguments.of(protocol, RDS_DATABASE, MysqlServer.AURORA, AuroraMysqlDialect.class, 3));
      // A cluster endpoint that turns out to be a Multi-AZ DB cluster rather than Aurora.
      b.add(Arguments.of(
          protocol, RDS_AURORA_DATABASE, MysqlServer.MULTI_AZ_CLUSTER, MultiAzClusterMysqlDialect.class, 2));
    }
    return b.build();
  }

  // --- PostgreSQL family ---

  @ParameterizedTest(name = "{0}")
  @MethodSource("getPgFamilyArguments")
  void testPgFamilyDetection(String host, PgServer server, Class<?> expectedDialect, int expectedRoundTrips)
      throws SQLException {
    stubPgServer(server);

    final PluginServiceImpl target = getPluginService(host, PG_PROTOCOL);
    target.updateDialect(mockConnection);

    assertEquals(expectedDialect, target.dialect.getClass());
    verify(mockStatement, times(expectedRoundTrips)).executeQuery(anyString());
  }

  static Stream<Arguments> getPgFamilyArguments() {
    return Stream.of(
        Arguments.of(LOCALHOST, PgServer.COMMUNITY, PgDialect.class, 1),
        Arguments.of(LOCALHOST, PgServer.RDS, RdsPgDialect.class, 2),
        Arguments.of(LOCALHOST, PgServer.MULTI_AZ_CLUSTER, MultiAzClusterPgDialect.class, 2),
        Arguments.of(LOCALHOST, PgServer.AURORA, AuroraPgDialect.class, 2),
        Arguments.of(LOCALHOST, PgServer.GLOBAL_AURORA, GlobalAuroraPgDialect.class, 2),
        Arguments.of(RDS_DATABASE, PgServer.GLOBAL_AURORA, GlobalAuroraPgDialect.class, 2),
        Arguments.of(RDS_AURORA_DATABASE, PgServer.AURORA, AuroraPgDialect.class, 2),
        Arguments.of(RDS_AURORA_DATABASE, PgServer.MULTI_AZ_CLUSTER, MultiAzClusterPgDialect.class, 2),
        Arguments.of(RDS_AURORA_DATABASE, PgServer.RDS, RdsPgDialect.class, 2)
    );
  }

  // --- Unknown protocol ---

  @Test
  void testUnknownProtocolResolvesFromDriverMetadata() throws SQLException {
    when(mockMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");
    stubPgServer(PgServer.AURORA);

    final PluginServiceImpl target = getPluginService(LOCALHOST, UNKNOWN_PROTOCOL);
    target.updateDialect(mockConnection);

    assertEquals(AuroraPgDialect.class, target.dialect.getClass());
  }

  @Test
  void testUnknownProtocolWithoutFingerprintThrows() throws SQLException {
    doThrow(new SQLException("not supported")).when(mockStatement).executeQuery(anyString());

    final PluginServiceImpl target = getPluginService(LOCALHOST, UNKNOWN_PROTOCOL);
    assertThrows(SQLException.class, () -> target.updateDialect(mockConnection));
  }

  // --- Fingerprint unavailable: the provisional dialect is kept ---

  @Test
  void testFingerprintFailureKeepsProvisionalDialect() throws SQLException {
    doThrow(new SQLException("permission denied")).when(mockStatement).executeQuery(anyString());

    final PluginServiceImpl target = getPluginService(RDS_AURORA_DATABASE, PG_PROTOCOL);
    target.updateDialect(mockConnection);

    assertEquals(AuroraPgDialect.class, target.dialect.getClass());
    assertTrue(target.isDialectConfirmed());
  }

  // --- Endpoint cache ---

  @Test
  void testResolvedDialectIsCachedByHost() throws SQLException {
    stubMysqlServer(MysqlServer.AURORA);
    final PluginServiceImpl first = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    first.updateDialect(mockConnection);
    assertEquals(AuroraMysqlDialect.class, first.dialect.getClass());

    // Same host, different URL parameters: no further statements, dialect confirmed up front.
    final Connection unusedConnection = mock(Connection.class);
    final PluginServiceImpl second = getPluginService(LOCALHOST, MYSQL_PROTOCOL, "/other_db?param=1");
    assertEquals(AuroraMysqlDialect.class, second.dialect.getClass());
    assertTrue(second.isDialectConfirmed());
    second.updateDialect(unusedConnection);
    verify(unusedConnection, never()).createStatement();
  }

  @Test
  void testCacheIsPerHost() throws SQLException {
    stubMysqlServer(MysqlServer.AURORA);
    getPluginService(LOCALHOST, MYSQL_PROTOCOL).updateDialect(mockConnection);

    final PluginServiceImpl other = getPluginService(RDS_DATABASE, MYSQL_PROTOCOL);
    assertEquals(RdsMysqlDialect.class, other.dialect.getClass());
  }

  // --- Server models and statement stubs ---

  enum MysqlServer { COMMUNITY, MARIADB, RDS, MULTI_AZ_CLUSTER, AURORA, GLOBAL_AURORA }

  enum PgServer { COMMUNITY, RDS, MULTI_AZ_CLUSTER, AURORA, GLOBAL_AURORA }

  /**
   * Routes each statement the detector may run to the answer the modelled server would give.
   * Anything else fails, as it would on a real server of the other family.
   */
  private void stubMysqlServer(final MysqlServer server) throws SQLException {
    final Map<String, String> vars = new LinkedHashMap<>();
    final int topology;
    final int routines;
    final int globalTable;
    final int regions;
    switch (server) {
      case MARIADB:
        vars.put("version", "10.11.6-MariaDB-log");
        vars.put("version_comment", "mariadb.org binary distribution");
        topology = 0;
        routines = 0;
        globalTable = 0;
        regions = 0;
        break;
      case RDS:
        vars.put("version", "8.0.35");
        vars.put("version_comment", "Source distribution");
        vars.put("report_host", "");
        topology = 1;
        routines = 1;
        globalTable = 0;
        regions = 0;
        break;
      case MULTI_AZ_CLUSTER:
        vars.put("version", "8.0.35");
        vars.put("version_comment", "Source distribution");
        vars.put("report_host", "10.0.0.1");
        topology = 1;
        routines = 1;
        globalTable = 0;
        regions = 0;
        break;
      case AURORA:
        vars.put("version", "8.0.32");
        vars.put("version_comment", "Source distribution");
        vars.put("aurora_version", "3.05.2");
        vars.put("report_host", "");
        topology = 1;
        routines = 1;
        globalTable = 1;
        regions = 1;
        break;
      case GLOBAL_AURORA:
        vars.put("version", "8.0.32");
        vars.put("version_comment", "Source distribution");
        vars.put("aurora_version", "3.05.2");
        vars.put("report_host", "");
        topology = 1;
        routines = 1;
        globalTable = 1;
        regions = 2;
        break;
      case COMMUNITY:
      default:
        vars.put("version", "8.0.36");
        vars.put("version_comment", "MySQL Community Server - GPL");
        vars.put("report_host", "");
        topology = 0;
        routines = 0;
        globalTable = 0;
        regions = 0;
        break;
    }

    doAnswer(invocation -> {
      final String sql = invocation.getArgument(0);
      if (sql.startsWith("SHOW VARIABLES")) {
        return variablesResultSet(vars);
      }
      if (sql.contains("information_schema.routines")) {
        final ResultSet rs = singleRow();
        when(rs.getInt("rds_topology")).thenReturn(topology);
        when(rs.getInt("rds_routines")).thenReturn(routines);
        when(rs.getInt("global_db_status")).thenReturn(globalTable);
        return rs;
      }
      if (sql.contains("information_schema.aurora_global_db_status") && globalTable > 0) {
        final ResultSet rs = singleRow();
        when(rs.getInt(1)).thenReturn(regions);
        return rs;
      }
      throw new SQLException("You have an error in your SQL syntax: " + sql);
    }).when(mockStatement).executeQuery(any());
  }

  private void stubPgServer(final PgServer server) throws SQLException {
    final boolean aurora = server == PgServer.AURORA || server == PgServer.GLOBAL_AURORA;
    final boolean rds = server != PgServer.COMMUNITY;
    final boolean multiAzCapable = server == PgServer.RDS || server == PgServer.MULTI_AZ_CLUSTER;
    final String multiAzId = server == PgServer.MULTI_AZ_CLUSTER ? "db-ABCDEF123456" : null;
    final int regions = server == PgServer.GLOBAL_AURORA ? 2 : 1;

    doAnswer(invocation -> {
      final String sql = invocation.getArgument(0);
      if (sql.contains("pg_catalog.pg_proc")) {
        final ResultSet rs = singleRow();
        when(rs.getBoolean("aurora")).thenReturn(aurora);
        when(rs.getBoolean("limitless")).thenReturn(false);
        when(rs.getBoolean("global_db_capable")).thenReturn(aurora);
        when(rs.getBoolean("multi_az_capable")).thenReturn(multiAzCapable);
        when(rs.getBoolean("rds")).thenReturn(rds);
        when(rs.getString("server_version")).thenReturn("15.4");
        return rs;
      }
      if (sql.contains("aurora_global_db_status()") && aurora) {
        final ResultSet rs = singleRow();
        when(rs.getInt(1)).thenReturn(regions);
        return rs;
      }
      if (sql.contains("rds_tools.multi_az_db_cluster_source_dbi_resource_id()") && multiAzCapable) {
        final ResultSet rs = singleRow();
        when(rs.getString(1)).thenReturn(multiAzId);
        return rs;
      }
      throw new SQLException("ERROR: syntax error: " + sql);
    }).when(mockStatement).executeQuery(any());
  }

  private static ResultSet singleRow() throws SQLException {
    final ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true, false);
    return rs;
  }

  private static ResultSet variablesResultSet(final Map<String, String> vars) throws SQLException {
    final ResultSet rs = mock(ResultSet.class);
    final String[] names = vars.keySet().toArray(new String[0]);
    final String[] values = vars.values().toArray(new String[0]);
    final int[] cursor = {-1};
    when(rs.next()).thenAnswer(inv -> ++cursor[0] < names.length);
    when(rs.getString(1)).thenAnswer(inv -> names[cursor[0]]);
    when(rs.getString(2)).thenAnswer(inv -> values[cursor[0]]);
    return rs;
  }
}
