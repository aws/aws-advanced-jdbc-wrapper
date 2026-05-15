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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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

public class DialectDetectionTests {
  private static final String LOCALHOST = "localhost";
  private static final String RDS_DATABASE = "database-1.xyz.us-east-2.rds.amazonaws.com";
  private static final String RDS_AURORA_DATABASE = "database-2.cluster-xyz.us-east-2.rds.amazonaws.com";
  private static final String MYSQL_PROTOCOL = "jdbc:mysql://";
  private static final String PG_PROTOCOL = "jdbc:postgresql://";
  private static final String MARIA_PROTOCOL = "jdbc:mariadb://";
  private final Properties props = new Properties();
  private AutoCloseable closeable;
  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private StorageService mockStorageService;
  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
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
    when(this.mockHost.getUrl()).thenReturn("url");
    mockPluginManager.plugins = new ArrayList<>();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    DialectManager.resetEndpointCache();
  }

  PluginServiceImpl getPluginService(String host, String protocol) throws SQLException {
    PluginServiceImpl pluginService = spy(
        new PluginServiceImpl(
            mockServicesContainer,
            new ExceptionManager(),
            props,
            protocol + host,
            protocol,
            null,
            mockTargetDriverDialect,
            null,
            null));

    when(this.mockServicesContainer.getHostListProviderService()).thenReturn(pluginService);
    doNothing().when(pluginService).updateHostListProvider();
    return pluginService;
  }

  // --- Initial dialect detection tests ---

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

  // --- MySQL dialect update tests ---

  @Test
  void testUpdateDialectMysqlUnchanged() throws SQLException {
    // All candidate isDialect checks fail -> dialect stays as MysqlDialect.
    // MysqlDialect candidates: GLOBAL_AURORA_MYSQL, AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());

    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMysqlToAurora() throws SQLException {
    // MysqlDialect candidates: GLOBAL_AURORA_MYSQL, AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL
    // GlobalAuroraMysqlDialect.isDialect: checkExistenceQueries for global tables -> fails (default).
    // AuroraMysqlDialect.isDialect: checkExistenceQueries("SHOW VARIABLES LIKE 'aurora_version'") -> returns row.
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement)
        .executeQuery(eq("SHOW VARIABLES LIKE 'aurora_version'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    when(mockServicesContainer.getPluginService()).thenReturn(target);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(AuroraMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMysqlToMultiAz() throws SQLException {
    // MysqlDialect candidates: GLOBAL_AURORA_MYSQL, AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL
    // GlobalAuroraMysqlDialect and AuroraMysqlDialect fail (default).
    // MultiAzClusterMysqlDialect.isDialect:
    //   1. checkExistenceQueries(TOPOLOGY_TABLE_EXISTS_QUERY, TOPOLOGY_QUERY) -> both return rows
    //   2. SHOW VARIABLES LIKE 'report_host' -> returns non-empty value (IP address)
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement).executeQuery(eq(
        "SELECT 1 AS tmp FROM information_schema.tables WHERE"
            + " table_schema = 'mysql' AND table_name = 'rds_topology'"));
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement).executeQuery(eq(
        "SELECT id, endpoint, port FROM mysql.rds_topology"));
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(2)).thenReturn("10.0.0.1");
      return rs;
    }).when(mockStatement).executeQuery(eq("SHOW VARIABLES LIKE 'report_host'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MultiAzClusterMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMysqlToRds() throws SQLException {
    // MysqlDialect candidates: GLOBAL_AURORA_MYSQL, AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL
    // All earlier candidates fail.
    // RdsMysqlDialect.isDialect:
    //   1. super.isDialect (MysqlDialect): VERSION_QUERY -> "Source distribution" (not "mysql") -> returns false
    //   2. RdsMysqlDialect's own check: VERSION_QUERY -> "Source distribution" -> matches
    //   3. REPORT_HOST_EXISTS_QUERY -> returns a row with empty string for column 2
    // Note: VERSION_QUERY is called twice (by super and by RdsMysqlDialect), so we use doAnswer.
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(2)).thenReturn("Source distribution");
      return rs;
    }).when(mockStatement).executeQuery(eq("SHOW VARIABLES LIKE 'version_comment'"));
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(2)).thenReturn("");
      return rs;
    }).when(mockStatement).executeQuery(eq("SHOW VARIABLES LIKE 'report_host'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(RdsMysqlDialect.class, target.dialect.getClass());
  }

  // --- PostgreSQL dialect update tests ---

  @Test
  void testUpdateDialectPgUnchanged() throws SQLException {
    // All candidate isDialect checks fail -> dialect stays as PgDialect.
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());

    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(PgDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectPgToAurora() throws SQLException {
    // PgDialect candidates: GLOBAL_AURORA_PG, AURORA_PG, RDS_MULTI_AZ_PG_CLUSTER, RDS_PG
    // GlobalAuroraPgDialect.isDialect: checks aurora_stat_utils (succeeds), then global funcs -> fails.
    // AuroraPgDialect.isDialect:
    //   1. super.isDialect (PgDialect): SELECT 1 FROM pg_catalog.pg_proc LIMIT 1 -> returns row
    //   2. AURORA_UTILS_EXIST_QUERY -> returns row with aurora_stat_utils = true
    //   3. TOPOLOGY_EXISTS_QUERY -> returns row
    // Note: Both GlobalAuroraPgDialect and AuroraPgDialect query AURORA_UTILS_EXIST_QUERY,
    // so we need to return fresh ResultSets each time.
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement).executeQuery(eq(
        "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1"));
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getBoolean("aurora_stat_utils")).thenReturn(true);
      return rs;
    }).when(mockStatement).executeQuery(eq(
        "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
            + "FROM pg_catalog.pg_settings "
            + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'"));
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement).executeQuery(eq(
        "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(AuroraPgDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectPgToMultiAz() throws SQLException {
    // PgDialect candidates: GLOBAL_AURORA_PG, AURORA_PG, RDS_MULTI_AZ_PG_CLUSTER, RDS_PG
    // GlobalAuroraPgDialect and AuroraPgDialect fail.
    // MultiAzClusterPgDialect.isDialect:
    //   SELECT multi_az_db_cluster_source_dbi_resource_id FROM
    //     rds_tools.multi_az_db_cluster_source_dbi_resource_id()
    //   -> returns a row with non-null string
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(1)).thenReturn("db-ABCDEF123456");
      return rs;
    }).when(mockStatement).executeQuery(eq(
        "SELECT multi_az_db_cluster_source_dbi_resource_id FROM "
            + "rds_tools.multi_az_db_cluster_source_dbi_resource_id()"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MultiAzClusterPgDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectPgToRds() throws SQLException {
    // PgDialect candidates: GLOBAL_AURORA_PG, AURORA_PG, RDS_MULTI_AZ_PG_CLUSTER, RDS_PG
    // GlobalAuroraPgDialect and AuroraPgDialect fail (aurora_stat_utils query throws for Global,
    //   and for Aurora the pg_proc check may succeed but aurora_utils or topology fails).
    // RdsPgDialect.isDialect:
    //   1. super.isDialect (PgDialect): SELECT 1 FROM pg_catalog.pg_proc LIMIT 1 -> returns row
    //   2. EXTENSIONS_EXIST_SQL -> returns row with rds_tools=true, aurora_stat_utils=false
    // Note: pg_proc query may be called multiple times by different candidates.
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement).executeQuery(eq(
        "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1"));
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getBoolean("rds_tools")).thenReturn(true);
      when(rs.getBoolean("aurora_stat_utils")).thenReturn(false);
      return rs;
    }).when(mockStatement).executeQuery(eq(
        "SELECT (setting LIKE '%rds_tools%') AS rds_tools, "
            + "(setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
            + "FROM pg_catalog.pg_settings "
            + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(RdsPgDialect.class, target.dialect.getClass());
  }

  // --- MariaDB dialect update tests ---

  @Test
  void testUpdateDialectMariaUnchanged() throws SQLException {
    // All candidate isDialect checks fail -> dialect stays as MariaDbDialect.
    // MariaDbDialect candidates: AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL, MYSQL
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());

    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MariaDbDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMariaToMysqlAurora() throws SQLException {
    // MariaDbDialect candidates: AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL, MYSQL
    // AuroraMysqlDialect.isDialect: SHOW VARIABLES LIKE 'aurora_version' -> returns a row.
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement)
        .executeQuery(eq("SHOW VARIABLES LIKE 'aurora_version'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    when(mockServicesContainer.getPluginService()).thenReturn(target);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(AuroraMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMariaToMysqlMultiAz() throws SQLException {
    // MariaDbDialect candidates: AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL, MYSQL
    // AuroraMysqlDialect.isDialect fails.
    // MultiAzClusterMysqlDialect.isDialect:
    //   1. checkExistenceQueries(TOPOLOGY_TABLE_EXISTS_QUERY, TOPOLOGY_QUERY) -> both return rows
    //   2. SHOW VARIABLES LIKE 'report_host' -> returns non-empty value
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement).executeQuery(eq(
        "SELECT 1 AS tmp FROM information_schema.tables WHERE"
            + " table_schema = 'mysql' AND table_name = 'rds_topology'"));
    doAnswer(invocation -> createMockResultSet(true)).when(mockStatement).executeQuery(eq(
        "SELECT id, endpoint, port FROM mysql.rds_topology"));
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(2)).thenReturn("10.0.0.1");
      return rs;
    }).when(mockStatement).executeQuery(eq("SHOW VARIABLES LIKE 'report_host'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MultiAzClusterMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMariaToMysqlRds() throws SQLException {
    // MariaDbDialect candidates: AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL, MYSQL
    // AuroraMysqlDialect and MultiAzClusterMysqlDialect fail.
    // RdsMysqlDialect.isDialect:
    //   1. super.isDialect (MysqlDialect) must return false
    //   2. VERSION_QUERY returns "Source distribution"
    //   3. REPORT_HOST_EXISTS_QUERY returns a row with empty string
    // Note: VERSION_QUERY is called twice (by super and by RdsMysqlDialect).
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(2)).thenReturn("Source distribution");
      return rs;
    }).when(mockStatement).executeQuery(eq("SHOW VARIABLES LIKE 'version_comment'"));
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(2)).thenReturn("");
      return rs;
    }).when(mockStatement).executeQuery(eq("SHOW VARIABLES LIKE 'report_host'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(RdsMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMariaToMysql() throws SQLException {
    // MariaDbDialect candidates: AURORA_MYSQL, RDS_MULTI_AZ_MYSQL_CLUSTER, RDS_MYSQL, MYSQL
    // All earlier candidates fail.
    // MysqlDialect.isDialect: SHOW VARIABLES LIKE 'version_comment' -> returns "MySQL Community Server (GPL)"
    doThrow(new SQLException("not found")).when(mockStatement).executeQuery(any());
    doAnswer(invocation -> {
      ResultSet rs = createMockResultSet(true);
      when(rs.getString(2)).thenReturn("MySQL Community Server (GPL)");
      return rs;
    }).when(mockStatement).executeQuery(eq("SHOW VARIABLES LIKE 'version_comment'"));

    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MysqlDialect.class, target.dialect.getClass());
  }

  // --- Helper methods ---

  /**
   * Creates a mock ResultSet that returns the specified value on the first call to next(),
   * then returns false on subsequent calls.
   */
  private ResultSet createMockResultSet(boolean hasRows) throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    if (hasRows) {
      when(rs.next()).thenReturn(true, false);
    } else {
      when(rs.next()).thenReturn(false);
    }
    return rs;
  }
}
