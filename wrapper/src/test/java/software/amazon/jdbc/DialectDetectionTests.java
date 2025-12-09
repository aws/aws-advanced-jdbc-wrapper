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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
  @Mock private ResultSet mockSuccessResultSet;
  @Mock private ResultSet mockFailResultSet;
  @Mock private ResultSetMetaData mockResultSetMetaData;
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
    when(this.mockFailResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(this.mockResultSetMetaData.getColumnCount()).thenReturn(4);
    when(this.mockFailResultSet.next()).thenReturn(false);
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
    return pluginService;
  }

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

  @Test
  void testUpdateDialectMysqlUnchanged() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMysqlToRds() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet);
    when(mockStatement.executeQuery("SHOW VARIABLES LIKE 'version_comment'")).thenReturn(mockSuccessResultSet);
    when(mockStatement.executeQuery("SHOW VARIABLES LIKE 'report_host'")).thenReturn(mockSuccessResultSet);
    when(mockSuccessResultSet.getString(2)).thenReturn(
        "Source distribution", "Source distribution", "");
    when(mockSuccessResultSet.next()).thenReturn(true, false, true, true);
    when(mockSuccessResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockFailResultSet.next()).thenReturn(false);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(RdsMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  @Disabled
  // TODO: fix me: need to split this test into two separate tests:
  // 1) test DialectManager.getDialect() to return RdsMultiAzDbClusterMysqlDialect
  // 2) test PluginServiceImpl.updateDialect() with mocked DialectManager.getDialect()
  void testUpdateDialectMysqlToTaz() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet, mockSuccessResultSet);
    when(mockSuccessResultSet.next()).thenReturn(true);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(AuroraMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMysqlToAurora() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet);
    when(mockStatement.executeQuery("SHOW VARIABLES LIKE 'aurora_version'")).thenReturn(mockSuccessResultSet);
    when(mockSuccessResultSet.next()).thenReturn(true, false);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MYSQL_PROTOCOL);
    when(mockServicesContainer.getPluginService()).thenReturn(target);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(AuroraMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectPgUnchanged() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet);
    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(PgDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectPgToRds() throws SQLException {
    when(mockStatement.executeQuery(any()))
        .thenReturn(mockSuccessResultSet, mockFailResultSet, mockFailResultSet, mockSuccessResultSet);
    when(mockSuccessResultSet.getBoolean(any())).thenReturn(false);
    when(mockSuccessResultSet.getBoolean("rds_tools")).thenReturn(true);
    when(mockSuccessResultSet.getBoolean("aurora_stat_utils")).thenReturn(false);
    when(mockSuccessResultSet.next()).thenReturn(true);
    when(mockFailResultSet.next()).thenReturn(false);
    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(RdsPgDialect.class, target.dialect.getClass());
  }

  @Test
  @Disabled
  // TODO: fix me: need to split this test into two separate tests:
  // 1) test DialectManager.getDialect() to return RdsMultiAzDbClusterMysqlDialect
  // 2) test PluginServiceImpl.updateDialect() with mocked DialectManager.getDialect()
  void testUpdateDialectPgToTaz() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockSuccessResultSet);
    when(mockSuccessResultSet.getBoolean(any())).thenReturn(false);
    when(mockSuccessResultSet.next()).thenReturn(true);
    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MultiAzClusterPgDialect.class, target.dialect.getClass());
  }

  @Test
  @Disabled
  // TODO: fix me: need to split this test into two separate tests:
  // 1) test DialectManager.getDialect() to return RdsMultiAzDbClusterMysqlDialect
  // 2) test PluginServiceImpl.updateDialect() with mocked DialectManager.getDialect()
  void testUpdateDialectPgToAurora() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockSuccessResultSet);
    when(mockSuccessResultSet.next()).thenReturn(true);
    when(mockSuccessResultSet.getBoolean(any())).thenReturn(true);
    final PluginServiceImpl target = getPluginService(LOCALHOST, PG_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(AuroraPgDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMariaUnchanged() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MariaDbDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMariaToMysqlRds() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet);
    when(mockStatement.executeQuery("SHOW VARIABLES LIKE 'version_comment'")).thenReturn(mockSuccessResultSet);
    when(mockStatement.executeQuery("SHOW VARIABLES LIKE 'report_host'")).thenReturn(mockSuccessResultSet);
    when(mockSuccessResultSet.getString(2)).thenReturn(
        "Source distribution", "Source distribution", "");
    when(mockSuccessResultSet.next()).thenReturn(true, false, true, true);
    when(mockSuccessResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockFailResultSet.next()).thenReturn(false);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(RdsMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  @Disabled
  // TODO: fix me: need to split this test into two separate tests:
  // 1) test DialectManager.getDialect() to return RdsMultiAzDbClusterMysqlDialect
  // 2) test PluginServiceImpl.updateDialect() with mocked DialectManager.getDialect()
  void testUpdateDialectMariaToMysqlTaz() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet, mockSuccessResultSet);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(MultiAzClusterMysqlDialect.class, target.dialect.getClass());
  }

  @Test
  void testUpdateDialectMariaToMysqlAurora() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(mockFailResultSet);
    when(mockStatement.executeQuery("SHOW VARIABLES LIKE 'aurora_version'")).thenReturn(mockSuccessResultSet);
    when(mockSuccessResultSet.next()).thenReturn(true, false);
    final PluginServiceImpl target = getPluginService(LOCALHOST, MARIA_PROTOCOL);
    when(mockServicesContainer.getPluginService()).thenReturn(target);
    target.setInitialConnectionHostSpec(mockHost);
    target.updateDialect(mockConnection);
    assertEquals(AuroraMysqlDialect.class, target.dialect.getClass());
  }
}
