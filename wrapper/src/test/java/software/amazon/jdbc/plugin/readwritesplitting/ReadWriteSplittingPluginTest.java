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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.util.SqlState;

public class ReadWriteSplittingPluginTest {
  private static final String TEST_PROTOCOL = "jdbc:postgresql:";
  private static final int TEST_PORT = 5432;
  private static final Properties defaultProps = new Properties();

  private final HostSpec writerHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-0").port(TEST_PORT).build();
  private final HostSpec readerHostSpec1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-1").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpec2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-2").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpec3 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-3").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpecWithIncorrectRole = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-1").port(TEST_PORT).role(HostRole.WRITER).build();
  private final HostSpec instanceUrlHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("jdbc:aws-wrapper:postgresql://my-instance-name.XYZ.us-east-2.rds.amazonaws.com").port(TEST_PORT)
      .build();
  private final HostSpec ipUrlHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("10.10.10.10").port(TEST_PORT).build();
  private final HostSpec clusterUrlHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com").port(TEST_PORT).build();
  private final List<HostSpec> defaultHosts = Arrays.asList(
      writerHostSpec,
      readerHostSpec1,
      readerHostSpec2,
      readerHostSpec3);
  private final List<HostSpec> singleReaderTopology = Arrays.asList(
      writerHostSpec,
      readerHostSpec1);

  private AutoCloseable closeable;

  @Mock private JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock private JdbcCallable<ResultSet, SQLException> mockSqlFunction;
  @Mock private PluginService mockPluginService;
  @Mock private Dialect mockDialect;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private Connection mockWriterConn;
  @Mock private Connection mockNewWriterConn;
  @Mock private Connection mockClosedWriterConn;
  @Mock private Connection mockReaderConn1;
  @Mock private Connection mockReaderConn2;
  @Mock private Connection mockReaderConn3;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private EnumSet<NodeChangeOptions> mockChanges;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    mockDefaultBehavior();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    defaultProps.clear();
  }

  void mockDefaultBehavior() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(this.mockPluginService.getAllHosts()).thenReturn(defaultHosts);
    when(this.mockPluginService.getHosts()).thenReturn(defaultHosts);
    when(this.mockPluginService.getHostSpecByStrategy(eq(HostRole.READER), eq("random")))
        .thenReturn(readerHostSpec1);
    when(this.mockPluginService.connect(eq(writerHostSpec), any(Properties.class)))
        .thenReturn(mockWriterConn);
    when(this.mockPluginService.connect(eq(writerHostSpec), any(Properties.class), any()))
        .thenReturn(mockWriterConn);
    when(this.mockPluginService.getInitialConnectionHostSpec()).thenReturn(writerHostSpec);
    when(this.mockPluginService.getHostRole(mockWriterConn)).thenReturn(HostRole.WRITER);
    when(this.mockPluginService.getHostRole(mockReaderConn1)).thenReturn(HostRole.READER);
    when(this.mockPluginService.getHostRole(mockReaderConn2)).thenReturn(HostRole.READER);
    when(this.mockPluginService.getHostRole(mockReaderConn3)).thenReturn(HostRole.READER);
    when(this.mockPluginService.connect(eq(readerHostSpec1), any(Properties.class)))
        .thenReturn(mockReaderConn1);
    when(this.mockPluginService.connect(eq(readerHostSpec1), any(Properties.class), any()))
        .thenReturn(mockReaderConn1);
    when(this.mockPluginService.connect(eq(readerHostSpec2), any(Properties.class)))
        .thenReturn(mockReaderConn2);
    when(this.mockPluginService.connect(eq(readerHostSpec2), any(Properties.class), any()))
        .thenReturn(mockReaderConn2);
    when(this.mockPluginService.connect(eq(readerHostSpec3), any(Properties.class)))
        .thenReturn(mockReaderConn3);
    when(this.mockPluginService.connect(eq(readerHostSpec3), any(Properties.class), any()))
        .thenReturn(mockReaderConn3);
    when(this.mockPluginService.acceptsStrategy(any(), eq("random"))).thenReturn(true);
    when(this.mockConnectFunc.call()).thenReturn(mockWriterConn);
    when(mockWriterConn.createStatement()).thenReturn(mockStatement);
    when(mockReaderConn1.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(any(String.class))).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockClosedWriterConn.isClosed()).thenReturn(true);
  }

  @Test
  public void testSetReadOnly_trueFalse() throws SQLException {
    when(this.mockPluginService.getAllHosts()).thenReturn(singleReaderTopology);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null);
    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn1), not(eq(writerHostSpec)));
    verify(mockPluginService, times(0))
        .setCurrentConnection(eq(mockWriterConn), any(HostSpec.class));
    assertEquals(mockReaderConn1, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);

    plugin.switchConnectionIfRequired(false);

    verify(mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn1), not(eq(writerHostSpec)));
    verify(mockPluginService, times(1))
        .setCurrentConnection(eq(mockWriterConn), eq(writerHostSpec));
    assertEquals(mockReaderConn1, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnlyTrue_alreadyOnReader() throws SQLException {
    when(this.mockPluginService.getAllHosts()).thenReturn(singleReaderTopology);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        mockReaderConn1);

    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(0))
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertEquals(mockReaderConn1, plugin.getReaderConnection());
    assertNull(plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnlyFalse_alreadyOnWriter() throws SQLException {
    when(this.mockPluginService.getAllHosts()).thenReturn(singleReaderTopology);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null);
    plugin.switchConnectionIfRequired(false);

    verify(mockPluginService, times(0))
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnly_falseInTransaction() {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);
    when(this.mockPluginService.getAllHosts()).thenReturn(singleReaderTopology);
    when(mockPluginService.isInTransaction()).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        mockReaderConn1);

    final SQLException e =
        assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(SqlState.ACTIVE_SQL_TRANSACTION.getState(), e.getSQLState());
  }

  @Test
  public void testSetReadOnly_true() throws SQLException {
    final ReadWriteSplittingPlugin plugin =
        new ReadWriteSplittingPlugin(mockPluginService, defaultProps);
    plugin.switchConnectionIfRequired(true);

    assertEquals(mockReaderConn1, plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnly_false() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        mockReaderConn1);
    plugin.switchConnectionIfRequired(false);

    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_true_oneHost() throws SQLException {
    when(this.mockPluginService.getHosts()).thenReturn(Collections.singletonList(writerHostSpec));

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null);
    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(0))
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnly_false_writerConnectionFails() throws SQLException {
    when(mockPluginService.connect(eq(writerHostSpec), eq(defaultProps), any()))
        .thenThrow(SQLException.class);
    when(this.mockPluginService.getAllHosts()).thenReturn(singleReaderTopology);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn1);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec1);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        mockReaderConn1);

    final SQLException e =
        assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e.getSQLState());
    verify(mockPluginService, times(0))
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  public void testSetReadOnly_true_readerConnectionFailed() throws SQLException {
    when(this.mockPluginService.connect(eq(readerHostSpec1), eq(defaultProps), any()))
        .thenThrow(SQLException.class);
    when(this.mockPluginService.connect(eq(readerHostSpec2), eq(defaultProps), any()))
        .thenThrow(SQLException.class);
    when(this.mockPluginService.connect(eq(readerHostSpec3), eq(defaultProps), any()))
        .thenThrow(SQLException.class);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null);
    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(0))
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnlyOnClosedConnection() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockClosedWriterConn);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockClosedWriterConn,
        null);

    final SQLException e =
        assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(SqlState.CONNECTION_NOT_OPEN.getState(), e.getSQLState());
    verify(mockPluginService, times(0))
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testExecute_failoverToNewWriter() throws SQLException {
    when(mockSqlFunction.call()).thenThrow(FailoverSuccessSQLException.class);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockNewWriterConn);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null);

    assertThrows(
        SQLException.class,
        () -> plugin.execute(
            ResultSet.class,
            SQLException.class,
            mockStatement,
            "Statement.executeQuery",
            mockSqlFunction,
            new Object[] {
                "begin"}));
    verify(mockWriterConn, times(1)).close();
  }

  @Test
  public void testNotifyConnectionChange() {
    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null);

    final OldConnectionSuggestedAction suggestion = plugin.notifyConnectionChanged(mockChanges);

    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(OldConnectionSuggestedAction.NO_OPINION, suggestion);
  }

  @Test
  public void testConnectNonInitialConnection() throws SQLException {
    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null);

    final Connection connection =
        plugin.connect(TEST_PROTOCOL, writerHostSpec, defaultProps, false, this.mockConnectFunc);

    assertEquals(mockWriterConn, connection);
    verify(mockConnectFunc).call();
    verify(mockHostListProviderService, times(0)).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  public void testConnectRdsInstanceUrl() throws SQLException {
    when(this.mockPluginService.getInitialConnectionHostSpec()).thenReturn(readerHostSpecWithIncorrectRole);
    when(this.mockConnectFunc.call()).thenReturn(mockReaderConn1);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null);
    final Connection connection = plugin.connect(
        TEST_PROTOCOL,
        instanceUrlHostSpec,
        defaultProps,
        true,
        this.mockConnectFunc);

    assertEquals(mockReaderConn1, connection);
    verify(mockConnectFunc).call();
    verify(mockHostListProviderService, times(1)).setInitialConnectionHostSpec(eq(readerHostSpec1));
  }

  @Test
  public void testConnectReaderIpUrl() throws SQLException {
    when(this.mockConnectFunc.call()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getInitialConnectionHostSpec()).thenReturn(readerHostSpecWithIncorrectRole);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null);
    final Connection connection =
        plugin.connect(TEST_PROTOCOL, ipUrlHostSpec, defaultProps, true, this.mockConnectFunc);

    assertEquals(mockReaderConn1, connection);
    verify(mockConnectFunc).call();
    verify(mockHostListProviderService, times(1)).setInitialConnectionHostSpec(eq(readerHostSpec1));
  }

  @Test
  public void testConnectClusterUrl() throws SQLException {
    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null);
    final Connection connection =
        plugin.connect(TEST_PROTOCOL, clusterUrlHostSpec, defaultProps, true, this.mockConnectFunc);

    assertEquals(mockWriterConn, connection);
    verify(mockConnectFunc).call();
    verify(mockHostListProviderService, times(0)).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  public void testConnect_errorUpdatingHostSpec() throws SQLException {
    when(this.mockConnectFunc.call()).thenReturn(mockReaderConn1);
    when(this.mockPluginService.getHostRole(mockReaderConn1)).thenReturn(null);
    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null);

    assertThrows(
        SQLException.class,
        () -> plugin.connect(
            TEST_PROTOCOL,
            ipUrlHostSpec,
            defaultProps,
            true,
            this.mockConnectFunc));
    verify(mockHostListProviderService, times(0)).setInitialConnectionHostSpec(any(HostSpec.class));
  }

  @Test
  public void testExecuteClearWarnings() throws SQLException {
    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
            mockPluginService,
            defaultProps,
            mockHostListProviderService,
            mockWriterConn,
            mockReaderConn1);

    plugin.execute(
            ResultSet.class,
            SQLException.class,
            mockStatement,
            "Connection.clearWarnings",
            mockSqlFunction,
            new Object[] {}
    );
    verify(mockWriterConn, times(1)).clearWarnings();
    verify(mockReaderConn1, times(1)).clearWarnings();
  }

  @Test
  public void testExecuteClearWarningsOnClosedConnectionsIsNotCalled() throws SQLException {
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockReaderConn1.isClosed()).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
            mockPluginService,
            defaultProps,
            mockHostListProviderService,
            mockWriterConn,
            mockReaderConn1);

    plugin.execute(
            ResultSet.class,
            SQLException.class,
            mockStatement,
            "Connection.clearWarnings",
            mockSqlFunction,
            new Object[] {}
    );
    verify(mockWriterConn, never()).clearWarnings();
    verify(mockReaderConn1, never()).clearWarnings();
  }

  @Test
  public void testExecuteClearWarningsOnNullConnectionsIsNotCalled() throws SQLException {
    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null);

    // calling clearWarnings() on nullified connection would throw an exception
    assertDoesNotThrow(() -> {
      plugin.execute(
          ResultSet.class,
          SQLException.class,
          mockStatement,
          "Connection.clearWarnings",
          mockSqlFunction,
          new Object[] {}
      );
    });
  }

  @Test
  public void testClosePooledReaderConnectionAfterSetReadOnly() throws SQLException {
    doReturn(writerHostSpec)
        .doReturn(writerHostSpec)
        .doReturn(readerHostSpec1)
        .when(this.mockPluginService).getCurrentHostSpec();
    doReturn(mockReaderConn1).when(mockPluginService).connect(readerHostSpec1, null);
    when(mockPluginService.getDriverProtocol()).thenReturn("jdbc:postgresql://");
    when(mockPluginService.isPooledConnectionProvider(any(), any())).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null);
    final ReadWriteSplittingPlugin spyPlugin = spy(plugin);

    spyPlugin.switchConnectionIfRequired(true);
    spyPlugin.switchConnectionIfRequired(false);

    verify(spyPlugin, times(1)).closeConnectionIfIdle(eq(mockReaderConn1));
  }

  @Test
  public void testClosePooledWriterConnectionAfterSetReadOnly() throws SQLException {
    doReturn(writerHostSpec)
        .doReturn(writerHostSpec)
        .doReturn(readerHostSpec1)
        .doReturn(readerHostSpec1)
        .doReturn(writerHostSpec)
        .when(this.mockPluginService).getCurrentHostSpec();
    doReturn(mockWriterConn).when(mockPluginService).connect(writerHostSpec, null);
    when(mockPluginService.getDriverProtocol()).thenReturn("jdbc:postgresql://");
    when(mockPluginService.isPooledConnectionProvider(any(), any())).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null);
    final ReadWriteSplittingPlugin spyPlugin = spy(plugin);

    spyPlugin.switchConnectionIfRequired(true);
    spyPlugin.switchConnectionIfRequired(false);
    spyPlugin.switchConnectionIfRequired(true);

    verify(spyPlugin, times(1)).closeConnectionIfIdle(eq(mockWriterConn));
  }

  private static HikariConfig getHikariConfig(HostSpec hostSpec, Properties props) {
    final HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(3);
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(10000);
    return config;
  }

  private static String getPoolKey(HostSpec hostSpec, Properties props) {
    final String user = props.getProperty(PropertyDefinition.USER.name);
    final String somePropertyValue = props.getProperty("somePropertyValue");
    return hostSpec.getUrl() + user + somePropertyValue;
  }
}
