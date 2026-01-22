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

package software.amazon.jdbc.plugin.srw;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingSQLException;

public class SimpleReadWriteSplittingPluginTest {
  private static final int TEST_PORT = 5432;
  private static final String WRITE_ENDPOINT = "writer.cluster-xyz.us-east-1.rds.amazonaws.com";
  private static final String READ_ENDPOINT = "reader.cluster-xyz.us-east-1.rds.amazonaws.com";
  private static final Properties defaultProps = new Properties();

  private final HostSpec writerHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(WRITE_ENDPOINT).port(TEST_PORT).role(HostRole.WRITER).build();
  private final HostSpec readerHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(READ_ENDPOINT).port(TEST_PORT).role(HostRole.READER).build();

  private AutoCloseable closeable;

  @Mock private JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock private PluginService mockPluginService;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private Connection mockWriterConn;
  @Mock private Connection mockClosedWriterConn;
  @Mock private Connection mockReaderConn;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private EnumSet<NodeChangeOptions> mockChanges;
  @Mock private HostSpecBuilder mockHostSpecBuilder;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    mockDefaultBehavior();
    setupDefaultProperties();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    defaultProps.clear();
  }

  void setupDefaultProperties() {
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(defaultProps, WRITE_ENDPOINT);
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(defaultProps, READ_ENDPOINT);
  }

  void mockDefaultBehavior() throws SQLException {
    when(this.mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(this.mockPluginService.connect(eq(writerHostSpec), any(Properties.class), any()))
        .thenReturn(mockWriterConn);
    when(this.mockPluginService.getHostRole(mockWriterConn)).thenReturn(HostRole.WRITER);
    when(this.mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);
    when(this.mockPluginService.connect(eq(readerHostSpec), any(Properties.class), any()))
        .thenReturn(mockReaderConn);
    when(this.mockConnectFunc.call()).thenReturn(mockWriterConn);
    when(mockWriterConn.createStatement()).thenReturn(mockStatement);
    when(mockReaderConn.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(any(String.class))).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockClosedWriterConn.isClosed()).thenReturn(true);
    when(mockHostListProviderService.getHostSpecBuilder()).thenReturn(mockHostSpecBuilder);
    when(mockHostListProviderService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockHostSpecBuilder.host(any())).thenReturn(mockHostSpecBuilder);
    when(mockHostSpecBuilder.port(any(Integer.class))).thenReturn(mockHostSpecBuilder);
    when(mockHostSpecBuilder.role(any())).thenReturn(mockHostSpecBuilder);
    when(mockHostSpecBuilder.availability(any())).thenReturn(mockHostSpecBuilder);
    when(mockHostSpecBuilder.build()).thenReturn(writerHostSpec, readerHostSpec);
  }

  @Test
  public void testConstructor_missingWriteEndpoint() {
    Properties props = new Properties();
    // No write endpoint set
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(props, READ_ENDPOINT);

    assertThrows(RuntimeException.class, () ->
        new SimpleReadWriteSplittingPlugin(mockPluginService, props, null, null, null, null, null));
  }

  @Test
  public void testSwitchToReader_noReaderEndpoint() throws SQLException {
    Properties props = new Properties();
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(props, WRITE_ENDPOINT);
    // No read endpoint set

    assertThrows(RuntimeException.class, () ->
        new SimpleReadWriteSplittingPlugin(mockPluginService, props, null, null, null, null, null));
  }

  @Test
  public void testSetReadOnly_trueFalse() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null,
        writerHostSpec,
        readerHostSpec);

    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn), eq(readerHostSpec));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec);

    plugin.switchConnectionIfRequired(false);

    verify(mockPluginService, times(1))
        .setCurrentConnection(eq(mockWriterConn), eq(writerHostSpec));
  }

  @Test
  public void testSetReadOnly_trueFalse_endpointsWithPort() throws SQLException {
    int port = 1234;
    Properties props = new Properties();
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(props, WRITE_ENDPOINT + ":" + port);
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(props, READ_ENDPOINT + ":" + port);

    when(this.mockPluginService.connect(any(), eq(props), any()))
        .thenReturn(mockReaderConn);

    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        props,
        mockHostListProviderService,
        null,
        null,
        null,
        null);

    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(1))
        .setCurrentConnection(eq(mockReaderConn), any());
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(READ_ENDPOINT, plugin.getReaderHostSpec().getHost());
    assertEquals(port, plugin.getReaderHostSpec().getPort());

    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec);

    when(this.mockPluginService.connect(any(), eq(props), any()))
        .thenReturn(mockWriterConn);

    plugin.switchConnectionIfRequired(false);

    verify(mockPluginService, times(1))
        .setCurrentConnection(eq(mockWriterConn), any());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(WRITE_ENDPOINT, plugin.getWriterHostSpec().getHost());
    assertEquals(port, plugin.getWriterHostSpec().getPort());
  }

  @Test
  public void testSetReadOnlyTrue_alreadyOnReader() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        mockReaderConn,
        null, null);

    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  public void testSetReadOnlyFalse_alreadyOnWriter() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        mockReaderConn,
        null, null);

    plugin.switchConnectionIfRequired(false);

    verify(mockPluginService, never())
        .setCurrentConnection(any(Connection.class), any(HostSpec.class));
  }

  @Test
  public void testSetReadOnlyFalse_inTransaction() {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(true);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        mockReaderConn,
        null, null);

    assertThrows(ReadWriteSplittingSQLException.class, () ->
        plugin.switchConnectionIfRequired(false));
  }

  @Test
  public void testSetReadOnly_closedConnection() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockClosedWriterConn);
    when(mockClosedWriterConn.isClosed()).thenReturn(true);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        mockReaderConn,
        null, null);

    assertThrows(ReadWriteSplittingSQLException.class, () ->
        plugin.switchConnectionIfRequired(true));
  }

  @Test
  public void testNotifyConnectionChanged_inReadWriteSplit() {
    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        mockReaderConn,
        writerHostSpec,
        readerHostSpec);

    // Simulate being in read-write split mode
    try {
      plugin.switchConnectionIfRequired(true);
    } catch (SQLException e) {
      // ignore for test setup
    }

    OldConnectionSuggestedAction result = plugin.notifyConnectionChanged(mockChanges);
    assertEquals(OldConnectionSuggestedAction.PRESERVE, result);
  }

  @Test
  public void testNotifyConnectionChanged_notInReadWriteSplit() {
    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        mockReaderConn,
        null, null);

    OldConnectionSuggestedAction result = plugin.notifyConnectionChanged(mockChanges);
    assertEquals(OldConnectionSuggestedAction.NO_OPINION, result);
  }

  @Test
  public void testReleaseResources() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockReaderConn.isClosed()).thenReturn(false);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        mockReaderConn,
        null, null);

    plugin.releaseResources();

    verify(mockReaderConn, times(1)).close();
  }

  @Test
  public void testWrongRoleConnection_writerEndpointToReader() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockPluginService.getHostRole(any())).thenReturn(HostRole.READER); // Wrong role

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        null,
        null);

    assertThrows(ReadWriteSplittingSQLException.class, () ->
        plugin.switchConnectionIfRequired(false));
  }

  @Test
  public void testWrongRoleConnection_readerEndpointToWriter() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockPluginService.getHostRole(any())).thenReturn(HostRole.WRITER); // Wrong role for reader

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        null, null);

    plugin.switchConnectionIfRequired(true);

    // While it should use the current connection as fallback, it should not store it.
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testGetVerifiedConnection_wrongRoleRetryReader() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockPluginService.connect(eq(readerHostSpec), any(Properties.class), any()))
        .thenReturn(mockWriterConn) // First call returns wrong role
        .thenReturn(mockReaderConn); // Second call returns correct role
    when(mockPluginService.getHostRole(mockWriterConn)).thenReturn(HostRole.WRITER);
    when(mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        null,
        readerHostSpec);

    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(2))
        .connect(eq(readerHostSpec), any(Properties.class), any());
    verify(mockWriterConn, times(1)).close();
    assertEquals(mockReaderConn, plugin.getReaderConnection());
  }

  @Test
  public void testGetVerifiedConnection_wrongRoleRetryWriter() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(readerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockPluginService.connect(eq(writerHostSpec), any(Properties.class), any()))
        .thenReturn(mockReaderConn) // First call returns wrong role
        .thenReturn(mockWriterConn); // Second call returns correct role
    when(mockPluginService.getHostRole(mockWriterConn)).thenReturn(HostRole.WRITER);
    when(mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        writerHostSpec,
        null);

    plugin.switchConnectionIfRequired(false);

    verify(mockPluginService, times(2))
        .connect(eq(writerHostSpec), any(Properties.class), any());
    verify(mockReaderConn, times(1)).close();
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testGetVerifiedConnection_sqlExceptionRetry() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockPluginService.connect(eq(readerHostSpec), any(Properties.class), any()))
        .thenThrow(new SQLException("Connection failed"))
        .thenReturn(mockReaderConn);
    when(mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);
    when(mockPluginService.isLoginException(any(SQLException.class), any())).thenReturn(false);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        null,
        readerHostSpec);

    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(2))
        .connect(eq(readerHostSpec), any(Properties.class), any());
    assertEquals(mockReaderConn, plugin.getReaderConnection());
  }

  @Test
  public void testGetVerifiedConnection_loginExceptionRetry() throws SQLException {
    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);
    when(mockPluginService.connect(eq(readerHostSpec), any(Properties.class), any()))
        .thenThrow(new SQLException("Login exception"))
        .thenReturn(mockReaderConn);
    when(mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);
    when(mockPluginService.isLoginException(any(SQLException.class), any())).thenReturn(true);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        null,
        readerHostSpec);

    plugin.switchConnectionIfRequired(true);

    verify(mockPluginService, times(1))
        .connect(eq(readerHostSpec), any(Properties.class), any());
    // While it should use the current connection as fallback, it should not store it.
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testConnect_nonInitialConnection() throws SQLException {
    when(mockConnectFunc.call()).thenReturn(mockWriterConn);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService, defaultProps);

    Connection result = plugin.connect("jdbc:postgresql", writerHostSpec, defaultProps, false, mockConnectFunc);

    assertEquals(mockWriterConn, result);
    verify(mockConnectFunc, times(1)).call();
    verify(mockPluginService, times(0)).getHostRole(mockWriterConn);
  }

  @Test
  public void testConnect_verificationDisabled() throws SQLException {
    Properties props = new Properties();
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(props, WRITE_ENDPOINT);
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(props, READ_ENDPOINT);
    SimpleReadWriteSplittingPlugin.VERIFY_NEW_SRW_CONNECTIONS.set(props, "false");

    when(mockConnectFunc.call()).thenReturn(mockWriterConn);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService, props);

    Connection result = plugin.connect("jdbc:postgresql", writerHostSpec, props, true, mockConnectFunc);

    assertEquals(mockWriterConn, result);
    verify(mockConnectFunc, times(1)).call();
    verify(mockPluginService, times(0)).getHostRole(mockWriterConn);
  }

  @Test
  public void testConnect_writerClusterEndpoint() throws SQLException {
    final HostSpec writerClusterHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com").port(TEST_PORT).role(HostRole.WRITER).build();

    when(mockPluginService.connect(eq(writerClusterHost), any(Properties.class), any()))
        .thenReturn(mockWriterConn);
    when(mockPluginService.getHostRole(mockWriterConn)).thenReturn(HostRole.WRITER);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        null,
        null);

    Connection result = plugin.connect("jdbc:postgresql", writerClusterHost, defaultProps, true, null);

    assertEquals(mockWriterConn, result);
    verify(mockPluginService, times(1)).connect(eq(writerClusterHost), any(Properties.class), any());
    verify(mockPluginService, times(1)).getHostRole(mockWriterConn);
  }

  @Test
  public void testConnect_readerClusterEndpoint() throws SQLException {
    HostSpec readerClusterHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("test-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com").port(TEST_PORT).role(HostRole.READER).build();

    when(mockConnectFunc.call()).thenReturn(mockReaderConn);
    when(mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        null,
        null);

    Connection result = plugin.connect("jdbc:postgresql", readerClusterHost, defaultProps, true, mockConnectFunc);

    assertEquals(mockReaderConn, result);
    verify(mockPluginService, times(0)).connect(eq(readerClusterHost), any(Properties.class), any());
    verify(mockPluginService, times(1)).getHostRole(mockReaderConn);
    verify(mockConnectFunc, times(1)).call();
  }

  @Test
  public void testConnect_verificationFailsFallback() throws SQLException {
    final HostSpec writerClusterHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com").port(TEST_PORT).role(HostRole.WRITER).build();

    Properties timeoutProps = new Properties();
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(timeoutProps, WRITE_ENDPOINT);
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(timeoutProps, READ_ENDPOINT);
    SimpleReadWriteSplittingPlugin.SRW_CONNECT_RETRY_TIMEOUT_MS.set(timeoutProps, "5");

    when(mockConnectFunc.call()).thenReturn(mockReaderConn);
    when(mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService, timeoutProps);

    Connection result = plugin.connect("jdbc:postgresql", writerClusterHost, timeoutProps, true, mockConnectFunc);

    assertEquals(mockReaderConn, result);
    verify(mockPluginService, times(1)).getHostRole(mockReaderConn);
    verify(mockConnectFunc, times(2)).call();
  }

  @Test
  public void testConnect_nonRdsClusterEndpoint() throws SQLException {
    final HostSpec customHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("custom-db.example.com").port(TEST_PORT).role(HostRole.WRITER).build();

    when(mockConnectFunc.call()).thenReturn(mockWriterConn);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService, defaultProps);

    Connection result = plugin.connect("jdbc:postgresql", customHost, defaultProps, true, mockConnectFunc);

    assertEquals(mockWriterConn, result);
    verify(mockPluginService, times(0)).getHostRole(mockWriterConn);
    verify(mockConnectFunc, times(1)).call();
  }

  @Test
  public void testConnect_nonRdsClusterEndpointWriterVerify() throws SQLException {
    final HostSpec customHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("custom-db.example.com").port(TEST_PORT).role(HostRole.WRITER).build();

    Properties props = new Properties();
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(props, WRITE_ENDPOINT);
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(props, READ_ENDPOINT);
    SimpleReadWriteSplittingPlugin.VERIFY_INITIAL_CONNECTION_TYPE.set(props, "writer");

    when(mockConnectFunc.call()).thenReturn(mockWriterConn);
    when(mockPluginService.getHostRole(mockWriterConn)).thenReturn(HostRole.WRITER);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        props,
        mockHostListProviderService,
        null,
        null,
        null,
        null);

    Connection result = plugin.connect("jdbc:postgresql", customHost, defaultProps, true, mockConnectFunc);

    assertEquals(mockWriterConn, result);
    verify(mockPluginService, times(0)).connect(eq(customHost), any(Properties.class), any());
    verify(mockPluginService, times(1)).getHostRole(mockWriterConn);
    verify(mockConnectFunc, times(1)).call();
  }

  @Test
  public void testConnect_nonRdsClusterEndpointReaderVerify() throws SQLException {
    final HostSpec customHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("custom-db.example.com").port(TEST_PORT).role(HostRole.READER).build();

    Properties props = new Properties();
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(props, WRITE_ENDPOINT);
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(props, READ_ENDPOINT);
    SimpleReadWriteSplittingPlugin.VERIFY_INITIAL_CONNECTION_TYPE.set(props, "reader");

    when(mockConnectFunc.call()).thenReturn(mockReaderConn);
    when(mockPluginService.getHostRole(mockReaderConn)).thenReturn(HostRole.READER);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        props,
        mockHostListProviderService,
        null,
        null,
        null,
        null);

    Connection result = plugin.connect("jdbc:postgresql", customHost, defaultProps, true, mockConnectFunc);

    assertEquals(mockReaderConn, result);
    verify(mockPluginService, times(0)).connect(eq(customHost), any(Properties.class), any());
    verify(mockPluginService, times(1)).getHostRole(mockReaderConn);
    verify(mockConnectFunc, times(1)).call();
  }

  @Test
  public void testClosePooledReaderConnectionAfterSetReadOnly() throws SQLException {
    doReturn(writerHostSpec)
        .doReturn(writerHostSpec)
        .doReturn(readerHostSpec)
        .when(this.mockPluginService).getCurrentHostSpec();
    doReturn(mockReaderConn).when(mockPluginService).connect(readerHostSpec, null);
    when(mockPluginService.getDriverProtocol()).thenReturn("jdbc:postgresql://");
    when(mockPluginService.isPooledConnection()).thenReturn(true);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        mockWriterConn,
        null,
        writerHostSpec,
        readerHostSpec);
    final SimpleReadWriteSplittingPlugin spyPlugin = spy(plugin);

    spyPlugin.switchConnectionIfRequired(true);
    spyPlugin.switchConnectionIfRequired(false);

    verify(spyPlugin, times(1)).closeConnectionIfIdle(eq(mockReaderConn));
  }

  @Test
  public void testClosePooledWriterConnectionAfterSetReadOnly() throws SQLException {
    doReturn(writerHostSpec)
        .doReturn(writerHostSpec)
        .doReturn(readerHostSpec)
        .doReturn(readerHostSpec)
        .doReturn(writerHostSpec)
        .when(this.mockPluginService).getCurrentHostSpec();
    doReturn(mockWriterConn).when(mockPluginService).connect(writerHostSpec, null);
    when(mockPluginService.getDriverProtocol()).thenReturn("jdbc:postgresql://");
    when(mockPluginService.isPooledConnection()).thenReturn(true);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        defaultProps,
        mockHostListProviderService,
        null,
        null,
        writerHostSpec,
        readerHostSpec);
    final SimpleReadWriteSplittingPlugin spyPlugin = spy(plugin);

    spyPlugin.switchConnectionIfRequired(true);
    spyPlugin.switchConnectionIfRequired(false);
    spyPlugin.switchConnectionIfRequired(true);

    verify(spyPlugin, times(1)).closeConnectionIfIdle(eq(mockWriterConn));
  }
}
