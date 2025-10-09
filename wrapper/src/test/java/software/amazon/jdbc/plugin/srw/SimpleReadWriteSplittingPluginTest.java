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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
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
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
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
    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(props, READ_ENDPOINT);

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
  public void testSwitchToReader_noReaderEndpoint() throws SQLException {
    Properties props = new Properties();
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(props, WRITE_ENDPOINT);
    // No read endpoint set

    when(mockPluginService.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(mockPluginService.isInTransaction()).thenReturn(false);

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(
        mockPluginService,
        props,
        mockHostListProviderService,
        mockWriterConn,
        null,
        writerHostSpec,
        null);

    // Should fall back to writer connection when no reader endpoint
    plugin.switchConnectionIfRequired(true);

    assertEquals(mockWriterConn, plugin.getWriterConnection());
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

    // While it should use the invalid roled reader, it should not store it.
    assertEquals(null, plugin.getReaderConnection());
  }
}
