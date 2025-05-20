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

package software.amazon.jdbc.plugin.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class ClusterAwareWriterFailoverHandlerTest {

  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock ReaderFailoverHandler mockReaderFailover;
  @Mock Connection mockWriterConnection;
  @Mock Connection mockNewWriterConnection;
  @Mock Connection mockReaderAConnection;
  @Mock Connection mockReaderBConnection;
  @Mock Dialect mockDialect;

  private AutoCloseable closeable;
  private final Properties properties = new Properties();
  private final HostSpec newWriterHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("new-writer-host").build();
  private final HostSpec writer = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer-host").build();
  private final HostSpec readerA = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-a-host").build();
  private final HostSpec readerB = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-b-host").build();
  private final List<HostSpec> topology = Arrays.asList(writer, readerA, readerB);
  private final List<HostSpec> newTopology = Arrays.asList(newWriterHost, readerA, readerB);

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    writer.addAlias("writer-host");
    newWriterHost.addAlias("new-writer-host");
    readerA.addAlias("reader-a-host");
    readerB.addAlias("reader-b-host");
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testReconnectToWriter_taskBReaderException() throws SQLException {
    when(mockPluginService.forceConnect(refEq(writer), eq(properties))).thenReturn(mockConnection);
    when(mockPluginService.forceConnect(refEq(readerA), eq(properties))).thenThrow(SQLException.class);
    when(mockPluginService.forceConnect(refEq(readerB), eq(properties))).thenThrow(SQLException.class);

    when(mockPluginService.getAllHosts()).thenReturn(topology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList())).thenThrow(SQLException.class);

    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockPluginService,
            mockReaderFailover,
            properties,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(topology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockConnection);

    final InOrder inOrder = Mockito.inOrder(mockPluginService);
    inOrder.verify(mockPluginService).setAvailability(eq(writer.asAliases()), eq(HostAvailability.AVAILABLE));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>Topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB.
   * TaskA: successfully re-connect to initial writer; return new connection.
   * TaskB: successfully connect to readerA and then new writer, but it takes more time than taskA.
   * Expected test result: new connection by taskA.
   */
  @Test
  public void testReconnectToWriter_SlowReaderA() throws SQLException {
    when(mockPluginService.forceConnect(refEq(writer), eq(properties))).thenReturn(mockWriterConnection);
    when(mockPluginService.forceConnect(refEq(readerB), eq(properties))).thenThrow(SQLException.class);
    when(mockPluginService.forceConnect(refEq(newWriterHost), eq(properties))).thenReturn(mockNewWriterConnection);
    when(mockPluginService.getAllHosts()).thenReturn(topology).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenAnswer(
            (Answer<ReaderFailoverResult>)
                invocation -> {
                  Thread.sleep(5000);
                  return new ReaderFailoverResult(mockReaderAConnection, readerA, true);
                });

    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockPluginService,
            mockReaderFailover,
            properties,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(topology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockPluginService);
    inOrder.verify(mockPluginService).setAvailability(eq(writer.asAliases()), eq(HostAvailability.AVAILABLE));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>Topology: no changes.
   * TaskA: successfully re-connect to writer; return new connection.
   * TaskB: successfully connect to readerA and retrieve topology, but latest writer is not new (defer to taskA).
   * Expected test result: new connection by taskA.
   */
  @Test
  public void testReconnectToWriter_taskBDefers() throws SQLException {
    when(mockPluginService.forceConnect(refEq(writer), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockPluginService.forceConnect(refEq(readerB), eq(properties))).thenThrow(SQLException.class);

    when(mockPluginService.getAllHosts()).thenReturn(topology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, readerA, true));

    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockPluginService,
            mockReaderFailover,
            properties,
            60000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(topology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockPluginService);
    inOrder.verify(mockPluginService).setAvailability(eq(writer.asAliases()), eq(HostAvailability.AVAILABLE));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>Topology: changes to [new-writer, reader-A, reader-B] for taskB, taskA sees no changes.
   * taskA: successfully re-connect to writer; return connection to initial writer, but it takes more
   * time than taskB.
   * TaskB: successfully connect to readerA and then to new-writer.
   * Expected test result: new connection to writer by taskB.
   */
  @Test
  public void testConnectToReaderA_SlowWriter() throws SQLException {
    when(mockPluginService.forceConnect(refEq(writer), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockPluginService.forceConnect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.forceConnect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.forceConnect(refEq(newWriterHost), eq(properties))).thenReturn(mockNewWriterConnection);

    when(mockPluginService.getAllHosts()).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, readerA, true));

    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockPluginService,
            mockReaderFailover,
            properties,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(topology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(3, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockPluginService, times(1)).setAvailability(eq(newWriterHost.asAliases()), eq(HostAvailability.AVAILABLE));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>Topology: changes to [new-writer, initial-writer, reader-A, reader-B].
   * TaskA: successfully reconnect, but initial-writer is now a reader (defer to taskB).
   * TaskB: successfully connect to readerA and then to new-writer.
   * Expected test result: new connection to writer by taskB.
   */
  @Test
  public void testConnectToReaderA_taskADefers() throws SQLException {
    when(mockPluginService.forceConnect(writer, properties)).thenReturn(mockConnection);
    when(mockPluginService.forceConnect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.forceConnect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.forceConnect(refEq(newWriterHost), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockNewWriterConnection;
                });

    final List<HostSpec> newTopology = Arrays.asList(newWriterHost, writer, readerA, readerB);
    when(mockPluginService.getAllHosts()).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, readerA, true));

    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockPluginService,
            mockReaderFailover,
            properties,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(topology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(4, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockPluginService, atLeastOnce()).forceRefreshHostList(any(Connection.class));
    verify(mockPluginService, times(1)).setAvailability(eq(newWriterHost.asAliases()), eq(HostAvailability.AVAILABLE));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>Topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB.
   * TaskA: fail to re-connect to writer due to failover timeout.
   * TaskB: successfully connect to readerA and then fail to connect to writer due to failover timeout.
   * Expected test result: no connection.
   */
  @Test
  public void testFailedToConnect_failoverTimeout() throws SQLException {
    when(mockPluginService.forceConnect(refEq(writer), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockWriterConnection;
                });
    when(mockPluginService.forceConnect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.forceConnect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.forceConnect(refEq(newWriterHost), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockNewWriterConnection;
                });
    when(mockPluginService.getAllHosts()).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, readerA, true));

    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockPluginService,
            mockReaderFailover,
            properties,
            5000,
            2000,
            2000);

    final long startTimeNano = System.nanoTime();
    final WriterFailoverResult result = target.failover(topology);
    final long durationNano = System.nanoTime() - startTimeNano;

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockPluginService, atLeastOnce()).forceRefreshHostList(any(Connection.class));

    // 5s is a max allowed failover timeout; add 1s for inaccurate measurements
    assertTrue(TimeUnit.NANOSECONDS.toMillis(durationNano) < 6000);
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>Topology: changes to [new-writer, reader-A, reader-B] for taskB.
   * TaskA: fail to re-connect to writer due to exception.
   * TaskB: successfully connect to readerA and then fail to connect to writer due to exception.
   * Expected test result: no connection.
   */
  @Test
  public void testFailedToConnect_taskAException_taskBWriterException() throws SQLException {
    final SQLException exception = new SQLException("exception", "08S01", null);
    when(mockPluginService.forceConnect(refEq(writer), eq(properties))).thenThrow(exception);
    when(mockPluginService.forceConnect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.forceConnect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.forceConnect(refEq(newWriterHost), eq(properties))).thenThrow(exception);
    when(mockPluginService.isNetworkException(exception)).thenReturn(true);

    when(mockPluginService.getAllHosts()).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, readerA, true));

    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockPluginService,
            mockReaderFailover,
            properties,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(topology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockPluginService, atLeastOnce())
        .setAvailability(eq(newWriterHost.asAliases()), eq(HostAvailability.NOT_AVAILABLE));
  }
}
