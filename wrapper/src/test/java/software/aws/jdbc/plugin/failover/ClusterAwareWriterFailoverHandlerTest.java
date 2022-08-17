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

package software.aws.jdbc.plugin.failover;

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
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import software.aws.jdbc.HostSpec;
import software.aws.jdbc.PluginService;
import software.aws.jdbc.hostlistprovider.AuroraHostListProvider;

class ClusterAwareWriterFailoverHandlerTest {

  @Mock AuroraHostListProvider mockHostListProvider;
  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;
  @Mock ReaderFailoverHandler mockReaderFailover;
  @Mock Connection mockWriterConnection;
  @Mock Connection mockNewWriterConnection;
  @Mock Connection mockReaderAConnection;
  @Mock Connection mockReaderBConnection;

  private AutoCloseable closeable;
  private final Properties properties = new Properties();
  private final HostSpec newWriterHost = new HostSpec("new-writer-host");
  private final HostSpec writer = new HostSpec("writer-host");
  private final HostSpec readerA = new HostSpec("reader-a-host");
  private final HostSpec readerB = new HostSpec("reader-b-host");
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
    when(mockPluginService.connect(refEq(writer), eq(properties))).thenReturn(mockConnection);
    when(mockPluginService.connect(refEq(readerA), eq(properties))).thenThrow(SQLException.class);
    when(mockPluginService.connect(refEq(readerB), eq(properties))).thenThrow(SQLException.class);

    when(mockHostListProvider.getTopology(any(Connection.class), eq(true))).thenReturn(topology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList())).thenThrow(SQLException.class);

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockHostListProvider,
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

    final InOrder inOrder = Mockito.inOrder(mockHostListProvider);
    inOrder.verify(mockHostListProvider).addToDownHostList(refEq(writer));
    inOrder.verify(mockHostListProvider).removeFromDownHostList(refEq(writer));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: successfully re-connect to initial writer; return new connection taskB: successfully
   * connect to readerA and then new writer, but it takes more time than taskA expected test result:
   * new connection by taskA
   */
  @Test
  public void testReconnectToWriter_SlowReaderA() throws SQLException {
    when(mockPluginService.connect(refEq(writer), eq(properties))).thenReturn(mockWriterConnection);
    when(mockPluginService.connect(refEq(readerB), eq(properties))).thenThrow(SQLException.class);

    when(mockHostListProvider.getTopology(eq(mockWriterConnection), eq(true))).thenReturn(topology);
    when(mockHostListProvider.getTopology(eq(mockReaderAConnection), eq(true))).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenAnswer(
            (Answer<ReaderFailoverResult>)
                invocation -> {
                  Thread.sleep(5000);
                  return new ReaderFailoverResult(mockReaderAConnection, 1, true);
                });

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockHostListProvider,
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

    final InOrder inOrder = Mockito.inOrder(mockHostListProvider);
    inOrder.verify(mockHostListProvider).addToDownHostList(refEq(writer));
    inOrder.verify(mockHostListProvider).removeFromDownHostList(refEq(writer));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes taskA: successfully re-connect to writer; return new connection taskB:
   * successfully connect to readerA and retrieve topology, but latest writer is not new (defer to
   * taskA) expected test result: new connection by taskA
   */
  @Test
  public void testReconnectToWriter_taskBDefers() throws SQLException {
    when(mockPluginService.connect(refEq(writer), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockPluginService.connect(refEq(readerB), eq(properties))).thenThrow(SQLException.class);

    when(mockHostListProvider.getTopology(any(Connection.class), eq(true))).thenReturn(topology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockHostListProvider,
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

    final InOrder inOrder = Mockito.inOrder(mockHostListProvider);
    inOrder.verify(mockHostListProvider).addToDownHostList(refEq(writer));
    inOrder.verify(mockHostListProvider).removeFromDownHostList(refEq(writer));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB, taskA sees no changes
   * taskA: successfully re-connect to writer; return connection to initial writer but it takes more
   * time than taskB taskB: successfully connect to readerA and then to new-writer expected test
   * result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_SlowWriter() throws SQLException {
    when(mockPluginService.connect(refEq(writer), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockPluginService.connect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.connect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.connect(refEq(newWriterHost), eq(properties))).thenReturn(mockNewWriterConnection);

    when(mockHostListProvider.getTopology(eq(mockWriterConnection), eq(true))).thenReturn(topology);
    when(mockHostListProvider.getTopology(eq(mockReaderAConnection), eq(true))).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockHostListProvider,
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

    verify(mockHostListProvider, times(1)).addToDownHostList(refEq(writer));
    verify(mockHostListProvider, times(1)).removeFromDownHostList(refEq(newWriterHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, initial-writer, reader-A, reader-B] taskA: successfully
   * reconnect, but initial-writer is now a reader (defer to taskB) taskB: successfully connect to
   * readerA and then to new-writer expected test result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_taskADefers() throws SQLException {
    when(mockPluginService.connect(writer, properties)).thenReturn(mockConnection);
    when(mockPluginService.connect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.connect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.connect(refEq(newWriterHost), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockNewWriterConnection;
                });

    final List<HostSpec> newTopology = Arrays.asList(newWriterHost, writer, readerA, readerB);

    when(mockHostListProvider.getTopology(any(Connection.class), eq(true))).thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockHostListProvider,
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

    verify(mockHostListProvider, times(1)).addToDownHostList(refEq(writer));
    verify(mockHostListProvider, times(1)).removeFromDownHostList(refEq(newWriterHost));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: fail to re-connect to writer due to failover timeout taskB: successfully connect to
   * readerA and then fail to connect to writer due to failover timeout expected test result: no
   * connection
   */
  @Test
  public void testFailedToConnect_failoverTimeout() throws SQLException {
    when(mockPluginService.connect(refEq(writer), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockWriterConnection;
                });
    when(mockPluginService.connect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.connect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.connect(refEq(newWriterHost), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockNewWriterConnection;
                });

    when(mockHostListProvider.getTopology(eq(mockWriterConnection), any(Boolean.class)))
        .thenReturn(topology);
    when(mockHostListProvider.getTopology(eq(mockNewWriterConnection), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            mockReaderFailover,
            properties,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(topology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockHostListProvider, times(1)).addToDownHostList(refEq(writer));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB taskA: fail to re-connect to
   * writer due to exception taskB: successfully connect to readerA and then fail to connect to
   * writer due to exception expected test result: no connection
   */
  @Test
  public void testFailedToConnect_taskAException_taskBWriterException() throws SQLException {
    when(mockPluginService.connect(refEq(writer), eq(properties))).thenThrow(
        new SQLException("exception", "08S01", null));
    when(mockPluginService.connect(refEq(readerA), eq(properties))).thenReturn(mockReaderAConnection);
    when(mockPluginService.connect(refEq(readerB), eq(properties))).thenReturn(mockReaderBConnection);
    when(mockPluginService.connect(refEq(newWriterHost), eq(properties))).thenThrow(SQLException.class);

    when(mockHostListProvider.getTopology(any(Connection.class), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderAConnection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            mockReaderFailover,
            properties,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(topology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockHostListProvider, times(1)).addToDownHostList(refEq(writer));
    verify(mockHostListProvider, atLeastOnce()).addToDownHostList(refEq(newWriterHost));
  }
}
