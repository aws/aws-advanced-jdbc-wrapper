/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc.plugin.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.awslabs.jdbc.HostRole;
import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.PluginService;
import com.amazon.awslabs.jdbc.hostlistprovider.AuroraHostListProvider;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class ClusterAwareReaderFailoverHandlerTest {

  @Mock AuroraHostListProvider mockHostListProvider;
  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;

  private AutoCloseable closeable;
  private final Properties properties = new Properties();
  private final List<HostSpec> defaultHosts = Arrays.asList(
      new HostSpec("writer", 1234, HostRole.WRITER),
      new HostSpec("reader1", 1234, HostRole.READER),
      new HostSpec("reader2", 1234, HostRole.READER),
      new HostSpec("reader3", 1234, HostRole.READER),
      new HostSpec("reader4", 1234, HostRole.READER),
      new HostSpec("reader5", 1234, HostRole.READER)
  );

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testFailover() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: successful connection for host at index 4
    final List<HostSpec> hosts = defaultHosts;
    final int currentHostIndex = 2;
    final int successHostIndex = 4;
    for (int i = 0; i < hosts.size(); i++) {
      if (i != successHostIndex) {
        when(mockPluginService.connect(hosts.get(i), properties))
            .thenThrow(new SQLException("exception", "08S01", null));
      } else {
        when(mockPluginService.connect(hosts.get(i), properties)).thenReturn(mockConnection);
      }
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(hosts.get(hostIndex).getUrl());
    }
    when(mockHostListProvider.getDownHosts()).thenReturn(downHosts);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties);
    final ReaderFailoverResult result = target.failover(hosts, hosts.get(currentHostIndex));

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(successHostIndex, result.getConnectionIndex());

    final HostSpec successHost = hosts.get(successHostIndex);
    verify(mockHostListProvider, atLeast(4)).addToDownHostList(any());
    verify(mockHostListProvider, never()).addToDownHostList(eq(successHost));
    verify(mockHostListProvider, times(1)).removeFromDownHostList(eq(successHost));
  }

  @Test
  public void testFailover_timeout() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: failure to get reader since process is limited to 5s and each attempt
    // to connect takes 20s
    final List<HostSpec> hosts = defaultHosts;
    final int currentHostIndex = 2;
    for (HostSpec host : hosts) {
      when(mockPluginService.connect(host, properties))
          .thenAnswer((Answer<Connection>) invocation -> {
            Thread.sleep(20000);
            return mockConnection;
          });
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(hosts.get(hostIndex).getUrl());
    }
    when(mockHostListProvider.getDownHosts()).thenReturn(downHosts);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties,
            5000,
            30000);
    final ReaderFailoverResult result =
        target.failover(hosts, hosts.get(currentHostIndex));

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());
  }

  @Test
  public void testFailover_nullOrEmptyHostList() throws SQLException {
    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties);
    final HostSpec currentHost = new HostSpec("writer", 1234);

    ReaderFailoverResult result = target.failover(null, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());

    final List<HostSpec> hosts = new ArrayList<>();
    result = target.failover(hosts, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());
  }

  @Test
  public void testGetReader_connectionSuccess() throws SQLException {
    // even number of connection attempts
    // first connection attempt to return succeeds, second attempt cancelled
    // expected test result: successful connection for host at index 2
    when(mockHostListProvider.getDownHosts()).thenReturn(new HashSet<>());
    final List<HostSpec> hosts = defaultHosts.subList(0, 3); // 2 connection attempts (writer not attempted)
    final HostSpec slowHost = hosts.get(1);
    final HostSpec fastHost = hosts.get(2);
    when(mockPluginService.connect(slowHost, properties))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(20000);
                  return mockConnection;
                });
    when(mockPluginService.connect(eq(fastHost), eq(properties))).thenReturn(mockConnection);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(2, result.getConnectionIndex());

    verify(mockHostListProvider, never()).addToDownHostList(any());
    verify(mockHostListProvider, times(1)).removeFromDownHostList(eq(fastHost));
  }

  @Test
  public void testGetReader_connectionFailure() throws SQLException {
    // odd number of connection attempts
    // first connection attempt to return fails
    // expected test result: failure to get reader
    when(mockHostListProvider.getDownHosts()).thenReturn(new HashSet<>());
    final List<HostSpec> hosts = defaultHosts.subList(0, 4); // 3 connection attempts (writer not attempted)
    when(mockPluginService.connect(any(), eq(properties))).thenThrow(new SQLException("exception", "08S01", null));

    final int currentHostIndex = 2;

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());

    final HostSpec currentHost = hosts.get(currentHostIndex);
    verify(mockHostListProvider, atLeastOnce()).addToDownHostList(eq(currentHost));
    verify(mockHostListProvider, never())
        .addToDownHostList(
            eq(hosts.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX)));
  }

  @Test
  public void testGetReader_connectionAttemptsTimeout() throws SQLException {
    // connection attempts time out before they can succeed
    // first connection attempt to return times out
    // expected test result: failure to get reader
    when(mockHostListProvider.getDownHosts()).thenReturn(new HashSet<>());
    final List<HostSpec> hosts = defaultHosts.subList(0, 3); // 2 connection attempts (writer not attempted)
    when(mockPluginService.connect(any(), eq(properties)))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  try {
                    Thread.sleep(5000);
                  } catch (InterruptedException exception) {
                    // ignore
                  }
                  return mockConnection;
                });

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties,
            60000,
            1000);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());

    verify(mockHostListProvider, never()).addToDownHostList(any());
  }

  @Test
  public void testGetHostTuplesByPriority() {
    final List<HostSpec> originalHosts = defaultHosts;

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(originalHosts.get(hostIndex).getUrl());
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties);
    final List<ClusterAwareReaderFailoverHandler.HostTuple> tuplesByPriority =
        target.getHostTuplesByPriority(originalHosts, downHosts);

    final int activeReaderOriginalIndex = 1;
    final int downReaderOriginalIndex = 5;

    // get new positions of active reader, writer, down reader in tuplesByPriority
    final int activeReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(activeReaderOriginalIndex, tuplesByPriority);
    final int writerTupleIndex =
        getHostTupleIndexFromOriginalIndex(
            FailoverConnectionPlugin.WRITER_CONNECTION_INDEX, tuplesByPriority);
    final int downReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(downReaderOriginalIndex, tuplesByPriority);

    // assert the following priority ordering: active readers, writer, down readers
    final int numActiveReaders = 2;
    assertTrue(writerTupleIndex > activeReaderTupleIndex);
    assertEquals(numActiveReaders, writerTupleIndex);
    assertTrue(downReaderTupleIndex > writerTupleIndex);
    assertEquals(6, tuplesByPriority.size());
  }

  private int getHostTupleIndexFromOriginalIndex(
      int originalIndex, List<ClusterAwareReaderFailoverHandler.HostTuple> tuples) {
    for (int i = 0; i < tuples.size(); i++) {
      ClusterAwareReaderFailoverHandler.HostTuple tuple = tuples.get(i);
      if (tuple.getIndex() == originalIndex) {
        return i;
      }
    }
    return -1;
  }

  @Test
  public void testGetReaderTuplesByPriority() {
    final List<HostSpec> originalHosts = defaultHosts;

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(originalHosts.get(hostIndex).getUrl());
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockHostListProvider,
            mockPluginService,
            properties);
    final List<ClusterAwareReaderFailoverHandler.HostTuple> readerTuples =
        target.getReaderTuplesByPriority(originalHosts, downHosts);

    final int activeReaderOriginalIndex = 1;
    final int downReaderOriginalIndex = 5;

    // get new positions of active reader, down reader in readerTuples
    final int activeReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(activeReaderOriginalIndex, readerTuples);
    final int downReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(downReaderOriginalIndex, readerTuples);

    // assert the following priority ordering: active readers, down readers
    final int numActiveReaders = 2;
    final ClusterAwareReaderFailoverHandler.HostTuple writerTuple =
        new ClusterAwareReaderFailoverHandler.HostTuple(
            originalHosts.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX),
            FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    assertTrue(downReaderTupleIndex > activeReaderTupleIndex);
    assertTrue(downReaderTupleIndex >= numActiveReaders);
    assertFalse(readerTuples.contains(writerTuple));
    assertEquals(5, readerTuples.size());
  }
}
