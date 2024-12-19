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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.jdbc.plugin.failover.ClusterAwareReaderFailoverHandler.DEFAULT_FAILOVER_TIMEOUT;
import static software.amazon.jdbc.plugin.failover.ClusterAwareReaderFailoverHandler.DEFAULT_READER_CONNECT_TIMEOUT;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class ClusterAwareReaderFailoverHandlerTest {

  @Mock PluginService mockPluginService;
  @Mock Connection mockConnection;

  private AutoCloseable closeable;
  private final Properties properties = new Properties();
  private final List<HostSpec> defaultHosts = Arrays.asList(
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("writer").port(1234).role(HostRole.WRITER).build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("reader1").port(1234).role(HostRole.READER).build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("reader2").port(1234).role(HostRole.READER).build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("reader3").port(1234).role(HostRole.READER).build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("reader4").port(1234).role(HostRole.READER).build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("reader5").port(1234).role(HostRole.READER).build()
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
        final SQLException exception = new SQLException("exception", "08S01", null);
        when(mockPluginService.forceConnect(hosts.get(i), properties))
            .thenThrow(exception);
        when(mockPluginService.isNetworkException(exception)).thenReturn(true);
      } else {
        when(mockPluginService.forceConnect(hosts.get(i), properties)).thenReturn(mockConnection);
      }
    }

    hosts.get(2).setAvailability(HostAvailability.NOT_AVAILABLE);
    hosts.get(4).setAvailability(HostAvailability.NOT_AVAILABLE);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties);
    final ReaderFailoverResult result = target.failover(hosts, hosts.get(currentHostIndex));

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(hosts.get(successHostIndex), result.getHost());

    final HostSpec successHost = hosts.get(successHostIndex);
    verify(mockPluginService, atLeast(4)).setAvailability(any(), eq(HostAvailability.NOT_AVAILABLE));
    verify(mockPluginService, never())
        .setAvailability(eq(successHost.asAliases()), eq(HostAvailability.NOT_AVAILABLE));
    verify(mockPluginService, times(1))
        .setAvailability(eq(successHost.asAliases()), eq(HostAvailability.AVAILABLE));
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
      when(mockPluginService.forceConnect(host, properties))
          .thenAnswer((Answer<Connection>) invocation -> {
            Thread.sleep(20000);
            return mockConnection;
          });
    }

    hosts.get(2).setAvailability(HostAvailability.NOT_AVAILABLE);
    hosts.get(4).setAvailability(HostAvailability.NOT_AVAILABLE);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties,
            5000,
            30000,
            false);

    final long startTimeNano = System.nanoTime();
    final ReaderFailoverResult result =
        target.failover(hosts, hosts.get(currentHostIndex));
    final long durationNano = System.nanoTime() - startTimeNano;

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());

    // 5s is a max allowed failover timeout; add 1s for inaccurate measurements
    assertTrue(TimeUnit.NANOSECONDS.toMillis(durationNano) < 6000);
  }

  @Test
  public void testFailover_nullOrEmptyHostList() throws SQLException {
    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties);
    final HostSpec currentHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("writer")
        .port(1234).build();

    ReaderFailoverResult result = target.failover(null, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());

    final List<HostSpec> hosts = new ArrayList<>();
    result = target.failover(hosts, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());
  }

  @Test
  public void testGetReader_connectionSuccess() throws SQLException {
    // even number of connection attempts
    // first connection attempt to return succeeds, second attempt cancelled
    // expected test result: successful connection for host at index 2
    final List<HostSpec> hosts = defaultHosts.subList(0, 3); // 2 connection attempts (writer not attempted)
    final HostSpec slowHost = hosts.get(1);
    final HostSpec fastHost = hosts.get(2);
    when(mockPluginService.forceConnect(slowHost, properties))
        .thenAnswer(
            (Answer<Connection>)
                invocation -> {
                  Thread.sleep(20000);
                  return mockConnection;
                });
    when(mockPluginService.forceConnect(eq(fastHost), eq(properties))).thenReturn(mockConnection);

    Dialect mockDialect = Mockito.mock(Dialect.class);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));
    when(mockPluginService.getDialect()).thenReturn(mockDialect);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(hosts.get(2), result.getHost());

    verify(mockPluginService, never()).setAvailability(any(), eq(HostAvailability.NOT_AVAILABLE));
    verify(mockPluginService, times(1))
        .setAvailability(eq(fastHost.asAliases()), eq(HostAvailability.AVAILABLE));
  }

  @Test
  public void testGetReader_connectionFailure() throws SQLException {
    // odd number of connection attempts
    // first connection attempt to return fails
    // expected test result: failure to get reader
    final List<HostSpec> hosts = defaultHosts.subList(0, 4); // 3 connection attempts (writer not attempted)
    when(mockPluginService.forceConnect(any(), eq(properties))).thenThrow(new SQLException("exception", "08S01", null));

    Dialect mockDialect = Mockito.mock(Dialect.class);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));
    when(mockPluginService.getDialect()).thenReturn(mockDialect);

    final int currentHostIndex = 2;

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());
  }

  @Test
  public void testGetReader_connectionAttemptsTimeout() throws SQLException {
    // connection attempts time out before they can succeed
    // first connection attempt to return times out
    // expected test result: failure to get reader
    final List<HostSpec> hosts = defaultHosts.subList(0, 3); // 2 connection attempts (writer not attempted)
    when(mockPluginService.forceConnect(any(), eq(properties)))
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

    Dialect mockDialect = Mockito.mock(Dialect.class);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));
    when(mockPluginService.getDialect()).thenReturn(mockDialect);

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties,
            60000,
            1000,
            false);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());
  }

  @Test
  public void testGetHostTuplesByPriority() {
    final List<HostSpec> originalHosts = defaultHosts;
    originalHosts.get(2).setAvailability(HostAvailability.NOT_AVAILABLE);
    originalHosts.get(4).setAvailability(HostAvailability.NOT_AVAILABLE);
    originalHosts.get(5).setAvailability(HostAvailability.NOT_AVAILABLE);

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties);
    final List<HostSpec> hostsByPriority = target.getHostsByPriority(originalHosts);

    int i = 0;

    // expecting active readers
    while (i < hostsByPriority.size()
        && hostsByPriority.get(i).getRole() == HostRole.READER
        && hostsByPriority.get(i).getAvailability() == HostAvailability.AVAILABLE) {
      i++;
    }

    // expecting a writer
    while (i < hostsByPriority.size()
        && hostsByPriority.get(i).getRole() == HostRole.WRITER) {
      i++;
    }

    // expecting down readers
    while (i < hostsByPriority.size()
        && hostsByPriority.get(i).getRole() == HostRole.READER
        && hostsByPriority.get(i).getAvailability() == HostAvailability.NOT_AVAILABLE) {
      i++;
    }

    assertEquals(hostsByPriority.size(), i);
  }

  @Test
  public void testGetReaderTuplesByPriority() {
    final List<HostSpec> originalHosts = defaultHosts;
    originalHosts.get(2).setAvailability(HostAvailability.NOT_AVAILABLE);
    originalHosts.get(4).setAvailability(HostAvailability.NOT_AVAILABLE);
    originalHosts.get(5).setAvailability(HostAvailability.NOT_AVAILABLE);

    Dialect mockDialect = Mockito.mock(Dialect.class);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));
    when(mockPluginService.getDialect()).thenReturn(mockDialect);

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockPluginService,
            properties);
    final List<HostSpec> hostsByPriority = target.getReaderHostsByPriority(originalHosts);

    int i = 0;

    // expecting active readers
    while (i < hostsByPriority.size()
        && hostsByPriority.get(i).getRole() == HostRole.READER
        && hostsByPriority.get(i).getAvailability() == HostAvailability.AVAILABLE) {
      i++;
    }

    // expecting down readers
    while (i < hostsByPriority.size()
        && hostsByPriority.get(i).getRole() == HostRole.READER
        && hostsByPriority.get(i).getAvailability() == HostAvailability.NOT_AVAILABLE) {
      i++;
    }

    assertEquals(hostsByPriority.size(), i);
  }

  @Test
  public void testHostFailoverStrictReaderEnabled() {

    final HostSpec writer = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer").port(1234).role(HostRole.WRITER).build();
    final HostSpec reader = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader1").port(1234).role(HostRole.READER).build();
    final List<HostSpec> hosts = Arrays.asList(writer, reader);

    Dialect mockDialect = Mockito.mock(Dialect.class);
    when(mockDialect.getFailoverRestrictions()).thenReturn(EnumSet.noneOf(FailoverRestriction.class));
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    final ClusterAwareReaderFailoverHandler target =
            new ClusterAwareReaderFailoverHandler(
                    mockPluginService,
                    properties,
                    DEFAULT_FAILOVER_TIMEOUT,
                    DEFAULT_READER_CONNECT_TIMEOUT,
                    true);

    // The writer is included because the original writer has likely become a reader.
    List<HostSpec> expectedHostsByPriority = Arrays.asList(reader, writer);

    List<HostSpec> hostsByPriority = target.getHostsByPriority(hosts);
    assertEquals(expectedHostsByPriority, hostsByPriority);

    // Should pick the reader even if unavailable. The unavailable reader will be lower priority than the writer.
    reader.setAvailability(HostAvailability.NOT_AVAILABLE);
    expectedHostsByPriority = Arrays.asList(writer, reader);

    hostsByPriority = target.getHostsByPriority(hosts);
    assertEquals(expectedHostsByPriority, hostsByPriority);

    // Writer node will only be picked if it is the only node in topology;
    List<HostSpec> expectedWriterHost = Collections.singletonList(writer);

    hostsByPriority = target.getHostsByPriority(Collections.singletonList(writer));
    assertEquals(expectedWriterHost, hostsByPriority);
  }
}
