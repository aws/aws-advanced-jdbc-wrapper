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

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.balancer.LoadBalancingPolicy;
import software.amazon.jdbc.plugin.readwritesplitting.source.ReaderCandidateSource;

/** Unit tests for {@link TopologyReaderResolver}. */
public class TopologyReaderResolverTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private ReaderCandidateSource candidateSource;
  @Mock private LoadBalancingPolicy loadBalancingPolicy;
  @Mock private WriterResolver writerFallback;
  @Mock private Connection readerConn;
  @Mock private Connection writerConn;

  private final Properties props = new Properties();
  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer").port(5432).role(HostRole.WRITER).build();
  private final HostSpec reader1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-1").port(5432).role(HostRole.READER).build();
  private final HostSpec reader2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-2").port(5432).role(HostRole.READER).build();

  private TopologyReaderResolver resolver;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.properties()).thenReturn(props);
    resolver = new TopologyReaderResolver(candidateSource, loadBalancingPolicy, writerFallback);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void happyPath_selectsAndBindsReader() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1, reader2));
    when(candidateSource.candidates(ctx)).thenReturn(Arrays.asList(reader1, reader2));
    when(loadBalancingPolicy.pickReader(any(), anyList())).thenReturn(reader1);
    when(ctx.connect(reader1, props)).thenReturn(readerConn);

    resolver.switchToReader(ctx);

    verify(ctx).bindReader(readerConn, reader1);
    verify(ctx).switchCurrentConnectionTo(readerConn, reader1);
  }

  @Test
  void singleHostCluster_fallsBackToWriter() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Collections.singletonList(writerHost));
    when(ctx.isConnectionUsable(any())).thenReturn(false);
    when(ctx.writerHostSpec()).thenReturn(writerHost);
    when(writerFallback.resolveWriter(ctx)).thenReturn(WriterResolution.connected(writerConn, writerHost));

    resolver.switchToReader(ctx);

    verify(ctx).bindWriter(writerConn, writerHost);
    verify(ctx).switchCurrentConnectionTo(writerConn, writerHost);
    // No reader candidate is consulted for a single-host cluster.
    verify(loadBalancingPolicy, never()).pickReader(any(), anyList());
  }

  @Test
  void nonLoginFailure_removesCandidateAndTriesNext() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1, reader2));
    when(candidateSource.candidates(ctx)).thenReturn(Arrays.asList(reader1, reader2));
    when(loadBalancingPolicy.pickReader(any(), anyList())).thenReturn(reader1).thenReturn(reader2);
    when(ctx.connect(reader1, props)).thenThrow(new SQLException("unreachable"));
    when(ctx.connect(reader2, props)).thenReturn(readerConn);
    when(pluginService.isLoginException(any(Throwable.class), any())).thenReturn(false);

    resolver.switchToReader(ctx);

    verify(ctx).connect(reader1, props);
    verify(ctx).connect(reader2, props);
    verify(ctx).bindReader(readerConn, reader2);
  }

  @Test
  void loginFailure_rethrownImmediately() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1, reader2));
    when(candidateSource.candidates(ctx)).thenReturn(Arrays.asList(reader1, reader2));
    when(loadBalancingPolicy.pickReader(any(), anyList())).thenReturn(reader1);
    when(ctx.connect(reader1, props)).thenThrow(new SQLException("bad password"));
    when(pluginService.isLoginException(any(Throwable.class), any())).thenReturn(true);

    assertThrows(SQLException.class, () -> resolver.switchToReader(ctx));

    // A login failure stops the retry loop after a single attempt.
    verify(ctx, times(1)).connect(reader1, props);
    verify(ctx, never()).connect(reader2, props);
    verify(ctx, never()).bindReader(any(), any());
  }

  @Test
  void allCandidatesFail_throwsNoReadersAvailable() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1));
    when(candidateSource.candidates(ctx)).thenReturn(Collections.singletonList(reader1));
    when(loadBalancingPolicy.pickReader(any(), anyList())).thenReturn(reader1);
    when(ctx.connect(reader1, props)).thenThrow(new SQLException("unreachable"));
    when(pluginService.isLoginException(any(Throwable.class), any())).thenReturn(false);
    doThrow(new SQLException("no readers available")).when(ctx).logAndThrow(anyString(), any());

    assertThrows(SQLException.class, () -> resolver.switchToReader(ctx));
    verify(ctx, never()).bindReader(any(), any());
  }

  @Test
  void selectorReturnsNull_stopsWithoutConnecting() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1));
    when(candidateSource.candidates(ctx)).thenReturn(Collections.singletonList(reader1));
    when(loadBalancingPolicy.pickReader(any(), anyList())).thenReturn(null);
    doThrow(new SQLException("no readers available")).when(ctx).logAndThrow(anyString(), any());

    assertThrows(SQLException.class, () -> resolver.switchToReader(ctx));
    verify(ctx, never()).connect(any(HostSpec.class), any(Properties.class));
  }

  @Test
  void closeStaleReader_closesWhenReaderNoLongerInTopology() {
    final HostSpec staleReader = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("gone").port(5432).role(HostRole.READER).build();
    when(ctx.readerHostSpec()).thenReturn(staleReader);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1));

    resolver.closeStaleReaderIfNecessary(ctx);

    verify(ctx).closeReaderConnectionIfIdle();
  }

  @Test
  void closeStaleReader_keepsWhenReaderStillInTopology() {
    when(ctx.readerHostSpec()).thenReturn(reader1);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1));

    resolver.closeStaleReaderIfNecessary(ctx);

    verify(ctx, never()).closeReaderConnectionIfIdle();
  }
}
