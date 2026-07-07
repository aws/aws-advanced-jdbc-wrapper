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

package software.amazon.jdbc.plugin.readwritesplitting.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.GdbSettings;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/** Unit tests for the reader candidate sources. */
public class CandidateSourcesTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private HostListProviderService hostListProviderService;
  @Mock private GdbSettings settings;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer").port(5432).role(HostRole.WRITER).build();
  private final HostSpec reader1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-1").port(5432).role(HostRole.READER).build();
  private final HostSpec reader2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-2").port(5432).role(HostRole.READER).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    lenient().when(ctx.pluginService()).thenReturn(pluginService);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  // ---- TopologyHostsCandidateSource ----

  @Test
  void topology_returnsAllHosts() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(writerHost, reader1, reader2);
    when(pluginService.getHosts()).thenReturn(hosts);
    assertEquals(hosts, new TopologyHostsCandidateSource().candidates(ctx));
  }

  // ---- SingleEndpointCandidateSource ----

  @Test
  void singleEndpoint_yieldsSingleReaderFromEndpoint() {
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(writerHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final List<HostSpec> candidates =
        new SingleEndpointCandidateSource("reader.endpoint").candidates(ctx);

    assertEquals(1, candidates.size());
    assertEquals("reader.endpoint", candidates.get(0).getHost());
    assertEquals(HostRole.READER, candidates.get(0).getRole());
  }

  // ---- GdbRegionFilteredHostsCandidateSource ----

  @Test
  void gdb_noRestriction_returnsAllHosts() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1, reader2));
    when(settings.accessibleRegions()).thenReturn(null);
    when(settings.restrictReaderToHomeRegion()).thenReturn(false);

    final List<HostSpec> candidates =
        new GdbRegionFilteredHostsCandidateSource(settings).candidates(ctx);

    assertEquals(3, candidates.size());
  }

  @Test
  void gdb_restrictToHomeRegion_filtersToHomeRegionHosts() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1, reader2));
    when(settings.accessibleRegions()).thenReturn(null);
    when(settings.restrictReaderToHomeRegion()).thenReturn(true);
    when(settings.isInHomeRegion(reader1)).thenReturn(true);
    when(settings.isInHomeRegion(reader2)).thenReturn(false);
    when(settings.isInHomeRegion(writerHost)).thenReturn(false);

    final List<HostSpec> candidates =
        new GdbRegionFilteredHostsCandidateSource(settings).candidates(ctx);

    assertEquals(Collections.singletonList(reader1), candidates);
  }

  @Test
  void gdb_noHostsInHomeRegion_throws() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1));
    when(settings.accessibleRegions()).thenReturn(null);
    when(settings.restrictReaderToHomeRegion()).thenReturn(true);
    when(settings.isInHomeRegion(org.mockito.ArgumentMatchers.any())).thenReturn(false);
    doThrow(new SQLException("no readers in home region")).when(ctx).logAndThrow(anyString());

    assertThrows(SQLException.class,
        () -> new GdbRegionFilteredHostsCandidateSource(settings).candidates(ctx));
  }

  @Test
  void gdb_accessibleRegionsFilter_applied() throws SQLException {
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writerHost, reader1, reader2));
    when(settings.accessibleRegions()).thenReturn(Collections.singleton("us-east-1"));
    when(settings.restrictReaderToHomeRegion()).thenReturn(false);
    when(settings.isInAccessibleRegion(writerHost)).thenReturn(true);
    when(settings.isInAccessibleRegion(reader1)).thenReturn(true);
    when(settings.isInAccessibleRegion(reader2)).thenReturn(false);

    final List<HostSpec> candidates =
        new GdbRegionFilteredHostsCandidateSource(settings).candidates(ctx);

    assertEquals(2, candidates.size());
    assertTrue(candidates.contains(writerHost));
    assertTrue(candidates.contains(reader1));
  }
}
