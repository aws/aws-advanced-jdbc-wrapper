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

package software.amazon.jdbc.plugin.staledns;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Tests for {@link AuroraStaleDnsHelper}.
 */
public class AuroraStaleDnsHelperTest {

  private static final SimpleHostAvailabilityStrategy AVAILABILITY_STRATEGY =
      new SimpleHostAvailabilityStrategy();

  private static final String WRITER_CLUSTER_DNS =
      "mydb.cluster-abc123.eu-central-1.rds.amazonaws.com";

  private static final String WRITER_INSTANCE_DNS =
      "mydb-instance-1.abc123.eu-central-1.rds.amazonaws.com";

  private static final String READER_INSTANCE_DNS =
      "mydb-instance-2.abc123.eu-central-1.rds.amazonaws.com";

  @Mock private PluginService pluginService;
  @Mock private HostListProviderService hostListProviderService;
  @Mock private Connection mockConnection;
  @Mock private Connection mockWriterConnection;
  @Mock private TelemetryFactory telemetryFactory;
  @Mock private TelemetryCounter telemetryCounter;

  private AutoCloseable closeable;

  private static HostSpec hostSpec(String host, HostRole role) {
    return new HostSpecBuilder(AVAILABILITY_STRATEGY)
        .host(host).port(3306).role(role).build();
  }

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(pluginService.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(telemetryFactory.createCounter(anyString())).thenReturn(telemetryCounter);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  /**
   * Happy path: topology has instance-level DNS, connected to writer.
   */
  @Test
  void test_getVerifiedConnection_returnsConnection_whenWriterHasInstanceDns()
      throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(
        hostSpec(WRITER_INSTANCE_DNS, HostRole.WRITER),
        hostSpec(READER_INSTANCE_DNS, HostRole.READER));

    when(pluginService.getAllHosts()).thenReturn(hosts);
    when(pluginService.getHosts()).thenReturn(hosts);
    when(pluginService.getHostRole(mockConnection)).thenReturn(HostRole.WRITER);

    final AuroraStaleDnsHelper helper = new AuroraStaleDnsHelper(pluginService);

    final Connection result = helper.getVerifiedConnection(
        false,
        hostListProviderService,
        hostSpec(WRITER_CLUSTER_DNS, HostRole.WRITER),
        new Properties(),
        () -> mockConnection);

    assertNotNull(result);
    assertSame(mockConnection, result);
  }

  /**
   * Stale DNS: cluster writer endpoint resolved to a reader node. The helper
   * should force-refresh the topology, discover the real writer, reconnect to
   * it, and close the bad reader connection. This must work without the
   * initialConnection plugin being enabled.
   */
  @Test
  void test_getVerifiedConnection_reconnectsToWriter_whenStaleDnsResolvesToReader()
      throws SQLException {
    final HostSpec writerHost = hostSpec(WRITER_INSTANCE_DNS, HostRole.WRITER);
    final List<HostSpec> hosts = Arrays.asList(
        writerHost,
        hostSpec(READER_INSTANCE_DNS, HostRole.READER));

    when(pluginService.getAllHosts()).thenReturn(hosts);
    when(pluginService.getHosts()).thenReturn(hosts);
    // connectFunc returns a connection that landed on a reader (stale DNS).
    when(pluginService.getHostRole(mockConnection)).thenReturn(HostRole.READER);
    // The helper reconnects directly to the discovered writer.
    when(pluginService.connect(any(HostSpec.class), any(Properties.class)))
        .thenReturn(mockWriterConnection);

    final AuroraStaleDnsHelper helper = new AuroraStaleDnsHelper(pluginService);

    final Connection result = helper.getVerifiedConnection(
        false,
        hostListProviderService,
        hostSpec(WRITER_CLUSTER_DNS, HostRole.WRITER),
        new Properties(),
        () -> mockConnection);

    // Should return the new writer connection, not the stale reader one.
    assertNotNull(result);
    assertSame(mockWriterConnection, result);
    // The bad reader connection must be closed.
    verify(mockConnection).close();
    // Stale DNS triggers a force refresh, not a regular refresh.
    verify(pluginService).forceRefreshHostList();
  }

  /**
   * Non-writer-cluster endpoints bypass the stale DNS check entirely.
   */
  @Test
  void test_getVerifiedConnection_bypassesCheck_forNonWriterClusterEndpoints()
      throws SQLException {
    final AuroraStaleDnsHelper helper = new AuroraStaleDnsHelper(pluginService);

    final Connection result = helper.getVerifiedConnection(
        false,
        hostListProviderService,
        hostSpec(WRITER_INSTANCE_DNS, HostRole.WRITER),
        new Properties(),
        () -> mockConnection);

    assertNotNull(result);
    assertSame(mockConnection, result);

    verify(pluginService, never()).getAllHosts();
    verify(pluginService, never()).refreshHostList();
    verify(pluginService, never()).forceRefreshHostList();
  }
}
