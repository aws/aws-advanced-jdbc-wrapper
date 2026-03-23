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
 * Tests for {@link AuroraStaleDnsHelper}, specifically verifying that
 * {@code getVerifiedConnection()} never returns null when a valid connection
 * has been established.
 *
 * <p>Previously, when the topology contained a writer with cluster DNS (not
 * instance-level DNS), the method returned null. This caused connection pools
 * like HikariCP to silently fail to create connections, eventually draining
 * the pool to zero (total=0, active=0, idle=0).
 */
public class AuroraStaleDnsHelperTest {

  private static final SimpleHostAvailabilityStrategy AVAILABILITY_STRATEGY =
      new SimpleHostAvailabilityStrategy();

  // Aurora cluster DNS: xxx.cluster-yyy.region.rds.amazonaws.com
  private static final String WRITER_CLUSTER_DNS =
      "mydb.cluster-abc123.eu-central-1.rds.amazonaws.com";

  // Aurora instance DNS: xxx.yyy.region.rds.amazonaws.com
  private static final String WRITER_INSTANCE_DNS =
      "mydb-instance-1.abc123.eu-central-1.rds.amazonaws.com";

  private static final String READER_INSTANCE_DNS =
      "mydb-instance-2.abc123.eu-central-1.rds.amazonaws.com";

  @Mock private PluginService pluginService;
  @Mock private HostListProviderService hostListProviderService;
  @Mock private Connection mockConnection;
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
   * Verifies that getVerifiedConnection returns the connection (not null) when
   * the topology contains a writer with cluster DNS. This is the scenario that
   * previously returned null.
   *
   * <p>When the writer in the topology has cluster DNS rather than instance DNS,
   * stale DNS detection cannot be performed (there is no instance IP to compare
   * against). The correct behavior is to return the already-established connection.
   */
  @Test
  void test_getVerifiedConnection_returnsConnection_whenWriterHasClusterDns() throws SQLException {
    // The topology contains a writer identified by cluster DNS, not instance DNS.
    // This can happen during topology transitions or when the topology provider
    // hasn't resolved instance-level endpoints yet.
    final List<HostSpec> hosts = Arrays.asList(
        hostSpec(WRITER_INSTANCE_DNS, HostRole.WRITER),
        hostSpec(READER_INSTANCE_DNS, HostRole.READER));

    // First call: guard check (v3.2+) sees instance DNS — passes through.
    // Second call: after refreshHostList, topology now has cluster DNS for the writer.
    final List<HostSpec> hostsAfterRefresh = Arrays.asList(
        hostSpec(WRITER_CLUSTER_DNS, HostRole.WRITER),
        hostSpec(READER_INSTANCE_DNS, HostRole.READER));

    when(pluginService.getAllHosts())
        .thenReturn(hosts)          // guard check
        .thenReturn(hostsAfterRefresh);  // post-refresh topology
    when(pluginService.getHostRole(mockConnection)).thenReturn(HostRole.WRITER);

    final AuroraStaleDnsHelper helper = new AuroraStaleDnsHelper(pluginService);
    final HostSpec connectHost = hostSpec(WRITER_CLUSTER_DNS, HostRole.WRITER);

    final Connection result = helper.getVerifiedConnection(
        false,
        hostListProviderService,
        connectHost,
        new Properties(),
        () -> mockConnection);

    assertNotNull(result,
        "getVerifiedConnection must not return null when a valid connection exists. "
            + "Returning null causes connection pools to silently drain.");
    assertSame(mockConnection, result);
  }

  /**
   * Verifies that getVerifiedConnection works correctly when the writer has
   * instance-level DNS — the normal happy path.
   */
  @Test
  void test_getVerifiedConnection_returnsConnection_whenWriterHasInstanceDns() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(
        hostSpec(WRITER_INSTANCE_DNS, HostRole.WRITER),
        hostSpec(READER_INSTANCE_DNS, HostRole.READER));

    when(pluginService.getAllHosts()).thenReturn(hosts);
    when(pluginService.getHosts()).thenReturn(hosts);
    when(pluginService.getHostRole(mockConnection)).thenReturn(HostRole.WRITER);

    final AuroraStaleDnsHelper helper = new AuroraStaleDnsHelper(pluginService);
    final HostSpec connectHost = hostSpec(WRITER_CLUSTER_DNS, HostRole.WRITER);

    final Connection result = helper.getVerifiedConnection(
        false,
        hostListProviderService,
        connectHost,
        new Properties(),
        () -> mockConnection);

    assertNotNull(result);
    assertSame(mockConnection, result);
  }

  /**
   * Verifies that non-writer-cluster endpoints bypass the stale DNS check entirely.
   * This confirms that reader pools are never affected by this code path.
   */
  @Test
  void test_getVerifiedConnection_bypassesCheck_forNonWriterClusterEndpoints() throws SQLException {
    final AuroraStaleDnsHelper helper = new AuroraStaleDnsHelper(pluginService);

    // An instance endpoint (not a cluster endpoint)
    final HostSpec instanceHost = hostSpec(WRITER_INSTANCE_DNS, HostRole.WRITER);

    final Connection result = helper.getVerifiedConnection(
        false,
        hostListProviderService,
        instanceHost,
        new Properties(),
        () -> mockConnection);

    assertNotNull(result);
    assertSame(mockConnection, result);

    // No topology operations should have been performed
    verify(pluginService, never()).getAllHosts();
    verify(pluginService, never()).refreshHostList();
    verify(pluginService, never()).forceRefreshHostList();
  }

  /**
   * Verifies that repeated calls with cluster DNS in the topology always return
   * a valid connection. This simulates connection pool replenishment — each call
   * must succeed for the pool to stay healthy.
   */
  @Test
  void test_getVerifiedConnection_neverReturnsNull_onRepeatedCalls() throws SQLException {
    final List<HostSpec> hostsWithInstance = Arrays.asList(
        hostSpec(WRITER_INSTANCE_DNS, HostRole.WRITER),
        hostSpec(READER_INSTANCE_DNS, HostRole.READER));

    final List<HostSpec> hostsWithCluster = Arrays.asList(
        hostSpec(WRITER_CLUSTER_DNS, HostRole.WRITER),
        hostSpec(READER_INSTANCE_DNS, HostRole.READER));

    when(pluginService.getHostRole(mockConnection)).thenReturn(HostRole.WRITER);

    for (int i = 0; i < 5; i++) {
      when(pluginService.getAllHosts())
          .thenReturn(hostsWithInstance)
          .thenReturn(hostsWithCluster);

      final AuroraStaleDnsHelper helper = new AuroraStaleDnsHelper(pluginService);
      final HostSpec connectHost = hostSpec(WRITER_CLUSTER_DNS, HostRole.WRITER);

      final Connection result = helper.getVerifiedConnection(
          false,
          hostListProviderService,
          connectHost,
          new Properties(),
          () -> mockConnection);

      assertNotNull(result,
          "Attempt " + (i + 1) + ": getVerifiedConnection must not return null. "
              + "Null causes the connection pool to silently drain.");
    }
  }
}
