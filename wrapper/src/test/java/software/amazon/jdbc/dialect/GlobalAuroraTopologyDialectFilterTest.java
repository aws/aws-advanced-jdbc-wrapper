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

package software.amazon.jdbc.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class GlobalAuroraTopologyDialectFilterTest {

  private HostSpec createHost(String host, HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(host)
        .role(role)
        .build();
  }

  private List<HostSpec> sampleHosts() {
    return Arrays.asList(
        createHost("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER),
        createHost("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER),
        createHost("instance3.cluster-xyz.us-west-2.rds.amazonaws.com", HostRole.WRITER),
        createHost("instance4.cluster-ro-xyz.us-west-2.rds.amazonaws.com", HostRole.READER),
        createHost("instance5.cluster-ro-xyz.eu-central-1.rds.amazonaws.com", HostRole.READER));
  }

  @Test
  void testGlobalAuroraMysqlDialectFiltersByAccessibleRegions() {
    GlobalAuroraMysqlDialect dialect = new GlobalAuroraMysqlDialect();
    Set<String> accessibleRegions = new HashSet<>(Arrays.asList("us-east-1", "us-west-2"));

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, accessibleRegions);

    assertEquals(4, filtered.size());
    assertTrue(filtered.stream().allMatch(h ->
        h.getHost().contains("us-east-1") || h.getHost().contains("us-west-2")));
  }

  @Test
  void testGlobalAuroraPgDialectFiltersByAccessibleRegions() {
    GlobalAuroraPgDialect dialect = new GlobalAuroraPgDialect();
    Set<String> accessibleRegions = new HashSet<>(Collections.singletonList("us-east-1"));

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, accessibleRegions);

    assertEquals(2, filtered.size());
    assertTrue(filtered.stream().allMatch(h -> h.getHost().contains("us-east-1")));
  }

  @Test
  void testGlobalAuroraMysqlDialectReturnsAllHostsWhenNoRestriction() {
    GlobalAuroraMysqlDialect dialect = new GlobalAuroraMysqlDialect();

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, null);

    assertSame(hosts, filtered);
  }

  @Test
  void testGlobalAuroraPgDialectReturnsAllHostsWhenNoRestriction() {
    GlobalAuroraPgDialect dialect = new GlobalAuroraPgDialect();

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, null);

    assertSame(hosts, filtered);
  }

  @Test
  void testGlobalAuroraMysqlDialectReturnsAllHostsWhenEmptySet() {
    GlobalAuroraMysqlDialect dialect = new GlobalAuroraMysqlDialect();

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, Collections.emptySet());

    assertSame(hosts, filtered);
  }

  @Test
  void testGlobalAuroraMysqlDialectFilterIsCaseInsensitive() {
    GlobalAuroraMysqlDialect dialect = new GlobalAuroraMysqlDialect();
    Set<String> accessibleRegions = new HashSet<>(Collections.singletonList("us-east-1"));

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, accessibleRegions);

    assertEquals(2, filtered.size());
  }

  @Test
  void testGlobalAuroraMysqlDialectExcludesNonRdsHosts() {
    GlobalAuroraMysqlDialect dialect = new GlobalAuroraMysqlDialect();
    Set<String> accessibleRegions = new HashSet<>(Collections.singletonList("us-east-1"));

    List<HostSpec> hosts = Arrays.asList(
        createHost("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER),
        createHost("custom-domain.example.com", HostRole.READER));

    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, accessibleRegions);

    assertEquals(1, filtered.size());
    assertEquals("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", filtered.get(0).getHost());
  }

  @Test
  void testNonGlobalAuroraDialectReturnsAllHostsByDefault() {
    // MysqlDialect is a non-global dialect. Default Dialect.filterAvailableHosts returns the input as-is.
    MysqlDialect dialect = new MysqlDialect();
    Set<String> accessibleRegions = new HashSet<>(Collections.singletonList("us-east-1"));

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, accessibleRegions);

    assertSame(hosts, filtered);
  }

  @Test
  void testAuroraMysqlDialectReturnsAllHostsByDefault() {
    // AuroraMysqlDialect is a non-global Aurora dialect. It should not filter.
    AuroraMysqlDialect dialect = new AuroraMysqlDialect();
    Set<String> accessibleRegions = new HashSet<>(Collections.singletonList("us-east-1"));

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, accessibleRegions);

    assertSame(hosts, filtered);
  }

  @Test
  void testPgDialectReturnsAllHostsByDefault() {
    PgDialect dialect = new PgDialect();
    Set<String> accessibleRegions = new HashSet<>(Collections.singletonList("us-east-1"));

    List<HostSpec> hosts = sampleHosts();
    List<HostSpec> filtered = dialect.filterAvailableHosts(hosts, accessibleRegions);

    assertSame(hosts, filtered);
  }
}
