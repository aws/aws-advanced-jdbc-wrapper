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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class HighestLoadHostSelectorTest {

  private static final Properties EMPTY_PROPS = new Properties();

  private HighestLoadHostSelector selector;

  @BeforeEach
  void setUp() {
    selector = new HighestLoadHostSelector();
  }

  private HostSpec reader(final String id, final float cpu, final float lag) {
    return reader(id, cpu, lag, HostAvailability.AVAILABLE);
  }

  private HostSpec reader(final String id, final float cpu, final float lag, final HostAvailability availability) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(id)
        .hostId(id)
        .role(HostRole.READER)
        .availability(availability)
        .cpuPercent(cpu)
        .lagMs(lag)
        .build();
  }

  @Test
  void test_emptyHostList_returnsNull() throws SQLException {
    assertNull(selector.getHost(Collections.emptyList(), HostRole.READER, EMPTY_PROPS));
  }

  @Test
  void test_noEligibleHosts_returnsNull() throws SQLException {
    final List<HostSpec> writers = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("w1").hostId("w1")
            .role(HostRole.WRITER).cpuPercent(1f).lagMs(1f).build());
    assertNull(selector.getHost(writers, HostRole.READER, EMPTY_PROPS));
  }

  @Test
  void test_picksHighestLoadHost() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(
        reader("low1", 10f, 10f),
        reader("low2", 11f, 12f),
        reader("medium1", 50f, 50f),
        reader("medium1", 51f, 51f),
        reader("high1", 9000f, 9000f),
        reader("high2", 9001f, 9001f));

    final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
    assertNotNull(h);
    assertEquals("high2", h.getHostId());
  }

  @Test
  void test_filtersByRoleAndAvailability() throws SQLException {
    final List<HostSpec> hosts = Arrays.asList(
        reader("avail-high", 9000f, 9000f),
        reader("not-avail-higher", 9999f, 9999f, HostAvailability.NOT_AVAILABLE),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("writer").hostId("writer")
            .role(HostRole.WRITER).cpuPercent(10000f).lagMs(10000f).build());

    final HostSpec h = selector.getHost(hosts, HostRole.READER, EMPTY_PROPS);
    assertNotNull(h);
    assertEquals("avail-high", h.getHostId());
  }

  @Test
  void test_noLoadDataAtAll_returnsNull() throws SQLException {
    final HostSpec a = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("a").hostId("a").role(HostRole.READER).build();
    final HostSpec b = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("b").hostId("b").role(HostRole.READER).build();

    final HostSpec h = selector.getHost(Arrays.asList(a, b), HostRole.READER, EMPTY_PROPS);
    assertNull(h, "should return null when no load data is available");
  }

  @Test
  void test_nullCpuPercent_usesDefault() throws SQLException {
    // Host with null cpuPercent should use CPU_DEFAULT (40) in the calculation.
    // A host with explicit high cpu (99) will have higher load than the default (40).
    final HostSpec withNullCpu = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("null-cpu").hostId("null-cpu").role(HostRole.READER)
        .cpuPercent(null).lagMs(10f).build();
    final HostSpec withHighCpu = reader("high-cpu", 99f, 10f);

    final HostSpec h = selector.getHost(Arrays.asList(withNullCpu, withHighCpu), HostRole.READER, EMPTY_PROPS);
    assertNotNull(h);
    assertEquals("high-cpu", h.getHostId());
  }

  @Test
  void test_nullLagMs_usesDefault() throws SQLException {
    // Host with null lagMs should use LAG_MS_DEFAULT (1000) in the calculation.
    // This makes it appear heavily loaded, so highestLoad should prefer it.
    final HostSpec withNullLag = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("null-lag").hostId("null-lag").role(HostRole.READER)
        .cpuPercent(10f).lagMs(null).build();
    final HostSpec withLowLag = reader("low-lag", 10f, 5f);

    final HostSpec h = selector.getHost(Arrays.asList(withNullLag, withLowLag), HostRole.READER, EMPTY_PROPS);
    assertNotNull(h);
    assertEquals("null-lag", h.getHostId());
  }
}
