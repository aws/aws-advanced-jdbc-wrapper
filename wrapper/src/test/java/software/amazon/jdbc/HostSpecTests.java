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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class HostSpecTests {

  @Mock HostAvailabilityStrategy mockHostAvailabilityStrategy;

  private AutoCloseable closeable;

  HostSpec hostSpec;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    hostSpec = new HostSpecBuilder(this.mockHostAvailabilityStrategy).host("someUrl").build();
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testSetAvailabilityCallsHostAvailabilityStrategy() {
    final HostAvailability hostAvailability = HostAvailability.NOT_AVAILABLE;
    hostSpec.setAvailability(hostAvailability);
    verify(mockHostAvailabilityStrategy, times(1)).setHostAvailability(hostAvailability);
  }

  @Test
  public void testGetAvailabilityCallsHostAvailabilityStrategy() {
    hostSpec.getAvailability();
    verify(mockHostAvailabilityStrategy, times(1)).getHostAvailability(hostSpec.availability);
  }

  // --- equals() tests for cpuPercent and lagMs ---

  @Test
  public void testEquals_allFieldsMatch_returnsTrue() {
    final HostSpec a = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host1").port(5432).role(HostRole.READER)
        .availability(HostAvailability.AVAILABLE).weight(100)
        .cpuPercent(50f).lagMs(200f).build();
    final HostSpec b = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host1").port(5432).role(HostRole.READER)
        .availability(HostAvailability.AVAILABLE).weight(100)
        .cpuPercent(50f).lagMs(200f).build();
    assertEquals(a, b);
  }

  @Test
  public void testEquals_differentCpuPercent_returnsFalse() {
    final HostSpec a = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host1").port(5432).role(HostRole.READER).cpuPercent(50f).lagMs(100f).build();
    final HostSpec b = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host1").port(5432).role(HostRole.READER).cpuPercent(90f).lagMs(100f).build();
    assertNotEquals(a, b);
  }

  @Test
  public void testEquals_differentLagMs_returnsFalse() {
    final HostSpec a = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host1").port(5432).role(HostRole.READER).cpuPercent(50f).lagMs(100f).build();
    final HostSpec b = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host1").port(5432).role(HostRole.READER).cpuPercent(50f).lagMs(500f).build();
    assertNotEquals(a, b);
  }

  // --- toString() tests for cpuPercent and lagMs ---

  @Test
  public void testToString_containsCpuPercent() {
    final HostSpec spec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("my-host").port(5432).role(HostRole.READER).cpuPercent(42f).lagMs(123f).build();
    final String result = spec.toString();
    assertTrue(result.contains("cpuPercent="), "toString should contain cpuPercent; got: " + result);
  }

  @Test
  public void testToString_containsLagMs() {
    final HostSpec spec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("my-host").port(5432).role(HostRole.READER).cpuPercent(42f).lagMs(123f).build();
    final String result = spec.toString();
    assertTrue(result.contains("lagMs="), "toString should contain lagMs; got: " + result);
  }

  @Test
  public void testToString_invalidatedAfterSetCpuPercent() {
    final HostSpec spec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("my-host").port(5432).role(HostRole.READER).cpuPercent(42f).lagMs(123f).build();
    final String before = spec.toString();
    spec.setCpuPercent(99f);
    final String after = spec.toString();
    assertNotEquals(before, after, "toString should be invalidated after setCpuPercent");
  }

  @Test
  public void testToString_invalidatedAfterSetLagMs() {
    final HostSpec spec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("my-host").port(5432).role(HostRole.READER).cpuPercent(42f).lagMs(123f).build();
    final String before = spec.toString();
    spec.setLagMs(999f);
    final String after = spec.toString();
    assertNotEquals(before, after, "toString should be invalidated after setLagMs");
  }
}
