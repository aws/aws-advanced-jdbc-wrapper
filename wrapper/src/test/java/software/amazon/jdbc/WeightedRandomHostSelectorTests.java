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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static software.amazon.jdbc.WeightedRandomHostSelector.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

/**
 * Tests for WeightedRandomHostSelector using cumulative weight selection algorithm.
 * 
 * The algorithm works as follows:
 * 1. Calculate total weight of all eligible hosts
 * 2. Generate a random value in [0, totalWeight)
 * 3. Iterate through hosts, subtracting each weight from the random value
 * 4. Return the host where the random value becomes negative
 * 
 * Example with weights [3, 2, 1] (total = 6):
 * - roll in [0, 3) → selects host 1
 * - roll in [3, 5) → selects host 2
 * - roll in [5, 6) → selects host 3
 */
class WeightedRandomHostSelectorTests {

  @Mock Random mockRandom;
  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  // ==================== Tests with property-based weights ====================

  @Test
  void testGetHost_emptyHostList_withProperty() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:1");
    final List<HostSpec> emptyHostList = Collections.emptyList();
    assertThrows(SQLException.class, () -> hostSelector.getHost(emptyHostList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_noEligibleHosts_withProperty() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:1,instance-2:1,instance-3:1");
    final List<HostSpec> noEligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.READER).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.READER).build()
    );
    assertThrows(SQLException.class,
        () -> hostSelector.getHost(noEligibleHostsList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_invalidWeight() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:3,instance-2:2,instance-3:0");
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build()
    );
    assertThrows(SQLException.class,
        () -> hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_invalidProps() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "someInvalidString");
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build()
    );
    assertThrows(SQLException.class,
        () -> hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props));
  }

  /**
   * With weights [3, 2, 1], total = 6:
   * - roll = 0 (0.0 * 6) → 0 < 3, selects instance-1
   */
  @Test
  void testGetHost_selectsFirstHost_withProperty() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:3,instance-2:2,instance-3:1");
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build()
    );

    // roll = 0.0 * 6 = 0, which is < 3, so instance-1 is selected
    when(mockRandom.nextDouble()).thenReturn(0.0);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-1", actualHost.getHost());
  }

  /**
   * With weights [3, 2, 1], total = 6:
   * - roll = 3 (0.5 * 6) → 3 >= 3, subtract 3 → roll = 0
   * - roll = 0 < 2, selects instance-2
   */
  @Test
  void testGetHost_selectsSecondHost_withProperty() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:3,instance-2:2,instance-3:1");
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build()
    );

    // roll = 0.5 * 6 = 3, skip instance-1 (3), roll becomes 0, which is < 2, so instance-2
    when(mockRandom.nextDouble()).thenReturn(0.5);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-2", actualHost.getHost());
  }

  /**
   * With weights [3, 2, 1], total = 6:
   * - roll = 5 (0.9 * 6 = 5.4, truncated to 5) → 5 >= 3, subtract 3 → roll = 2
   * - roll = 2 >= 2, subtract 2 → roll = 0
   * - roll = 0 < 1, selects instance-3
   */
  @Test
  void testGetHost_selectsThirdHost_withProperty() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:3,instance-2:2,instance-3:1");
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build()
    );

    // roll = 0.9 * 6 = 5.4 → 5, skip instance-1 (3) → 2, skip instance-2 (2) → 0 < 1, instance-3
    when(mockRandom.nextDouble()).thenReturn(0.9);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-3", actualHost.getHost());
  }

  /**
   * With weights [2, 1 (default), 1], total = 4:
   * - roll = 3 (0.8 * 4 = 3.2, truncated to 3) → 3 >= 2, subtract 2 → roll = 1
   * - roll = 1 >= 1, subtract 1 → roll = 0
   * - roll = 0 < 1, selects instance-3
   */
  @Test
  void testGetHost_usesDefaultWeightForHostsNotInWeightMap() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:2,instance-3:1");
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).build()
    );

    // instance-2 not in map gets DEFAULT_WEIGHT (1), total = 4
    // roll = 0.8 * 4 = 3.2 → 3
    // skip instance-1 (2) → 1, skip instance-2 (1) → 0 < 1, instance-3
    when(mockRandom.nextDouble()).thenReturn(0.8);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-3", actualHost.getHost());
  }

  // ==================== Tests with HostSpec.getWeight() fallback ====================

  @Test
  void testGetHost_usesHostSpecWeight_selectsFirst() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(3).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(2).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build()
    );

    // roll = 0.0 * 6 = 0 < 3, instance-1
    when(mockRandom.nextDouble()).thenReturn(0.0);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-1", actualHost.getHost());
  }

  @Test
  void testGetHost_usesHostSpecWeight_selectsSecond() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(3).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(2).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build()
    );

    // roll = 0.5 * 6 = 3, skip instance-1 → 0 < 2, instance-2
    when(mockRandom.nextDouble()).thenReturn(0.5);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-2", actualHost.getHost());
  }

  @Test
  void testGetHost_usesHostSpecWeight_selectsThird() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(3).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(2).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build()
    );

    // roll = 0.9 * 6 = 5.4 → 5, skip instance-1 → 2, skip instance-2 → 0 < 1, instance-3
    when(mockRandom.nextDouble()).thenReturn(0.9);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-3", actualHost.getHost());
  }

  @Test
  void testGetHost_usesDefaultWeight_whenHostSpecWeightIsZero() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(0).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(0).build()
    );

    // Both use DEFAULT_WEIGHT (1), total = 2
    // roll = 0.6 * 2 = 1.2 → 1, skip instance-1 → 0 < 1, instance-2
    when(mockRandom.nextDouble()).thenReturn(0.6);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-2", actualHost.getHost());
  }

  @Test
  void testGetHost_emptyHostList_withoutProperty() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    final List<HostSpec> emptyHostList = Collections.emptyList();
    assertThrows(SQLException.class, () -> hostSelector.getHost(emptyHostList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_noEligibleHosts_withoutProperty() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    final List<HostSpec> noEligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.READER)
            .weight(1).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.NOT_AVAILABLE).weight(1).build()
    );
    assertThrows(SQLException.class,
        () -> hostSelector.getHost(noEligibleHostsList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_filtersUnavailableHosts() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.NOT_AVAILABLE).weight(1).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build()
    );

    // instance-2 unavailable, total = 2
    // roll = 0.6 * 2 = 1.2 → 1, skip instance-1 → 0 < 1, instance-3
    when(mockRandom.nextDouble()).thenReturn(0.6);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-3", actualHost.getHost());
  }

  @Test
  void testGetHost_filtersWrongRole() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.READER)
            .availability(HostAvailability.AVAILABLE).weight(1).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build()
    );

    // instance-2 is READER, total = 2 for WRITER
    // roll = 0.0 * 2 = 0 < 1, instance-1
    when(mockRandom.nextDouble()).thenReturn(0.0);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-1", actualHost.getHost());
  }

  @Test
  void testGetHost_nullProps_usesHostSpecWeight() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(5).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(5).build()
    );

    // total = 10, roll = 0.0 * 10 = 0 < 5, instance-1
    when(mockRandom.nextDouble()).thenReturn(0.0);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, null);
    assertEquals("instance-1", actualHost.getHost());
  }
}
