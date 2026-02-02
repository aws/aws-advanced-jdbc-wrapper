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

    // Reservoir sampling with nextDouble():
    // Host 1 (weight 3): totalWeight=3, random*3 < 3 → select
    // Host 2 (weight 2): totalWeight=5, random*5 >= 2 → keep host 1
    // Host 3 (weight 1): totalWeight=6, random*6 >= 1 → keep host 1
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.9, 0.9);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-1", actualHost.getHost());
  }

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

    // Host 1: select, Host 2: random*5=0.1*5=0.5 < 2 → select, Host 3: keep
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.1, 0.9);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-2", actualHost.getHost());
  }

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

    // Host 1: select, Host 2: keep, Host 3: random*6=0.0 < 1 → select
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.9, 0.0);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-3", actualHost.getHost());
  }

  @Test
  void testGetHost_skipsHostsNotInWeightMap() throws SQLException {
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

    // instance-2 skipped (not in map), select instance-3
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.0);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals("instance-3", actualHost.getHost());
  }


  @Test
  void testGetHost_usesHostSpecWeight_whenPropertyNotSet() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    // No WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS property set
    final List<HostSpec> eligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(3).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(2).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER)
            .availability(HostAvailability.AVAILABLE).weight(1).build()
    );

    // Select first host
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.9, 0.9);

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

    // Select second host
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.1, 0.9);

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

    // Select third host
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.9, 0.0);

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

    // Both hosts have weight 0, should use DEFAULT_WEIGHT (1)
    // Select second host
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.0);

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

    // instance-2 is unavailable, select instance-3
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.0);

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

    // instance-2 is READER, keep instance-1
    when(mockRandom.nextDouble()).thenReturn(0.0, 0.9);

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

    when(mockRandom.nextDouble()).thenReturn(0.0, 0.9);

    final HostSpec actualHost = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, null);
    assertEquals("instance-1", actualHost.getHost());
  }
}
