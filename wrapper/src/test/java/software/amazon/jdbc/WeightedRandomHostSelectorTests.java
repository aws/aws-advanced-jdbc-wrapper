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
import static org.mockito.ArgumentMatchers.anyInt;
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
  void testGetHost_emptyHostList() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    final List<HostSpec> emptyHostList = Collections.emptyList();
    assertThrows(
        SQLException.class, () -> hostSelector.getHost(emptyHostList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_noEligibleHosts() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    final List<HostSpec> noEligibleHostsList =
        Arrays.asList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-1")
                .role(HostRole.READER)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-2")
                .role(HostRole.WRITER)
                .availability(HostAvailability.NOT_AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-3")
                .role(HostRole.READER)
                .build());
    assertThrows(
        SQLException.class,
        () -> hostSelector.getHost(noEligibleHostsList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_invalidWeight() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    props.setProperty(
        WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:3,instance-2:2,instance-3:0");
    final List<HostSpec> eligibleHostsList =
        Arrays.asList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-1")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-2")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-3")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build());
    assertThrows(
        SQLException.class, () -> hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_invalidProps() {
    final HostSelector hostSelector = new WeightedRandomHostSelector();
    final Properties props = new Properties();
    props.setProperty(WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "someInvalidString");
    final List<HostSpec> eligibleHostsList =
        Arrays.asList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-1")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-2")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-3")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build());
    assertThrows(
        SQLException.class, () -> hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();
    props.setProperty(
        WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:3,instance-2:2,instance-3:01");
    final List<HostSpec> eligibleHostsList =
        Arrays.asList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-1")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-2")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-3")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build());

    when(mockRandom.nextInt(anyInt())).thenReturn(1, 2, 3, 4, 5, 6);

    final HostSpec actualHost1 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(0).getHost(), actualHost1.getHost());

    final HostSpec actualHost2 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(0).getHost(), actualHost2.getHost());

    final HostSpec actualHost3 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(0).getHost(), actualHost3.getHost());

    final HostSpec actualHost4 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost4.getHost());

    final HostSpec actualHost5 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost5.getHost());

    final HostSpec actualHost6 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(2).getHost(), actualHost6.getHost());
  }

  @Test
  void testGetHost_changeWeights() throws SQLException {
    final WeightedRandomHostSelector hostSelector = new WeightedRandomHostSelector(mockRandom);
    final Properties props = new Properties();

    props.setProperty(
        WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:3,instance-2:2,instance-3:01");
    final List<HostSpec> eligibleHostsList =
        Arrays.asList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-1")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-2")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
                .host("instance-3")
                .role(HostRole.WRITER)
                .availability(HostAvailability.AVAILABLE)
                .build());

    when(mockRandom.nextInt(anyInt())).thenReturn(1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 7);

    final HostSpec actualHost1 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(0).getHost(), actualHost1.getHost());

    final HostSpec actualHost2 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(0).getHost(), actualHost2.getHost());

    final HostSpec actualHost3 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(0).getHost(), actualHost3.getHost());

    final HostSpec actualHost4 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost4.getHost());

    final HostSpec actualHost5 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost5.getHost());

    final HostSpec actualHost6 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(2).getHost(), actualHost6.getHost());

    props.setProperty(
        WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name, "instance-1:1,instance-2:4,instance-3:2");

    final HostSpec actualHost7 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(0).getHost(), actualHost7.getHost());

    final HostSpec actualHost8 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost8.getHost());

    final HostSpec actualHost9 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost9.getHost());

    final HostSpec actualHost10 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost10.getHost());

    final HostSpec actualHost11 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(1).getHost(), actualHost11.getHost());

    final HostSpec actualHost12 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(2).getHost(), actualHost12.getHost());

    final HostSpec actualHost13 = hostSelector.getHost(eligibleHostsList, HostRole.WRITER, props);
    assertEquals(eligibleHostsList.get(2).getHost(), actualHost13.getHost());
  }
}
