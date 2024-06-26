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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class RoundRobinHostSelectorTest {
  private static final int TEST_PORT = 5432;
  private static Properties defaultProps;
  private static Properties weightedProps;

  private final HostSpec writerHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-0").port(TEST_PORT).build();
  private final HostSpec readerHostSpec1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-1").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpec2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-2").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpec3 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-3").port(TEST_PORT).role(HostRole.READER).build();
  private final HostSpec readerHostSpec4 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-4").port(TEST_PORT).role(HostRole.READER).build();

  // Each number at the end of the host list represents which readers have been added.
  private final List<HostSpec> hostsList123 = Arrays.asList(
      writerHostSpec,
      readerHostSpec2,
      readerHostSpec3,
      readerHostSpec1);

  private final List<HostSpec> hostsList1234 = Arrays.asList(
      writerHostSpec,
      readerHostSpec4,
      readerHostSpec2,
      readerHostSpec3,
      readerHostSpec1);
  private final List<HostSpec> hostsList13 = Arrays.asList(
      writerHostSpec,
      readerHostSpec3,
      readerHostSpec1);
  private final List<HostSpec> hostsList14 = Arrays.asList(
      writerHostSpec,
      readerHostSpec4,
      readerHostSpec1);
  private final List<HostSpec> hostsList23 = Arrays.asList(
      writerHostSpec,
      readerHostSpec3,
      readerHostSpec2);
  private final List<HostSpec> writerHostsList = Collections.singletonList(writerHostSpec);
  private static RoundRobinHostSelector roundRobinHostSelector;

  @BeforeEach
  public void setUp() {
    roundRobinHostSelector = new RoundRobinHostSelector();
    defaultProps = new Properties();
    weightedProps = new Properties();
    final String hostWeights =
        "instance-0:1,"
            + "instance-1:3,"
            + "instance-2:2,"
            + "instance-3:1";
    weightedProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
  }

  @AfterEach
  public void cleanUp() {
    roundRobinHostSelector.clearCache();
  }

  @Test
  void testSetup_EmptyHost() {
    final String hostWeights =
        "instance-0:1,"
            + ":3,"
            + "instance-2:2,"
            + "instance-3:3";
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_EmptyWeight() {
    final String hostWeights =
        "instance-0:1,"
            + "instance-1:,"
            + "instance-2:2,"
            + "instance-3:3";
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_ZeroWeight() {
    final String hostWeights =
        "instance-0:1,"
            + "instance-1:0,"
            + "instance-2:2,"
            + "instance-3:3";
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_ZeroDefaultWeight() {
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_DEFAULT_WEIGHT.name, "0");
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_BadWeightFormat() {
    final String hostWeights =
        "instance-0:1,"
            + "instance-1:1:3,"
            + "instance-2:2,"
            + "instance-3:3";
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_FloatWeights() {
    final String hostWeights =
        "instance-0:1,"
            + "instance-1:1.123,"
            + "instance-2:2.456,"
            + "instance-3:3.789";
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_FloatDefaultWeight() {
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_DEFAULT_WEIGHT.name, "1.123");
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_NegativeWeights() {
    final String hostWeights =
        "instance-0:1,"
            + "instance-1:-1,"
            + "instance-2:-2,"
            + "instance-3:-3";
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_NegativeDefaultWeight() {
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_DEFAULT_WEIGHT.name, "-1");
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_ParseWeightError() {
    final String hostWeights = "instance-0:1,instance-1:1a";
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, hostWeights);
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetup_ParseDefaultWeightError() {
    defaultProps.put(RoundRobinHostSelector.ROUND_ROBIN_DEFAULT_WEIGHT.name, "1a");
    assertThrows(
        SQLException.class,
        () -> roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testGetHost_NoReaders() {
    assertThrows(SQLException.class,
        () -> roundRobinHostSelector.getHost(writerHostsList, HostRole.READER, defaultProps));
  }

  @Test
  void testGetHost() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testGetHostNullProperties() throws SQLException {
    defaultProps = null;
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testGetHost_Weighted() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
  }

  @Test
  void testGetHost_WeightChange() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());

    final Properties newWeightedProps = new Properties();
    final String newHostWeightsPropsValue = "instance-0:1,"
        + "instance-1:1,"
        + "instance-2:3,"
        + "instance-3:2";
    newWeightedProps.setProperty(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, newHostWeightsPropsValue);

    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
  }

  @Test
  void testGetHost_HostWeightPairPropertyChangeToEmpty() throws SQLException {
    final Properties emptyHostWeightPairProps = new Properties();
    final String emptyString = "";
    emptyHostWeightPairProps.setProperty(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, emptyString);

    roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps);
    assertEquals(4,
        RoundRobinHostSelector.roundRobinCache.get(readerHostSpec1.getHost()).clusterWeightsMap.size());
    roundRobinHostSelector.getHost(hostsList123, HostRole.READER, emptyHostWeightPairProps);
    assertEquals(0,
        RoundRobinHostSelector.roundRobinCache.get(readerHostSpec1.getHost()).clusterWeightsMap.size());
  }

  @Test
  void testGetHost_HostWeightPairPropertyChangeToNull() throws SQLException {
    final Properties nullHostWeightPairProps = new Properties();

    roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps);
    assertEquals(4,
        RoundRobinHostSelector.roundRobinCache.get(readerHostSpec1.getHost()).clusterWeightsMap.size());
    roundRobinHostSelector.getHost(hostsList123, HostRole.READER, nullHostWeightPairProps);
    assertEquals(4,
        RoundRobinHostSelector.roundRobinCache.get(readerHostSpec1.getHost()).clusterWeightsMap.size());
  }

  @Test
  void testGetHost_WeightChangeFromNone() throws SQLException {
    final Properties emptyProps = new Properties();
    roundRobinHostSelector.getHost(hostsList123, HostRole.READER, emptyProps);
    assertEquals(0,
        RoundRobinHostSelector.roundRobinCache.get(readerHostSpec1.getHost()).clusterWeightsMap.size());

    roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps);
    assertEquals(4,
        RoundRobinHostSelector.roundRobinCache.get(readerHostSpec1.getHost()).clusterWeightsMap.size());
  }

  @Test
  void testGetHost_MultipleWeightChanges() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, weightedProps).getHost());

    final Properties emptyProps = new Properties();
    emptyProps.setProperty(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, "");

    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, emptyProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, emptyProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, emptyProps).getHost());

    final Properties newWeightedProps = new Properties();
    final String newHostWeightsPropsValue = "instance-0:1,"
        + "instance-1:1,"
        + "instance-2:3,"
        + "instance-3:2";
    newWeightedProps.setProperty(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name, newHostWeightsPropsValue);

    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, newWeightedProps).getHost());
  }

  @Test
  void testGetHost_CacheEntryExpired() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());

    roundRobinHostSelector.clearCache();

    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testGetHost_ScaleUp() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec4.getHost(),
        roundRobinHostSelector.getHost(hostsList1234, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testGetHost_ScaleDown() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList13, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList13, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testGetHost_LastHostNotInHostsList() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList123, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList13, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec3.getHost(),
        roundRobinHostSelector.getHost(hostsList13, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testGetHost_AllHostsChanged() throws SQLException {
    assertEquals(
        readerHostSpec1.getHost(),
        roundRobinHostSelector.getHost(hostsList14, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec2.getHost(),
        roundRobinHostSelector.getHost(hostsList23, HostRole.READER, defaultProps).getHost());
    assertEquals(
        readerHostSpec4.getHost(),
        roundRobinHostSelector.getHost(hostsList14, HostRole.READER, defaultProps).getHost());
  }

  @Test
  void testSetRoundRobinHostWeightPairsProperty() {
    final String expectedPropertyValue = "instance-1:2,instance-2:1,instance-3:0";

    final List<HostSpec> hosts = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-1")
            .weight(2)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-2")
            .weight(1)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-3")
            .weight(0)
            .build()
    );
    final Properties properties = new Properties();
    RoundRobinHostSelector.setRoundRobinHostWeightPairsProperty(properties, hosts);

    final String actualPropertyValue = properties.getProperty(
        RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name);

    assertEquals(expectedPropertyValue, actualPropertyValue);
  }
}
