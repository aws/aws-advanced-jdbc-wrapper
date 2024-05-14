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

package software.amazon.jdbc.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class HighestWeightHostSelectorTest {

  private static final HostSpec highestWeightHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-highest-weight").role(HostRole.WRITER).weight(Long.MAX_VALUE).build();

  private static HighestWeightHostSelector highestWeightHostSelector;
  private static Properties props = new Properties();

  @BeforeEach
  public void setUp() {
    highestWeightHostSelector = new HighestWeightHostSelector();
  }

  @Test
  void testGetHost_emptyHostList() {
    final List<HostSpec> emptyHostList = Collections.emptyList();
    assertThrows(SQLException.class, () -> highestWeightHostSelector.getHost(emptyHostList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost_noEligibleHosts() {
    final List<HostSpec> noEligibleHostsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.READER).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.READER).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.READER).build()
    );
    assertThrows(SQLException.class,
        () -> highestWeightHostSelector.getHost(noEligibleHostsList, HostRole.WRITER, props));
  }

  @Test
  void testGetHost() throws SQLException {

    final List<HostSpec> hostList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build(),
        highestWeightHostSpec
    );

    final HostSpec actualHost = highestWeightHostSelector.getHost(hostList, HostRole.WRITER, props);

    assertEquals(highestWeightHostSpec, actualHost);
  }
}
