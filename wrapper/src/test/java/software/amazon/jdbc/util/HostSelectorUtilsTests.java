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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class HostSelectorUtilsTests {
  @Test
  void testSetHostWeightPairsProperty() {
    final String expectedPropertyValue = "instance-1-id:2,instance-2-id:1,instance-3-id:0";

    final List<HostSpec> hosts = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-1")
            .hostId("instance-1-id")
            .weight(2)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-2")
            .hostId("instance-2-id")
            .weight(1)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-3")
            .hostId("instance-3-id")
            .weight(0)
            .build()
    );
    final Properties properties = new Properties();
    HostSelectorUtils.setHostWeightPairsProperty(
        RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS,
        properties, hosts);

    final String actualPropertyValue = properties.getProperty(
        RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name);

    assertEquals(expectedPropertyValue, actualPropertyValue);
  }
}
