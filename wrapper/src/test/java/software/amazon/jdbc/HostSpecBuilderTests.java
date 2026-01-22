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

import java.sql.Timestamp;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class HostSpecBuilderTests {

  private final int defaultExpectedPort = HostSpec.NO_PORT;
  private final HostAvailability defaultExpectedAvailability = HostAvailability.AVAILABLE;
  private final HostRole defaultExpectedRole = HostRole.WRITER;
  private final long defaultExpectedWeight = 100;

  @Test
  void testBuild() {
    String hostUrl = "someHostUrl";
    HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();

    HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
    HostSpec hostSpec = hostSpecBuilder.host(hostUrl).build();

    assertEquals(hostUrl, hostSpec.getHost());
    assertEquals(availabilityStrategy, hostSpec.getHostAvailabilityStrategy());
    assertEquals(defaultExpectedPort, hostSpec.getPort());
    assertEquals(defaultExpectedAvailability, hostSpec.getAvailability());
    assertEquals(defaultExpectedRole, hostSpec.getRole());
    assertEquals(defaultExpectedWeight, hostSpec.getWeight());
  }

  @Test
  void testBuildGivenPort() {
    String hostUrl = "someHostUrl";
    HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();
    int expectedPort = 555;

    HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
    HostSpec hostSpec = hostSpecBuilder
        .host(hostUrl)
        .port(expectedPort)
        .build();

    assertEquals(hostUrl, hostSpec.getHost());
    assertEquals(availabilityStrategy, hostSpec.getHostAvailabilityStrategy());
    assertEquals(expectedPort, hostSpec.getPort());
    assertEquals(defaultExpectedAvailability, hostSpec.getAvailability());
    assertEquals(defaultExpectedRole, hostSpec.getRole());
    assertEquals(defaultExpectedWeight, hostSpec.getWeight());
  }

  @Test
  void testBuildGivenAvailability() {
    String hostUrl = "someHostUrl";
    HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();
    HostAvailability expectedAvailability = HostAvailability.NOT_AVAILABLE;

    HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
    HostSpec hostSpec = hostSpecBuilder
        .host(hostUrl)
        .availability(expectedAvailability)
        .build();

    assertEquals(hostUrl, hostSpec.getHost());
    assertEquals(availabilityStrategy, hostSpec.getHostAvailabilityStrategy());
    assertEquals(defaultExpectedPort, hostSpec.getPort());
    assertEquals(expectedAvailability, hostSpec.getAvailability());
    assertEquals(defaultExpectedRole, hostSpec.getRole());
    assertEquals(defaultExpectedWeight, hostSpec.getWeight());
  }

  @Test
  void testBuildGivenRole() {
    String hostUrl = "someHostUrl";
    HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();
    HostRole expectedRole = HostRole.READER;

    HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
    HostSpec hostSpec = hostSpecBuilder
        .host(hostUrl)
        .role(expectedRole)
        .build();

    assertEquals(hostUrl, hostSpec.getHost());
    assertEquals(availabilityStrategy, hostSpec.getHostAvailabilityStrategy());
    assertEquals(defaultExpectedPort, hostSpec.getPort());
    assertEquals(defaultExpectedAvailability, hostSpec.getAvailability());
    assertEquals(expectedRole, hostSpec.getRole());
    assertEquals(defaultExpectedWeight, hostSpec.getWeight());
  }

  @Test
  void testBuildGivenWeight() {
    String hostUrl = "someHostUrl";
    HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();
    long expectedWeight = 555;

    HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
    HostSpec hostSpec = hostSpecBuilder
        .host(hostUrl)
        .weight(expectedWeight)
        .build();

    assertEquals(hostUrl, hostSpec.getHost());
    assertEquals(availabilityStrategy, hostSpec.getHostAvailabilityStrategy());
    assertEquals(defaultExpectedPort, hostSpec.getPort());
    assertEquals(defaultExpectedAvailability, hostSpec.getAvailability());
    assertEquals(defaultExpectedRole, hostSpec.getRole());
    assertEquals(expectedWeight, hostSpec.getWeight());
  }

  @Test
  void testBuildGivenPortAvailabilityRoleAndWeight() {
    String hostUrl = "someHostUrl";
    HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();
    int expectedPort = 555;
    HostAvailability expectedAvailability = HostAvailability.NOT_AVAILABLE;
    HostRole expectedRole = HostRole.READER;
    long expectedWeight = 777;

    HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
    HostSpec hostSpec = hostSpecBuilder
        .host(hostUrl)
        .port(expectedPort)
        .availability(expectedAvailability)
        .role(expectedRole)
        .weight(expectedWeight)
        .build();

    assertEquals(hostUrl, hostSpec.getHost());
    assertEquals(availabilityStrategy, hostSpec.getHostAvailabilityStrategy());
    assertEquals(expectedPort, hostSpec.getPort());
    assertEquals(expectedAvailability, hostSpec.getAvailability());
    assertEquals(expectedRole, hostSpec.getRole());
    assertEquals(expectedWeight, hostSpec.getWeight());
  }

  @Test
  void testCopyConstructorIsDeepCopy() {
    HostAvailabilityStrategy hostAvailabilityStrategy = new SimpleHostAvailabilityStrategy();
    HostSpecBuilder hostSpecBuilderOriginal = new HostSpecBuilder(hostAvailabilityStrategy)
        .host("someUrl")
        .port(1111)
        .availability(HostAvailability.AVAILABLE)
        .role(HostRole.WRITER)
        .weight(111)
        .lastUpdateTime(new Timestamp(1));
    HostSpecBuilder hostSpecBuilderModifiedCopy = new HostSpecBuilder(hostSpecBuilderOriginal)
        .host("someOtherUrl")
        .port(2222)
        .availability(HostAvailability.NOT_AVAILABLE)
        .role(HostRole.READER)
        .weight(222)
        .lastUpdateTime(new Timestamp(2));

    HostSpec fromOriginalBuilder = hostSpecBuilderOriginal.build();
    HostSpec fromModifiedCopyBuilder = hostSpecBuilderModifiedCopy.build();

    assertNotEquals(fromOriginalBuilder.getHost(), fromModifiedCopyBuilder.getHost());
    assertNotEquals(fromOriginalBuilder.getPort(), fromModifiedCopyBuilder.getPort());
    assertNotEquals(fromOriginalBuilder.getAvailability(), fromModifiedCopyBuilder.getAvailability());
    assertNotEquals(fromOriginalBuilder.getRole(), fromModifiedCopyBuilder.getRole());
    assertNotEquals(fromOriginalBuilder.getWeight(), fromModifiedCopyBuilder.getWeight());
    assertNotEquals(fromOriginalBuilder.getLastUpdateTime(), fromModifiedCopyBuilder.getLastUpdateTime());
    assertEquals(fromOriginalBuilder.getHostAvailabilityStrategy(),
        fromModifiedCopyBuilder.getHostAvailabilityStrategy());
  }
}
