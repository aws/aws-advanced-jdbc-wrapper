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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.RepeatedTest;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.RandomHostSelector;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class RandomHostSelectorTests {

  private static final HostRole HOST_ROLE = HostRole.READER;

  @RepeatedTest(value = 50)
  void testGetHostGivenUnavailbleHost() throws SQLException {

    final HostSpec unavailableHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("someUnavailableHost")
        .role(HOST_ROLE)
        .availability(HostAvailability.NOT_AVAILABLE)
        .build();

    final HostSpec availableHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("someAvailableHost")
        .role(HOST_ROLE)
        .availability(HostAvailability.AVAILABLE)
        .build();

    final RandomHostSelector hostSelector = new RandomHostSelector();
    final HostSpec actualHost = hostSelector.getHost(Arrays.asList(unavailableHost, availableHost), HOST_ROLE,
        new Properties());

    assertEquals(availableHost, actualHost);
  }

  @RepeatedTest(value = 50)
  void testGetHostGivenMultipleUnavailableHosts() throws SQLException {
    List<HostSpec> hostSpecTestsList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("someUnavailableHost")
            .role(HOST_ROLE)
            .availability(HostAvailability.NOT_AVAILABLE)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("someUnavailableHost")
            .role(HOST_ROLE)
            .availability(HostAvailability.NOT_AVAILABLE)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("someAvailableHost")
            .role(HOST_ROLE)
            .availability(HostAvailability.AVAILABLE)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("someAvailableHost")
            .role(HOST_ROLE)
            .availability(HostAvailability.AVAILABLE)
            .build()
    );

    final RandomHostSelector hostSelector = new RandomHostSelector();
    final HostSpec actualHost = hostSelector.getHost(hostSpecTestsList, HOST_ROLE, new Properties());
    assertEquals(HostAvailability.AVAILABLE, actualHost.getAvailability());
  }
}
