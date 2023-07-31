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

package software.amazon.jdbc.hostavailability;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import org.junit.jupiter.api.Test;

public class HostAvailabilityStrategyFactoryTests {

  @Test
  public void testCreateDefaultAvailabilityStrategyGivenEmptyProperties() {
    HostAvailabilityStrategyFactory factory = new HostAvailabilityStrategyFactory();
    HostAvailabilityStrategy availabilityStrategy = factory.create(new Properties());
    assertEquals(SimpleHostAvailabilityStrategy.class, availabilityStrategy.getClass());
  }

  @Test
  public void testCreateDefaultAvailabilityStrategyGivenOverrideProperty() {

    Properties props = new Properties();
    props.setProperty(HostAvailabilityStrategyFactory.DEFAULT_HOST_AVAILABILITY_STRATEGY.name,
        ExponentialBackoffHostAvailabilityStrategy.NAME);

    HostAvailabilityStrategyFactory factory = new HostAvailabilityStrategyFactory();
    HostAvailabilityStrategy availabilityStrategy = factory.create(props);
    assertEquals(ExponentialBackoffHostAvailabilityStrategy.class, availabilityStrategy.getClass());
  }
}
