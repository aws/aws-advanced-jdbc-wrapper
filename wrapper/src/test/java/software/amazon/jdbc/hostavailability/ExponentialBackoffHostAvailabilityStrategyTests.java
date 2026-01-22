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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class ExponentialBackoffHostAvailabilityStrategyTests {

  @Test
  void testGetHostAvailabilityReturnsAvailable() {
    Properties props = new Properties();
    ExponentialBackoffHostAvailabilityStrategy availabilityStrategy =
        new ExponentialBackoffHostAvailabilityStrategy(props);
    HostAvailability actualHostAvailability =
        availabilityStrategy.getHostAvailability(HostAvailability.AVAILABLE);
    HostAvailability expectedHostAvailability = HostAvailability.AVAILABLE;
    assertEquals(expectedHostAvailability, actualHostAvailability);
  }

  @Test
  void testGetHostAvailabilityMaxRetriesExceeded() throws InterruptedException {
    Properties props = new Properties();
    props.setProperty(HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.name, "1");
    props.setProperty(HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.name, "3");
    ExponentialBackoffHostAvailabilityStrategy availabilityStrategy =
        new ExponentialBackoffHostAvailabilityStrategy(props);
    availabilityStrategy.setHostAvailability(HostAvailability.NOT_AVAILABLE);
    availabilityStrategy.setHostAvailability(HostAvailability.NOT_AVAILABLE);

    Thread.sleep(3000);

    HostAvailability actualHostAvailability = availabilityStrategy.getHostAvailability(HostAvailability.NOT_AVAILABLE);
    HostAvailability expectedHostAvailability = HostAvailability.NOT_AVAILABLE;
    assertEquals(expectedHostAvailability, actualHostAvailability);
  }

  @Test
  void testGetHostAvailabilityPastThreshold() throws InterruptedException {
    Properties props = new Properties();
    props.setProperty(HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.name, "3");
    ExponentialBackoffHostAvailabilityStrategy availabilityStrategy =
        new ExponentialBackoffHostAvailabilityStrategy(props);

    Thread.sleep(3001);

    HostAvailability actualHostAvailability = availabilityStrategy.getHostAvailability(HostAvailability.NOT_AVAILABLE);
    HostAvailability expectedHostAvailability = HostAvailability.AVAILABLE;
    assertEquals(expectedHostAvailability, actualHostAvailability);
  }

  @Test
  void testGetHostAvailabilityBeforeThreshold() throws InterruptedException {
    Properties props = new Properties();
    props.setProperty(HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.name, "3");
    ExponentialBackoffHostAvailabilityStrategy availabilityStrategy =
        new ExponentialBackoffHostAvailabilityStrategy(props);

    Thread.sleep(2500);

    HostAvailability actualHostAvailability = availabilityStrategy.getHostAvailability(HostAvailability.NOT_AVAILABLE);
    HostAvailability expectedHostAvailability = HostAvailability.NOT_AVAILABLE;
    assertEquals(expectedHostAvailability, actualHostAvailability);
  }

  @Test
  void testConstructorThrowsWhenInvalidMaxRetries() {
    Properties props = new Properties();
    props.setProperty(HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.name, "0");
    assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffHostAvailabilityStrategy(props));
  }

  @Test
  void testConstructorThrowsWhenInvalidBackoffTime() {
    Properties props = new Properties();
    props.setProperty(HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.name, "0");
    assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffHostAvailabilityStrategy(props));
  }
}
