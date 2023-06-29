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


import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;

public class HostSpecTests {

  @Mock HostAvailabilityStrategy mockHostAvailabilityStrategy;

  private AutoCloseable closeable;

  HostSpec hostSpec;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    hostSpec = new HostSpecBuilder(this.mockHostAvailabilityStrategy).host("someUrl").build();
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testSetAvailabilityCallsHostAvailabilityStrategy() {
    final HostAvailability hostAvailability = HostAvailability.NOT_AVAILABLE;
    hostSpec.setAvailability(hostAvailability);
    verify(mockHostAvailabilityStrategy, times(1)).setHostAvailability(hostAvailability);
  }

  @Test
  public void testGetAvailabilityCallsHostAvailabilityStrategy() {
    hostSpec.getAvailability();
    verify(mockHostAvailabilityStrategy, times(1)).getHostAvailability(hostSpec.availability);
  }
}
