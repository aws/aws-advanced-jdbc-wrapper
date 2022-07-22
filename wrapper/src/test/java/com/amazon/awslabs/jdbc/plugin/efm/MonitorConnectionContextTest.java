/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc.plugin.efm;

import com.amazon.awslabs.jdbc.plugin.efm.MonitorConnectionContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

class MonitorConnectionContextTest {

  private static final Set<String> NODE_KEYS =
      new HashSet<>(Collections.singletonList("any.node.domain"));
  private static final int FAILURE_DETECTION_TIME_MILLIS = 10;
  private static final int FAILURE_DETECTION_INTERVAL_MILLIS = 100;
  private static final int FAILURE_DETECTION_COUNT = 3;
  private static final int VALIDATION_INTERVAL_MILLIS = 50;

  private MonitorConnectionContext context;
  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    context =
        new MonitorConnectionContext(
            null,
            NODE_KEYS,
            FAILURE_DETECTION_TIME_MILLIS,
            FAILURE_DETECTION_INTERVAL_MILLIS,
            FAILURE_DETECTION_COUNT);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  public void test_1_isNodeUnhealthyWithConnection_returnFalse() {
    long currentTimeMillis = System.currentTimeMillis();
    context.setConnectionValid(true, currentTimeMillis, currentTimeMillis);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(0, this.context.getFailureCount());
  }

  @Test
  public void test_2_isNodeUnhealthyWithInvalidConnection_returnFalse() {
    long currentTimeMillis = System.currentTimeMillis();
    context.setConnectionValid(false, currentTimeMillis, currentTimeMillis);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(1, this.context.getFailureCount());
  }

  @Test
  public void test_3_isNodeUnhealthyExceedsFailureDetectionCount_returnTrue() {
    final int expectedFailureCount = FAILURE_DETECTION_COUNT + 1;
    context.setFailureCount(FAILURE_DETECTION_COUNT);
    context.resetInvalidNodeStartTime();

    long currentTimeMillis = System.currentTimeMillis();
    context.setConnectionValid(false, currentTimeMillis, currentTimeMillis);

    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(expectedFailureCount, context.getFailureCount());
    Assertions.assertTrue(context.isInvalidNodeStartTimeDefined());
  }

  @Test
  public void test_4_isNodeUnhealthyExceedsFailureDetectionCount() {
    long currentTimeMillis = System.currentTimeMillis();
    context.setFailureCount(0);
    context.resetInvalidNodeStartTime();

    // Simulate monitor loop that reports invalid connection for 5 times with interval 50 msec to
    // wait 250 msec in total
    for (int i = 0; i < 5; i++) {
      long statusCheckStartTime = currentTimeMillis;
      long statusCheckEndTime = currentTimeMillis + VALIDATION_INTERVAL_MILLIS;

      context.setConnectionValid(false, statusCheckStartTime, statusCheckEndTime);
      Assertions.assertFalse(context.isNodeUnhealthy());

      currentTimeMillis += VALIDATION_INTERVAL_MILLIS;
    }

    // Simulate waiting another 50 msec that makes total waiting time to 300 msec
    // Expected max waiting time for this context is 300 msec (interval 100 msec, count 3)
    // So it's expected that this run turns node status to "unhealthy" since we reached max allowed
    // waiting time.

    long statusCheckStartTime = currentTimeMillis;
    long statusCheckEndTime = currentTimeMillis + VALIDATION_INTERVAL_MILLIS;

    context.setConnectionValid(false, statusCheckStartTime, statusCheckEndTime);
    Assertions.assertTrue(context.isNodeUnhealthy());
  }
}
