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

package software.amazon.jdbc.plugin.efm;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class MonitorConnectionContextTest {

  private static final Set<String> NODE_KEYS =
      new HashSet<>(Collections.singletonList("any.node.domain"));
  private static final long FAILURE_DETECTION_TIME_MILLIS = 10;
  private static final long FAILURE_DETECTION_INTERVAL_MILLIS = 100;
  private static final long FAILURE_DETECTION_COUNT = 3;
  private static final long VALIDATION_INTERVAL_MILLIS = 50;

  private MonitorConnectionContext context;
  private AutoCloseable closeable;

  @Mock Connection connectionToAbort;

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
  public void test_isNodeUnhealthyWithConnection_returnFalse() {
    long currentTimeNano = System.nanoTime();
    context.setConnectionValid(true, currentTimeNano, currentTimeNano);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(0, this.context.getFailureCount());
  }

  @Test
  public void test_isNodeUnhealthyWithInvalidConnection_returnFalse() {
    long currentTimeNano = System.nanoTime();
    context.setConnectionValid(false, currentTimeNano, currentTimeNano);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(1, this.context.getFailureCount());
  }

  @Test
  public void test_isNodeUnhealthyExceedsFailureDetectionCount_returnTrue() {
    final long expectedFailureCount = FAILURE_DETECTION_COUNT + 1;
    context.setFailureCount(FAILURE_DETECTION_COUNT);
    context.resetInvalidNodeStartTime();

    long currentTimeNano = System.nanoTime();
    context.setConnectionValid(false, currentTimeNano, currentTimeNano);

    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(expectedFailureCount, context.getFailureCount());
    Assertions.assertTrue(context.isInvalidNodeStartTimeDefined());
  }

  @Test
  public void test_isNodeUnhealthyExceedsFailureDetectionCount() {
    long currentTimeNano = System.nanoTime();
    context.setFailureCount(0);
    context.resetInvalidNodeStartTime();

    // Simulate monitor loop that reports invalid connection for 5 times with interval 50 msec to
    // wait 250 msec in total
    for (int i = 0; i < 5; i++) {
      long statusCheckStartTime = currentTimeNano;
      long statusCheckEndTime = currentTimeNano + TimeUnit.MILLISECONDS.toNanos(VALIDATION_INTERVAL_MILLIS);

      context.setConnectionValid(false, statusCheckStartTime, statusCheckEndTime);
      Assertions.assertFalse(context.isNodeUnhealthy());

      currentTimeNano += TimeUnit.MILLISECONDS.toNanos(VALIDATION_INTERVAL_MILLIS);
    }

    // Simulate waiting another 50 msec that makes total waiting time to 300 msec
    // Expected max waiting time for this context is 300 msec (interval 100 msec, count 3)
    // So it's expected that this run turns node status to "unhealthy" since we reached max allowed
    // waiting time.

    long statusCheckStartTime = currentTimeNano;
    long statusCheckEndTime = currentTimeNano + TimeUnit.MILLISECONDS.toNanos(VALIDATION_INTERVAL_MILLIS);

    context.setConnectionValid(false, statusCheckStartTime, statusCheckEndTime);
    Assertions.assertTrue(context.isNodeUnhealthy());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void test_updateConnectionStatus_inactiveContext(boolean isValid) {
    final long currentTime = System.nanoTime();
    final long statusCheckStartTime = System.nanoTime() - FAILURE_DETECTION_TIME_MILLIS;

    final MonitorConnectionContext spyContext = spy(context);

    spyContext.updateConnectionStatus(statusCheckStartTime, currentTime, isValid);

    verify(spyContext).setConnectionValid(eq(isValid), eq(statusCheckStartTime), eq(currentTime));
  }

  @Test
  void test_updateConnectionStatus() {
    final long currentTime = System.nanoTime();
    final long statusCheckStartTime = System.nanoTime() - 1000;
    context.invalidate();

    final MonitorConnectionContext spyContext = spy(context);

    spyContext.updateConnectionStatus(statusCheckStartTime, currentTime, true);

    verify(spyContext, never()).setConnectionValid(eq(true), eq(statusCheckStartTime), eq(currentTime));
  }

  @Test
  void test_abortConnection_ignoresSqlException() throws SQLException {
    context =
        new MonitorConnectionContext(
            connectionToAbort,
            NODE_KEYS,
            FAILURE_DETECTION_TIME_MILLIS,
            FAILURE_DETECTION_INTERVAL_MILLIS,
            FAILURE_DETECTION_COUNT);

    doThrow(new SQLException("unexpected SQLException during abort")).when(connectionToAbort).close();

    // An exception will be thrown inside this call, but it should not be propagated.
    context.abortConnection();
  }
}
