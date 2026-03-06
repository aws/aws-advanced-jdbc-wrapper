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

package software.amazon.jdbc.plugin.efm.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;

class HostMonitorConnectionContextV1Test {

  @Mock Connection connection;
  @Mock TelemetryCounter counter;
  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_defaultConstructor() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1();
    assertTrue(ctx.isActiveContext());
    assertEquals(0, ctx.getFailureCount());
  }

  @Test
  void test_singleArgConstructor() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(connection);
    assertNotNull(ctx.getConnection());
  }

  @Test
  void test_initContext() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1();
    ctx.initContext(connection, 50, 100, 3, counter);

    assertEquals(100, ctx.getFailureDetectionIntervalMillis());
    assertEquals(3, ctx.getFailureDetectionCount());
    assertFalse(ctx.isNodeUnhealthy());
  }

  @Test
  void test_setStartMonitorTimeNano() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    long startTime = System.nanoTime();
    ctx.setStartMonitorTimeNano(startTime);

    assertEquals(startTime + TimeUnit.MILLISECONDS.toNanos(100), ctx.getExpectedActiveMonitoringStartTimeNano());
  }

  @Test
  void test_setInvalidNodeStartTimeNano() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    long time = System.nanoTime();
    ctx.setInvalidNodeStartTimeNano(time);

    assertTrue(ctx.isInvalidNodeStartTimeDefined());
    assertEquals(time, ctx.getInvalidNodeStartTimeNano());
  }

  @Test
  void test_abortConnection_nullConnectionRef() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    ctx.abortConnection();
    verify(counter, never()).inc();
  }

  @Test
  void test_abortConnection_inactiveContext() throws SQLException {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(connection, 100, 50, 3, counter);
    ctx.setInactive();
    ctx.abortConnection();

    verify(connection, never()).abort(any());
  }

  @Test
  void test_abortConnection_success() throws SQLException {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(connection, 100, 50, 3, counter);
    ctx.abortConnection();

    verify(connection).abort(any());
    verify(connection).close();
    verify(counter).inc();
  }

  @Test
  void test_abortConnection_nullCounter() throws SQLException {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(connection, 100, 50, 3, null);
    ctx.abortConnection();

    verify(connection).abort(any());
    verify(connection).close();
  }

  @Test
  void test_updateConnectionStatus_beforeGracePeriod() {
    HostMonitorConnectionContextV1 ctx = spy(new HostMonitorConnectionContextV1(null, 100, 50, 3, counter));
    long startTime = System.nanoTime();
    ctx.setStartMonitorTimeNano(startTime);

    long checkStart = startTime + TimeUnit.MILLISECONDS.toNanos(50);
    long checkEnd = checkStart + 1000;

    ctx.updateConnectionStatus("host", checkStart, checkEnd, false);
    verify(ctx, never()).setConnectionValid(anyString(), anyBoolean(), anyLong(), anyLong());
  }

  @Test
  void test_setConnectionValid_recoveryAfterFailure() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    long time = System.nanoTime();

    ctx.setConnectionValid("host", false, time, time);
    assertEquals(1, ctx.getFailureCount());
    assertTrue(ctx.isInvalidNodeStartTimeDefined());

    ctx.setConnectionValid("host", true, time, time);
    assertEquals(0, ctx.getFailureCount());
    assertFalse(ctx.isInvalidNodeStartTimeDefined());
    assertFalse(ctx.isNodeUnhealthy());
  }

  @Test
  void test_getLock() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1();
    assertNotNull(ctx.getLock());
  }

  @Test
  void test_resetInvalidNodeStartTime() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    ctx.setInvalidNodeStartTimeNano(System.nanoTime());
    assertTrue(ctx.isInvalidNodeStartTimeDefined());

    ctx.resetInvalidNodeStartTime();
    assertFalse(ctx.isInvalidNodeStartTimeDefined());
  }

  @Test
  void test_setFailureCount() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    ctx.setFailureCount(5);
    assertEquals(5, ctx.getFailureCount());
  }

  @Test
  void test_setInactive() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1();
    assertTrue(ctx.isActiveContext());

    ctx.setInactive();
    assertFalse(ctx.isActiveContext());
  }

  @Test
  void test_getFailureDetectionIntervalMillis() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    assertEquals(50, ctx.getFailureDetectionIntervalMillis());
  }

  @Test
  void test_getFailureDetectionCount() {
    HostMonitorConnectionContextV1 ctx = new HostMonitorConnectionContextV1(null, 100, 50, 3, counter);
    assertEquals(3, ctx.getFailureDetectionCount());
  }
}
