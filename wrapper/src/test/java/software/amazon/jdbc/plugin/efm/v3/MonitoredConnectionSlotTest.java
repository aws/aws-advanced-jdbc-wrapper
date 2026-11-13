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

package software.amazon.jdbc.plugin.efm.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostSpec;

class MonitoredConnectionSlotTest {

  private static final long T0 = 1_000_000_000_000L;

  private Connection connection;
  private HostSpec hostSpec;
  private MonitoredConnectionSlot slot;

  @BeforeEach
  void init() {
    connection = mock(Connection.class);
    hostSpec = mock(HostSpec.class);
    slot = new MonitoredConnectionSlot(connection, hostSpec, T0);
  }

  @Test
  void newSlot_isIdleAndKeepsConnectionAndHost() {
    assertEquals(MonitoredConnectionSlot.IDLE, slot.getInFlightSinceNanos());
    assertSame(connection, slot.getConnection());
    assertSame(hostSpec, slot.getHostSpec());
    assertEquals(T0, slot.getLastActivityNanos());
  }

  @Test
  void enterAndExit_publishStartTimeAndResetToIdle() {
    assertFalse(slot.enter(T0 + 10));
    assertEquals(T0 + 10, slot.getInFlightSinceNanos());

    assertFalse(slot.exit(T0 + 20));
    assertEquals(MonitoredConnectionSlot.IDLE, slot.getInFlightSinceNanos());
    assertEquals(T0 + 20, slot.getLastActivityNanos());
  }

  @Test
  void nestedCalls_keepTheOutermostStartTime() {
    slot.enter(T0 + 10);
    slot.enter(T0 + 50);
    assertEquals(T0 + 10, slot.getInFlightSinceNanos(), "inner call must not overwrite the outer start time");

    assertFalse(slot.exit(T0 + 60));
    assertEquals(T0 + 10, slot.getInFlightSinceNanos(), "inner exit must not reset the slot");

    assertFalse(slot.exit(T0 + 70));
    assertEquals(MonitoredConnectionSlot.IDLE, slot.getInFlightSinceNanos());
  }

  @Test
  void claimWhileInFlight_isReportedOnExit() {
    slot.enter(T0 + 10);
    final long observed = slot.getInFlightSinceNanos();

    assertTrue(slot.claim(observed));
    assertEquals(MonitoredConnectionSlot.ABORTED, slot.getInFlightSinceNanos());

    assertTrue(slot.exit(T0 + 20), "the application thread must learn that its call was aborted");
    assertEquals(MonitoredConnectionSlot.IDLE, slot.getInFlightSinceNanos());
  }

  @Test
  void claimAfterTheCallCompleted_fails() {
    slot.enter(T0 + 10);
    final long observed = slot.getInFlightSinceNanos();
    slot.exit(T0 + 20);

    assertFalse(slot.claim(observed), "a completed call must never be reported as aborted");
    assertEquals(MonitoredConnectionSlot.IDLE, slot.getInFlightSinceNanos());
  }

  @Test
  void claimAfterANewCallStarted_fails() {
    slot.enter(T0 + 10);
    final long observed = slot.getInFlightSinceNanos();
    slot.exit(T0 + 20);
    slot.enter(T0 + 30);

    assertFalse(slot.claim(observed), "a stale observation must not claim a newer call");
    assertEquals(T0 + 30, slot.getInFlightSinceNanos());
  }

  @Test
  void claimIdle_isReportedOnNextEnter() {
    assertTrue(slot.claim(MonitoredConnectionSlot.IDLE));
    assertEquals(MonitoredConnectionSlot.ABORTED, slot.getInFlightSinceNanos());

    assertTrue(slot.enter(T0 + 10), "the next call must learn that the idle connection was aborted");
    assertEquals(T0 + 10, slot.getInFlightSinceNanos());
    assertFalse(slot.exit(T0 + 20));
  }

  @Test
  void claim_isNeverGrantedTwice() {
    slot.enter(T0 + 10);
    assertTrue(slot.claim(T0 + 10));
    assertFalse(slot.claim(T0 + 10));
    assertFalse(slot.claim(MonitoredConnectionSlot.ABORTED));
    assertFalse(slot.claim(MonitoredConnectionSlot.IDLE));
  }
}
