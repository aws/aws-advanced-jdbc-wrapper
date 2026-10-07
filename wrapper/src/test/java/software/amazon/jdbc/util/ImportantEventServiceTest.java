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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.util.ImportantEventService.ImportantEvent;

class ImportantEventServiceTest {

  private static final long TTL_MS = 60000;

  private static List<String> descriptions(final List<ImportantEvent> events) {
    return events.stream().map(e -> e.description).collect(Collectors.toList());
  }

  private static String executing(final String methodName) {
    return Messages.get("DefaultConnectionPlugin.executingMethod", new Object[] {methodName});
  }

  @Test
  void testRegisterEvent_keepsEventsInOrder() {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 10, 4);
    service.registerEvent("event-1");
    service.registerEvent(() -> "event-2");

    final List<ImportantEvent> events = service.getEvents();
    assertEquals(Arrays.asList("event-1", "event-2"), descriptions(events));
    assertEquals(Thread.currentThread().getName(), events.get(0).threadName);
  }

  @Test
  void testRegisterEvent_dropsOldestEventsAboveMaxSize() {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 3, 4);
    for (int i = 1; i <= 10; i++) {
      service.registerEvent("event-" + i);
    }

    assertEquals(Arrays.asList("event-8", "event-9", "event-10"), descriptions(service.getEvents()));
  }

  @Test
  void testRegisterEvent_maxSizeHoldsUnderConcurrency() throws InterruptedException {
    final int maxEvents = 16;
    final int threads = 8;
    final int eventsPerThread = 10000;
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, maxEvents, 4);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final Thread worker = new Thread(() -> {
        try {
          start.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (int i = 0; i < eventsPerThread; i++) {
          service.registerEvent("event");
          service.registerMethodCall("Statement.executeQuery");
        }
      });
      workers.add(worker);
      worker.start();
    }
    start.countDown();
    for (Thread worker : workers) {
      worker.join();
    }

    final List<ImportantEvent> events = service.getEvents();
    final long registered = events.stream().filter(e -> "event".equals(e.description)).count();
    final long methodCalls = events.size() - registered;
    assertEquals(maxEvents, registered);
    assertEquals(4, methodCalls);
  }

  @Test
  void testRegisterEvent_expiresEventsAfterTtl() throws InterruptedException {
    final ImportantEventService service = new ImportantEventService(true, 50, 10, 4);
    service.registerEvent("old-event");
    service.registerMethodCall("Connection.isValid");
    Thread.sleep(200);
    service.registerEvent("new-event");

    assertEquals(Arrays.asList("new-event"), descriptions(service.getEvents()));
  }

  @Test
  void testRegisterMethodCall_keepsLastCallsInOrder() {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 10, 4);
    for (int i = 1; i <= 10; i++) {
      service.registerMethodCall("method" + i);
    }

    final List<ImportantEvent> events = service.getEvents();
    assertEquals(
        Arrays.asList(executing("method7"), executing("method8"), executing("method9"), executing("method10")),
        descriptions(events));
    for (ImportantEvent event : events) {
      assertEquals(Thread.currentThread().getName(), event.threadName);
    }
  }

  @Test
  void testRegisterMethodCall_recordsCallingThread() throws InterruptedException {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 10, 4);
    final Thread worker = new Thread(() -> service.registerMethodCall("Statement.executeQuery"), "worker-1");
    worker.start();
    worker.join();

    final List<ImportantEvent> events = service.getEvents();
    assertEquals(1, events.size());
    assertEquals("worker-1", events.get(0).threadName);
    assertEquals(executing("Statement.executeQuery"), events.get(0).description);
  }

  @Test
  void testGetEvents_mergesEventsAndMethodCallsByTimestamp() throws InterruptedException {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 10, 4);
    service.registerMethodCall("Connection.setAutoCommit");
    Thread.sleep(5);
    service.registerEvent("failover started");
    Thread.sleep(5);
    service.registerMethodCall("Statement.executeQuery");

    final List<ImportantEvent> events = service.getEvents();
    assertEquals(
        Arrays.asList(executing("Connection.setAutoCommit"), "failover started", executing("Statement.executeQuery")),
        descriptions(events));
    for (int i = 1; i < events.size(); i++) {
      final Instant previous = events.get(i - 1).timestamp;
      assertFalse(events.get(i).timestamp.isBefore(previous));
    }
  }

  @Test
  void testDisabled_recordsNothing() {
    final ImportantEventService service = new ImportantEventService(false, TTL_MS, 10, 4);
    service.registerEvent("event");
    service.registerEvent(() -> "event");
    service.registerMethodCall("Statement.executeQuery");

    assertTrue(service.getEvents().isEmpty());
  }

  @Test
  void testZeroMethodHistorySize_disablesMethodHistory() {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 10, 0);
    service.registerMethodCall("Statement.executeQuery");
    service.registerEvent("event");

    assertEquals(Arrays.asList("event"), descriptions(service.getEvents()));
  }

  @Test
  void testClear_removesEventsAndMethodCalls() {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 2, 4);
    service.registerEvent("event-1");
    service.registerEvent("event-2");
    service.registerMethodCall("Statement.executeQuery");

    service.clear();
    assertTrue(service.getEvents().isEmpty());

    // The size limit still applies after clearing.
    service.registerEvent("event-3");
    service.registerEvent("event-4");
    service.registerEvent("event-5");
    assertEquals(Arrays.asList("event-4", "event-5"), descriptions(service.getEvents()));
  }

  @Test
  void testTwoArgConstructor_usesDefaultLimits() {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS);
    for (int i = 0; i < ImportantEventService.DEFAULT_QUEUE_MAX_SIZE + 10; i++) {
      service.registerEvent("event");
    }
    for (int i = 0; i < ImportantEventService.DEFAULT_METHOD_HISTORY_SIZE + 10; i++) {
      service.registerMethodCall("Statement.executeQuery");
    }

    assertEquals(
        ImportantEventService.DEFAULT_QUEUE_MAX_SIZE + ImportantEventService.DEFAULT_METHOD_HISTORY_SIZE,
        service.getEvents().size());
  }

  @Test
  void testOutOfRangeLimits_areClamped() {
    final ImportantEventService service = new ImportantEventService(true, TTL_MS, 0, -1);
    service.registerEvent("event-1");
    service.registerEvent("event-2");
    service.registerMethodCall("Statement.executeQuery");

    assertEquals(Arrays.asList("event-2"), descriptions(service.getEvents()));
  }
}
