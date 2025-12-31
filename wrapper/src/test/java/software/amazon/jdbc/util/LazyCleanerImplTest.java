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

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class LazyCleanerImplTest {

  @Test
  void phantomCleaner() throws Exception {
    List<Object> list = new ArrayList<>(Arrays.asList(
        new Object(), new Object(), new Object()));

    LazyCleanerImpl t = new LazyCleanerImpl("Cleaner", ofSeconds(5));
    String[] collected = new String[list.size()];
    List<LazyCleaner.Cleanable> cleaners = new ArrayList<>();

    for (int i = 0; i < list.size(); i++) {
      final int ii = i;
      cleaners.add(
          t.register(
              list.get(i),
              leak -> {
                collected[ii] = leak ? "LEAK" : "NO LEAK";
                if (ii == 0) {
                  throw new RuntimeException(
                      "Exception from cleanup action to verify if the cleaner thread would survive"
                  );
                }
              }
          )
      );
    }

    assertTrue(t.isThreadRunning(), "cleanup thread should be running, and it should wait for the leaks");

    try {
      cleaners.get(1).clean();
    } catch (Throwable e) {
      // Expected for test
    }
    list.set(0, null);
    System.gc();
    System.gc();
    list.clear();
    System.gc();
    System.gc();

    until(
        "The cleanup thread should detect leaks and terminate within 5-10 seconds after GC",
        ofSeconds(10),
        () -> !t.isThreadRunning()
    );

    assertEquals(
        Arrays.asList("LEAK", "NO LEAK", "LEAK").toString(),
        Arrays.asList(collected).toString(),
        "Second object has been released properly, so it should be reported as NO LEAK"
    );
  }

  @Test
  void cleanupCompletesAfterManualClean() throws Exception {
    String threadName = UUID.randomUUID().toString();
    LazyCleanerImpl t = new LazyCleanerImpl(threadName, ofSeconds(5));
    AtomicBoolean cleaned = new AtomicBoolean();
    List<Object> list = new ArrayList<>();
    list.add(new Object());

    LazyCleaner.Cleanable cleanable = t.register(
        list.get(0),
        leak -> cleaned.set(true)
    );

    assertTrue(t.isThreadRunning(), "cleanup thread should be running when there are objects to monitor");

    try {
      cleanable.clean();
    } catch (Throwable e) {
      // Expected for test
    }
    assertTrue(cleaned.get(), "Object should be cleaned after manual clean");

    list.clear();
    System.gc();
    System.gc();

    until(
        "Cleanup thread should stop when no objects remain",
        ofSeconds(10),
        () -> !t.isThreadRunning()
    );
  }

  @Test
  void exceptionsDuringCleanupAreHandled() throws InterruptedException {
    LazyCleanerImpl t = new LazyCleanerImpl("test-cleaner", ofSeconds(5));
    AtomicInteger cleanupCount = new AtomicInteger(0);
    List<Object> list = new ArrayList<>();

    list.add(new Object());
    t.register(
        list.get(0),
        leak -> {
          cleanupCount.incrementAndGet();
          throw new IllegalStateException("test exception from CleaningAction");
        }
    );

    list.add(new Object());
    AtomicBoolean secondCleaned = new AtomicBoolean(false);
    t.register(
        list.get(1),
        leak -> secondCleaned.set(true)
    );

    assertTrue(t.isThreadRunning(), "cleanup thread should be running when there are objects to monitor");

    list.clear();
    System.gc();
    System.gc();

    until(
        "Both cleanups should complete despite exception",
        ofSeconds(10),
        () -> cleanupCount.get() == 1 && secondCleaned.get()
    );

    until(
        "Cleanup thread should stop after all objects are cleaned",
        ofSeconds(10),
        () -> !t.isThreadRunning()
    );
  }

  @Test
  void exceptionOnCleanRethrowsToCaller() {
    LazyCleanerImpl t = new LazyCleanerImpl("test-cleaner", ofSeconds(5));
    Object obj = new Object();
    RuntimeException expectedException = new RuntimeException("test exception");

    LazyCleaner.Cleanable cleanable = t.register(obj, leak -> {
      throw expectedException;
    });

    RuntimeException thrownException = assertThrows(RuntimeException.class, cleanable::clean);
    assertSame(expectedException, thrownException, "Expected same exception instance to be thrown");
  }

  private static void until(String message, Duration timeout, Condition condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (!condition.get()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met within " + timeout + ": " + message);
      }
      Thread.sleep(100);
    }
  }

  private interface Condition {
    boolean get();
  }
}
