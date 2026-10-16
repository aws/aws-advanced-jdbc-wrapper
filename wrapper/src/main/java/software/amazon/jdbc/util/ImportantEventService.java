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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Collects a short history of important events so that it can be attached to an exception as context.
 *
 * <p>Two kinds of history are kept, both bounded in size so that memory use does not grow with throughput:
 * <ul>
 *   <li>Events registered with {@link #registerEvent(String)} or {@link #registerEvent(Supplier)}, such as
 *   failover or topology changes. They are kept for {@code eventQueueMs} milliseconds, and at most
 *   {@code maxEvents} of them are retained; once the limit is reached the oldest event is dropped.</li>
 *   <li>JDBC method calls registered with {@link #registerMethodCall(String)} on every call that goes through
 *   the plugin pipeline. Only the most recent {@code methodHistorySize} calls are kept, in a fixed-size ring,
 *   and their descriptions are formatted only when {@link #getEvents()} is called.</li>
 * </ul>
 */
public class ImportantEventService {

  private static final Logger LOGGER = Logger.getLogger(ImportantEventService.class.getName());

  static final String ENABLED_PROPERTY = "aws.jdbc.config.exception.context.enabled";
  static final String CONNECTION_QUEUE_TTL_PROPERTY = "aws.jdbc.config.exception.context.connection.queue.ttl";
  static final String CONNECTION_QUEUE_MAX_SIZE_PROPERTY =
      "aws.jdbc.config.exception.context.connection.queue.max.size";
  static final String DRIVER_QUEUE_TTL_PROPERTY = "aws.jdbc.config.exception.context.driver.queue.ttl";
  static final String DRIVER_QUEUE_MAX_SIZE_PROPERTY = "aws.jdbc.config.exception.context.driver.queue.max.size";
  static final String METHOD_HISTORY_SIZE_PROPERTY = "aws.jdbc.config.exception.context.method.history.size";

  static final long DEFAULT_QUEUE_TTL_MS = 60000;
  static final int DEFAULT_QUEUE_MAX_SIZE = 256;
  static final int DEFAULT_METHOD_HISTORY_SIZE = 64;

  private final Queue<ImportantEvent> events = new ConcurrentLinkedQueue<>();
  // ConcurrentLinkedQueue.size() traverses the whole queue, so the number of queued events is tracked here.
  private final AtomicInteger eventCount = new AtomicInteger();
  private final long eventQueueMs;
  private final boolean isEnabled;
  private final int maxEvents;

  private final AtomicReferenceArray<MethodCall> methodCalls;
  private final AtomicLong methodCallCount = new AtomicLong();

  public ImportantEventService() {
    this(
        isEnabledByDefault(),
        getLongProperty(CONNECTION_QUEUE_TTL_PROPERTY, DEFAULT_QUEUE_TTL_MS),
        getIntProperty(CONNECTION_QUEUE_MAX_SIZE_PROPERTY, DEFAULT_QUEUE_MAX_SIZE),
        defaultMethodHistorySize());
  }

  public ImportantEventService(final boolean isEnabled, final long eventQueueMs) {
    this(isEnabled, eventQueueMs, DEFAULT_QUEUE_MAX_SIZE, DEFAULT_METHOD_HISTORY_SIZE);
  }

  /**
   * Creates an event service.
   *
   * @param isEnabled whether events are collected at all
   * @param eventQueueMs how long, in milliseconds, events and method calls are reported after they occur
   * @param maxEvents the maximum number of events registered with {@code registerEvent} that are retained;
   *     values below 1 are treated as 1
   * @param methodHistorySize the number of most recent method calls that are retained; 0 or a negative value
   *     disables the method call history, and so does {@code isEnabled} being false
   */
  public ImportantEventService(
      final boolean isEnabled, final long eventQueueMs, final int maxEvents, final int methodHistorySize) {
    this.isEnabled = isEnabled;
    this.eventQueueMs = eventQueueMs;
    this.maxEvents = Math.max(1, maxEvents);
    this.methodCalls = new AtomicReferenceArray<>(isEnabled ? Math.max(0, methodHistorySize) : 0);
  }

  static boolean isEnabledByDefault() {
    return Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));
  }

  static int defaultMethodHistorySize() {
    return getIntProperty(METHOD_HISTORY_SIZE_PROPERTY, DEFAULT_METHOD_HISTORY_SIZE);
  }

  /**
   * Reads a numeric system property. A value that is not a number must not
   * fail the connection, or the class initialization of
   * {@link DriverImportantEventService}, over a diagnostic setting, so it is
   * logged and replaced with the default.
   */
  static long getLongProperty(final String name, final long defaultValue) {
    final String value = System.getProperty(name);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (final NumberFormatException e) {
      logInvalidProperty(name, value, defaultValue);
      return defaultValue;
    }
  }

  /**
   * Reads a numeric system property the same way as {@link #getLongProperty},
   * so a value outside the {@code int} range is also replaced with the default.
   */
  static int getIntProperty(final String name, final int defaultValue) {
    final String value = System.getProperty(name);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (final NumberFormatException e) {
      logInvalidProperty(name, value, defaultValue);
      return defaultValue;
    }
  }

  private static void logInvalidProperty(final String name, final String value, final long defaultValue) {
    LOGGER.warning(() -> Messages.get(
        "ImportantEventService.invalidPropertyValue", new Object[] {name, value, defaultValue}));
  }

  public void clear() {
    while (events.poll() != null) {
      eventCount.decrementAndGet();
    }
    for (int i = 0; i < methodCalls.length(); i++) {
      methodCalls.set(i, null);
    }
  }

  public void registerEvent(Supplier<String> descriptionSupplier) {
    if (!this.isEnabled) {
      return;
    }
    removeExpiredEvents();
    addEvent(new ImportantEvent(Instant.now(), descriptionSupplier.get()));
  }

  public void registerEvent(String description) {
    if (!this.isEnabled) {
      return;
    }
    removeExpiredEvents();
    addEvent(new ImportantEvent(Instant.now(), description));
  }

  /**
   * Records that a JDBC method is being executed. This is called for every call that goes through the
   * plugin pipeline, so it only stores a small record in a fixed-size ring of the most recent calls; the
   * description is formatted when {@link #getEvents()} is called.
   *
   * @param methodName the name of the JDBC method, for example {@code Statement.executeQuery}
   */
  public void registerMethodCall(final String methodName) {
    final int historySize = this.methodCalls.length();
    if (!this.isEnabled || historySize == 0) {
      return;
    }
    final int slot = (int) (this.methodCallCount.getAndIncrement() % historySize);
    // lazySet (an ordered store without a full fence) is enough: the history is diagnostic, and a concurrent
    // reader may at worst miss the most recent calls or see a slot that was just overwritten.
    this.methodCalls.lazySet(
        slot, new MethodCall(System.currentTimeMillis(), methodName, Thread.currentThread().getName()));
  }

  /**
   * Returns the retained events and method calls that are not older than {@code eventQueueMs}. The events are
   * not sorted: registered events come first, then method calls in call order.
   *
   * @return a new list of events
   */
  public List<ImportantEvent> getEvents() {
    if (!this.isEnabled) {
      return new ArrayList<>();
    }

    removeExpiredEvents();
    final List<ImportantEvent> result = new ArrayList<>(this.events);

    final int historySize = this.methodCalls.length();
    if (historySize > 0) {
      final long oldestMs = System.currentTimeMillis() - this.eventQueueMs;
      final long count = this.methodCallCount.get();
      // Walk the ring from the oldest retained call to the newest one.
      for (long i = Math.max(0, count - historySize); i < count; i++) {
        final MethodCall call = this.methodCalls.get((int) (i % historySize));
        if (call != null && call.epochMs >= oldestMs) {
          result.add(new ImportantEvent(
              Instant.ofEpochMilli(call.epochMs),
              Messages.get("DefaultConnectionPlugin.executingMethod", new Object[] {call.methodName}),
              call.threadName));
        }
      }
    }

    return result;
  }

  private void addEvent(final ImportantEvent event) {
    this.events.add(event);
    // Keep at most maxEvents: once the limit is exceeded, drop the oldest event.
    if (this.eventCount.incrementAndGet() > this.maxEvents && this.events.poll() != null) {
      this.eventCount.decrementAndGet();
    }
  }

  private void removeExpiredEvents() {
    if (!this.isEnabled) {
      return;
    }

    final Instant current = Instant.now();
    while (true) {
      final ImportantEvent head = events.peek();
      if (head == null || Duration.between(head.timestamp, current).toMillis() <= this.eventQueueMs) {
        break;
      }
      if (events.poll() != null) {
        eventCount.decrementAndGet();
      }
    }
  }

  public static class ImportantEvent {
    public final Instant timestamp;
    public final String description;
    public final String threadName;

    ImportantEvent(Instant timestamp, String description) {
      this(timestamp, description, Thread.currentThread().getName());
    }

    ImportantEvent(Instant timestamp, String description, String threadName) {
      this.timestamp = timestamp;
      this.description = description;
      this.threadName = threadName;
    }
  }

  private static final class MethodCall {
    final long epochMs;
    final String methodName;
    final String threadName;

    MethodCall(final long epochMs, final String methodName, final String threadName) {
      this.epochMs = epochMs;
      this.methodName = methodName;
      this.threadName = threadName;
    }
  }
}
