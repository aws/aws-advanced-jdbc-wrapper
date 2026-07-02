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

package software.amazon.jdbc.util.events;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.ResourceLock;

/**
 * An event publisher that periodically publishes a batch of all unique events encountered during the latest time
 * interval. Batches do not contain duplicate events; if the current batch receives a duplicate, it will not be
 * added to the batch and the original event will only be published once, when the entire batch is published.
 */
public class BatchingEventPublisher implements EventPublisher {
  protected static final long DEFAULT_MESSAGE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
  protected final Map<Class<? extends Event>, Set<EventSubscriber>> subscribersMap = new ConcurrentHashMap<>();
  // ConcurrentHashMap.newKeySet() is the recommended way to get a concurrent set. A set is used to prevent duplicate
  // event messages from being sent in the same message batch.
  protected final Set<Event> eventMessages = ConcurrentHashMap.newKeySet();
  private static final ResourceLock lock = new ResourceLock();
  protected static volatile ScheduledExecutorService publishingExecutor =
      ExecutorFactory.newSingleThreadScheduledThreadExecutor("bep");
  protected final long messageIntervalNanos;

  public BatchingEventPublisher() {
    this(DEFAULT_MESSAGE_INTERVAL_NANOS);
  }

  /**
   * Constructs a PeriodicEventPublisher instance and submits a thread to periodically send message batches.
   *
   * @param messageIntervalNanos the rate at which messages batches should be sent, in nanoseconds.
   */
  // initPublishingThread only schedules a task on the shared publishingExecutor; the task runs
  // after construction completes. The checker cannot see this, hence the localized suppression.
  @SuppressWarnings("method.invocation")
  public BatchingEventPublisher(long messageIntervalNanos) {
    this.messageIntervalNanos = messageIntervalNanos;
    initPublishingThread(messageIntervalNanos);
  }

  protected void initPublishingThread(long messageIntervalNanos) {
    getOrCreatePublishingExecutor().scheduleAtFixedRate(
        this::sendMessages, messageIntervalNanos, messageIntervalNanos, TimeUnit.NANOSECONDS);
  }

  private static ScheduledExecutorService getOrCreatePublishingExecutor() {
    ScheduledExecutorService executor = publishingExecutor;
    if (executor.isShutdown()) {
      try (ResourceLock ignored = lock.obtain()) {
        executor = publishingExecutor;
        if (executor.isShutdown()) {
          executor = ExecutorFactory.newSingleThreadScheduledThreadExecutor("bep");
          publishingExecutor = executor;
        }
      }
    }
    return executor;
  }

  protected void sendMessages() {
    Iterator<Event> iterator = eventMessages.iterator();
    while (iterator.hasNext()) {
      Event event = iterator.next();
      iterator.remove();
      this.deliverEvent(event);
    }
  }

  protected void deliverEvent(Event event) {
    Set<EventSubscriber> subscribers = this.subscribersMap.get(event.getClass());
    if (subscribers == null) {
      return;
    }

    for (EventSubscriber subscriber : subscribers) {
      subscriber.processEvent(event);
    }
  }

  @Override
  public void subscribe(EventSubscriber subscriber, Set<Class<? extends Event>> eventClasses) {
    for (Class<? extends Event> eventClass : eventClasses) {
      // The subscriber collection is a weakly referenced set so that we avoid garbage collection issues.
      this.subscribersMap.computeIfAbsent(
          eventClass, (k) -> Collections.newSetFromMap(new WeakHashMap<>())).add(subscriber);
    }
  }

  @Override
  public void unsubscribe(EventSubscriber subscriber, Set<Class<? extends Event>> eventClasses) {
    for (Class<? extends Event> eventClass : eventClasses) {
      subscribersMap.computeIfPresent(eventClass, (k, v) -> {
        v.remove(subscriber);
        return v.isEmpty() ? null : v;
      });
    }
  }

  @Override
  public void publish(Event event) {
    if (event.isImmediateDelivery()) {
      this.deliverEvent(event);
    } else {
      ensureExecutorRunning();
      eventMessages.add(event);
    }
  }

  private void ensureExecutorRunning() {
    if (publishingExecutor.isShutdown()) {
      initPublishingThread(this.messageIntervalNanos);
    }
  }

  public static void releaseResources() {
    publishingExecutor.shutdownNow();
  }
}
