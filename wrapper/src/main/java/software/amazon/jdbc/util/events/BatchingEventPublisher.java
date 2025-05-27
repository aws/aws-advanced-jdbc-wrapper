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
  protected static final ScheduledExecutorService publishingExecutor =
      ExecutorFactory.newSingleThreadScheduledThreadExecutor("bep");

  public BatchingEventPublisher() {
    this(DEFAULT_MESSAGE_INTERVAL_NANOS);
  }

  /**
   * Constructs a PeriodicEventPublisher instance and submits a thread to periodically send message batches.
   *
   * @param messageIntervalNanos the rate at which messages batches should be sent, in nanoseconds.
   */
  public BatchingEventPublisher(long messageIntervalNanos) {
    initPublishingThread(messageIntervalNanos);
  }

  protected void initPublishingThread(long messageIntervalNanos) {
    publishingExecutor.scheduleAtFixedRate(
        this::sendMessages, messageIntervalNanos, messageIntervalNanos, TimeUnit.NANOSECONDS);
  }

  protected void sendMessages() {
    Iterator<Event> iterator = eventMessages.iterator();
    while (iterator.hasNext()) {
      Event event = iterator.next();
      iterator.remove();
      Set<EventSubscriber> subscribers = subscribersMap.get(event.getClass());
      if (subscribers == null) {
        continue;
      }

      for (EventSubscriber subscriber : subscribers) {
        subscriber.processEvent(event);
      }
    }
  }

  @Override
  public void subscribe(EventSubscriber subscriber, Set<Class<? extends Event>> eventClasses) {
    for (Class<? extends Event> eventClass : eventClasses) {
      // The subscriber collection is a weakly referenced set so that we avoid garbage collection issues.
      subscribersMap.computeIfAbsent(
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
    eventMessages.add(event);
  }
}
