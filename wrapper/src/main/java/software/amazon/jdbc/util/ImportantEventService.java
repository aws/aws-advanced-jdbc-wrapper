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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ImportantEventService {

  private final Queue<ImportantEvent> events = new ConcurrentLinkedQueue<>();
  private final long eventQueueMs;
  private final boolean isEnabled;

  public ImportantEventService() {
    this(
        Boolean.parseBoolean(
          System.getProperty("aws.jdbc.config.exception.context.enabled", "true")),
        Long.parseLong(
            System.getProperty("aws.jdbc.config.exception.context.connection.queue.ttl", "60000")));
  }

  public ImportantEventService(final boolean isEnabled, final long eventQueueMs) {
    this.isEnabled = isEnabled;
    this.eventQueueMs = eventQueueMs;
  }

  public void clear() {
    events.clear();
  }

  public void registerEvent(Supplier<String> descriptionSupplier) {
    if (!this.isEnabled) {
      return;
    }
    removeExpiredEvents();
    events.add(new ImportantEvent(Instant.now(), descriptionSupplier.get()));
  }

  public void registerEvent(String description) {
    if (!this.isEnabled) {
      return;
    }
    removeExpiredEvents();
    events.add(new ImportantEvent(Instant.now(), description));
  }

  public List<ImportantEvent> getEvents() {
    if (!this.isEnabled) {
      return new ArrayList<>();
    }

    removeExpiredEvents();
    return new ArrayList<>(this.events);
  }

  private void removeExpiredEvents() {
    if (!this.isEnabled) {
      return;
    }

    Instant current = Instant.now();
    while (!events.isEmpty() && Duration.between(current, events.peek().timestamp).toMillis() > this.eventQueueMs) {
      events.poll();
    }
  }

  public static class ImportantEvent {
    public final Instant timestamp;
    public final String description;
    public final String threadName;

    ImportantEvent(Instant timestamp, String description) {
      this.timestamp = timestamp;
      this.description = description;
      this.threadName = Thread.currentThread().getName();
    }
  }
}
