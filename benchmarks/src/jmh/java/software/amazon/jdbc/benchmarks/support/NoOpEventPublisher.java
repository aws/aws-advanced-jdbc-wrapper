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

package software.amazon.jdbc.benchmarks.support;

import java.util.Set;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.events.EventSubscriber;

/**
 * An {@link EventPublisher} that drops everything.
 *
 * <p>Used so storage-service benchmarks measure the cache, not the event fan-out. Note that
 * {@code StorageServiceImpl.get} publishes a {@code DataAccessEvent} on every hit, so the
 * allocation of that event is still included in those measurements - only the delivery is not.
 */
public class NoOpEventPublisher implements EventPublisher {

  @Override
  public void subscribe(final EventSubscriber subscriber, final Set<Class<? extends Event>> eventClasses) {
    // no-op
  }

  @Override
  public void unsubscribe(final EventSubscriber subscriber, final Set<Class<? extends Event>> eventClasses) {
    // no-op
  }

  @Override
  public void publish(final Event event) {
    // no-op
  }
}
