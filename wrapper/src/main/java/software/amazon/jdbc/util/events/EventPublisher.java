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

import java.util.Set;

/**
 * An event publisher that publishes events to subscribers. Subscribers can specify which types of events they would
 * like to receive.
 */
public interface EventPublisher {
  /**
   * Registers the given subscriber for the given event classes.
   *
   * @param subscriber   the subscriber to be notified when the given event classes occur.
   * @param eventClasses the classes of events that the subscriber should be notified of.
   */
  void subscribe(EventSubscriber subscriber, Set<Class<? extends Event>> eventClasses);

  /**
   * Unsubscribes the given subscriber from the given event classes.
   *
   * @param subscriber   the subscriber to unsubscribe from the given event classes.
   * @param eventClasses the classes of events that the subscriber wants to unsubscribe from.
   */
  void unsubscribe(EventSubscriber subscriber, Set<Class<? extends Event>> eventClasses);

  /**
   * Publishes an event. All subscribers to the given event class will be notified of the event.
   *
   * @param event the event to publish.
   */
  void publish(Event event);
}
