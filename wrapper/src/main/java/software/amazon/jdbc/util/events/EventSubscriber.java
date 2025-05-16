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
 * An event subscriber. Subscribers can subscribe to a publisher's events using
 * {@link EventPublisher#subscribe(EventSubscriber, Set)}. Subscribers will typically be stored in a
 * {@link java.util.HashSet} to prevent duplicate subscriptions, so classes implementing this interface should consider
 * whether they need to override {@link Object#equals(Object)} and {@link Object#hashCode()}.
 *
 * @see EventPublisher
 */
public interface EventSubscriber {
  /**
   * Processes an event. This method will only be called on this subscriber if it has subscribed to the event class via
   * {@link EventPublisher#subscribe}.
   *
   * @param event the event to process.
   */
  void processEvent(Event event);
}
