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

import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class defining a data invalidation event. The class specifies the class of the cached data that
 * is no longer valid and the key it is stored under. It is the counterpart of {@link
 * DataAccessEvent}: where that event reports that cached data was read, this one reports that cached
 * data must be discarded.
 *
 * <p>Publish this when the source of a cached item has gone away, so that whoever holds the item
 * drops it instead of serving it to the next reader. A typical case is a monitor being torn down: the
 * data it collected is keyed by an identifier that a replacement monitor will reuse, so leaving it
 * behind would hand the replacement a view of an environment that no longer exists.
 */
public class DataInvalidationEvent implements Event {
  protected final @NonNull Class<?> dataClass;
  protected final @NonNull Object key;

  /**
   * Constructor for a DataInvalidationEvent.
   *
   * @param dataClass the class of the cached data that is no longer valid.
   * @param key       the key for the cached data that is no longer valid.
   */
  public DataInvalidationEvent(@NonNull Class<?> dataClass, @NonNull Object key) {
    this.dataClass = dataClass;
    this.key = key;
  }

  public @NonNull Class<?> getDataClass() {
    return dataClass;
  }

  public @NonNull Object getKey() {
    return key;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    DataInvalidationEvent event = (DataInvalidationEvent) obj;
    return Objects.equals(this.dataClass, event.dataClass)
        && Objects.equals(this.key, event.key);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + this.dataClass.hashCode();
    result = prime * result + this.key.hashCode();
    return result;
  }

  /**
   * Invalidation is delivered immediately rather than batched. Batched delivery would leave the stale
   * item readable for up to the publisher's message interval, which defeats the purpose of the event.
   *
   * @return always true.
   */
  @Override
  public boolean isImmediateDelivery() {
    return true;
  }
}
