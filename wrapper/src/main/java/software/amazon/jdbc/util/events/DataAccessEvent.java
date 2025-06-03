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

/**
 * A class defining a data access event. The class specifies the class of the data that was accessed and the key for the
 * data.
 */
public class DataAccessEvent implements Event {
  protected @NonNull Class<?> dataClass;
  protected @NonNull Object key;

  /**
   * Constructor for a DataAccessEvent.
   *
   * @param dataClass the class of the data that was accessed.
   * @param key       the key for the data that was accessed.
   */
  public DataAccessEvent(@NonNull Class<?> dataClass, @NonNull Object key) {
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    DataAccessEvent event = (DataAccessEvent) obj;
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
}
