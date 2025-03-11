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

public class DataAccessEvent implements Event {
  protected @NonNull String dataCategory;
  protected @NonNull Class<?> dataClass;
  protected @NonNull Object key;

  public DataAccessEvent(@NonNull String dataCategory, @NonNull Class<?> dataClass, @NonNull Object key) {
    this.dataCategory = dataCategory;
    this.dataClass = dataClass;
    this.key = key;
  }

  public @NonNull String getDataCategory() {
    return dataCategory;
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
    return Objects.equals(this.dataCategory, event.dataCategory)
        && Objects.equals(this.dataClass, event.dataClass)
        && Objects.equals(this.key, event.key);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + this.dataCategory.hashCode();
    result = prime * result + this.dataClass.hashCode();
    result = prime * result + this.key.hashCode();
    return result;
  }
}
