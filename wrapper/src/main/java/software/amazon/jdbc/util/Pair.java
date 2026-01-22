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

public final class Pair<T1, T2> {

  private final T1 value1;
  private final T2 value2;

  private Pair(T1 value1, T2 value2) {
    this.value1 = value1;
    this.value2 = value2;
  }

  public T1 getValue1() {
    return this.value1;
  }

  public T2 getValue2() {
    return this.value2;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Pair)) {
      return false;
    }

    Pair other = (Pair) obj;

    return (this.value1 == null && other.value1 == null || other.value1 != null && other.value1.equals(this.value1))
        && (this.value2 == null && other.value2 == null || other.value2 != null && other.value2.equals(value2));
  }

  @Override
  public int hashCode() {
    return getClass().hashCode()
        + (this.value1 != null ? this.value1.hashCode() : 0)
        + (this.value2 != null ? this.value2.hashCode() : 0);
  }

  @Override
  public String toString() {
    return String.format("Pair(value1=%s, value2=%s)", this.value1, this.value2);
  }

  public static <T1, T2> Pair<T1, T2> create(T1 value1, T2 value2) {
    return new Pair<>(value1, value2);
  }
}
