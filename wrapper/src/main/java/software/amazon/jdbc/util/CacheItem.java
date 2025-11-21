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

import java.util.Objects;

public class CacheItem<V> {

  final V item;
  final long expirationTime;

  public CacheItem(final V item, final long expirationTime) {
    this.item = item;
    this.expirationTime = expirationTime;
  }

  public boolean isExpired() {
    if (expirationTime <= 0) {
      // No expiration time.
      return false;
    }
    return System.nanoTime() > expirationTime;
  }

  public V get() {
    return get(false);
  }

  public V get(final boolean returnExpired) {
    return (this.isExpired() && !returnExpired) ? null : item;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(item);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CacheItem)) {
      return false;
    }
    CacheItem<?> other = (CacheItem<?>) obj;
    return Objects.equals(this.item, other.item);
  }

  @Override
  public String toString() {
    return "CacheItem [item=" + item + ", expirationTime=" + expirationTime + "]";
  }
}
