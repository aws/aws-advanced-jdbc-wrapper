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

package software.amazon.jdbc.util.storage;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A container class that holds a cache value together with the time at which the value should be considered expired.
 */
public class CacheItem<V> {
  protected final @NonNull V item;
  protected long expirationTimeNanos;

  /**
   * Constructs a CacheItem.
   *
   * @param item                the item value.
   * @param expirationTimeNanos the time at which the CacheItem should be considered expired.
   */
  protected CacheItem(final @NonNull V item, final long expirationTimeNanos) {
    this.item = item;
    this.expirationTimeNanos = expirationTimeNanos;
  }

  /**
   * Indicates whether this item is expired.
   *
   * @return true if this item is expired, otherwise returns false.
   */
  protected boolean isExpired() {
    return System.currentTimeMillis() > expirationTimeNanos;
  }

  /**
   * Renews a cache item's expiration time.
   */
  protected void extendExpiration(long timeToLiveNanos) {
    this.expirationTimeNanos = System.nanoTime() + timeToLiveNanos;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + item.hashCode();
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CacheItem<?> other = (CacheItem<?>) obj;
    return item.equals(other.item);
  }

  @Override
  public String toString() {
    return "CacheItem [item=" + item + ", expirationTimeNanos=" + expirationTimeNanos + "]";
  }
}
