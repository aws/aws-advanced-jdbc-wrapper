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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExternallyManagedCache<K, V> {
  private static final Logger LOGGER = Logger.getLogger(ExpirationCache.class.getName());
  protected final Map<K, CacheItem> cache = new ConcurrentHashMap<>();
  protected final boolean isRenewableExpiration;
  protected final long timeToLiveNanos;

  public ExternallyManagedCache(boolean isRenewableExpiration, long timeToLiveNanos) {
    this.isRenewableExpiration = isRenewableExpiration;
    this.timeToLiveNanos = timeToLiveNanos;
  }

  public @Nullable V put(@NonNull K key, @NonNull V value) {
    CacheItem cacheItem = this.cache.put(key, new CacheItem(value, timeToLiveNanos));
    if (cacheItem == null) {
      return null;
    }

    return cacheItem.item;
  }

  public @NonNull V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    final CacheItem cacheItem = cache.compute(
        key,
        (k, valueItem) -> {
          if (valueItem == null) {
            // The key is absent; compute and store the new value.
            return new CacheItem(
                mappingFunction.apply(k),
                System.nanoTime() + this.timeToLiveNanos);
          }

          // The existing value is non-expired or renewable. Keep the existing value.
          if (this.isRenewableExpiration) {
            valueItem.extendExpiration();
          }

          return valueItem;
        });

    return cacheItem.item;
  }

  public @Nullable V remove(K key) {
    CacheItem cacheItem = cache.remove(key);
    if (cacheItem == null) {
      return null;
    }

    return cacheItem.item;
  }

  public @Nullable V removeIf(K key, Predicate<V> predicate) {
    // The function only returns a value if it was removed. A list is used to store the removed item since lambdas
    // require references to outer variables to be final.
    final List<V> removedItemList = new ArrayList<>(1);
    cache.computeIfPresent(
        key,
        (k, valueItem) -> {
          if (predicate.test(valueItem.item)) {
            removedItemList.add(valueItem.item);
            return null;
          }

          return valueItem;
        });

    if (removedItemList.isEmpty()) {
      return null;
    } else {
      return removedItemList.get(0);
    }
  }

  public @Nullable V removeExpiredIf(K key, Predicate<V> predicate) {
    // The function only returns a value if it was removed. A list is used to store the removed item since lambdas
    // require references to outer variables to be final.
    final List<V> removedItemList = new ArrayList<>(1);
    cache.computeIfPresent(
        key,
        (k, valueItem) -> {
          if (valueItem.isExpired() && predicate.test(valueItem.item)) {
            removedItemList.add(valueItem.item);
            return null;
          }

          return valueItem;
        });

    if (removedItemList.isEmpty()) {
      return null;
    } else {
      return removedItemList.get(0);
    }
  }

  public Map<K, V> getEntries() {
    final Map<K, V> entries = new HashMap<>();
    for (final Map.Entry<K, CacheItem> entry : this.cache.entrySet()) {
      entries.put(entry.getKey(), entry.getValue().item);
    }

    return entries;
  }

  protected class CacheItem {
    private final @NonNull V item;
    private long expirationTimeNanos;

    /**
     * CacheItem constructor.
     *
     * @param item                the item value
     * @param expirationTimeNanos the amount of time before a CacheItem should be marked as expired.
     */
    protected CacheItem(@NonNull final V item, final long expirationTimeNanos) {
      this.item = item;
      this.expirationTimeNanos = expirationTimeNanos;
    }

    protected boolean isExpired() {
      return System.nanoTime() >= expirationTimeNanos;
    }

    /**
     * Renews a cache item's expiration time.
     */
    protected void extendExpiration() {
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
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      // First check null and type (use instanceof for correct type checking)
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      CacheItem other = (CacheItem) obj;
      return this.item.equals(other.item) && this.expirationTimeNanos == other.expirationTimeNanos;
    }

    @Override
    public String toString() {
      return "CacheItem [item=" + item + ", expirationTime=" + expirationTimeNanos + "]";
    }
  }
}
