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

/**
 * A cache with expiration functionality that does not automatically remove expired entries. The removal of expired
 * entries is instead handled by an external class. This class is similar to {@link ExpirationCache}, but allows users
 * to manually renew item expiration and provides more control over the conditions in which items are removed. Disposal
 * of removed items should be handled outside of this class.
 *
 * @param <K> the type of the keys in the cache.
 * @param <V> the type of the values in the cache.
 */
public class ExternallyManagedCache<K, V> {
  private static final Logger LOGGER = Logger.getLogger(ExpirationCache.class.getName());
  protected final Map<K, CacheItem> cache = new ConcurrentHashMap<>();
  protected final long timeToLiveNanos;

  /**
   * Constructs an externally managed cache.
   *
   * @param timeToLiveNanos       the duration that the item should sit in the cache before being considered expired, in
   *                              nanoseconds.
   */
  public ExternallyManagedCache(long timeToLiveNanos) {
    this.timeToLiveNanos = timeToLiveNanos;
  }

  /**
   * Stores the given value in the cache at the given key.
   *
   * @param key   the key for the value.
   * @param value the value to store.
   * @return the previous value stored at the key, or null if there was no value stored at the key.
   */
  public @Nullable V put(@NonNull K key, @NonNull V value) {
    CacheItem cacheItem = this.cache.put(key, new CacheItem(value, System.nanoTime() + timeToLiveNanos));
    if (cacheItem == null) {
      return null;
    }

    return cacheItem.item;
  }

  /**
   * If a value does not exist for the given key, stores the value returned by the given mapping function, unless the
   * function returns null, in which case the key will be removed. If the
   *
   * @param key             the key for the new or existing value.
   * @param mappingFunction the function to call to compute a new value.
   * @return the current (existing or computed) value associated with the specified key, or null if the computed value
   *     is null.
   */
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

          valueItem.extendExpiration();
          return valueItem;
        });

    return cacheItem.item;
  }

  /**
   * Extends the expiration of the item stored at the given key, if it exists.
   *
   * @param key the key for the value whose expiration should be extended.
   */
  public void extendExpiration(K key) {
    final CacheItem cacheItem = cache.get(key);
    // TODO: should we log if the key doesn't exist?

    if (cacheItem != null) {
      cacheItem.extendExpiration();
    }
  }

  /**
   * Removes the value stored at the given key from the cache.
   *
   * @param key the key for the value to be removed.
   * @return the value that was removed, or null if the key did not exist.
   */
  public @Nullable V remove(K key) {
    CacheItem cacheItem = cache.remove(key);
    if (cacheItem == null) {
      return null;
    }

    return cacheItem.item;
  }

  /**
   * Removes the value stored at the given key if the given predicate returns true for the value. Otherwise, does
   * nothing.
   *
   * @param key       the key for the value to assess for removal.
   * @param predicate a predicate lambda that defines the condition under which the value should be removed.
   * @return the removed value, or null if no value was removed.
   */
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

  /**
   * Removes the value stored at the given key if it is expired and the given predicate returns true for the value.
   * Otherwise, does nothing.
   *
   * @param key       the key for the value to assess for removal.
   * @param predicate a predicate lambda that defines the condition under which the value should be removed if it is
   *                  also expired.
   * @return the removed value, or null if no value was removed.
   */
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

  /**
   * Gets a map copy of all entries in the cache, including expired entries.
   *
   * @return a map copy of all entries in the cache, including expired entries.
   */
  public Map<K, V> getEntries() {
    final Map<K, V> entries = new HashMap<>();
    for (final Map.Entry<K, CacheItem> entry : this.cache.entrySet()) {
      entries.put(entry.getKey(), entry.getValue().item);
    }

    return entries;
  }

  /**
   * A container class that holds a cache value together with the time at which the value should be considered expired.
   */
  protected class CacheItem {
    private final @NonNull V item;
    private long expirationTimeNanos;

    /**
     * Constructs a CacheItem.
     *
     * @param item                the item value.
     * @param expirationTimeNanos the amount of time before a CacheItem should be marked as expired.
     */
    protected CacheItem(@NonNull final V item, final long expirationTimeNanos) {
      this.item = item;
      this.expirationTimeNanos = expirationTimeNanos;
    }

    /**
     * Indicates whether this item is expired.
     *
     * @return true if this item is expired, otherwise returns false.
     */
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
