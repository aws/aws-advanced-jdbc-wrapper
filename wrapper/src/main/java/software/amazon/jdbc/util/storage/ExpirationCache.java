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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;

/**
 * A cache that can be used to store values that expire after a configured period of time. Entries are disposed when
 * removed from the cache if an {@link ItemDisposalFunc} is defined.
 *
 * @param <K> the type of the keys in the cache.
 * @param <V> the type of the values in the cache.
 */
public class ExpirationCache<K, V> {
  private static final Logger LOGGER = Logger.getLogger(ExpirationCache.class.getName());
  protected static final long DEFAULT_TIME_TO_LIVE_NANOS = TimeUnit.MINUTES.toNanos(5);
  protected final Map<K, CacheItem> cache = new ConcurrentHashMap<>();
  protected final boolean isRenewableExpiration;
  protected final long timeToLiveNanos;
  protected final ShouldDisposeFunc<V> shouldDisposeFunc;
  protected final ItemDisposalFunc<V> itemDisposalFunc;

  public ExpirationCache() {
    this(false, DEFAULT_TIME_TO_LIVE_NANOS, null, null);
  }

  /**
   * Constructs an ExpirationCache instance.
   *
   * @param isRenewableExpiration controls whether an item's expiration should be renewed when retrieved. If the item is
   *                              expired when it is retrieved and isRenewableExpiration is true, the item's expiration
   *                              will be renewed and the item will be returned.
   * @param timeToLiveNanos       the duration that the item should sit in the cache before being considered expired, in
   *                              nanoseconds.
   * @param shouldDisposeFunc     a function defining the conditions under which an expired entry should be cleaned up.
   *                              If null is passed, the entry will always be cleaned up if it is expired.
   * @param itemDisposalFunc      a function defining how to dispose of an item when it is removed. If null is passed,
   *                              the item will be removed without performing any additional operations.
   */
  public ExpirationCache(
      final boolean isRenewableExpiration,
      final long timeToLiveNanos,
      final @Nullable ShouldDisposeFunc<V> shouldDisposeFunc,
      final @Nullable ItemDisposalFunc<V> itemDisposalFunc) {
    this.isRenewableExpiration = isRenewableExpiration;
    this.timeToLiveNanos = timeToLiveNanos;
    this.shouldDisposeFunc = shouldDisposeFunc;
    this.itemDisposalFunc = itemDisposalFunc;
  }

  /**
   * Stores the given value at the given key.
   *
   * @param key   the key at which the value should be stored.
   * @param value the value to store.
   * @return the previous value stored at the given key, or null if there was no previous value. If there was a previous
   *     value it will also be disposed.
   */
  public @Nullable V put(
      final K key,
      final V value) {
    final CacheItem cacheItem =
        cache.put(key, new CacheItem(value, System.nanoTime() + this.timeToLiveNanos));
    if (cacheItem == null) {
      return null;
    }

    // cacheItem is the previous value associated with the key.
    if (this.itemDisposalFunc != null) {
      this.itemDisposalFunc.dispose(cacheItem.item);
    }

    return cacheItem.item;
  }

  /**
   * If a value does not exist for the given key or the existing value is expired and non-renewable, stores the value
   * returned by the given mapping function, unless the function returns null, in which case the key will be removed.
   *
   * @param key             the key for the new or existing value.
   * @param mappingFunction the function to call to compute a new value.
   * @return the current (existing or computed) value associated with the specified key, or null if the computed value
   *     is null.
   */
  public @Nullable V computeIfAbsent(
      final K key,
      Function<? super K, ? extends V> mappingFunction) {
    // A list is used to store the cached item for later disposal since lambdas require references to outer variables
    // to be final. This allows us to dispose of the item after it has been removed and the cache has been unlocked,
    // which is important because the disposal function may be long-running.
    final List<V> toDisposeList = new ArrayList<>(1);
    final CacheItem cacheItem = cache.compute(
        key,
        (k, valueItem) -> {
          if (valueItem == null) {
            // The key is absent; compute and store the new value.
            return new CacheItem(
                mappingFunction.apply(k),
                System.nanoTime() + this.timeToLiveNanos);
          }

          if (valueItem.shouldCleanup() && !this.isRenewableExpiration) {
            // The existing value is expired and non-renewable. Mark it for disposal and store the new value.
            toDisposeList.add(valueItem.item);
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

    if (this.itemDisposalFunc != null && !toDisposeList.isEmpty()) {
      this.itemDisposalFunc.dispose(toDisposeList.get(0));
    }

    return cacheItem.item;
  }

  /**
   * Retrieves the value stored at the given key.
   *
   * @param key the key for the value.
   * @return the value stored at the given key, or null if there is no existing value or the existing value is expired.
   */
  public @Nullable V get(final K key) {
    final CacheItem cacheItem = cache.get(key);
    if (cacheItem == null) {
      return null;
    }

    if (this.isRenewableExpiration) {
      cacheItem.extendExpiration();
    } else if (cacheItem.shouldCleanup()) {
      return null;
    }

    return cacheItem.item;
  }

  /**
   * Indicates whether a non-expired value is stored at the given key.
   *
   * @param key the key for the value.
   * @return true if there is a non-expired value stored at the given key, otherwise returns false.
   */
  public boolean exists(final K key) {
    final CacheItem cacheItem = cache.get(key);
    return cacheItem != null && !cacheItem.shouldCleanup();
  }

  /**
   * Removes and disposes of the value stored at the given key.
   *
   * @param key the key associated with the value to be removed and disposed.
   * @return the value removed from the cache, or null if the key does not exist in the cache. If the value was expired,
   *     it will still be returned.
   */
  public @Nullable V remove(final K key) {
    return removeAndDispose(key);
  }

  protected @Nullable V removeAndDispose(K key) {
    final CacheItem cacheItem = cache.remove(key);
    if (cacheItem == null) {
      return null;
    }

    if (itemDisposalFunc != null) {
      itemDisposalFunc.dispose(cacheItem.item);
    }

    return cacheItem.item;
  }

  /**
   * Removes and disposes of all expired entries in the cache.
   */
  public void removeExpiredEntries() {
    cache.forEach((key, value) -> {
      try {
        removeIfExpired(key);
      } catch (Exception ex) {
        LOGGER.fine(Messages.get("ExpirationCache.exceptionWhileRemovingEntry", new Object[] {key, value, ex}));
      }
    });
  }

  /**
   * Removes and disposes of the item stored at the given key if it is expired and the {@link ShouldDisposeFunc}
   * (if defined) returns true for the item. Otherwise, does nothing.
   *
   * @param key the key for the value to check for removal.
   */
  public void removeIfExpired(K key) {
    // A list is used to store the cached item for later disposal since lambdas require references to outer variables
    // to be final. This allows us to dispose of the item after it has been removed and the cache has been unlocked,
    // which is important because the disposal function may be long-running.
    final List<V> itemList = new ArrayList<>(1);
    cache.computeIfPresent(key, (k, cacheItem) -> {
      if (cacheItem.shouldCleanup()) {
        itemList.add(cacheItem.item);
        // Removes the item from the cache map.
        return null;
      }

      return cacheItem;
    });

    if (itemList.isEmpty()) {
      return;
    }

    V item = itemList.get(0);
    if (item != null && itemDisposalFunc != null) {
      itemDisposalFunc.dispose(item);
    }
  }

  /**
   * Removes and disposes of all entries in the cache.
   */
  public void clear() {
    for (K key : cache.keySet()) {
      removeAndDispose(key);
    }
    cache.clear();
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
   * Gets the current size of the cache, including expired entries.
   *
   * @return the current size of the cache, including expired entries.
   */
  public int size() {
    return this.cache.size();
  }

  /**
   * A container class that holds a cache value together with the time at which the value should be considered expired.
   */
  protected class CacheItem {
    private final V item;
    private long expirationTimeNanos;

    /**
     * Constructs a CacheItem.
     *
     * @param item                the item value.
     * @param expirationTimeNanos the time at which a CacheItem should be considered expired.
     */
    protected CacheItem(final V item, final long expirationTimeNanos) {
      this.item = item;
      this.expirationTimeNanos = expirationTimeNanos;
    }

    /**
     * Determines if a cache item should be cleaned up. An item should be cleaned up if it has past its expiration time
     * and the {@link ShouldDisposeFunc} (if defined) indicates that it should be cleaned up.
     *
     * @return true if the cache item should be cleaned up. Otherwise, returns false.
     */
    protected boolean shouldCleanup() {
      final boolean isExpired = this.expirationTimeNanos != 0 && System.nanoTime() > this.expirationTimeNanos;
      if (shouldDisposeFunc != null) {
        return isExpired && shouldDisposeFunc.shouldDispose(this.item);
      }
      return isExpired;
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
      result = prime * result + ((item == null) ? 0 : item.hashCode());
      return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

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
