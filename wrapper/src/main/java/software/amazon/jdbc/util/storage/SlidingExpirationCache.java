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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class SlidingExpirationCache<K, V> {

  protected final Map<K, CacheItem<V>> cache = new ConcurrentHashMap<>();
  protected long cleanupIntervalNanos = TimeUnit.MINUTES.toNanos(10);
  protected final AtomicLong cleanupTimeNanos = new AtomicLong(System.nanoTime() + cleanupIntervalNanos);
  protected final AtomicReference<ShouldDisposeFunc<V>> shouldDisposeFunc = new AtomicReference<>(null);
  protected final ItemDisposalFunc<V> itemDisposalFunc;

  /**
   * A cache that periodically cleans up expired entries. Fetching an expired entry marks that entry
   * as not-expired and renews its expiration time.
   */
  public SlidingExpirationCache() {
    this.shouldDisposeFunc.set(null);
    this.itemDisposalFunc = null;
  }

  /**
   * A cache that periodically cleans up expired entries. Fetching an expired entry marks that entry
   * as not-expired and renews its expiration time.
   *
   * @param shouldDisposeFunc a function defining the conditions under which an expired entry should
   *                          be cleaned up when we hit the cleanup time
   * @param itemDisposalFunc  a function that will be called on any item that meets the cleanup
   *                          criteria at cleanup time. The criteria for cleanup is that the item
   *                          is both expired and marked for cleanup via a call to
   *                          shouldDisposeFunc.
   */
  public SlidingExpirationCache(
      final ShouldDisposeFunc<V> shouldDisposeFunc,
      final ItemDisposalFunc<V> itemDisposalFunc) {
    this.shouldDisposeFunc.set(shouldDisposeFunc);
    this.itemDisposalFunc = itemDisposalFunc;
  }

  public SlidingExpirationCache(
      final ShouldDisposeFunc<V> shouldDisposeFunc,
      final ItemDisposalFunc<V> itemDisposalFunc,
      final long cleanupIntervalNanos) {
    this.shouldDisposeFunc.set(shouldDisposeFunc);
    this.itemDisposalFunc = itemDisposalFunc;
    this.cleanupIntervalNanos = cleanupIntervalNanos;
  }

  public void setShouldDisposeFunc(final ShouldDisposeFunc<V> shouldDisposeFunc) {
    this.shouldDisposeFunc.set(shouldDisposeFunc);
  }

  /**
   * In addition to performing the logic defined by {@link Map#computeIfAbsent}, cleans up expired
   * entries if we have hit cleanup time. If an expired entry is requested and we have not hit
   * cleanup time or {@link ShouldDisposeFunc} indicated the entry should not be closed, the entry
   * will be marked as non-expired.
   *
   * @param key                the key with which the specified value is to be associated
   * @param mappingFunction    the function to compute a value
   * @param itemExpirationNano the expiration time of the new or renewed entry
   * @return the current (existing or computed) value associated with the specified key, or null if
   *     the computed value is null
   */
  public V computeIfAbsent(
      final K key,
      Function<? super K, ? extends V> mappingFunction,
      final long itemExpirationNano) {

    cleanUp();
    final CacheItem<V> cacheItem = cache.computeIfAbsent(
        key,
        k -> new CacheItem<>(
            mappingFunction.apply(k),
            System.nanoTime() + itemExpirationNano,
            this.shouldDisposeFunc.get()));
    cacheItem.extendExpiration(itemExpirationNano);
    return cacheItem.item;
  }

  public V put(
      final K key,
      final V value,
      final long itemExpirationNano) {
    cleanUp();
    final CacheItem<V> cacheItem = cache.put(
        key, new CacheItem<>(value, System.nanoTime() + itemExpirationNano));
    if (cacheItem == null) {
      return null;
    }

    cacheItem.extendExpiration(itemExpirationNano);
    return cacheItem.item;
  }

  public V get(final K key, final long itemExpirationNano) {
    cleanUp();
    final CacheItem<V> cacheItem = cache.get(key);
    if (cacheItem == null) {
      return null;
    }

    cacheItem.extendExpiration(itemExpirationNano);
    return cacheItem.item;
  }

  /**
   * Cleanup expired entries if we have hit the cleanup time, then remove and dispose the value
   * associated with the given key.
   *
   * @param key the key associated with the value to be removed/disposed
   */
  public void remove(final K key) {
    removeAndDispose(key);
    cleanUp();
  }

  protected void removeAndDispose(K key) {
    final CacheItem<V> cacheItem = cache.remove(key);
    if (cacheItem != null && itemDisposalFunc != null) {
      itemDisposalFunc.dispose(cacheItem.item);
    }
  }

  protected void removeIfExpired(K key) {
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
   * Remove and dispose of all entries in the cache.
   */
  public void clear() {
    for (K key : cache.keySet()) {
      removeAndDispose(key);
    }
    cache.clear();
  }

  /**
   * Get a map copy of all entries in the cache, including expired entries.
   *
   * @return a map copy of all entries in the cache, including expired entries
   */
  public Map<K, V> getEntries() {
    final Map<K, V> entries = new HashMap<>();
    for (final Map.Entry<K, CacheItem<V>> entry : this.cache.entrySet()) {
      entries.put(entry.getKey(), entry.getValue().item);
    }
    return entries;
  }

  /**
   * Get the current size of the cache, including expired entries.
   *
   * @return the current size of the cache, including expired entries.
   */
  public int size() {
    return this.cache.size();
  }

  protected void cleanUp() {
    if (this.cleanupTimeNanos.get() > System.nanoTime()) {
      return;
    }

    this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
    cache.forEach((key, value) -> removeIfExpired(key));
  }

  /**
   * Set the cleanup interval for the cache. At cleanup time, expired entries marked for cleanup via
   * {@link ShouldDisposeFunc} (if defined) are disposed.
   *
   * @param cleanupIntervalNanos the time interval defining when we should clean up expired
   *                             entries marked for cleanup, in nanoseconds
   */
  public void setCleanupIntervalNanos(long cleanupIntervalNanos) {
    this.cleanupIntervalNanos = cleanupIntervalNanos;
    this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
  }

  // For testing purposes only
  Map<K, CacheItem<V>> getCache() {
    return cache;
  }
}
