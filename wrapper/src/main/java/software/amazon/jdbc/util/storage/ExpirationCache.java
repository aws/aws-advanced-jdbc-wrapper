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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.ItemDisposalFunc;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ShouldDisposeFunc;

public class ExpirationCache<K, V> {
  private static final Logger LOGGER =
      Logger.getLogger(ExpirationCache.class.getName());

  protected final ExecutorService cleanupThreadPool = Executors.newFixedThreadPool(1, runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    return monitoringThread;
  });

  protected final Map<K, CacheItem> cache = new ConcurrentHashMap<>();
  protected final Class<V> valueClass;
  protected final boolean isRenewableExpiration;
  protected final long cleanupIntervalNanos;
  protected final long timeToLiveNanos;
  protected final AtomicReference<ShouldDisposeFunc<V>> shouldDisposeFunc = new AtomicReference<>(null);
  protected final ItemDisposalFunc<V> itemDisposalFunc;

  public ExpirationCache(
      final Class<V> valueClass,
      final boolean isRenewableExpiration,
      final long cleanupIntervalNanos,
      final long timeToLiveNanos,
      final @Nullable ShouldDisposeFunc<V> shouldDisposeFunc,
      final @Nullable ItemDisposalFunc<V> itemDisposalFunc) {
    this.valueClass = valueClass;
    this.isRenewableExpiration = isRenewableExpiration;
    this.cleanupIntervalNanos = cleanupIntervalNanos;
    this.timeToLiveNanos = timeToLiveNanos;
    this.shouldDisposeFunc.set(shouldDisposeFunc);
    this.itemDisposalFunc = itemDisposalFunc;
    this.initCleanupThread();
  }

  protected void initCleanupThread() {
    cleanupThreadPool.submit(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          TimeUnit.NANOSECONDS.sleep(this.cleanupIntervalNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.fine(Messages.get("ExpirationCache.cleanupThreadInterrupted"));
          break;
        }

        LOGGER.finest("ExpirationCache.cleaningUpCache");
        cache.forEach((key, value) -> {
          try {
            removeIfExpired(key);
          } catch (Exception ex) {
            // ignore
          }
        });
      }
    });
    cleanupThreadPool.shutdown();
  }

  /**
   * In addition to performing the logic defined by {@link Map#computeIfAbsent}, cleans up expired
   * entries if we have hit cleanup time. If an expired entry is requested and we have not hit
   * cleanup time or {@link ShouldDisposeFunc} indicated the entry should not be closed, the entry
   * will be marked as non-expired.
   *
   * @param key                the key with which the specified value is to be associated
   * @param mappingFunction    the function to compute a value
   * @return the current (existing or computed) value associated with the specified key, or null if
   *     the computed value is null.
   */
  public @Nullable V computeIfAbsent(
      final K key,
      Function<? super K, ? extends V> mappingFunction) {
    final CacheItem cacheItem = cache.computeIfAbsent(
        key,
        k -> new CacheItem(
            mappingFunction.apply(k),
            System.nanoTime() + this.timeToLiveNanos));

    if (this.isRenewableExpiration) {
      cacheItem.extendExpiration(this.timeToLiveNanos);
    } else if (cacheItem.shouldCleanup()) {
      return null;
    }

    return cacheItem.item;
  }

  public @Nullable V put(
      final K key,
      final V value) {
    final CacheItem
        cacheItem = cache.put(key, new CacheItem(value, System.nanoTime() + this.timeToLiveNanos));
    if (cacheItem == null) {
      return null;
    }

    // cacheItem is the previous value associated with the key. Since it has now been replaced with the new value,
    // its expiration does not need to be extended.
    if (cacheItem.shouldCleanup()) {
      return null;
    }

    return cacheItem.item;
  }

  public @Nullable V get(final K key) {
    final CacheItem cacheItem = cache.get(key);
    if (cacheItem == null) {
      return null;
    }

    if (this.isRenewableExpiration)  {
      cacheItem.extendExpiration(this.timeToLiveNanos);
    } else if (cacheItem.shouldCleanup()) {
      return null;
    }

    return cacheItem.item;
  }

  public boolean exists(final K key) {
    final CacheItem cacheItem = cache.get(key);
    return cacheItem != null && !cacheItem.shouldCleanup();
  }

  /**
   * Cleanup expired entries if we have hit the cleanup time, then remove and dispose the value
   * associated with the given key.
   *
   * @param key the key associated with the value to be removed/disposed
   * @return the value removed from the cache. If the value was expired, it will still be returned.
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
    for (final Map.Entry<K, CacheItem> entry : this.cache.entrySet()) {
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

  public Class<V> getValueClass() {
    return this.valueClass;
  }

  protected class CacheItem {
    private final V item;
    private long expirationTimeNanos;

    /**
     * CacheItem constructor.
     *
     * @param item               the item value
     * @param expirationTimeNanos the amount of time before a CacheItem should be marked as expired.
     */
    public CacheItem(final V item, final long expirationTimeNanos) {
      this.item = item;
      this.expirationTimeNanos = expirationTimeNanos;
    }

    /**
     * Determines if a cache item should be cleaned up. An item should be cleaned up if it has past
     * its expiration time and {@link ShouldDisposeFunc} (if defined) indicates that it should be
     * cleaned up.
     *
     * @return true if the cache item should be cleaned up at cleanup time. Otherwise, returns
     *     false.
     */
    boolean shouldCleanup() {
      final boolean isExpired = this.expirationTimeNanos != 0 && System.nanoTime() > this.expirationTimeNanos;
      final ShouldDisposeFunc<V> tempShouldDisposeFunc = shouldDisposeFunc.get();
      if (tempShouldDisposeFunc != null) {
        return isExpired && tempShouldDisposeFunc.shouldDispose(this.item);
      }
      return isExpired;
    }

    /**
     * Renew a cache item's expiration time and return the value.
     *
     * @param timeToLiveNanos the new expiration duration for the item
     */
    public void extendExpiration(final long timeToLiveNanos) {
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
