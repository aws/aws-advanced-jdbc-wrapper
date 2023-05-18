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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class CacheMap<K, V> {

  private final Map<K, CacheItem> cache = new ConcurrentHashMap<>();
  private long cleanupIntervalNanos = TimeUnit.MINUTES.toNanos(10);
  private final AtomicLong cleanupTimeNanos = new AtomicLong(System.nanoTime() + cleanupIntervalNanos);
  private final IsItemValidFunc<V> isItemValidFunc;
  private final ItemDisposalFunc<V> itemDisposalFunc;

  public CacheMap() {
    this.isItemValidFunc = null;
    this.itemDisposalFunc = null;
  }

  public CacheMap(
      final IsItemValidFunc<V> isItemValidFunc,
      final ItemDisposalFunc<V> itemDisposalFunc) {
    this.isItemValidFunc = isItemValidFunc;
    this.itemDisposalFunc = itemDisposalFunc;
  }

  public V get(final K key) {
    final CacheItem cacheItem = cache.get(key);
    if (cacheItem == null || cacheItem.isExpired()) {
      cache.remove(key);
      if (cacheItem != null && itemDisposalFunc != null) {
        itemDisposalFunc.dispose(cacheItem.item);
      }
      return null;
    } else {
      return cacheItem.item;
    }
  }

  public V get(final K key, final V defaultItemValue, final long itemExpirationNano) {
    CacheItem cacheItem = cache.get(key);
    if (cacheItem == null || cacheItem.isExpired()) {
      cache.remove(key);
      if (cacheItem != null && itemDisposalFunc != null) {
        itemDisposalFunc.dispose(cacheItem.item);
      }
      cacheItem = new CacheItem(defaultItemValue, System.nanoTime() + itemExpirationNano);
      cache.put(key, cacheItem);
    }
    return cacheItem.item;
  }

  public V getWithExtendExpiration(final K key, final long itemExpirationNano) {
    final CacheItem cacheItem = cache.get(key);
    if (cacheItem == null || cacheItem.isExpired()) {
      cache.remove(key);
      if (cacheItem != null && itemDisposalFunc != null) {
        itemDisposalFunc.dispose(cacheItem.item);
      }
      return null;
    } else {
      return cacheItem.withExtendExpiration(itemExpirationNano).item;
    }
  }

  public void put(final K key, final V item, final long itemExpirationNano) {
    cleanUp();
    final CacheItem oldItem = cache.get(key);
    if (oldItem != null && itemDisposalFunc != null) {
      itemDisposalFunc.dispose(oldItem.item);
    }
    cache.put(key, new CacheItem(item, System.nanoTime() + itemExpirationNano));
  }

  public void putIfAbsent(final K key, final V item, final long itemExpirationNano) {
    cleanUp();
    final CacheItem currentItem = cache.get(key);
    if (currentItem == null || currentItem.isExpired()) {
      cache.remove(key);
      if (currentItem != null && itemDisposalFunc != null) {
        itemDisposalFunc.dispose(currentItem.item);
      }
    }
    cache.putIfAbsent(key, new CacheItem(item, System.nanoTime() + itemExpirationNano));
  }

  public V computeIfAbsent(
      final K key,
      Function<? super K, ? extends V> mappingFunction,
      final long itemExpirationNano) {

    cleanUp();
    final CacheItem currentItem = cache.get(key);
    if (currentItem == null || currentItem.isExpired()) {
      cache.remove(key);
      if (currentItem != null && itemDisposalFunc != null) {
        itemDisposalFunc.dispose(currentItem.item);
      }
    }
    final CacheItem cacheItem = cache.computeIfAbsent(
        key,
        k -> new CacheItem(
            mappingFunction.apply(k),
            System.nanoTime() + itemExpirationNano));
    return cacheItem.withExtendExpiration(itemExpirationNano).item;
  }

  public void remove(final K key) {
    cleanUp();
    CacheItem cacheItem = cache.remove(key);
    if (itemDisposalFunc != null) {
      itemDisposalFunc.dispose(cacheItem.item);
    }
  }

  public void clear() {
    for (K key : cache.keySet()) {
      CacheItem cacheItem = cache.remove(key);
      if (itemDisposalFunc != null) {
        itemDisposalFunc.dispose(cacheItem.item);
      }
    }
    cache.clear();
  }

  public Map<K, V> getEntries() {
    final Map<K, V> entries = new HashMap<>();
    for (final Map.Entry<K, CacheItem> entry : this.cache.entrySet()) {
      entries.put(entry.getKey(), entry.getValue().item);
    }
    return entries;
  }

  public int size() {
    return this.cache.size();
  }

  private void cleanUp() {
    if (this.cleanupTimeNanos.get() > System.nanoTime()) {
      return;
    }

    this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
    cache.forEach((key, value) -> {
      if (value != null && !value.isExpired()) {
        return;
      }

      cache.remove(key);
      if (value != null && itemDisposalFunc != null) {
        itemDisposalFunc.dispose(value.item);
      }
    });
  }

  public interface IsItemValidFunc<V> {
    boolean isValid(V item);
  }

  public interface ItemDisposalFunc<V> {
    void dispose(V item);
  }

  class CacheItem {
    private final V item;
    private long expirationTime;

    public CacheItem(final V item, final long expirationTime) {
      this.item = item;
      this.expirationTime = expirationTime;
    }

    boolean isExpired() {
      if (isItemValidFunc != null) {
        return System.nanoTime() > expirationTime && !isItemValidFunc.isValid(this.item);
      }
      return System.nanoTime() > expirationTime;
    }

    public CacheItem withExtendExpiration(final long itemExpirationNano) {
      this.expirationTime = System.nanoTime() + itemExpirationNano;
      return this;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((item == null) ? 0 : item.hashCode());
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
      final CacheItem other = (CacheItem) obj;
      if (item == null) {
        return other.item == null;
      } else {
        return item.equals(other.item);
      }
    }

    @Override
    public String toString() {
      return "CacheItem [item=" + item + ", expirationTime=" + expirationTime + "]";
    }
  }

  // For testing purposes
  void setCleanupIntervalNanos(long cleanupIntervalNanos) {
    this.cleanupIntervalNanos = cleanupIntervalNanos;
    this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
  }

  Map<K, CacheItem> getCache() {
    return cache;
  }
}
