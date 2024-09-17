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
import org.checkerframework.checker.nullness.qual.Nullable;

public class CacheMap<K, V> {

  protected final Map<K, CacheItem<V>> cache = new ConcurrentHashMap<>();
  protected final long cleanupIntervalNanos = TimeUnit.MINUTES.toNanos(10);
  protected final AtomicLong cleanupTimeNanos = new AtomicLong(System.nanoTime() + cleanupIntervalNanos);

  public CacheMap() {
  }

  public @Nullable V get(final K key) {
    final CacheItem<V> cacheItem = cache.computeIfPresent(key, (kk, vv) -> vv.isExpired() ? null : vv);
    return cacheItem == null ? null : cacheItem.item;
  }

  public V get(final K key, final V defaultItemValue, final long itemExpirationNano) {
    final CacheItem<V> cacheItem = cache.compute(key,
        (kk, vv) -> (vv == null || vv.isExpired())
            ? new CacheItem<>(defaultItemValue, System.nanoTime() + itemExpirationNano)
            : vv);
    return cacheItem.item;
  }

  public void put(final K key, final V item, final long itemExpirationNano) {
    cache.put(key, new CacheItem<>(item, System.nanoTime() + itemExpirationNano));
    cleanUp();
  }

  public void putIfAbsent(final K key, final V item, final long itemExpirationNano) {
    cache.putIfAbsent(key, new CacheItem<>(item, System.nanoTime() + itemExpirationNano));
    cleanUp();
  }

  public void remove(final K key) {
    cache.remove(key);
    cleanUp();
  }

  public void clear() {
    cache.clear();
  }

  public Map<K, V> getEntries() {
    final Map<K, V> entries = new HashMap<>();
    for (final Map.Entry<K, CacheItem<V>> entry : this.cache.entrySet()) {
      entries.put(entry.getKey(), entry.getValue().item);
    }
    return entries;
  }

  public int size() {
    return this.cache.size();
  }

  protected void cleanUp() {
    if (this.cleanupTimeNanos.get() < System.nanoTime()) {
      this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
      cache.forEach((key, value) -> {
        if (value == null || value.isExpired()) {
          cache.remove(key);
          if (value != null && value.item instanceof AutoCloseable) {
            try {
              ((AutoCloseable) value.item).close();
            } catch (Exception e) {
              // ignore
            }
          }
        }
      });
    }
  }

  static class CacheItem<V> {
    final V item;
    final long expirationTime;

    public CacheItem(final V item, final long expirationTime) {
      this.item = item;
      this.expirationTime = expirationTime;
    }

    boolean isExpired() {
      return System.nanoTime() > expirationTime;
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
      final CacheItem<?> other = (CacheItem<?>) obj;
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
}
