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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Expiring Cache class. This cache uses a LinkedHashMap to store values with a specific time they
 * were stored. Items that exceed the expired time will be removed when attempting to retrieve them.
 *
 * @param <K> The type of the key to store
 * @param <V> The type of the value to store
 */
public class ExpiringCache<K, V> implements Map<K, V> {

  private long expireTimeMs;
  private @Nullable OnEvictRunnable onEvictRunnable;
  private final ReentrantLock reentrantLock = new ReentrantLock();

  /** The HashMap which stores the key-value pair. */
  private final LinkedHashMap<K, Hit<V>> linkedHashMap =
      new LinkedHashMap<K, Hit<V>>(1, 0.75F, true) {

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, Hit<V>> eldest) {

          // removeEldestEntry() method is called on inserting a new entry either.
          // It makes sense to skip processing of such a new entry.
          // When this method is called with an old and expired entry, we can
          // iterate through all entries and remove all expired ones.

          if (eldest.getValue().isExpire(expireTimeMs)) {

            Iterator<Hit<V>> i = values().iterator();

            while (i.hasNext()) {
              Hit<V> hit = i.next();
              if (!hit.isExpire(expireTimeMs)) {
                continue;
              }
              i.remove();
              if (onEvictRunnable != null) {
                try {
                  onEvictRunnable.call(hit.payload);
                } catch (Exception ex) {
                  // ignore
                }
              }
            }
          }
          return false;
        }
      };

  /**
   * Expiring cache constructor.
   *
   * @param expireTimeMs The expired time in Ms
   */
  public ExpiringCache(long expireTimeMs) {
    this.expireTimeMs = expireTimeMs;
  }

  /**
   * Expiring cache constructor.
   *
   * @param expireTimeMs The expired time in Ms
   * @param onEvictRunnable A function that will be called on each evicted cache entry
   */
  public ExpiringCache(long expireTimeMs, final @Nullable OnEvictRunnable onEvictRunnable) {
    this.expireTimeMs = expireTimeMs;
    this.onEvictRunnable = onEvictRunnable;
  }

  /**
   * Mutator method for expireTimeMs. Sets the expired time for the cache.
   *
   * @param expireTimeMs The expired time in Ms
   */
  public void setExpireTime(long expireTimeMs) {
    this.expireTimeMs = expireTimeMs;
  }

  /**
   * Accessor method for expireTimeMs.
   *
   * @return Returns the time it takes for the cache to be "expired"
   */
  public long getExpireTime() {
    return this.expireTimeMs;
  }

  /**
   * Retrieves the number of items stored.
   *
   * @return The size of items stored
   */
  @Override
  public int size() {
    try {
      this.reentrantLock.lock();

      return (int)
          this.linkedHashMap.values().stream().filter(x -> !x.isExpire(expireTimeMs)).count();
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Checks whether it is empty.
   *
   * @return True if it's empty
   */
  @Override
  public boolean isEmpty() {
    try {
      this.reentrantLock.lock();

      return this.linkedHashMap.values().stream().allMatch(x -> x.isExpire(expireTimeMs));
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Checks if the cache contains a specific key.
   *
   * @param key The key to search for
   * @return True if the cache contains the key
   */
  @Override
  public boolean containsKey(Object key) {
    try {
      this.reentrantLock.lock();

      V payload = this.get(key);
      return payload != null;
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Checks if the cache contains a specific value.
   *
   * @param value The value to search for
   * @return True if the cache contains that value
   */
  @Override
  public boolean containsValue(Object value) {
    try {
      this.reentrantLock.lock();

      return this.linkedHashMap.values().stream()
          .anyMatch(x -> !x.isExpire(expireTimeMs) && x.payload == value);
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Retrieves the value from a key.
   *
   * @param key The key in the key-value pair
   * @return The value from the key
   */
  @Override
  public @Nullable V get(Object key) {
    try {
      this.reentrantLock.lock();

      Hit<V> hit = this.linkedHashMap.get(key);

      if (hit == null) {
        return null;
      }

      if (hit.isExpire(expireTimeMs)) {
        this.linkedHashMap.remove(key);
        return null;
      }

      return hit.payload;
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Associates a specified value with a specified key in this map.
   *
   * @param key The key in the key-value pair
   * @param value The value in the key-value pair
   * @return The previously associated value of the key. If there isn't any then it would return
   *     null
   */
  @Override
  public @Nullable V put(K key, V value) {
    try {
      this.reentrantLock.lock();

      Hit<V> prevValue = this.linkedHashMap.put(key, new Hit<>(value));
      return prevValue == null ? null : prevValue.payload;
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Removes the mapping for a key if it's present.
   *
   * @param key The key in the map
   * @return The value associated with the key, or null if there were no mappings for the key.
   */
  @Override
  public @Nullable V remove(Object key) {
    try {
      this.reentrantLock.lock();

      Hit<V> prevValue = this.linkedHashMap.remove(key);
      return prevValue == null ? null : prevValue.payload;
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Copies the content from one map to the current map.
   *
   * @param m The map to copy from
   */
  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    try {
      this.reentrantLock.lock();

      for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
        this.linkedHashMap.put(entry.getKey(), new Hit<>(entry.getValue()));
      }
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /** Clears all mapping from the cache. */
  @Override
  public void clear() {
    try {
      this.reentrantLock.lock();

      this.linkedHashMap.clear();
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Returns a {@link Set} view of the keys.
   *
   * @return A set view of the keys in the cache
   */
  @Override
  public Set<@KeyFor("this") K> keySet() {
    try {
      this.reentrantLock.lock();

      return this.linkedHashMap.entrySet().stream()
          .filter(x -> !x.getValue().isExpire(expireTimeMs))
          .map(Entry::getKey)
          .collect(Collectors.toSet());
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Returns a {@link Collection} view of the keys.
   *
   * @return A collection view of the keys in the cache
   */
  @Override
  public Collection<V> values() {
    try {
      this.reentrantLock.lock();

      return this.linkedHashMap.values().stream()
          .filter(x -> !x.isExpire(expireTimeMs))
          .map(x -> x.payload)
          .collect(Collectors.toList());
    } finally {
      this.reentrantLock.unlock();
    }
  }

  /**
   * Returns a {@link Set} view of the keys.
   *
   * @return A set view of the mappings in the cache
   */
  @Override
  public Set<Entry<@KeyFor("this") K, V>> entrySet() {
    try {
      this.reentrantLock.lock();

      return this.linkedHashMap.entrySet().stream()
          .filter(x -> !x.getValue().isExpire(expireTimeMs))
          .map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue().payload))
          .collect(Collectors.toSet());
    } finally {
      this.reentrantLock.unlock();
    }
  }

  public void setOnEvict(OnEvictRunnable onEvictRunnable) {
    try {
      this.reentrantLock.lock();

      this.onEvictRunnable = onEvictRunnable;
    } finally {
      this.reentrantLock.unlock();
    }
  }

  public ReentrantLock getLock() {
    return this.reentrantLock;
  }

  /**
   * Class to contain the time of when a value was stored.
   *
   * @param <V> Type of value
   */
  private static class Hit<V> {

    private final long time; // in nanos
    private final V payload;

    /**
     * Constructor for Hit. Will record the current time the object will be stored.
     *
     * @param payload The value to store
     */
    Hit(V payload) {
      this.time = System.nanoTime();
      this.payload = payload;
    }

    /**
     * Checks if the item is expired.
     *
     * @param expireTimeMs The expired time
     * @return True if the object is expired
     */
    boolean isExpire(long expireTimeMs) {
      final long elapsedTimeNano = System.nanoTime() - this.time;
      return elapsedTimeNano >= TimeUnit.MILLISECONDS.toNanos(expireTimeMs);
    }
  }

  public interface OnEvictRunnable<V> {
    void call(final V value);
  }
}
