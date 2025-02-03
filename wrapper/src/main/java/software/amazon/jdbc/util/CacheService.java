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

import java.util.Map;
import java.util.function.Supplier;

public interface CacheService {
  /**
   * Adds a cache to CacheService if it does not already exist.
   *
   * @param cacheName
   * @param cacheSupplier
   */
  void addCacheIfAbsent(
      String cacheName, Supplier<SlidingExpirationCacheWithCleanupThread<Object, Object>> cacheSupplier);

  /**
   * Clears the given cache without deleting it from the CacheService. To delete the cache as well, use
   * {@link #deleteCache(String)}.
   *
   * @param cacheName
   */
  void clear(String cacheName);

  void deleteCache(String cacheName);

  // TODO: in the current code it seems the time-to-live we pass to get/set is always the same, would it be better to
  //  pass this in addCacheIfAbsent instead of every single get/set call? Less generic but more convenient.
  <T> T get(String cacheName, Object key, Class<T> dataClass, long timeToLiveNs);

  /**
   * Sets a value in the specified cache. If the specified cache does not exist, it will be created with default
   * settings.
   *
   * @param cacheName
   * @param key
   * @param value
   * @param timeToLiveNs
   * @param <T>
   */
  <T> void set(String cacheName, Object key, T value, long timeToLiveNs);

  /**
   * Sets a value in the specified cache if it exists. Otherwise, returns false without doing anything. This method can
   * be used if the caller does not want to create a default cache when the specified cache does not already exist.
   *
   * @param cacheName
   * @param key
   * @param value
   * @param timeToLiveNs
   * @return
   * @param <T>
   */
  <T> boolean setIfCacheExists(String cacheName, Object key, T value, long timeToLiveNs);

  void delete(String cacheName, Object key);

  /**
   * Returns a copy of the entries in the given cache.
   *
   * @param cacheName
   * @return
   * @param <T>
   */
  <T> Map<Object, T> getEntries(String cacheName);

  int size(String cacheName);

  /**
   * Deletes all caches from the CacheService.
   */
  void deleteAll();

  /**
   * Clears all data from all caches without deleting the caches themselves.
   */
  void clearAll();
}
