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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CacheServiceImpl implements CacheService {
  private static final Logger LOGGER = Logger.getLogger(CacheServiceImpl.class.getName());
  protected static final Set<String> defaultCaches =
      Stream.of("topology", "customEndpoint").collect(Collectors.toSet());
  private static CacheServiceImpl instance;
  protected ConcurrentHashMap<String, SlidingExpirationCacheWithCleanupThread<Object, Object>> caches =
      new ConcurrentHashMap<>();

  private CacheServiceImpl() {

  }

  public static CacheService getInstance() {
    if (instance == null) {
      instance = new CacheServiceImpl();
    }

    return instance;
  }

  @Override
  public void addCacheIfAbsent(
       String cacheName, Supplier<SlidingExpirationCacheWithCleanupThread<Object, Object>> cacheSupplier) {
    caches.computeIfAbsent(cacheName, name -> cacheSupplier.get());
  }

  @Override
  public void deleteCache(String cacheName) {
    final SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.remove(cacheName);
    if (cache != null) {
      cache.clear();
    }
  }

  @Override
  public <T> @Nullable T get(String cacheName, Object key, Class<T> dataClass, long timeToLiveNs) {
    final SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.get(cacheName);
    if (cache == null) {
      return null;
    }

    final Object value = cache.get(key, timeToLiveNs);
    if (dataClass.isInstance(value)) {
      return dataClass.cast(value);
    }

    LOGGER.fine(
        Messages.get("CacheServiceImpl.classMismatch", new Object[]{dataClass, value.getClass(), cacheName, key}));
    return null;
  }

  @Override
  public <T> void set(String cacheName, Object key, T value, long timeToLiveNs) {
    SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.get(cacheName);
    if (cache == null) {
      addCacheIfAbsent(cacheName, SlidingExpirationCacheWithCleanupThread::new);
      LOGGER.finest(Messages.get("CacheServiceImpl.autoCreatedCache", new Object[]{cacheName}));
    }

    cache = caches.get(cacheName);
    if (cache != null) {
      cache.put(key, value, timeToLiveNs);
    }
  }

  @Override
  public <T> boolean setIfCacheExists(String cacheName, Object key, T value, long timeToLiveNs) {
    final SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.get(cacheName);
    if (cache == null) {
      return false;
    }

    cache.put(key, value, timeToLiveNs);
    return true;
  }

  @Override
  public void delete(String cacheName, Object key) {
    final SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.get(cacheName);
    if (cache == null) {
      return;
    }

    cache.remove(key);
  }

  // TODO: this is needed to suppress the warning about the casted return value, is it fine to suppress?
  @SuppressWarnings("unchecked")
  @Override
  public <T> @Nullable Map<Object, T> getEntries(String cacheName) {
    final SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.get(cacheName);
    if (cache == null) {
      return null;
    }

    return (Map<Object, T>) cache.getEntries();
  }

  @Override
  public int size(String cacheName) {
    final SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.get(cacheName);
    if (cache == null) {
      return 0;
    }

    return cache.size();
  }

  @Override
  public void deleteAll() {
    for (SlidingExpirationCacheWithCleanupThread<Object, Object> cache : caches.values()) {
      cache.clear();
    }

    caches.clear();
  }

  @Override
  public void clearAll() {
    for (SlidingExpirationCacheWithCleanupThread<Object, Object> cache : caches.values()) {
      cache.clear();
    }
  }

  @Override
  public void clear(String cacheName) {
    final SlidingExpirationCacheWithCleanupThread<Object, Object> cache = caches.get(cacheName);
    if (cache == null) {
      return;
    }

    cache.clear();
  }
}
