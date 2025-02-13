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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.ItemDisposalFunc;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ShouldDisposeFunc;

public class StorageServiceImpl implements StorageService {
  private static final Logger LOGGER = Logger.getLogger(StorageServiceImpl.class.getName());
  protected static Map<String, ExpirationCache<Object, ?>> caches = new ConcurrentHashMap<>();

  public StorageServiceImpl() {

  }

  @Override
  public <V> void registerItemCategoryIfAbsent(
      String itemCategory,
      Class<V> itemClass,
      boolean isRenewableExpiration,
      long timeToLiveNanos,
      long cleanupIntervalNanos,
      @Nullable ShouldDisposeFunc<V> shouldDisposeFunc,
      @Nullable ItemDisposalFunc<V> itemDisposalFunc) {
    caches.computeIfAbsent(
        itemCategory,
        category -> new ExpirationCache<>(
            itemClass,
            isRenewableExpiration,
            timeToLiveNanos,
            cleanupIntervalNanos,
            shouldDisposeFunc,
            itemDisposalFunc));
  }

  @Override
  public <V> void set(String itemCategory, Object key, V value) {
    final ExpirationCache<Object, ?> cache = caches.get(itemCategory);
    if (cache == null) {
      // TODO: what should we do if the item category isn't registered?
      return;
    }

    Class<?> expectedType = cache.getValueClass();
    if (!expectedType.isInstance(value)) {
      throw new IllegalArgumentException(
          Messages.get(
              "StorageServiceImpl.incorrectValueType",
              new Object[]{itemCategory, expectedType, value.getClass(), value}));
    }

    ExpirationCache<Object, V> typedCache = (ExpirationCache<Object, V>) cache;
    typedCache.put(key, value);
  }

  @Override
  public <V> @Nullable V get(String itemCategory, Object key, Class<V> itemClass) {
    final ExpirationCache<Object, ?> cache = caches.get(itemCategory);
    if (cache == null) {
      return null;
    }

    final Object value = cache.get(key);
    if (value == null) {
      return null;
    }

    if (itemClass.isInstance(value)) {
      return itemClass.cast(value);
    }

    LOGGER.fine(
        Messages.get(
            "StorageServiceImpl.itemClassMismatch",
            new Object[]{key, itemCategory, itemClass, value.getClass()}));
    return null;
  }

  @Override
  public boolean exists(String itemCategory, Object key) {
    final ExpirationCache<Object, ?> cache = caches.get(itemCategory);
    if (cache == null) {
      return false;
    }

    return cache.exists(key);
  }

  @Override
  public void remove(String itemCategory, Object key) {
    final ExpirationCache<Object, ?> cache = caches.get(itemCategory);
    if (cache == null) {
      return;
    }

    cache.remove(key);
  }

  @Override
  public void clear(String itemCategory) {
    final ExpirationCache<Object, ?> cache = caches.get(itemCategory);
    if (cache != null) {
      cache.clear();
    }
  }

  @Override
  public void clearAll() {
    for (ExpirationCache<Object, ?> cache : caches.values()) {
      cache.clear();
    }
  }

  @Override
  public <K, V> @Nullable Map<K, V> getEntries(String itemCategory) {
    final ExpirationCache<?, ?> cache = caches.get(itemCategory);
    if (cache == null) {
      return null;
    }

    // TODO: fix this cast to be type safe, or remove this method after removing its callers
    return (Map<K, V>) cache.getEntries();
  }

  @Override
  public int size(String itemCategory) {
    final ExpirationCache<?, ?> cache = caches.get(itemCategory);
    if (cache == null) {
      return 0;
    }

    return cache.size();
  }
}
