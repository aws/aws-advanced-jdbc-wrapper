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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointInfo;
import software.amazon.jdbc.util.ItemDisposalFunc;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ShouldDisposeFunc;

public class StorageServiceImpl implements StorageService {
  private static final Logger LOGGER = Logger.getLogger(StorageServiceImpl.class.getName());
  protected static final Map<String, ExpirationCache<Object, ?>> caches = new ConcurrentHashMap<>();
  protected static final Map<String, Supplier<ExpirationCache<Object, ?>>> defaultCacheSuppliers;

  static {
    Map<String, Supplier<ExpirationCache<Object, ?>>> suppliers = new HashMap<>();
    suppliers.put(ItemCategory.TOPOLOGY, () -> new ExpirationCacheBuilder<>(Topology.class).build());
    suppliers.put(ItemCategory.CUSTOM_ENDPOINT, () -> new ExpirationCacheBuilder<>(CustomEndpointInfo.class).build());
    defaultCacheSuppliers = Collections.unmodifiableMap(suppliers);
  }

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
  @SuppressWarnings("unchecked")
  public <V> void set(String itemCategory, Object key, V value) {
    ExpirationCache<Object, ?> cache = caches.get(itemCategory);
    if (cache == null) {
      Supplier<ExpirationCache<Object, ?>> supplier = defaultCacheSuppliers.get(itemCategory);
      if (supplier == null) {
        throw new IllegalStateException(
            Messages.get("StorageServiceImpl.itemCategoryNotRegistered", new Object[] {itemCategory}));
      } else {
        cache = caches.computeIfAbsent(itemCategory, c -> supplier.get());
      }
    }

    if (!cache.getValueClass().isInstance(value)) {
      throw new IllegalArgumentException(
          Messages.get(
              "StorageServiceImpl.incorrectValueType",
              new Object[] {itemCategory, cache.getValueClass(), value.getClass(), value}));
    }

    try {
      ExpirationCache<Object, V> typedCache = (ExpirationCache<Object, V>) cache;
      typedCache.put(key, value);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          Messages.get(
              "StorageServiceImpl.incorrectValueType",
              new Object[]{itemCategory, cache.getValueClass(), value.getClass(), value}));
    }
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

    // TODO: fix this cast to be type safe, or remove this method after removing the suggestedClusterId logic
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
