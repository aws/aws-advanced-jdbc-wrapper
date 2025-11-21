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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.hostlistprovider.Topology;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenStatus;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.events.DataAccessEvent;
import software.amazon.jdbc.util.events.EventPublisher;

public class StorageServiceImpl implements StorageService {
  private static final Logger LOGGER = Logger.getLogger(StorageServiceImpl.class.getName());
  protected static final long DEFAULT_CLEANUP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(5);
  protected static final Map<Class<?>, Supplier<ExpirationCache<Object, ?>>> defaultCacheSuppliers;

  static {
    Map<Class<?>, Supplier<ExpirationCache<Object, ?>>> suppliers = new HashMap<>();
    suppliers.put(Topology.class, ExpirationCache::new);
    suppliers.put(AllowedAndBlockedHosts.class, ExpirationCache::new);
    suppliers.put(BlueGreenStatus.class,
        () -> new ExpirationCache<>(false, TimeUnit.MINUTES.toNanos(60), null, null));
    defaultCacheSuppliers = Collections.unmodifiableMap(suppliers);
  }

  protected final EventPublisher publisher;
  protected final Map<Class<?>, ExpirationCache<Object, ?>> caches = new ConcurrentHashMap<>();
  protected final ScheduledExecutorService cleanupExecutor =
      ExecutorFactory.newSingleThreadScheduledThreadExecutor("ssi");

  public StorageServiceImpl(EventPublisher publisher) {
    this(DEFAULT_CLEANUP_INTERVAL_NANOS, publisher);
  }

  public StorageServiceImpl(long cleanupIntervalNanos, EventPublisher publisher) {
    this.publisher = publisher;
    initCleanupThread(cleanupIntervalNanos);
  }

  protected void initCleanupThread(long cleanupIntervalNanos) {
    this.cleanupExecutor.scheduleAtFixedRate(
        this::removeExpiredItems, cleanupIntervalNanos, cleanupIntervalNanos, TimeUnit.NANOSECONDS);
  }

  protected void removeExpiredItems() {
    LOGGER.finest(Messages.get("StorageServiceImpl.removeExpiredItems"));
    for (ExpirationCache<Object, ?> cache : caches.values()) {
      cache.removeExpiredEntries();
    }
  }

  @Override
  public <V> void registerItemClassIfAbsent(
      Class<V> itemClass,
      boolean isRenewableExpiration,
      long timeToLiveNanos,
      @Nullable ShouldDisposeFunc<V> shouldDisposeFunc,
      @Nullable ItemDisposalFunc<V> itemDisposalFunc) {
    caches.computeIfAbsent(
        itemClass,
        k -> new ExpirationCache<>(
            isRenewableExpiration,
            timeToLiveNanos,
            shouldDisposeFunc,
            itemDisposalFunc));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> void set(Object key, V value) {
    ExpirationCache<Object, ?> cache = caches.get(value.getClass());
    if (cache == null) {
      Supplier<ExpirationCache<Object, ?>> supplier = defaultCacheSuppliers.get(value.getClass());
      if (supplier == null) {
        throw new IllegalStateException(
            Messages.get("StorageServiceImpl.itemClassNotRegistered", new Object[] {value.getClass()}));
      } else {
        cache = caches.computeIfAbsent(value.getClass(), c -> supplier.get());
      }
    }

    try {
      ExpirationCache<Object, V> typedCache = (ExpirationCache<Object, V>) cache;
      typedCache.put(key, value);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          Messages.get("StorageServiceImpl.unexpectedValueMismatch", new Object[] {value, value.getClass(), cache}));
    }
  }

  @Override
  public <V> @Nullable V get(Class<V> itemClass, @NotNull Object key) {
    final ExpirationCache<Object, ?> cache = caches.get(itemClass);
    if (cache == null) {
      return null;
    }

    final Object value = cache.get(key);
    if (value == null) {
      return null;
    }

    if (itemClass.isInstance(value)) {
      DataAccessEvent event = new DataAccessEvent(itemClass, key);
      this.publisher.publish(event);
      return itemClass.cast(value);
    }

    LOGGER.fine(
        Messages.get(
            "StorageServiceImpl.itemClassMismatch",
            new Object[] {key, itemClass, value, value.getClass()}));
    return null;
  }

  @Override
  public boolean exists(Class<?> itemClass, Object key) {
    final ExpirationCache<Object, ?> cache = caches.get(itemClass);
    if (cache == null) {
      return false;
    }

    return cache.exists(key);
  }

  @Override
  public void remove(Class<?> itemClass, Object key) {
    final ExpirationCache<Object, ?> cache = caches.get(itemClass);
    if (cache != null) {
      cache.remove(key);
    }
  }

  @Override
  public void clear(Class<?> itemClass) {
    final ExpirationCache<Object, ?> cache = caches.get(itemClass);
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
  public int size(Class<?> itemClass) {
    final ExpirationCache<?, ?> cache = caches.get(itemClass);
    if (cache == null) {
      return 0;
    }

    return cache.size();
  }
}
