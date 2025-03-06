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

package software.amazon.jdbc.util.monitoring;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ShouldDisposeFunc;
import software.amazon.jdbc.util.storage.ExpirationCache;

public class MonitorServiceImpl implements MonitorService {
  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());
  protected static final long DEFAULT_CLEANUP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  protected static final Map<Class<? extends Monitor>, CacheContainer> monitorCaches = new ConcurrentHashMap<>();
  protected static final AtomicBoolean isInitialized = new AtomicBoolean(false);
  protected static final ReentrantLock initLock = new ReentrantLock();
  protected static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor((r -> {
    final Thread thread = new Thread(r);
    thread.setDaemon(true);
    return thread;
  }));

  static {
    // TODO: add default caches once monitors have been adjusted to implement the Monitor interface
  }

  public MonitorServiceImpl() {
    initCleanupThread(DEFAULT_CLEANUP_INTERVAL_NANOS);
  }

  public MonitorServiceImpl(long cleanupIntervalNanos) {
    initCleanupThread(cleanupIntervalNanos);
  }

  protected void initCleanupThread(long cleanupIntervalNanos) {
    if (isInitialized.get()) {
      return;
    }

    initLock.lock();
    try {
      if (isInitialized.get()) {
        return;
      }

      cleanupExecutor.scheduleAtFixedRate(
          this::checkMonitors, cleanupIntervalNanos, cleanupIntervalNanos, TimeUnit.NANOSECONDS);
      cleanupExecutor.shutdown();
      isInitialized.set(true);
    } finally {
      initLock.unlock();
    }
  }

  protected void checkMonitors() {
    LOGGER.finest(Messages.get("MonitorServiceImpl.checkingMonitors"));
    for (CacheContainer container : monitorCaches.values()) {
      ExpirationCache<Object, MonitorItem> cache = container.getCache();
      // Note: the map returned by getEntries is a copy of the ExpirationCache map
      for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
        MonitorItem monitorItem = entry.getValue();
        if (monitorItem == null) {
          continue;
        }

        Monitor monitor = monitorItem.getMonitor();
        if (monitor.getState() == MonitorState.STOPPED) {
          cache.remove(entry.getKey());
        }

        MonitorSettings monitorSettings = container.getSettings();
        Set<MonitorErrorResponse> errorResponses = monitorSettings.getErrorResponses();
        if (System.nanoTime() - monitor.getLastUsedTimestampNanos() < monitorSettings.getInactiveTimeoutNanos()) {
          // Monitor has updated its last-used timestamp recently and is not considered stuck.
          cache.removeIfExpired(entry.getKey());
        }

        // Monitor has been inactive for longer than the inactive timeout and is considered stuck.
        LOGGER.fine(
            Messages.get("MonitorServiceImpl.monitorStuck",
                new Object[]{monitor, TimeUnit.NANOSECONDS.toSeconds(monitorSettings.getInactiveTimeoutNanos())}));
        if (errorResponses.contains(MonitorErrorResponse.RESTART)) {
          // Note: the put method disposes of the old item
          LOGGER.fine(Messages.get("MonitorServiceImpl.restartingMonitor", new Object[]{monitor}));
          cache.put(entry.getKey(), new MonitorItem(monitorItem.getMonitorSupplier()));
        } else {
          cache.remove(entry.getKey());
        }
      }
    }
  }

  @Override
  public <T extends Monitor> void registerMonitorTypeIfAbsent(
      Class<T> monitorClass,
      long expirationTimeoutNanos,
      long heartbeatTimeoutNanos,
      Set<MonitorErrorResponse> errorResponses,
      @Nullable ShouldDisposeFunc<T> shouldDisposeFunc) {
    monitorCaches.computeIfAbsent(
        monitorClass,
        mc -> {
          ShouldDisposeFunc<MonitorItem> wrappedShouldDisposeFunc = shouldDisposeFunc == null ? null
              : (monitorItem) -> shouldDisposeFunc.shouldDispose((T) monitorItem.getMonitor());
          ExpirationCache<Object, MonitorItem> cache = new ExpirationCache<>(
              true,
              expirationTimeoutNanos,
              wrappedShouldDisposeFunc,
              (monitorItem) -> monitorItem.getMonitor().stop());
          return new CacheContainer(new MonitorSettings(heartbeatTimeoutNanos, errorResponses), cache);
        });
  }

  @Override
  public <T extends Monitor> void runIfAbsent(Class<T> monitorClass, Object key, Supplier<T> monitorSupplier) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      throw new IllegalStateException(
          Messages.get("MonitorServiceImpl.monitorTypeNotRegistered", new Object[] {monitorClass}));
    }

    cacheContainer.getCache().computeIfAbsent(
        key,
        k -> new MonitorItem((Supplier<Monitor>) monitorSupplier));
  }

  @Override
  public void processMonitorError(Monitor monitor, Exception exception) {
    CacheContainer cacheContainer = monitorCaches.get(monitor.getClass());
    if (cacheContainer == null) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.unregisteredMonitorError", new Object[] {monitor, exception}));
      return;
    }

    Object monitorKey = null;
    Supplier<Monitor> monitorSupplier = null;
    ExpirationCache<Object, MonitorItem> cache = cacheContainer.getCache();
    for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
      MonitorItem monitorItem = entry.getValue();
      if (monitorItem != null && monitorItem.getMonitor() == monitor) {
        monitorKey = entry.getKey();
        monitorSupplier = entry.getValue().getMonitorSupplier();
        break;
      }
    }

    if (monitorKey == null) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.monitorErrorForMissingMonitor", new Object[] {monitor, exception}));
      return;
    }

    MonitorSettings settings = cacheContainer.getSettings();
    Set<MonitorErrorResponse> errorResponses = settings.getErrorResponses();
    if (errorResponses.contains(MonitorErrorResponse.RESTART)) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.restartingMonitor", new Object[]{monitor}));
      // Note: the put method automatically disposes of the old item.
      cache.put(monitorKey, new MonitorItem(monitorSupplier));
    } else {
      stopAndRemove(monitor.getClass(), monitorKey);
    }
  }

  @Override
  public <T extends Monitor> void stopAndRemove(Class<T> monitorClass, Object key) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      return;
    }

    // Note: remove() automatically closes the monitor.
    cacheContainer.getCache().remove(key);
  }

  @Override
  public <T extends Monitor> void stopAndRemoveMonitors(Class<T> monitorClass) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      return;
    }

    // Note: clear() automatically closes the monitors.
    cacheContainer.getCache().clear();
  }

  @Override
  public void stopAndRemoveAll() {
    for (Class<? extends Monitor> monitorClass : monitorCaches.keySet()) {
      stopAndRemoveMonitors(monitorClass);
    }
  }

  protected static class CacheContainer {
    private @NonNull final MonitorSettings settings;
    private @NonNull final ExpirationCache<Object, MonitorItem> cache;

    public CacheContainer(@NonNull MonitorSettings settings, @NonNull ExpirationCache<Object, MonitorItem> cache) {
      this.settings = settings;
      this.cache = cache;
    }

    public @NonNull MonitorSettings getSettings() {
      return settings;
    }

    public @NonNull ExpirationCache<Object, MonitorItem> getCache() {
      return cache;
    }
  }

  protected static class MonitorItem {
    private @NonNull final Supplier<Monitor> monitorSupplier;
    private @NonNull final Monitor monitor;

    protected MonitorItem(@NonNull Supplier<Monitor> monitorSupplier) {
      this.monitorSupplier = monitorSupplier;
      this.monitor = monitorSupplier.get();
    }

    public @NonNull Supplier<Monitor> getMonitorSupplier() {
      return monitorSupplier;
    }

    public @NonNull Monitor getMonitor() {
      return monitor;
    }
  }
}
