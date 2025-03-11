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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointMonitorImpl;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ShouldDisposeFunc;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.events.DataAccessEvent;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.events.EventSubscriber;
import software.amazon.jdbc.util.storage.ExternallyManagedCache;

public class MonitorServiceImpl implements MonitorService, EventSubscriber {
  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());
  protected static final long DEFAULT_CLEANUP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  protected static final Map<Class<? extends Monitor>, CacheContainer> monitorCaches = new ConcurrentHashMap<>();
  protected static final AtomicBoolean isInitialized = new AtomicBoolean(false);
  protected static final ReentrantLock initLock = new ReentrantLock();
  protected static final Map<Class<? extends Monitor>, Supplier<CacheContainer>> defaultSuppliers;
  protected static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor((r -> {
    final Thread thread = new Thread(r);
    thread.setDaemon(true);
    if (!StringUtils.isNullOrEmpty(thread.getName())) {
      thread.setName(thread.getName() + "-msi");
    }
    return thread;
  }));

  protected final EventPublisher publisher;

  static {
    Map<Class<? extends Monitor>, Supplier<CacheContainer>> suppliers = new HashMap<>();
    Set<MonitorErrorResponse> resetErrorResponse =
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RESTART));
    MonitorSettings defaultSettings = new MonitorSettings(
        TimeUnit.MINUTES.toNanos(5), TimeUnit.MINUTES.toNanos(1), resetErrorResponse);

    suppliers.put(
        CustomEndpointMonitorImpl.class, () -> new CacheContainer(defaultSettings, AllowedAndBlockedHosts.class));
    defaultSuppliers = Collections.unmodifiableMap(suppliers);
  }

  public MonitorServiceImpl(EventPublisher publisher) {
    this(DEFAULT_CLEANUP_INTERVAL_NANOS, publisher);
  }

  public MonitorServiceImpl(long cleanupIntervalNanos, EventPublisher publisher) {
    this.publisher = publisher;
    this.publisher.subscribe(this, new HashSet<>(Collections.singletonList(DataAccessEvent.class)));
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
      ExternallyManagedCache<Object, MonitorItem> cache = container.getCache();
      // Note: the map returned by getEntries is a copy of the ExternallyManagedCache map
      for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
        Object key = entry.getKey();
        MonitorItem removedItem = cache.removeIf(key, mi -> mi.getMonitor().getState() == MonitorState.STOPPED);
        if (removedItem != null) {
          removedItem.getMonitor().stop();
          continue;
        }

        MonitorSettings monitorSettings = container.getSettings();
        removedItem = cache.removeIf(key, mi -> mi.getMonitor().getState() == MonitorState.ERROR);
        if (removedItem != null) {
          handleMonitorError(container, key, removedItem);
          continue;
        }

        long inactiveTimeoutNanos = monitorSettings.getInactiveTimeoutNanos();
        removedItem = cache.removeIf(
            key, mi -> System.nanoTime() - mi.getMonitor().getLastActivityTimestampNanos() > inactiveTimeoutNanos);
        if (removedItem != null) {
          // Monitor has been inactive for longer than the inactive timeout and is considered stuck.
          LOGGER.fine(
              Messages.get("MonitorServiceImpl.monitorStuck",
                  new Object[]{removedItem.getMonitor(), TimeUnit.NANOSECONDS.toSeconds(inactiveTimeoutNanos)}));
          handleMonitorError(container, key, removedItem);
          continue;
        }

        removedItem = cache.removeExpiredIf(key, mi -> mi.getMonitor().canDispose());
        if (removedItem != null) {
          removedItem.getMonitor().stop();
        }
      }
    }
  }

  private void handleMonitorError(
      CacheContainer cacheContainer,
      Object key,
      MonitorItem oldMonitorItem) {
    Monitor monitor = oldMonitorItem.getMonitor();
    monitor.stop();

    Set<MonitorErrorResponse> errorResponses = cacheContainer.getSettings().getErrorResponses();
    if (errorResponses.contains(MonitorErrorResponse.RESTART)) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.restartingMonitor", new Object[]{monitor}));
      cacheContainer.getCache().computeIfAbsent(key, k -> {
        MonitorItem newMonitorItem = new MonitorItem(oldMonitorItem.getMonitorSupplier());
        newMonitorItem.getMonitor().start();
        return newMonitorItem;
      });
    }
  }

  @Override
  public <T extends Monitor> void registerMonitorTypeIfAbsent(
      Class<T> monitorClass,
      long expirationTimeoutNanos,
      long heartbeatTimeoutNanos,
      Set<MonitorErrorResponse> errorResponses,
      @Nullable ShouldDisposeFunc<T> shouldDisposeFunc,
      @Nullable Class<?> producedDataClass) {
    monitorCaches.computeIfAbsent(
        monitorClass,
        mc -> new CacheContainer(
            new MonitorSettings(expirationTimeoutNanos, heartbeatTimeoutNanos, errorResponses), producedDataClass));
  }

  @Override
  public <T extends Monitor> T runIfAbsent(Class<T> monitorClass, Object key, Supplier<T> monitorSupplier) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      Supplier<CacheContainer> supplier = defaultSuppliers.get(monitorClass);
      if (supplier == null) {
        throw new IllegalStateException(
            Messages.get("MonitorServiceImpl.monitorTypeNotRegistered", new Object[] {monitorClass}));
      }

      cacheContainer = monitorCaches.computeIfAbsent(monitorClass, k -> supplier.get());
    }

    Monitor monitor =
        cacheContainer.getCache().computeIfAbsent(key, k -> {
          MonitorItem monitorItem = new MonitorItem(monitorSupplier);
          monitorItem.getMonitor().start();
          return monitorItem;
        }).getMonitor();
    if (monitorClass.isInstance(monitor)) {
      return monitorClass.cast(monitor);
    }

    throw new IllegalStateException(
        Messages.get("MonitorServiceImpl.unexpectedMonitorClass", new Object[] {monitorClass, monitor}));
  }

  @Override
  public void reportMonitorError(Monitor monitor, Exception exception) {
    CacheContainer cacheContainer = monitorCaches.get(monitor.getClass());
    if (cacheContainer == null) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.unregisteredMonitorError", new Object[] {monitor, exception}));
      return;
    }

    ExternallyManagedCache<Object, MonitorItem> cache = cacheContainer.getCache();
    for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
      MonitorItem monitorItem = cache.removeIf(entry.getKey(), mi -> mi.getMonitor() == monitor);
      if (monitorItem != null) {
        handleMonitorError(cacheContainer, entry.getKey(), monitorItem);
        return;
      }
    }

    LOGGER.finest(Messages.get("MonitorServiceImpl.monitorErrorForMissingMonitor", new Object[] {monitor, exception}));
  }

  @Override
  public <T extends Monitor> void stopAndRemove(Class<T> monitorClass, Object key) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.stopAndRemoveMissingMonitorType", new Object[] {monitorClass, key}));
      return;
    }

    MonitorItem monitorItem = cacheContainer.getCache().remove(key);
    if (monitorItem != null) {
      monitorItem.getMonitor().stop();
    }
  }

  @Override
  public <T extends Monitor> void stopAndRemoveMonitors(Class<T> monitorClass) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.stopAndRemoveMonitorsMissingType", new Object[] {monitorClass}));
      return;
    }

    ExternallyManagedCache<Object, MonitorItem> cache = cacheContainer.getCache();
    for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
      MonitorItem monitorItem = cache.remove(entry.getKey());
      if (monitorItem != null) {
        monitorItem.getMonitor().stop();
      }
    }
  }

  @Override
  public void stopAndRemoveAll() {
    for (Class<? extends Monitor> monitorClass : monitorCaches.keySet()) {
      stopAndRemoveMonitors(monitorClass);
    }
  }

  @Override
  public void processEvent(Event event) {
    if (!(event instanceof DataAccessEvent)) {
      return;
    }

    DataAccessEvent accessEvent = (DataAccessEvent) event;
    for (CacheContainer container : monitorCaches.values()) {
      if (container.getProducedDataClass() == null
          || !accessEvent.getDataClass().equals(container.getProducedDataClass())) {
        continue;
      }

      container.getCache().extendExpiration(accessEvent.getKey());
    }
  }

  protected static class CacheContainer {
    private @NonNull final MonitorSettings settings;
    private @NonNull final ExternallyManagedCache<Object, MonitorItem> cache;
    private @Nullable final Class<?> producedDataClass;

    public CacheContainer(@NonNull final MonitorSettings settings, @Nullable Class<?> producedDataClass) {
      this.settings = settings;
      this.cache = new ExternallyManagedCache<>(true, settings.getExpirationTimeoutNanos());
      this.producedDataClass = producedDataClass;
    }

    public @NonNull MonitorSettings getSettings() {
      return settings;
    }

    public @NonNull ExternallyManagedCache<Object, MonitorItem> getCache() {
      return cache;
    }

    public @Nullable Class<?> getProducedDataClass() {
      return producedDataClass;
    }
  }

  protected static class MonitorItem {
    private @NonNull final Supplier<? extends Monitor> monitorSupplier;
    private @NonNull final Monitor monitor;

    protected MonitorItem(@NonNull Supplier<? extends Monitor> monitorSupplier) {
      this.monitorSupplier = monitorSupplier;
      this.monitor = monitorSupplier.get();
    }

    public @NonNull Supplier<? extends Monitor> getMonitorSupplier() {
      return monitorSupplier;
    }

    public @NonNull Monitor getMonitor() {
      return monitor;
    }
  }
}
