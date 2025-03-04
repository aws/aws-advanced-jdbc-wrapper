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
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ShouldDisposeFunc;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.storage.ExpirationCache;

public class MonitorServiceImpl implements MonitorService {
  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());
  protected static final long DEFAULT_CLEANUP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  protected static final Map<Class<? extends Monitor>, MonitorSettings> monitorSettingsByType =
      new ConcurrentHashMap<>();
  protected static final Map<Class<? extends Monitor>, ExpirationCache<Object, MonitorItem>> monitorCaches =
      new ConcurrentHashMap<>();
  protected static final AtomicBoolean isInitialized = new AtomicBoolean(false);
  protected static final ReentrantLock initLock = new ReentrantLock();
  protected static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor((r -> {
    final Thread thread = new Thread(r);
    thread.setDaemon(true);
    return thread;
  }));

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
    for (ExpirationCache<Object, MonitorItem> cache : monitorCaches.values()) {
      // Note: the map returned by getEntries is a copy of the ExpirationCache map
      for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
        MonitorItem monitorItem = entry.getValue();
        if (monitorItem == null) {
          continue;
        }

        Monitor monitor = monitorItem.getMonitor();
        if (monitor == null) {
          continue;
        }

        if (monitor.getState() == MonitorState.STOPPED) {
          cache.remove(entry.getKey());
        }

        MonitorSettings monitorSettings = monitorSettingsByType.get(monitor.getClass());
        if (monitorSettings == null) {
          cache.removeIfExpired(entry.getKey());
          continue;
        }

        Set<MonitorErrorResponse> errorResponses = monitorSettings.getErrorResponses();
        if (System.nanoTime() - monitor.getLastUsedTimestampNanos() > monitorSettings.getInactiveTimeoutNanos()) {
          LOGGER.fine(
              Messages.get("MonitorServiceImpl.monitorStuck",
                  new Object[]{monitor, TimeUnit.NANOSECONDS.toSeconds(monitorSettings.getInactiveTimeoutNanos())}));
          if (!Utils.isNullOrEmpty(errorResponses) && errorResponses.contains(MonitorErrorResponse.RESTART)) {
            // Note: the put method disposes of the old item
            LOGGER.fine(Messages.get("MonitorServiceImpl.restartingMonitor", new Object[]{monitor}));
            cache.put(entry.getKey(), new MonitorItem(monitorItem.getMonitorSupplier()));
            continue;
          } else {
            cache.remove(entry.getKey());
          }
        }

        if (monitor.getState() != MonitorState.ERROR) {
          cache.removeIfExpired(entry.getKey());
          continue;
        }

        LOGGER.fine(
            Messages.get("MonitorServiceImpl.errorInMonitor", new Object[]{monitor.getUnhandledException(), monitor}));

        if (!Utils.isNullOrEmpty(errorResponses) && errorResponses.contains(MonitorErrorResponse.RESTART)) {
          // Note: the put method disposes of the old item
          LOGGER.fine(Messages.get("MonitorServiceImpl.restartingMonitor", new Object[]{monitor}));
          cache.put(entry.getKey(), new MonitorItem(monitorItem.getMonitorSupplier()));
          continue;
        }

        cache.removeIfExpired(entry.getKey());
      }
    }
  }

  @Override
  public <T extends Monitor> void registerMonitorTypeIfAbsent(
      Class<T> monitorClass,
      long timeToLiveNanos,
      long inactiveTimeoutNanos,
      Set<MonitorErrorResponse> errorResponses,
      @Nullable ShouldDisposeFunc<T> shouldDisposeFunc) {
    monitorCaches.computeIfAbsent(
        monitorClass,
        mc -> {
          monitorSettingsByType.putIfAbsent(monitorClass, new MonitorSettings(inactiveTimeoutNanos, errorResponses));
          return new ExpirationCache<>(
              true,
              timeToLiveNanos,
              null,
              null
          );
        });
  }

  @Override
  public <T extends Monitor> void runIfAbsent(Class<T> monitorClass, Object key, Supplier<T> monitorSupplier) {
    ExpirationCache<Object, MonitorItem> cache = monitorCaches.get(monitorClass);
    if (cache == null) {
      throw new IllegalStateException(
          Messages.get("MonitorServiceImpl.monitorTypeNotRegistered", new Object[] {monitorClass}));
    }

    cache.computeIfAbsent(
        key,
        k -> new MonitorItem((Supplier<Monitor>) monitorSupplier));
  }

  @Override
  public <T extends Monitor> void stopAndRemove(Class<T> monitorClass, Object key) {
    ExpirationCache<Object, ?> cache = monitorCaches.get(monitorClass);
    if (cache == null) {
      return;
    }

    Object result = cache.remove(key);
    if (result instanceof MonitorItem) {
      MonitorItem monitorItem = (MonitorItem) result;
      Monitor monitor = monitorItem.getMonitor();
      if (monitor != null) {
        monitor.close();
      }
    }
  }

  @Override
  public <T extends Monitor> void stopAndRemoveMonitors(Class<T> monitorClass) {
    ExpirationCache<Object, ?> cache = monitorCaches.get(monitorClass);
    if (cache == null) {
      return;
    }

    for (Object value : cache.getEntries().values()) {
      if (value instanceof MonitorItem) {
        MonitorItem monitorItem = (MonitorItem) value;
        Monitor monitor = monitorItem.getMonitor();
        if (monitor != null) {
          monitor.close();
        }
      }
    }

    cache.clear();
  }

  @Override
  public void stopAndRemoveAll() {
    for (Class<? extends Monitor> monitorClass : monitorCaches.keySet()) {
      stopAndRemoveMonitors(monitorClass);
    }
  }

  protected static class MonitorItem {
    private final Supplier<Monitor> monitorSupplier;
    private final Monitor monitor;

    protected MonitorItem(Supplier<Monitor> monitorSupplier) {
      this.monitorSupplier = monitorSupplier;
      this.monitor = monitorSupplier.get();
    }

    public Supplier<Monitor> getMonitorSupplier() {
      return monitorSupplier;
    }

    public Monitor getMonitor() {
      return monitor;
    }
  }
}
