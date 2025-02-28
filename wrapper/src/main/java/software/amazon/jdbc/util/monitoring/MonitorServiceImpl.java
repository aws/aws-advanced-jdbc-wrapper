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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ShouldDisposeFunc;
import software.amazon.jdbc.util.storage.ExpirationCache;

public class MonitorServiceImpl implements MonitorService {
  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());
  protected static final long DEFAULT_CLEANUP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  protected static final Map<Class<? extends Monitor>, Set<MonitorErrorResponse>> errorResponsesByType = new ConcurrentHashMap<>();
  protected static final Map<Class<? extends Monitor>, ExpirationCache<Object, MonitorItem<? extends Monitor>>> monitorCaches = new ConcurrentHashMap<>();

  @Override
  public <T extends Monitor> void registerMonitorTypeIfAbsent(
      Class<T> monitorClass,
      Set<MonitorErrorResponse> errorResponses,
      long cleanupIntervalNanos, long timeToLiveNanos,
      @Nullable ShouldDisposeFunc<T> shouldDisposeFunc) {
    monitorCaches.computeIfAbsent(
        monitorClass,
        mc -> {
          errorResponsesByType.putIfAbsent(monitorClass, errorResponses);
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
    ExpirationCache<Object, MonitorItem<? extends Monitor>> cache = monitorCaches.get(monitorClass);
    if (cache == null) {
      throw new IllegalStateException(
          Messages.get("MonitorServiceImpl.monitorClassNotRegistered", new Object[] {monitorClass}));
    }

    ExpirationCache<Object, MonitorItem<T>> typedCache =
        (ExpirationCache<Object, MonitorItem<T>>) (ExpirationCache<?, ?>) cache;
    typedCache.computeIfAbsent(
        key,
        k -> {
          T monitor = monitorSupplier.get();
          monitor.start();
          return new MonitorItem<>(monitorSupplier, monitor);
        });
  }

  @Override
  public <T extends Monitor> void stopAndRemove(Class<T> monitorClass, Object key) {
    ExpirationCache<Object, ?> cache = monitorCaches.get(monitorClass);
    if (cache == null) {
      return;
    }

    Object result = cache.remove(key);
    if (result instanceof MonitorItem) {
      MonitorItem<? extends Monitor> monitorItem = (MonitorItem<? extends Monitor>) result;
      Monitor monitor = monitorItem.getMonitor();
      if (monitor != null) {
        monitor.stop();
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
        MonitorItem<? extends Monitor> monitorItem = (MonitorItem<? extends Monitor>) value;
        Monitor monitor = monitorItem.getMonitor();
        if (monitor != null) {
          monitor.stop();
        }
      }
    }

    cache.clear();
  }

  @Override
  public void stopAndRemoveAll() {
    for (Class<? extends Monitor> monitorClass : errorResponsesByType.keySet()) {
      stopAndRemoveMonitors(monitorClass);
    }
  }

  protected static class MonitorItem<T extends Monitor> {
    private final Supplier<T> monitorSupplier;
    private final T monitor;

    protected MonitorItem(Supplier<T> monitorSupplier, T monitor) {
      this.monitorSupplier = monitorSupplier;
      this.monitor = monitor;
    }

    public Supplier<T> getMonitorSupplier() {
      return monitorSupplier;
    }

    public T getMonitor() {
      return monitor;
    }
  }
}
