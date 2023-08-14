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

package software.amazon.jdbc.plugin.efm;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import software.amazon.jdbc.util.Messages;

/**
 * This singleton class keeps track of all the monitoring threads and handles the creation and clean
 * up of each monitoring thread.
 */
public class MonitorThreadContainer {
  private static MonitorThreadContainer singleton = null;
  private static final AtomicInteger CLASS_USAGE_COUNT = new AtomicInteger();
  private final Map<String, Monitor> monitorMap = new ConcurrentHashMap<>();
  private final Map<Monitor, Future<?>> tasksMap = new ConcurrentHashMap<>();
  private final Queue<Monitor> availableMonitors = new ConcurrentLinkedDeque<>();
  private final ExecutorService threadPool;
  private static final ReentrantLock LOCK_OBJECT = new ReentrantLock();

  /**
   * Create an instance of the {@link MonitorThreadContainer}.
   *
   * @return a singleton instance of the {@link MonitorThreadContainer}.
   */
  public static MonitorThreadContainer getInstance() {
    return getInstance(Executors::newCachedThreadPool);
  }

  static MonitorThreadContainer getInstance(final ExecutorServiceInitializer executorServiceInitializer) {
    MonitorThreadContainer singletonToReturn;
    LOCK_OBJECT.lock();
    try {
      if (singleton == null) {
        singleton = new MonitorThreadContainer(executorServiceInitializer);
        CLASS_USAGE_COUNT.set(0);
      }
      singletonToReturn = singleton;
      CLASS_USAGE_COUNT.getAndIncrement();
    } finally {
      LOCK_OBJECT.unlock();
    }
    return singletonToReturn;
  }

  /**
   * Release resources held in the {@link MonitorThreadContainer} and clear references to the
   * container.
   */
  public static void releaseInstance() {
    if (singleton == null) {
      return;
    }
    LOCK_OBJECT.lock();
    try {
      if (singleton != null && CLASS_USAGE_COUNT.decrementAndGet() <= 0) {
        singleton.releaseResources();
        singleton = null;
        CLASS_USAGE_COUNT.set(0);
      }
    } finally {
      LOCK_OBJECT.unlock();
    }
  }

  private MonitorThreadContainer(final ExecutorServiceInitializer executorServiceInitializer) {
    this.threadPool = executorServiceInitializer.createExecutorService();
  }

  public Map<String, Monitor> getMonitorMap() {
    return monitorMap;
  }

  public Map<Monitor, Future<?>> getTasksMap() {
    return tasksMap;
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  Monitor getMonitor(final String node) {
    return monitorMap.get(node);
  }

  Monitor getOrCreateMonitor(final Set<String> nodeKeys, final Supplier<Monitor> monitorSupplier) {
    if (nodeKeys.isEmpty()) {
      throw new IllegalArgumentException(Messages.get("MonitorThreadContainer.emptyNodeKeys"));
    }

    Monitor monitor = null;
    String anyNodeKey = null;
    for (final String nodeKey : nodeKeys) {
      monitor = monitorMap.get(nodeKey);
      anyNodeKey = nodeKey;
      if (monitor != null) {
        break;
      }
    }

    if (monitor == null) {
      monitor = monitorMap.computeIfAbsent(
          anyNodeKey,
          k -> {
            if (!availableMonitors.isEmpty()) {
              final Monitor availableMonitor = availableMonitors.remove();
              if (!availableMonitor.isStopped()) {
                return availableMonitor;
              }
              tasksMap.computeIfPresent(
                  availableMonitor,
                  (key, v) -> {
                    v.cancel(true);
                    return null;
                  });
            }

            final Monitor newMonitor = monitorSupplier.get();
            addTask(newMonitor);

            return newMonitor;
          });
    }

    populateMonitorMap(nodeKeys, monitor);
    return monitor;
  }

  private void populateMonitorMap(final Set<String> nodeKeys, final Monitor monitor) {
    for (final String nodeKey : nodeKeys) {
      monitorMap.putIfAbsent(nodeKey, monitor);
    }
  }

  void addTask(final Monitor monitor) {
    tasksMap.computeIfAbsent(monitor, k -> threadPool.submit(monitor));
  }

  /**
   * Clear all references used by the given monitor. Put the monitor in to a queue waiting to be
   * reused.
   *
   * @param monitor The monitor to reset.
   */
  public void resetResource(final Monitor monitor) {
    if (monitor == null) {
      return;
    }

    monitorMap.entrySet().removeIf(e -> e.getValue() == monitor);
    availableMonitors.add(monitor);
  }

  /**
   * Remove references to the given {@link MonitorImpl} object and stop the background monitoring
   * thread.
   *
   * @param monitor The {@link MonitorImpl} representing a monitoring thread.
   */
  public void releaseResource(final Monitor monitor) {
    if (monitor == null) {
      return;
    }

    final List<Monitor> monitorList = Collections.singletonList(monitor);
    monitorMap.values().removeAll(monitorList);
    tasksMap.computeIfPresent(
        monitor,
        (k, v) -> {
          v.cancel(true);
          return null;
        });
  }

  private void releaseResources() {
    monitorMap.clear();
    tasksMap.values().stream()
        .filter(val -> !val.isDone() && !val.isCancelled())
        .forEach(val -> val.cancel(true));

    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }
}
