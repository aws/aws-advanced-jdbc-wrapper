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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import software.amazon.jdbc.util.Messages;

/**
 * This singleton class keeps track of all the monitoring threads and handles the creation and clean
 * up of each monitoring thread.
 */
public class HostMonitorThreadContainer {

  private static HostMonitorThreadContainer singleton = null;
  private final Map<HostMonitor, Future<?>> tasksMap = new ConcurrentHashMap<>();
  // TODO: remove monitorMap and threadPool and submit monitors to MonitorService instead
  private final Map<String, HostMonitor> monitorMap = new ConcurrentHashMap<>();
  private final ExecutorService threadPool;
  private static final ReentrantLock LOCK_OBJECT = new ReentrantLock();
  private static final ReentrantLock MONITOR_LOCK_OBJECT = new ReentrantLock();

  /**
   * Create an instance of the {@link HostMonitorThreadContainer}.
   *
   * @return a singleton instance of the {@link HostMonitorThreadContainer}.
   */
  public static HostMonitorThreadContainer getInstance() {
    return getInstance(Executors::newCachedThreadPool);
  }

  static HostMonitorThreadContainer getInstance(final ExecutorServiceInitializer executorServiceInitializer) {
    HostMonitorThreadContainer singletonToReturn = singleton;

    if (singletonToReturn != null) {
      return singletonToReturn;
    }

    LOCK_OBJECT.lock();
    try {
      if (singleton == null) {
        singleton = new HostMonitorThreadContainer(executorServiceInitializer);
      }
      singletonToReturn = singleton;
    } finally {
      LOCK_OBJECT.unlock();
    }
    return singletonToReturn;
  }

  /**
   * Release resources held in the {@link HostMonitorThreadContainer} and clear references to the
   * container.
   */
  public static void releaseInstance() {
    if (singleton == null) {
      return;
    }
    LOCK_OBJECT.lock();
    try {
      if (singleton != null) {
        singleton.releaseResources();
        singleton = null;
      }
    } finally {
      LOCK_OBJECT.unlock();
    }
  }

  private HostMonitorThreadContainer(final ExecutorServiceInitializer executorServiceInitializer) {
    this.threadPool = executorServiceInitializer.createExecutorService();
  }

  public Map<String, HostMonitor> getMonitorMap() {
    return monitorMap;
  }

  public Map<HostMonitor, Future<?>> getTasksMap() {
    return tasksMap;
  }

  HostMonitor getMonitor(final String node) {
    return monitorMap.get(node);
  }

  HostMonitor getOrCreateMonitor(final Set<String> nodeKeys, final Supplier<HostMonitor> monitorSupplier) {
    if (nodeKeys.isEmpty()) {
      throw new IllegalArgumentException(Messages.get("HostMonitorThreadContainer.emptyNodeKeys"));
    }

    MONITOR_LOCK_OBJECT.lock();
    try {

      HostMonitor monitor = null;
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
              final HostMonitor newMonitor = monitorSupplier.get();
              addTask(newMonitor);
              return newMonitor;
            });
      }
      populateMonitorMap(nodeKeys, monitor);
      return monitor;

    } finally {
      MONITOR_LOCK_OBJECT.unlock();
    }
  }

  private void populateMonitorMap(final Set<String> nodeKeys, final HostMonitor monitor) {
    for (final String nodeKey : nodeKeys) {
      monitorMap.putIfAbsent(nodeKey, monitor);
    }
  }

  void addTask(final HostMonitor monitor) {
    tasksMap.computeIfAbsent(monitor, k -> threadPool.submit(monitor));
  }

  /**
   * Remove references to the given {@link HostMonitorImpl} object and stop the background monitoring
   * thread.
   *
   * @param monitor The {@link HostMonitorImpl} representing a monitoring thread.
   */
  public void releaseResource(final HostMonitor monitor) {
    if (monitor == null) {
      return;
    }

    final List<HostMonitor> monitorList = Collections.singletonList(monitor);

    MONITOR_LOCK_OBJECT.lock();
    try {
      monitorMap.values().removeAll(monitorList);
      tasksMap.computeIfPresent(
          monitor,
          (k, v) -> {
            v.cancel(true);
            return null;
          });
    } finally {
      MONITOR_LOCK_OBJECT.unlock();
    }
  }

  public void releaseResources() {
    MONITOR_LOCK_OBJECT.lock();
    try {
      monitorMap.clear();
      tasksMap.values().stream()
          .filter(val -> !val.isDone() && !val.isCancelled())
          .forEach(val -> val.cancel(true));

      if (threadPool != null) {
        threadPool.shutdownNow();
      }
    } finally {
      MONITOR_LOCK_OBJECT.unlock();
    }
  }
}
