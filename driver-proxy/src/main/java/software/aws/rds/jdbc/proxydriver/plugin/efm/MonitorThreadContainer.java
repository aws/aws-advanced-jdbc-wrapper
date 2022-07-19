/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

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
import java.util.function.Supplier;

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
  private static final Object LOCK_OBJECT = new Object();

  /**
   * Create an instance of the {@link MonitorThreadContainer}.
   *
   * @return a singleton instance of the {@link MonitorThreadContainer}.
   */
  public static MonitorThreadContainer getInstance() {
    return getInstance(Executors::newCachedThreadPool);
  }

  static MonitorThreadContainer getInstance(ExecutorServiceInitializer executorServiceInitializer) {
    if (singleton == null) {
      synchronized (LOCK_OBJECT) {
        singleton = new MonitorThreadContainer(executorServiceInitializer);
      }
      CLASS_USAGE_COUNT.set(0);
    }
    CLASS_USAGE_COUNT.getAndIncrement();
    return singleton;
  }

  /**
   * Release resources held in the {@link MonitorThreadContainer} and clear references to the
   * container.
   */
  public static void releaseInstance() {
    if (singleton == null) {
      return;
    }

    if (CLASS_USAGE_COUNT.decrementAndGet() <= 0) {
      synchronized (LOCK_OBJECT) {
        singleton.releaseResources();
        singleton = null;
      }
    }
  }

  private MonitorThreadContainer(ExecutorServiceInitializer executorServiceInitializer) {
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

  String getNode(Set<String> nodeKeys) {
    return getNode(nodeKeys, null);
  }

  String getNode(Set<String> nodeKeys, String defaultValue) {
    return nodeKeys.stream().filter(monitorMap::containsKey).findAny().orElse(defaultValue);
  }

  Monitor getMonitor(String node) {
    return monitorMap.get(node);
  }

  Monitor getOrCreateMonitor(Set<String> nodeKeys, Supplier<Monitor> monitorSupplier) {
    if (nodeKeys.isEmpty()) {
      throw new IllegalArgumentException("Provided node keys are empty.");
    }
    final String anyNodeKey = nodeKeys.iterator().next();
    final String node = getNode(nodeKeys, anyNodeKey);
    final Monitor monitor =
        monitorMap.computeIfAbsent(
            node,
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

              return monitorSupplier.get();
            });

    populateMonitorMap(nodeKeys, monitor);
    return monitor;
  }

  private void populateMonitorMap(Set<String> nodeKeys, Monitor monitor) {
    for (String nodeKey : nodeKeys) {
      monitorMap.putIfAbsent(nodeKey, monitor);
    }
  }

  void addTask(Monitor monitor) {
    tasksMap.computeIfAbsent(monitor, k -> threadPool.submit(monitor));
  }

  /**
   * Clear all references used by the given monitor. Put the monitor in to a queue waiting to be
   * reused.
   *
   * @param monitor The monitor to reset.
   */
  public void resetResource(Monitor monitor) {
    if (monitor == null) {
      return;
    }

    final List<Monitor> monitorList = Collections.singletonList(monitor);
    monitorMap.values().removeAll(monitorList);
    availableMonitors.add(monitor);
  }

  /**
   * Remove references to the given {@link MonitorImpl} object and stop the background monitoring
   * thread.
   *
   * @param monitor The {@link MonitorImpl} representing a monitoring thread.
   */
  public void releaseResource(Monitor monitor) {
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
