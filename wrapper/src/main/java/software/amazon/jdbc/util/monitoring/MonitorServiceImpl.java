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
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;

public class MonitorServiceImpl implements MonitorService {
  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());
  private final Map<String, MonitorGroup> monitorGroups = new ConcurrentHashMap<>();
  private static MonitorServiceImpl instance;

  private MonitorServiceImpl() {

  }

  public static MonitorService getInstance() {
    if (instance == null) {
      instance = new MonitorServiceImpl();
    }

    return instance;
  }


  @Override
  public void registerMonitorTypeIfAbsent(String monitorType,
      Supplier<SlidingExpirationCacheWithCleanupThread<Object, Monitor>> cacheSupplier,
      MonitorInitializer initializer,
      Set<MonitorExceptionResponse> exceptionResponses) {
    monitorGroups.computeIfAbsent(monitorType,
        key -> new MonitorGroup(monitorType, initializer, exceptionResponses, cacheSupplier.get()));
  }

  @Override
  public void runIfAbsent(
      String monitorType,
      Object monitorKey,
      long timeToLiveNs,
      Object... initializerParams) {
    MonitorGroup group = monitorGroups.get(monitorType);
    if (group == null) {
      throw new RuntimeException(
          "A monitor with key '" + monitorKey + "' was requested, but the monitor type '"
              + monitorType + "' does not exist.");
    }

    SlidingExpirationCacheWithCleanupThread<Object, Monitor> cache = group.getCache();
    cache.computeIfAbsent(
        monitorKey,
        key -> {
          Monitor monitor = group.getInitializer().initialize(initializerParams);
          monitor.start();
          return monitor;
        },
        timeToLiveNs);
  }

  @Override
  public int numMonitors(String monitorType) {
    MonitorGroup group = monitorGroups.get(monitorType);
    if (group == null) {
      return 0;
    }

    return group.getCache().size();
  }

  @Override
  public @Nullable MonitorStatus getStatus(String monitorType, Object monitorKey, long timeToLiveNs) {
    MonitorGroup group = monitorGroups.get(monitorType);
    if (group == null) {
      return null;
    }

    Monitor monitor = group.getCache().get(monitorKey, timeToLiveNs);
    if (monitor == null) {
      return null;
    }

    return monitor.getStatus();
  }

  @Override
  public void stopAndRemoveMonitor(String monitorType, Object monitorKey) {
    MonitorGroup group = monitorGroups.get(monitorType);
    if (group == null) {
      return;
    }

    Monitor monitor = group.getCache().get(monitorKey, 0);
    if (monitor != null) {
      group.getCache().remove(monitorKey);
      monitor.stop();
    }
  }

  @Override
  public void stopAndRemoveMonitors(String monitorType) {
    MonitorGroup group = monitorGroups.get(monitorType);
    if (group == null) {
      return;
    }

    for (Object monitorKey : group.getCache().getEntries().keySet()) {
      stopAndRemoveMonitor(monitorType, monitorKey);
    }
  }

  @Override
  public void deleteMonitorType(String monitorType) {
    MonitorGroup group = monitorGroups.remove(monitorType);
    if (group != null) {
      return;
    }

    stopAndRemoveMonitors(monitorType);
  }

  @Override
  public void stopAndRemoveAll() {
    for (String group : monitorGroups.keySet()) {
      stopAndRemoveMonitors(group);
    }
  }

  @Override
  public void deleteAll() {
    for (String group : monitorGroups.keySet()) {
      deleteMonitorType(group);
    }
  }
}
