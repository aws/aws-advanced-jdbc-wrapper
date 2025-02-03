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

import java.util.Set;
import java.util.function.Supplier;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.notifications.NotificationListener;

public interface MonitorService {
  boolean registerMonitorTypeIfAbsent(
      String monitorType,
      Supplier<SlidingExpirationCacheWithCleanupThread<Object, Monitor>> cacheSupplier,
      MonitorInitializer initializer,
      Set<MonitorExceptionResponse> exceptionResponses);

  void runIfAbsent(
      String monitorType, Object monitorKey, @Nullable NotificationListener listener, Object... initializerParams);

  int numMonitors(String monitorType);

  MonitorStatus getStatus(String monitorType, Object monitorKey);

  /**
   * Stops the given monitor and removes it from the cache of monitors.
   *
   * @param monitorType
   * @param monitorKey
   * @return
   */
  Monitor stopAndRemoveMonitor(String monitorType, Object monitorKey);

  /**
   * Stops all monitors of the given type, without deleting the cache for the monitor type.
   *
   * @param monitorType
   */
  void stopAndRemoveMonitors(String monitorType);

  /**
   * Stops all monitors of the given type and deletes the cache for the monitor type.
   *
   * @param monitorType
   */
  void deleteMonitorType(String monitorType);

  /**
   * Stops all monitors and removes them from their caches, without deleting the caches themselves.
   */
  void stopAndRemoveAll();

  /**
   * Stops all monitors and deletes all monitor caches.
   */
  void deleteAll();
}
