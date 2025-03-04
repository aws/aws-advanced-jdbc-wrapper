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
import software.amazon.jdbc.util.ShouldDisposeFunc;

public interface MonitorService {
  /**
   * Register a new monitor type with the monitor service. This method needs to be called before adding new types of
   * monitors to the monitor service, so that the monitor service knows when to dispose of a monitor.
   * Expected monitor types ("topology" and "customEndpoint") will be added automatically during driver initialization,
   * but this method can be called by users if they want to add a new monitor type.
   *
   * @param monitorClass         the class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param timeToLiveNanos      how long a monitor should be stored before being considered expired, in nanoseconds. If
   *                             the monitor is expired and shouldDisposeFunc returns `true`, the monitor will be
   *                             stopped.
   * @param inactiveTimeoutNanos a duration in nanoseconds defining the maximum amount of time that a monitor should
   *                             take between updating its last-updated timestamp. If a monitor has not updated its
   *                             last-updated timestamp within this value it will be considered stuck.
   * @param errorResponses       a Set defining actions to take if the monitor is in an error state.
   * @param shouldDisposeFunc    a function defining whether an item should be stopped if expired. If `null` is
   *                             passed, the monitor will always be stopped if the monitor is expired.
   */
  <T extends Monitor> void registerMonitorTypeIfAbsent(
      Class<T> monitorClass,
      long timeToLiveNanos,
      long inactiveTimeoutNanos,
      Set<MonitorErrorResponse> errorResponses,
      @Nullable ShouldDisposeFunc<T> shouldDisposeFunc);

  /**
   * Creates and starts the given monitor if it does not already exist and stores it under the given monitor type and
   * key. If the monitor already exists, its time-to-live duration will be renewed, even if it was already expired.
   *
   * @param monitorClass    the class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param key             the key for the monitor, eg
   *                        "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param monitorSupplier a supplier lambda that can be used to create the monitor if it is absent.
   */
  <T extends Monitor> void runIfAbsent(Class<T> monitorClass, Object key, Supplier<T> monitorSupplier);

  /**
   * Stops the given monitor and removes it from the monitor service.
   *
   * @param monitorClass the class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param key          the key for the monitor, eg
   *                     "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   */
  <T extends Monitor> void stopAndRemove(Class<T> monitorClass, Object key);

  /**
   * Stops all monitors for the given type and removes them from the monitor service.
   *
   * @param monitorClass the class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   */
  <T extends Monitor> void stopAndRemoveMonitors(Class<T> monitorClass);

  /**
   * Stops all monitors and removes them from the monitor service.
   */
  void stopAndRemoveAll();
}
