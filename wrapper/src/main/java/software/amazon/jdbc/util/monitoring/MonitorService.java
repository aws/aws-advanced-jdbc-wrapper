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
   * monitors to the monitor service, so that the monitor service knows when a running monitor should be stopped.
   * Expected monitor types ("topology" and "customEndpoint") will be added automatically during driver initialization,
   * but this method can be called by users if they want to add a new monitor type.
   *
   * @param monitorType       a String representing the monitor type, eg "customEndpoint".
   * @param errorResponses    a Set defining actions to take if the monitor is in an error state.
   * @param expirationTimeNs  how long a monitor should be stored before expiring, in nanoseconds. If the monitor is
   *                          expired and shouldDisposeFunc returns `true`, the monitor will be stopped.
   * @param shouldDisposeFunc a function defining whether an item should be stopped if expired. If `null` is passed, the
   *                          monitor will always be stopped if the monitor is expired.
   */
  void registerMonitorTypeIfAbsent(
      String monitorType,
      Set<MonitorErrorResponse> errorResponses,
      long expirationTimeNs,
      @Nullable ShouldDisposeFunc<Monitor> shouldDisposeFunc);

  /**
   * Creates and starts the given monitor if it does not already exist and stores it under the given monitor type and
   * key.
   *
   * @param monitorType     a String representing the monitor type, eg "customEndpoint".
   * @param key             the key for the monitor, eg
   *                        "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param monitorSupplier an initialization lambda that can be used to create the monitor if it is absent.
   */
  void runIfAbsent(
      String monitorType,
      Object key,
      Supplier<Monitor> monitorSupplier);

  /**
   * Stops the given monitor and removes it from the monitor service.
   *
   * @param monitorType a String representing the monitor type, eg "customEndpoint".
   * @param key         the key for the monitor, eg
   *                    "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   */
  void stopAndRemove(String monitorType, Object key);

  /**
   * Stops all monitors for the given type and removes them from the monitor service.
   *
   * @param monitorType a String representing the monitor type, eg "customEndpoint".
   */
  void stopAndRemoveMonitors(String monitorType);

  /**
   * Stops all monitors and removes them from the monitor service.
   */
  void stopAndRemoveAll();
}
