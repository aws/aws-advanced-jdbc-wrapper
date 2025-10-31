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

import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public interface MonitorService {
  /**
   * Registers a new monitor type with the monitor service. This method needs to be called before adding new types of
   * monitors to the monitor service, so that the monitor service knows when to dispose of a monitor. Expected monitor
   * types will be added automatically during driver initialization, but this method can be called by users if they want
   * to add a new monitor type.
   *
   * @param monitorClass           the class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param expirationTimeoutNanos how long a monitor should be stored without use before being considered expired, in
   *                               nanoseconds. Expired monitors may be removed and stopped.
   * @param heartbeatTimeoutNanos  a duration in nanoseconds defining the maximum amount of time that a monitor should
   *                               take between updating its last-updated timestamp. If a monitor has not updated its
   *                               last-updated timestamp within this duration it will be considered stuck.
   * @param errorResponses         a {@link EnumSet} defining actions to take if the monitor is stuck or in an error
   *                               state.
   * @param producedDataClass      the class of data produced by the monitor.
   * @param <T>                    the type of the monitor.
   */
  <T extends Monitor> void registerMonitorTypeIfAbsent(
      Class<T> monitorClass,
      long expirationTimeoutNanos,
      long heartbeatTimeoutNanos,
      EnumSet<MonitorErrorResponse> errorResponses,
      @Nullable Class<?> producedDataClass);

  /**
   * Creates and starts the given monitor if it does not already exist and stores it under the given monitor type and
   * key. If the monitor already exists, its expiration time will be renewed, even if it was already expired.
   *
   * @param monitorClass              the concrete class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param key                       the key for the monitor, eg
   *                                  "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param servicesContainer         the source service container for the monitor. The new monitor will create its own
   *                                  service container but will reuse some components from the source container.
   * @param originalProps             the properties of the original database connection.
   * @param initializer               an initializer function to use to create the monitor if it does not already exist.
   * @param <T>                       the type of the monitor.
   * @return the new or existing monitor.
   * @throws SQLException if an error occurs while trying to create the monitor.
   */
  <T extends Monitor> T runIfAbsent(
      Class<T> monitorClass,
      Object key,
      FullServicesContainer servicesContainer,
      Properties originalProps,
      MonitorInitializer initializer) throws SQLException;

  /**
   * Creates and starts the given monitor if it does not already exist and stores it under the given monitor type and
   * key. If the monitor already exists, its expiration time will be renewed, even if it was already expired.
   *
   * @param monitorClass              the concrete class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param key                       the key for the monitor, eg
   *                                  "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param storageService            the storage service for the monitor to use.
   * @param telemetryFactory          the telemetry factory for creating telemetry data.
   * @param defaultConnectionProvider the connection provider to use to create new connections if the monitor
   *                                  requires it.
   * @param originalUrl               the URL of the original database connection.
   * @param driverProtocol            the protocol for the underlying target driver.
   * @param driverDialect             the target driver dialect.
   * @param dbDialect                 the database dialect.
   * @param originalProps             the properties of the original database connection.
   * @param initializer               an initializer function to use to create the monitor if it does not already exist.
   * @param <T>                       the type of the monitor.
   * @return the new or existing monitor.
   * @throws SQLException if an error occurs while trying to create the monitor.
   */
  <T extends Monitor> T runIfAbsent(
      Class<T> monitorClass,
      Object key,
      StorageService storageService,
      TelemetryFactory telemetryFactory,
      ConnectionProvider defaultConnectionProvider,
      String originalUrl,
      String driverProtocol,
      TargetDriverDialect driverDialect,
      Dialect dbDialect,
      Properties originalProps,
      MonitorInitializer initializer) throws SQLException;

  /**
   * Gets the monitor stored at the given key.
   *
   * @param monitorClass the expected class of the monitor.
   * @param key          the key for the monitor.
   * @param <T>          the type of the monitor.
   * @return the monitor stored at the given key.
   */
  @Nullable
  <T extends Monitor> T get(Class<T> monitorClass, Object key);

  /**
   * Removes the monitor stored at the given key. If the expected monitor class does not match the actual monitor class
   * no action will be performed.
   *
   * @param monitorClass the expected class of the monitor.
   * @param key          the key for the monitor.
   * @param <T>          the type of the monitor.
   * @return the monitor that was removed. Returns null if there was no monitor at the given key or the expected monitor
   *     class did not match the actual monitor class.
   */
  @Nullable
  <T extends Monitor> T remove(Class<T> monitorClass, Object key);

  /**
   * Stops the given monitor and removes it from the monitor service.
   *
   * @param monitorClass the class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param key          the key for the monitor, eg
   *                     "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param <T>          the type of the monitor.
   */
  <T extends Monitor> void stopAndRemove(Class<T> monitorClass, Object key);

  /**
   * Stops all monitors for the given type and removes them from the monitor service.
   *
   * @param monitorClass the class of the monitor, eg `CustomEndpointMonitorImpl.class`.
   * @param <T>          the type of the monitor.
   */
  <T extends Monitor> void stopAndRemoveMonitors(Class<T> monitorClass);

  /**
   * Stops all monitors and removes them from the monitor service.
   */
  void stopAndRemoveAll();

  /**
   * Releases any resources opened by the monitor service, stops all monitors, and removes all monitors from the monitor
   * service.
   */
  void releaseResources();
}
