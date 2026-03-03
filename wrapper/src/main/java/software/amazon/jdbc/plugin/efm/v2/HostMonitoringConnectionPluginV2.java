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

package software.amazon.jdbc.plugin.efm.v2;

import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Monitor the server while the connection is executing methods for more sophisticated failure
 * detection.
 */
public class HostMonitoringConnectionPluginV2 extends HostMonitoringConnectionBasePlugin {

  private static final Logger LOGGER =
      Logger.getLogger(HostMonitoringConnectionPluginV2.class.getName());

  /**
   * Initialize the node monitoring plugin.
   *
   * @param servicesContainer The service container for the services required by this class.
   * @param properties        The property set used to initialize the active connection.
   */
  public HostMonitoringConnectionPluginV2(
      final @NonNull FullServicesContainer servicesContainer, final @NonNull Properties properties) {
    super(
        servicesContainer,
        properties,
        () -> new HostMonitorServiceV2Impl(servicesContainer, properties),
        new RdsUtils());
  }
}
