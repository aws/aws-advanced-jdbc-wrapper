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

package software.amazon.jdbc.plugin.efm2;

import java.util.Properties;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.ServicesContainerPluginFactory;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointPlugin;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;

/** Class initializing a {@link HostMonitoringConnectionPlugin}. */
public class HostMonitoringConnectionPluginFactory implements ServicesContainerPluginFactory {
  @Override
  public ConnectionPlugin getInstance(final PluginService pluginService, final Properties props) {
    throw new UnsupportedOperationException(
        Messages.get(
            "ServiceContainerPluginFactory.serviceContainerRequired",
            new Object[] {"efm2.HostMonitoringConnectionPlugin"}));
  }

  @Override
  public ConnectionPlugin getInstance(final FullServicesContainer servicesContainer, final Properties props) {
    return new HostMonitoringConnectionPlugin(servicesContainer, props);
  }
}
