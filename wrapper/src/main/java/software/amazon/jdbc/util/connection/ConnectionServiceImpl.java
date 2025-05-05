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

package software.amazon.jdbc.util.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.MonitorPluginService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.ServiceContainer;
import software.amazon.jdbc.util.ServiceContainerImpl;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class ConnectionServiceImpl implements ConnectionService {
  protected final String targetDriverProtocol;
  protected final ConnectionPluginManager pluginManager;

  public ConnectionServiceImpl(
      StorageService storageService,
      MonitorService monitorService,
      TelemetryFactory telemetryFactory,
      ConnectionProvider connectionProvider,
      TargetDriverDialect driverDialect,
      String targetDriverProtocol,
      String originalUrl,
      Properties props) throws SQLException {
    this.targetDriverProtocol = targetDriverProtocol;

    ServiceContainer serviceContainer = new ServiceContainerImpl(storageService, monitorService, telemetryFactory);
    this.pluginManager = new ConnectionPluginManager(
        connectionProvider,
        null,
        null,
        // TODO: is it okay to share the telemetry factory?
        telemetryFactory);
    serviceContainer.setConnectionPluginManager(this.pluginManager);

    MonitorPluginService monitorPluginService = new MonitorPluginService(
        serviceContainer,
        props,
        originalUrl,
        this.targetDriverProtocol,
        driverDialect);
    serviceContainer.setHostListProviderService(monitorPluginService);
    serviceContainer.setPluginService(monitorPluginService);
    serviceContainer.setPluginManagerService(monitorPluginService);

    this.pluginManager.init(monitorPluginService, props, monitorPluginService, null);
  }

  @Override
  public Connection createAuxiliaryConnection(HostSpec hostSpec, Properties props) throws SQLException {
    return this.pluginManager.forceConnect(this.targetDriverProtocol, hostSpec, props, true, null);
  }
}
