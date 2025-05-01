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
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.ServiceContainer;

public class ConnectionServiceImpl implements ConnectionService {
  protected final ServiceContainer serviceContainer;
  protected final ConnectionProvider connectionProvider;
  protected final TargetDriverDialect driverDialect;
  protected final String targetDriverProtocol;
  protected final ConnectionPluginManager pluginManager;
  protected final MonitorPluginService monitorPluginService;

  public ConnectionServiceImpl(
      ServiceContainer serviceContainer,
      ConnectionProvider connectionProvider,
      TargetDriverDialect driverDialect,
      Dialect dbDialect,
      String targetDriverProtocol,
      Properties props) throws SQLException {
    this.serviceContainer = serviceContainer;
    this.connectionProvider = connectionProvider;
    this.driverDialect = driverDialect;
    this.targetDriverProtocol = targetDriverProtocol;
    this.pluginManager = new ConnectionPluginManager(
        this.connectionProvider,
        null,
        null,
        // TODO: is it okay to share the telemetry factory?
        serviceContainer.getTelemetryFactory());
    this.monitorPluginService = new MonitorPluginService(
        this.serviceContainer,
        props,
        this.targetDriverProtocol,
        this.driverDialect,
        dbDialect);

    this.pluginManager.init(this.monitorPluginService, props, this.monitorPluginService, null);
  }

  @Override
  // TODO: do we need the props parameter or should we just use the props passed to the constructor? Not clear if they
  //  will differ.
  public Connection createAuxiliaryConnection(HostSpec hostSpec, Properties props) throws SQLException {
    return this.pluginManager.forceConnect(this.targetDriverProtocol, hostSpec, props, false, null);
  }
}
