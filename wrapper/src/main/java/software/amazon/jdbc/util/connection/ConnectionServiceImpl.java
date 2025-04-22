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
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.ServiceContainer;
import software.amazon.jdbc.util.ServiceContainerImpl;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class ConnectionServiceImpl implements ConnectionService {
  protected ServiceContainer serviceContainer;
  protected ConnectionProvider connectionProvider;
  protected TargetDriverDialect driverDialect;
  protected String targetDriverProtocol;

  public ConnectionServiceImpl(
      ServiceContainer serviceContainer,
      ConnectionProvider connectionProvider,
      TargetDriverDialect driverDialect,
      String targetDriverProtocol) {
    this.serviceContainer = serviceContainer;
    this.connectionProvider = connectionProvider;
    this.driverDialect = driverDialect;
    this.targetDriverProtocol = targetDriverProtocol;
  }

  @Override
  public Connection createAuxiliaryConnection(HostSpec hostSpec, Properties props) throws SQLException {
    // TODO: is it necessary to create a copy of the properties or are the passed props already a copy?
    final Properties auxiliaryProps = PropertyUtils.copyProperties(props);
    final String databaseName = PropertyDefinition.DATABASE.getString(auxiliaryProps) != null
            ? PropertyDefinition.DATABASE.getString(auxiliaryProps)
            : "";
    final String connString = this.targetDriverProtocol + hostSpec.getUrl() + databaseName;
    PropertyDefinition.IS_AUXILIARY_CONNECTION.set(auxiliaryProps, "true");

    // The auxiliary connection should have its own separate service container since the original container holds
    // services that should not be shared between connections (for example, PluginService).
    ServiceContainerImpl auxiliaryServiceContainer = new ServiceContainerImpl(
        this.serviceContainer.getStorageService(),
        this.serviceContainer.getMonitorService(),
        new DefaultTelemetryFactory(auxiliaryProps));

    // TODO: does the auxiliary connection need its own ConnectionProvider/TargetDriverDialect, or is it okay to share?
    return new ConnectionWrapper(
        auxiliaryServiceContainer,
        auxiliaryProps,
        connString,
        this.connectionProvider,
        null,
        this.driverDialect,
        null
    );
  }
}
