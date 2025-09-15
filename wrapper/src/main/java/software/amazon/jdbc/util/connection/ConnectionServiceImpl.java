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
import software.amazon.jdbc.PartialPluginService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.FullServicesContainerImpl;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * @deprecated This class is deprecated and will be removed in a future version. Use
 * {@link software.amazon.jdbc.util.ServiceUtility#createServiceContainer} followed by
 * {@link PluginService#forceConnect} instead.
 */
@Deprecated
public class ConnectionServiceImpl implements ConnectionService {
  protected final String targetDriverProtocol;
  protected final ConnectionPluginManager pluginManager;
  protected final PluginService pluginService;

  /**
   * @deprecated Use {@link software.amazon.jdbc.util.ServiceUtility#createServiceContainer} instead.
   */
  @Deprecated
  public ConnectionServiceImpl(
      StorageService storageService,
      MonitorService monitorService,
      TelemetryFactory telemetryFactory,
      ConnectionProvider connectionProvider,
      String originalUrl,
      String targetDriverProtocol,
      TargetDriverDialect driverDialect,
      Dialect dbDialect,
      Properties props) throws SQLException {
    this.targetDriverProtocol = targetDriverProtocol;

    FullServicesContainer servicesContainer =
        new FullServicesContainerImpl(storageService, monitorService, connectionProvider, telemetryFactory);
    this.pluginManager = new ConnectionPluginManager(
        connectionProvider,
        null,
        null,
        telemetryFactory);
    servicesContainer.setConnectionPluginManager(this.pluginManager);

    Properties propsCopy = PropertyUtils.copyProperties(props);
    PartialPluginService partialPluginService = new PartialPluginService(
        servicesContainer,
        propsCopy,
        originalUrl,
        this.targetDriverProtocol,
        driverDialect,
        dbDialect
    );

    servicesContainer.setHostListProviderService(partialPluginService);
    servicesContainer.setPluginService(partialPluginService);
    servicesContainer.setPluginManagerService(partialPluginService);

    this.pluginService = partialPluginService;
    this.pluginManager.init(servicesContainer, propsCopy, partialPluginService, null);
  }

  @Override
  @Deprecated
  public Connection open(HostSpec hostSpec, Properties props) throws SQLException {
    return this.pluginManager.forceConnect(this.targetDriverProtocol, hostSpec, props, true, null);
  }

  @Override
  @Deprecated
  public PluginService getPluginService() {
    return this.pluginService;
  }
}
