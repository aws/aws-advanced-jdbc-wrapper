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

package software.amazon.jdbc.util;

import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.PartialPluginService;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.HostListProviderSupplier;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class ServiceUtility {
  private static final ServiceUtility instance = new ServiceUtility();

  private ServiceUtility() {}

  public static ServiceUtility getInstance() {
    return instance;
  }

  public FullServicesContainer createStandardServiceContainer(
      StorageService storageService,
      MonitorService monitorService,
      EventPublisher eventPublisher,
      ConnectionProvider defaultConnectionProvider,
      ConnectionProvider effectiveConnectionProvider,
      TelemetryFactory telemetryFactory,
      String originalUrl,
      String targetDriverProtocol,
      TargetDriverDialect driverDialect,
      Properties props,
      @Nullable ConfigurationProfile configurationProfile) throws SQLException {
    FullServicesContainer servicesContainer =
        new FullServicesContainerImpl(
            storageService, monitorService, eventPublisher, defaultConnectionProvider, telemetryFactory);

    ConnectionPluginManager pluginManager =
        new ConnectionPluginManager(props, telemetryFactory, defaultConnectionProvider, effectiveConnectionProvider);
    servicesContainer.setConnectionPluginManager(pluginManager);

    PluginServiceImpl pluginService = new PluginServiceImpl(
        servicesContainer,
        props,
        originalUrl,
        targetDriverProtocol,
        driverDialect,
        configurationProfile
    );

    servicesContainer.setHostListProviderService(pluginService);
    servicesContainer.setPluginService(pluginService);
    servicesContainer.setPluginManagerService(pluginService);

    pluginManager.initPlugins(servicesContainer, configurationProfile);
    final HostListProviderSupplier supplier = pluginService.getDialect().getHostListProviderSupplier();
    if (supplier != null) {
      final HostListProvider provider = supplier.getProvider(props, originalUrl, servicesContainer);
      pluginService.setHostListProvider(provider);
    }

    pluginManager.initHostProvider(targetDriverProtocol, originalUrl, props, pluginService);
    // This call initializes pluginService.allHosts with the stored topology if it exists or with the initial host spec
    // if it doesn't exist. Plugins may require this information even before connecting.
    pluginService.refreshHostList();
    return servicesContainer;
  }

  public FullServicesContainer createMinimalServiceContainer(
      StorageService storageService,
      MonitorService monitorService,
      EventPublisher eventPublisher,
      ConnectionProvider connectionProvider,
      TelemetryFactory telemetryFactory,
      String originalUrl,
      String targetDriverProtocol,
      TargetDriverDialect driverDialect,
      Dialect dbDialect,
      Properties props) throws SQLException {
    FullServicesContainer serviceContainer =
        new FullServicesContainerImpl(
            storageService, monitorService, eventPublisher, connectionProvider, telemetryFactory);
    ConnectionPluginManager pluginManager =
        new ConnectionPluginManager(props, telemetryFactory, connectionProvider, null);
    serviceContainer.setConnectionPluginManager(pluginManager);

    PartialPluginService pluginService = new PartialPluginService(
        serviceContainer,
        props,
        originalUrl,
        targetDriverProtocol,
        driverDialect,
        dbDialect
    );

    serviceContainer.setHostListProviderService(pluginService);
    serviceContainer.setPluginService(pluginService);
    serviceContainer.setPluginManagerService(pluginService);

    pluginManager.initPlugins(serviceContainer, null);
    return serviceContainer;
  }

  public FullServicesContainer createMinimalServiceContainer(FullServicesContainer servicesContainer, Properties props)
      throws SQLException {
    return createMinimalServiceContainer(
        servicesContainer.getStorageService(),
        servicesContainer.getMonitorService(),
        servicesContainer.getEventPublisher(),
        servicesContainer.getPluginService().getDefaultConnectionProvider(),
        servicesContainer.getTelemetryFactory(),
        servicesContainer.getPluginService().getOriginalUrl(),
        servicesContainer.getPluginService().getDriverProtocol(),
        servicesContainer.getPluginService().getTargetDriverDialect(),
        servicesContainer.getPluginService().getDialect(),
        props
    );
  }
}
