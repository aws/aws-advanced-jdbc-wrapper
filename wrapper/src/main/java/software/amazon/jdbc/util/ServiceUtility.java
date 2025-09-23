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
import java.util.concurrent.locks.ReentrantLock;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.PartialPluginService;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class ServiceUtility {
  private static volatile ServiceUtility instance;
  private static final ReentrantLock initLock = new ReentrantLock();

  private ServiceUtility() {
    if (instance != null) {
      throw new IllegalStateException("ServiceContainerUtility singleton instance already exists.");
    }
  }

  public static ServiceUtility getInstance() {
    if (instance != null) {
      return instance;
    }

    initLock.lock();
    try {
      if (instance == null) {
        instance = new ServiceUtility();
      }
    } finally {
      initLock.unlock();
    }

    return instance;
  }

  public FullServicesContainer createStandardServiceContainer(
      StorageService storageService,
      MonitorService monitorService,
      ConnectionWrapper connectionWrapper,
      ConnectionProvider defaultConnectionProvider,
      ConnectionProvider effectiveConnectionProvider,
      TelemetryFactory telemetryFactory,
      String originalUrl,
      String targetDriverProtocol,
      TargetDriverDialect driverDialect,
      Properties props,
      @Nullable ConfigurationProfile configurationProfile) throws SQLException {
    FullServicesContainer servicesContainer =
        new FullServicesContainerImpl(storageService, monitorService, defaultConnectionProvider, telemetryFactory);

    ConnectionPluginManager pluginManager =
        new ConnectionPluginManager(defaultConnectionProvider, effectiveConnectionProvider, telemetryFactory);
    servicesContainer.setConnectionPluginManager(pluginManager);

    PluginServiceImpl pluginServiceImpl = new PluginServiceImpl(
        servicesContainer,
        props,
        originalUrl,
        targetDriverProtocol,
        driverDialect,
        configurationProfile
    );

    servicesContainer.setHostListProviderService(pluginServiceImpl);
    servicesContainer.setPluginService(pluginServiceImpl);
    servicesContainer.setPluginManagerService(pluginServiceImpl);

    pluginManager.init(servicesContainer, props, pluginServiceImpl, configurationProfile);
    return servicesContainer;
  }

  public FullServicesContainer createMinimalServiceContainer(
      StorageService storageService,
      MonitorService monitorService,
      ConnectionProvider connectionProvider,
      TelemetryFactory telemetryFactory,
      String originalUrl,
      String targetDriverProtocol,
      TargetDriverDialect driverDialect,
      Dialect dbDialect,
      Properties props) throws SQLException {
    FullServicesContainer serviceContainer =
        new FullServicesContainerImpl(storageService, monitorService, connectionProvider, telemetryFactory);
    ConnectionPluginManager pluginManager =
        new ConnectionPluginManager(connectionProvider, null, telemetryFactory);
    serviceContainer.setConnectionPluginManager(pluginManager);

    PartialPluginService partialPluginService = new PartialPluginService(
        serviceContainer,
        props,
        originalUrl,
        targetDriverProtocol,
        driverDialect,
        dbDialect
    );

    serviceContainer.setHostListProviderService(partialPluginService);
    serviceContainer.setPluginService(partialPluginService);
    serviceContainer.setPluginManagerService(partialPluginService);

    pluginManager.init(serviceContainer, props, partialPluginService, null);
    return serviceContainer;
  }
}
