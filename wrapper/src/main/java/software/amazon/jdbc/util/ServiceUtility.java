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
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.MinimalPluginService;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

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

  public ServiceContainer createStandardServiceContainer(
      StorageService storageService,
      MonitorService monitorService,
      ConnectionProvider defaultConnectionProvider,
      TelemetryFactory telemetryFactory,
      String originalUrl,
      String targetDriverProtocol,
      TargetDriverDialect driverDialect,
      Properties props) throws SQLException {
    ServiceContainer serviceContainer =
        new StandardServiceContainer(storageService, monitorService, defaultConnectionProvider, telemetryFactory);
    ConnectionPluginManager pluginManager =
        new ConnectionPluginManager(defaultConnectionProvider, null, null, telemetryFactory);
    serviceContainer.setConnectionPluginManager(pluginManager);

    PluginServiceImpl pluginServiceImpl = new PluginServiceImpl(
        serviceContainer,
        props,
        originalUrl,
        targetDriverProtocol,
        driverDialect,
        null
    );

    serviceContainer.setHostListProviderService(pluginServiceImpl);
    serviceContainer.setPluginService(pluginServiceImpl);
    serviceContainer.setPluginManagerService(pluginServiceImpl);

    pluginManager.init(serviceContainer, props, pluginServiceImpl, null);
    return serviceContainer;
  }

  public ServiceContainer createMinimalServiceContainer(
      StorageService storageService,
      MonitorService monitorService,
      ConnectionProvider connectionProvider,
      TelemetryFactory telemetryFactory,
      String originalUrl,
      String targetDriverProtocol,
      TargetDriverDialect driverDialect,
      Dialect dbDialect,
      Properties props) throws SQLException {
    ServiceContainer serviceContainer =
        new StandardServiceContainer(storageService, monitorService, connectionProvider, telemetryFactory);
    ConnectionPluginManager pluginManager =
        new ConnectionPluginManager(connectionProvider, null, null, telemetryFactory);
    serviceContainer.setConnectionPluginManager(pluginManager);

    PluginManagerService pluginManagerService = new MinimalPluginService(
        serviceContainer,
        props,
        originalUrl,
        targetDriverProtocol,
        driverDialect,
        dbDialect
    );

    pluginManager.init(serviceContainer, props, pluginManagerService, null);
    return serviceContainer;
  }
}
