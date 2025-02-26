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

import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class ServiceContainerImpl implements ServiceContainer {
  private final StorageService storageService;
  private final ConnectionPluginManager connectionPluginManager;
  private final TelemetryFactory telemetryFactory;

  private HostListProviderService hostListProviderService;
  private PluginService pluginService;
  private PluginManagerService pluginManagerService;

  public ServiceContainerImpl(
      StorageService storageService,
      ConnectionPluginManager connectionPluginManager,
      TelemetryFactory telemetryFactory,
      HostListProviderService hostListProviderService,
      PluginService pluginService,
      PluginManagerService pluginManagerService) {
    this(storageService, connectionPluginManager, telemetryFactory);
    this.hostListProviderService = hostListProviderService;
    this.pluginService = pluginService;
    this.pluginManagerService = pluginManagerService;
  }

  public ServiceContainerImpl(
      StorageService storageService,
      ConnectionPluginManager connectionPluginManager,
      TelemetryFactory telemetryFactory) {
    this.storageService = storageService;
    this.connectionPluginManager = connectionPluginManager;
    this.telemetryFactory = telemetryFactory;
  }

  public StorageService getStorageService() {
    return storageService;
  }

  public ConnectionPluginManager getConnectionPluginManager() {
    return connectionPluginManager;
  }

  public TelemetryFactory getTelemetryFactory() {
    return telemetryFactory;
  }

  public HostListProviderService getHostListProviderService() {
    return hostListProviderService;
  }

  public PluginService getPluginService() {
    return pluginService;
  }

  public PluginManagerService getPluginManagerService() {
    return pluginManagerService;
  }

  @Override
  public StorageService setStorageService(StorageService storageService) {
    return null;
  }

  @Override
  public ConnectionPluginManager setConnectionPluginManager(ConnectionProviderManager connectionPluginManager) {
    return null;
  }

  @Override
  public TelemetryFactory setTelemetryFactory(TelemetryFactory telemetryFactory) {
    return null;
  }


  public void setHostListProviderService(HostListProviderService hostListProviderService) {
    this.hostListProviderService = hostListProviderService;
  }

  public void setPluginService(PluginService pluginService) {
    this.pluginService = pluginService;
  }

  public void setPluginManagerService(PluginManagerService pluginManagerService) {
    this.pluginManagerService = pluginManagerService;
  }
}
