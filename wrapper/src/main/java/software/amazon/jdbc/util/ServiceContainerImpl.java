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
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.connection.ConnectionService;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class ServiceContainerImpl implements ServiceContainer {
  private StorageService storageService;
  private MonitorService monitorService;
  private TelemetryFactory telemetryFactory;
  private ConnectionService connectionService;
  private ConnectionPluginManager connectionPluginManager;
  private HostListProviderService hostListProviderService;
  private PluginService pluginService;
  private PluginManagerService pluginManagerService;

  public ServiceContainerImpl(
      StorageService storageService,
      MonitorService monitorService,
      TelemetryFactory telemetryFactory,
      ConnectionService connectionService,
      ConnectionPluginManager connectionPluginManager,
      HostListProviderService hostListProviderService,
      PluginService pluginService,
      PluginManagerService pluginManagerService) {
    this(storageService, monitorService, telemetryFactory);
    this.connectionService = connectionService;
    this.connectionPluginManager = connectionPluginManager;
    this.hostListProviderService = hostListProviderService;
    this.pluginService = pluginService;
    this.pluginManagerService = pluginManagerService;
  }

  public ServiceContainerImpl(
      StorageService storageService,
      MonitorService monitorService,
      TelemetryFactory telemetryFactory) {
    this.storageService = storageService;
    this.monitorService = monitorService;
    this.telemetryFactory = telemetryFactory;
  }

  @Override
  public StorageService getStorageService() {
    return this.storageService;
  }

  @Override
  public MonitorService getMonitorService() {
    return this.monitorService;
  }

  @Override
  public TelemetryFactory getTelemetryFactory() {
    return this.telemetryFactory;
  }

  @Override
  public ConnectionService getConnectionService() {
    return connectionService;
  }

  @Override
  public ConnectionPluginManager getConnectionPluginManager() {
    return this.connectionPluginManager;
  }

  @Override
  public HostListProviderService getHostListProviderService() {
    return this.hostListProviderService;
  }

  @Override
  public PluginService getPluginService() {
    return this.pluginService;
  }

  @Override
  public PluginManagerService getPluginManagerService() {
    return this.pluginManagerService;
  }

  @Override
  public void setMonitorService(MonitorService monitorService) {
    this.monitorService = monitorService;
  }

  @Override
  public void setStorageService(StorageService storageService) {
    this.storageService = storageService;
  }

  @Override
  public void setTelemetryFactory(TelemetryFactory telemetryFactory) {
    this.telemetryFactory = telemetryFactory;
  }

  @Override
  public void setConnectionService(ConnectionService connectionService) {
    this.connectionService = connectionService;
  }

  @Override
  public void setConnectionPluginManager(ConnectionPluginManager connectionPluginManager) {
    this.connectionPluginManager = connectionPluginManager;
  }

  @Override
  public void setHostListProviderService(HostListProviderService hostListProviderService) {
    this.hostListProviderService = hostListProviderService;
  }

  @Override
  public void setPluginService(PluginService pluginService) {
    this.pluginService = pluginService;
  }

  @Override
  public void setPluginManagerService(PluginManagerService pluginManagerService) {
    this.pluginManagerService = pluginManagerService;
  }
}
