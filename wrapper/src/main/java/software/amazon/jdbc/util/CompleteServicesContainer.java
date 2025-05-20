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
import software.amazon.jdbc.util.monitoring.CoreMonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public interface CompleteServicesContainer {
  StorageService getStorageService();

  CoreMonitorService getMonitorService();

  TelemetryFactory getTelemetryFactory();

  ConnectionPluginManager getConnectionPluginManager();

  HostListProviderService getHostListProviderService();

  PluginService getPluginService();

  PluginManagerService getPluginManagerService();

  void setMonitorService(CoreMonitorService monitorService);

  void setStorageService(StorageService storageService);

  void setTelemetryFactory(TelemetryFactory telemetryFactory);

  void setConnectionPluginManager(ConnectionPluginManager connectionPluginManager);

  void setHostListProviderService(HostListProviderService hostListProviderService);

  void setPluginService(PluginService pluginService);

  void setPluginManagerService(PluginManagerService pluginManagerService);
}
