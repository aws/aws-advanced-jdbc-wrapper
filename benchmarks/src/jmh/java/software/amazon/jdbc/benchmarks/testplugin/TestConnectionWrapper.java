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

package software.amazon.jdbc.benchmarks.testplugin;

import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

// Test class allowing for mocks to be used with ConnectionWrapper logic
public class TestConnectionWrapper extends ConnectionWrapper {
  protected final ConnectionUrlParser urlParser = new ConnectionUrlParser();

  public TestConnectionWrapper(
      @NonNull final Properties props,
      @NonNull final String url,
      @NonNull final ConnectionProvider defaultConnectionProvider,
      @NonNull final ConnectionPluginManager connectionPluginManager,
      @NonNull final TelemetryFactory telemetryFactory,
      @NonNull final PluginService pluginService,
      @NonNull final HostListProviderService hostListProviderService,
      @NonNull final PluginManagerService pluginManagerService,
      @NonNull final StorageService storageService,
      @NonNull final MonitorService monitorService)
      throws SQLException {
    super(
        props,
        url,
        defaultConnectionProvider,
        connectionPluginManager,
        telemetryFactory,
        pluginService,
        hostListProviderService,
        pluginManagerService,
        storageService,
        monitorService);
  }
}
