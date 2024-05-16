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

package software.amazon.jdbc.plugin.endpoint;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class EndpointConnectionPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(EndpointConnectionPlugin.class.getName());
  protected static final AwsWrapperProperty INTERVAL_MILLIS = new AwsWrapperProperty(
      "endpointMonitorIntervalMs",
      "30000",
      "Interval in millis between polling for endpoints to the database.");
  protected final PluginService pluginService;
  protected final @NonNull Properties properties;
  private final @NonNull Supplier<EndpointService> endpointServiceSupplier;
  private EndpointService endpointService;
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
        }
      });

  static {
    PropertyDefinition.registerPluginProperties(EndpointConnectionPlugin.class);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  public EndpointConnectionPlugin(final PluginService pluginService, final @NonNull Properties properties) {
    this(pluginService,
        properties,
        EndpointServiceImpl::new);
  }

  public EndpointConnectionPlugin(
      final PluginService pluginService,
      final @NonNull Properties properties,
      final @NonNull Supplier<EndpointService> endpointServiceSupplier) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.endpointServiceSupplier = endpointServiceSupplier;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, isInitialConnection, connectFunc);
  }

  @Override
  public Connection forceConnect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, isInitialConnection, forceConnectFunc);
  }

  private Connection connectInternal(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection,
          SQLException> connectFunc) throws SQLException {
    initEndpointMonitorService();
    if (isInitialConnection) {
      this.endpointService.startMonitoring(pluginService, hostSpec, properties, INTERVAL_MILLIS.getInteger(properties));
    }

    List<HostSpec> endpoints = this.endpointService.getEndpoints(
        this.pluginService.getHostListProvider().getClusterId(), props);

    if (endpoints.isEmpty()) {
      LOGGER.warning(Messages.get("EndpointConnectionPlugin.emptyEndpointCache"));
      return connectFunc.call();
    } else if (endpoints.contains(hostSpec)) {
      return connectFunc.call();
    }

    final HostSpec selectedHostSpec = this.pluginService.getHostSpecByStrategy(endpoints,
        HostRole.WRITER, HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT);

    LOGGER.finest(Messages.get("EndpointConnectionPlugin.selectedHost", new Object[] {selectedHostSpec.getHost()}));

    return pluginService.connect(selectedHostSpec, props);
  }

  private void initEndpointMonitorService() {
    if (endpointService == null) {
      this.endpointService = this.endpointServiceSupplier.get();
    }
  }
}
