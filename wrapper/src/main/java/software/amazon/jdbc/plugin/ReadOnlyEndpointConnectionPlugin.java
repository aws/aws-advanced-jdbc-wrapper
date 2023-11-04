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

package software.amazon.jdbc.plugin;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.AuroraDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class ReadOnlyEndpointConnectionPlugin extends AbstractConnectionPlugin {

  private static final String CLUSTER_INSTANCE_HOST_PATTERN = "clusterInstanceHostPattern";
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("initHostProvider");
          add("connect");
          add("forceConnect");
        }
      });

  private final PluginService pluginService;
  private HostListProviderService hostListProviderService;
  private final RdsUtils rdsHelper;

  public ReadOnlyEndpointConnectionPlugin(final PluginService pluginService) {
    this(pluginService, new RdsUtils());
  }

  private ReadOnlyEndpointConnectionPlugin(final PluginService pluginService, final RdsUtils rdsHelper) {
    this.pluginService = pluginService;
    this.rdsHelper = rdsHelper;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {

    this.hostListProviderService = hostListProviderService;
    initHostProviderFunc.call();
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectInternal(hostSpec, props, connectFunc);
  }

  private Connection connectInternal(
      HostSpec hostSpec,
      Properties props,
      JdbcCallable<Connection, SQLException> connectFunc) {
    try {
      final Connection currentConnection = connectFunc.call();

      if (currentConnection != null && this.rdsHelper.isReaderClusterDns(hostSpec.getHost())) {
        final Dialect dialect = this.pluginService.getDialect();
        String nodeIdQuery = "";

        if (dialect instanceof AuroraDialect) {
          final AuroraDialect auroraDialect = (AuroraDialect) dialect;
          nodeIdQuery = auroraDialect.getNodeIdQuery();
        }

        if (!StringUtils.isNullOrEmpty(nodeIdQuery)) {
          final String connectedHostName = getCurrentlyConnectedInstance(currentConnection, nodeIdQuery);

          // Replace the connection created using the read only endpoint with an instance endpoint using the queried
          // instance name
          if (!StringUtils.isNullOrEmpty(connectedHostName)) {
            final String pattern = props.getProperty(CLUSTER_INSTANCE_HOST_PATTERN);
            String instanceEndpoint = !StringUtils.isNullOrEmpty(pattern) ? pattern :
                this.rdsHelper.getRdsInstanceHostPattern(hostSpec.getHost());
            instanceEndpoint = instanceEndpoint.replace("?", connectedHostName);
            final HostSpec newHostSpec = this.pluginService.getHostSpecBuilder().host(instanceEndpoint).build();
            this.hostListProviderService.setInitialConnectionHostSpec(newHostSpec);
            return this.pluginService.connect(newHostSpec, props);
          }
        }
      }

      return currentConnection;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String getCurrentlyConnectedInstance(
      final Connection connection,
      final String nodeIdQuery) throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      final ResultSet rs = statement.executeQuery(nodeIdQuery);
      if (rs.next()) {
        return rs.getString(1);
      }
      return null;
    }
  }
}
