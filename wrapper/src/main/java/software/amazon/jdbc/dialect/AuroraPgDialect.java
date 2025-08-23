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

package software.amazon.jdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.util.DriverInfo;

/**
 * Suitable for the following AWS PG configurations.
 * - Regional Cluster
 */
public class AuroraPgDialect extends PgDialect implements AuroraLimitlessDialect, BlueGreenDialect {
  private static final Logger LOGGER = Logger.getLogger(AuroraPgDialect.class.getName());

  private static final String extensionsSql =
      "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_settings "
          + "WHERE name='rds.extensions'";

  private static final String topologySql = "SELECT 1 FROM aurora_replica_status() LIMIT 1";

  private static final String TOPOLOGY_QUERY =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
          + "FROM aurora_replica_status() "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "OR LAST_UPDATE_TIMESTAMP IS NULL";

  private static final String IS_WRITER_QUERY =
      "SELECT SERVER_ID FROM aurora_replica_status() "
          + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = aurora_db_instance_identifier()";

  private static final String NODE_ID_QUERY = "SELECT aurora_db_instance_identifier()";
  private static final String IS_READER_QUERY = "SELECT pg_is_in_recovery()";
  protected static final String LIMITLESS_ROUTER_ENDPOINT_QUERY =
      "select router_endpoint, load from aurora_limitless_router_endpoints()";

  private static final String BG_STATUS_QUERY =
      "SELECT * FROM get_blue_green_fast_switchover_metadata('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')";

  private static final String TOPOLOGY_TABLE_EXIST_QUERY =
      "SELECT 'get_blue_green_fast_switchover_metadata'::regproc";

  @Override
  public boolean isDialect(final Connection connection, final Properties properties) {
    if (!super.isDialect(connection, properties)) {
      return false;
    }

    try {
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(extensionsSql)) {
        if (!rs.next()) {
          return false;
        }
        final boolean auroraUtils = rs.getBoolean("aurora_stat_utils");
        LOGGER.finest(() -> String.format("auroraUtils: %b", auroraUtils));
        if (!auroraUtils) {
          return false;
        }
      }

      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(topologySql)) {
        if (rs.next()) {
          LOGGER.finest(() -> "hasTopology: true");
          return true;
        }
        LOGGER.finest(() -> "hasTopology: false");
      }
    } catch (SQLException ex) {
      // do nothing
    }
    return false;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, servicesContainer) -> {
      final PluginService pluginService = servicesContainer.getPluginService();
      if (pluginService.isPluginInUse(FailoverConnectionPlugin.class)) {
        return new MonitoringRdsHostListProvider(
            properties,
            initialUrl,
            servicesContainer,
            TOPOLOGY_QUERY,
            NODE_ID_QUERY,
            IS_READER_QUERY,
            IS_WRITER_QUERY);
      }
      return new AuroraHostListProvider(
          properties,
          initialUrl,
          servicesContainer,
          TOPOLOGY_QUERY,
          NODE_ID_QUERY,
          IS_READER_QUERY);
    };
  }

  @Override
  public String getLimitlessRouterEndpointQuery() {
    return LIMITLESS_ROUTER_ENDPOINT_QUERY;
  }

  @Override
  public String getBlueGreenStatusQuery() {
    return BG_STATUS_QUERY;
  }

  @Override
  public boolean isBlueGreenStatusAvailable(final Connection connection) {
    try {
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(TOPOLOGY_TABLE_EXIST_QUERY)) {
        return rs.next();
      }
    } catch (SQLException ex) {
      return false;
    }
  }

  @Override
  public void prepareConnectProperties(
      final @NonNull Properties connectProperties,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec) {

    final String driverInfoOption = String.format(
        "-c aurora.connection_str=_jdbc_wrapper_name:aws_jdbc_driver,_jdbc_wrapper_version:%s,_jdbc_wrapper_plugins:%s",
        DriverInfo.DRIVER_VERSION,
        connectProperties.getProperty(ConnectionPluginManager.EFFECTIVE_PLUGIN_CODES_PROPERTY));
    connectProperties.setProperty("options",
        connectProperties.getProperty("options") == null
            ? driverInfoOption
            : connectProperties.getProperty("options") + " " + driverInfoOption);
    connectProperties.remove(ConnectionPluginManager.EFFECTIVE_PLUGIN_CODES_PROPERTY);
  }
}
