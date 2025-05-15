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
import java.util.logging.Logger;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;

/**
 * Suitable for the following AWS PG configurations.
 * - Regional Cluster
 */
public class AuroraPgDialect extends PgDialect implements AuroraLimitlessDialect {
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

  private static final String WRITER_ID_QUERY =
      "SELECT SERVER_ID FROM aurora_replica_status() "
          + "WHERE SESSION_ID = 'MASTER_SESSION_ID'";

  private static final String NODE_ID_QUERY = "SELECT aurora_db_instance_identifier()";
  private static final String IS_READER_QUERY = "SELECT pg_is_in_recovery()";
  protected static final String LIMITLESS_ROUTER_ENDPOINT_QUERY =
      "select router_endpoint, load from aurora_limitless_router_endpoints()";

  @Override
  public boolean isDialect(final Connection connection) {
    if (!super.isDialect(connection)) {
      return false;
    }

    Statement stmt = null;
    ResultSet rs = null;
    boolean hasExtensions = false;
    boolean hasTopology = false;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(extensionsSql);
      if (rs.next()) {
        final boolean auroraUtils = rs.getBoolean("aurora_stat_utils");
        LOGGER.finest(() -> String.format("auroraUtils: %b", auroraUtils));
        if (auroraUtils) {
          hasExtensions = true;
        }
      }
    } catch (SQLException ex) {
      // ignore
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
    }
    if (!hasExtensions) {
      return false;
    }
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(topologySql);
      if (rs.next()) {
        LOGGER.finest(() -> "hasTopology: true");
        hasTopology = true;
      }
    } catch (final SQLException ex) {
      // ignore
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
    }
    return hasExtensions && hasTopology;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, hostListProviderService, pluginService) -> {

      final FailoverConnectionPlugin failover2Plugin = pluginService.getPlugin(FailoverConnectionPlugin.class);

      if (failover2Plugin != null) {
        return new MonitoringRdsHostListProvider(
            properties,
            initialUrl,
            hostListProviderService,
            TOPOLOGY_QUERY,
            NODE_ID_QUERY,
            IS_READER_QUERY,
            WRITER_ID_QUERY,
            pluginService);
      }
      return new AuroraHostListProvider(
          properties,
          initialUrl,
          hostListProviderService,
          TOPOLOGY_QUERY,
          NODE_ID_QUERY,
          IS_READER_QUERY);
    };
  }

  @Override
  public String getLimitlessRouterEndpointQuery() {
    return LIMITLESS_ROUTER_ENDPOINT_QUERY;
  }
}
