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
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.util.DriverInfo;

public class AuroraPgDialect extends PgDialect implements TopologyDialect, AuroraLimitlessDialect, BlueGreenDialect {

  protected static final String AURORA_UTILS_EXIST_QUERY =
      "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_catalog.pg_settings "
          + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'";
  protected static final String TOPOLOGY_EXISTS_QUERY = "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1";
  protected static final String TOPOLOGY_QUERY =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
          + "FROM pg_catalog.aurora_replica_status() "
          // filter out instances that haven't been updated in the last 5 minutes
          + "WHERE EXTRACT("
          + "EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 "
          + "OR SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
          + "OR LAST_UPDATE_TIMESTAMP IS NULL";

  protected static final String INSTANCE_ID_QUERY = "SELECT pg_catalog.aurora_db_instance_identifier()";
  protected static final String WRITER_ID_QUERY =
      "SELECT SERVER_ID FROM pg_catalog.aurora_replica_status() "
          + "WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
          + "AND SERVER_ID OPERATOR(pg_catalog.=) pg_catalog.aurora_db_instance_identifier()";
  protected static final String IS_READER_QUERY = "SELECT pg_catalog.pg_is_in_recovery()";

  protected static final String LIMITLESS_ROUTER_ENDPOINT_QUERY =
      "select router_endpoint, load from pg_catalog.aurora_limitless_router_endpoints()";

  protected static final String BG_TOPOLOGY_EXISTS_QUERY =
      "SELECT 'pg_catalog.get_blue_green_fast_switchover_metadata'::regproc";
  protected static final String BG_STATUS_QUERY =
      "SELECT * FROM "
        + "pg_catalog.get_blue_green_fast_switchover_metadata('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')";

  private static final Logger LOGGER = Logger.getLogger(AuroraPgDialect.class.getName());

  protected final AuroraDialectUtils dialectUtils;

  public AuroraPgDialect() {
    super();
    this.dialectUtils = new AuroraDialectUtils(WRITER_ID_QUERY);
  }

  public AuroraPgDialect(AuroraDialectUtils dialectUtils) {
    super();
    this.dialectUtils = dialectUtils;
  }

  @Override
  public boolean isDialect(final Connection connection) {
    if (!super.isDialect(connection)) {
      return false;
    }

    boolean hasExtensions = false;
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(AURORA_UTILS_EXIST_QUERY)) {
      if (rs.next()) {
        final boolean auroraUtils = rs.getBoolean("aurora_stat_utils");
        LOGGER.finest(() -> String.format("auroraUtils: %b", auroraUtils));
        if (auroraUtils) {
          hasExtensions = true;
        }
      }
    } catch (SQLException ex) {
      return false;
    }

    if (!hasExtensions) {
      return false;
    }

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(TOPOLOGY_EXISTS_QUERY)) {
      if (rs.next()) {
        LOGGER.finest(() -> "hasTopology: true");
        return true;
      }
    } catch (final SQLException ex) {
      return false;
    }

    return false;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return Arrays.asList(DialectCodes.GLOBAL_AURORA_PG,
        DialectCodes.RDS_MULTI_AZ_PG_CLUSTER,
        DialectCodes.RDS_PG);
  }

  @Override
  public HostListProviderSupplier getHostListProviderSupplier() {
    return (properties, initialUrl, servicesContainer) -> {
      final PluginService pluginService = servicesContainer.getPluginService();
      if (pluginService.isPluginInUse(FailoverConnectionPlugin.class)) {
        return new MonitoringRdsHostListProvider(this, properties, initialUrl, servicesContainer);
      }
      return new RdsHostListProvider(this, properties, initialUrl, servicesContainer);
    };
  }

  @Override
  public String getTopologyQuery() {
    return TOPOLOGY_QUERY;
  }

  @Override
  public @Nullable List<TopologyQueryHostSpec> processTopologyResults(Connection conn, ResultSet rs)
      throws SQLException {
    return this.dialectUtils.processTopologyResults(rs);
  }

  @Override
  public String getInstanceIdQuery() {
    return INSTANCE_ID_QUERY;
  }

  @Override
  public boolean isWriterInstance(Connection connection) throws SQLException {
    return this.dialectUtils.isWriterInstance(connection);
  }

  @Override
  public String getIsReaderQuery() {
    return IS_READER_QUERY;
  }

  @Override
  public String getLimitlessRouterEndpointQuery() {
    return LIMITLESS_ROUTER_ENDPOINT_QUERY;
  }

  @Override
  public boolean isBlueGreenStatusAvailable(final Connection connection) {
    try {
      try (Statement statement = connection.createStatement();
           ResultSet rs = statement.executeQuery(BG_TOPOLOGY_EXISTS_QUERY)) {
        return rs.next();
      }
    } catch (SQLException ex) {
      return false;
    }
  }

  @Override
  public String getBlueGreenStatusQuery() {
    return BG_STATUS_QUERY;
  }
}
