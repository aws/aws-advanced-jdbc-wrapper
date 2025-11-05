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
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraTopologyUtils;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringGlobalAuroraHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;

public class GlobalAuroraPgDialect extends AuroraPgDialect implements GlobalAuroraTopologyDialect {

  protected static final String GLOBAL_STATUS_FUNC_EXISTS_QUERY = "select 'aurora_global_db_status'::regproc";
  protected static final String GLOBAL_INSTANCE_STATUS_FUNC_EXISTS_QUERY =
      "select 'aurora_global_db_instance_status'::regproc";

  protected static final String GLOBAL_TOPOLOGY_QUERY =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "VISIBILITY_LAG_IN_MSEC, AWS_REGION "
          + "FROM aurora_global_db_instance_status()";

  protected static final String REGION_COUNT_QUERY = "SELECT count(1) FROM aurora_global_db_status()";
  protected static final String REGION_BY_INSTANCE_ID_QUERY =
      "SELECT AWS_REGION FROM aurora_global_db_instance_status() WHERE SERVER_ID = ?";

  private static final Logger LOGGER = Logger.getLogger(GlobalAuroraPgDialect.class.getName());

  @Override
  public boolean isDialect(final Connection connection) {
    try {
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(AURORA_UTILS_EXIST_QUERY)) {
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
           ResultSet rs = stmt.executeQuery(GLOBAL_STATUS_FUNC_EXISTS_QUERY)) {
        if (!rs.next()) {
          return false;
        }
      }

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(GLOBAL_INSTANCE_STATUS_FUNC_EXISTS_QUERY)) {
        if (!rs.next()) {
          return false;
        }
      }

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(REGION_COUNT_QUERY)) {
        if (rs.next()) {
          int awsRegionCount = rs.getInt(1);
          return awsRegionCount > 1;
        }
      }
    } catch (final SQLException ex) {
      return false;
    }

    return false;
  }

  @Override
  public List</* dialect code */ String> getDialectUpdateCandidates() {
    return Collections.emptyList();
  }

  @Override
  public HostListProviderSupplier getHostListProviderSupplier() {
    return (properties, initialUrl, servicesContainer) -> {
      final PluginService pluginService = servicesContainer.getPluginService();
      final GlobalAuroraTopologyUtils topologyUtils =
          new GlobalAuroraTopologyUtils(this, pluginService.getHostSpecBuilder());
      if (pluginService.isPluginInUse(FailoverConnectionPlugin.class)) {
        return new MonitoringGlobalAuroraHostListProvider(topologyUtils, properties, initialUrl, servicesContainer);
      }
      return new GlobalAuroraHostListProvider(topologyUtils, properties, initialUrl, servicesContainer);
    };
  }

  @Override
  public String getTopologyQuery() {
    return GLOBAL_TOPOLOGY_QUERY;
  }

  @Override
  public String getRegionByInstanceIdQuery() {
    return REGION_BY_INSTANCE_ID_QUERY;
  }
}
