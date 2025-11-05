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
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraTopologyUtils;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringGlobalAuroraHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;

public class GlobalAuroraMysqlDialect extends AuroraMysqlDialect implements GlobalAuroraTopologyDialect {

  protected static final String GLOBAL_STATUS_TABLE_EXISTS_QUERY =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
          + " upper(table_schema) = 'INFORMATION_SCHEMA' AND upper(table_name) = 'AURORA_GLOBAL_DB_STATUS'";
  protected static final String GLOBAL_INSTANCE_STATUS_EXISTS_QUERY =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
          + " upper(table_schema) = 'INFORMATION_SCHEMA' AND upper(table_name) = 'AURORA_GLOBAL_DB_INSTANCE_STATUS'";

  protected static final String GLOBAL_TOPOLOGY_QUERY =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "VISIBILITY_LAG_IN_MSEC, AWS_REGION "
          + "FROM information_schema.aurora_global_db_instance_status ";

  protected static final String REGION_COUNT_QUERY = "SELECT count(1) FROM information_schema.aurora_global_db_status";
  protected static final String REGION_BY_INSTANCE_ID_QUERY =
      "SELECT AWS_REGION FROM information_schema.aurora_global_db_instance_status WHERE SERVER_ID = ?";


  @Override
  public boolean isDialect(final Connection connection) {
    try {
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(GLOBAL_STATUS_TABLE_EXISTS_QUERY)) {
        if (!rs.next()) {
          return false;
        }
      }

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(GLOBAL_INSTANCE_STATUS_EXISTS_QUERY)) {
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
