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
import software.amazon.jdbc.hostlistprovider.AuroraGlobalDbHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.AuroraGlobalDbMonitoringHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;

public class GlobalAuroraMysqlDialect extends AuroraMysqlDialect {

  protected final String globalDbStatusTableExistQuery =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
          + " upper(table_schema) = 'INFORMATION_SCHEMA' AND upper(table_name) = 'AURORA_GLOBAL_DB_STATUS'";

  protected final String globalDbStatusQuery =
      "SELECT count(1) FROM information_schema.aurora_global_db_status";

  protected final String globalDbInstanceStatusTableExistQuery =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
          + " upper(table_schema) = 'INFORMATION_SCHEMA' AND upper(table_name) = 'AURORA_GLOBAL_DB_INSTANCE_STATUS'";

  protected final String globalTopologyQuery =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "VISIBILITY_LAG_IN_MSEC, AWS_REGION "
          + "FROM information_schema.aurora_global_db_instance_status ";

  protected final String regionByNodeIdQuery =
      "SELECT AWS_REGION FROM information_schema.aurora_global_db_instance_status WHERE SERVER_ID = ?";

  @Override
  public boolean isDialect(final Connection connection) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(this.globalDbStatusTableExistQuery);

      if (rs.next()) {
        rs.close();
        stmt.close();

        stmt = connection.createStatement();
        rs = stmt.executeQuery(this.globalDbInstanceStatusTableExistQuery);

        if (rs.next()) {
          rs.close();
          stmt.close();

          stmt = connection.createStatement();
          rs = stmt.executeQuery(this.globalDbStatusQuery);

          if (rs.next()) {
            int awsRegionCount = rs.getInt(1);
            return awsRegionCount > 1;
          }
        }
      }
      return false;
    } catch (final SQLException ex) {
      // ignore
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
    }
    return false;
  }

  @Override
  public List</* dialect code */ String> getDialectUpdateCandidates() {
    return Collections.emptyList();
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, hostListProviderService, pluginService) -> {

      final FailoverConnectionPlugin failover2Plugin = pluginService.getPlugin(FailoverConnectionPlugin.class);

      if (failover2Plugin != null) {
        return new AuroraGlobalDbMonitoringHostListProvider(
            properties,
            initialUrl,
            hostListProviderService,
            this.globalTopologyQuery,
            this.nodeIdQuery,
            this.isReaderQuery,
            this.isWriterQuery,
            this.regionByNodeIdQuery,
            pluginService);
      }
      return new AuroraGlobalDbHostListProvider(
          properties,
          initialUrl,
          hostListProviderService,
          this.globalTopologyQuery,
          this.nodeIdQuery,
          this.isReaderQuery);
    };
  }
}
