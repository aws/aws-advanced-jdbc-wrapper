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
import software.amazon.jdbc.hostlistprovider.AuroraGlobalDbHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.AuroraGlobalDbMonitoringHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;

public class GlobalAuroraPgDialect extends AuroraPgDialect {

  private static final Logger LOGGER = Logger.getLogger(GlobalAuroraPgDialect.class.getName());

  protected final String globalDbStatusFuncExistQuery =
      "select 'aurora_global_db_status'::regproc";

  protected final String globalDbInstanceStatusFuncExistQuery =
      "select 'aurora_global_db_instance_status'::regproc";

  protected final String globalTopologyQuery =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "VISIBILITY_LAG_IN_MSEC, AWS_REGION "
          + "FROM aurora_global_db_instance_status()";

  protected final String globalDbStatusQuery =
      "SELECT count(1) FROM aurora_global_db_status()";

  protected final String regionByNodeIdQuery =
      "SELECT AWS_REGION FROM aurora_global_db_instance_status() WHERE SERVER_ID = ?";

  @Override
  public boolean isDialect(final Connection connection) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(this.extensionsSql);
      if (rs.next()) {
        final boolean auroraUtils = rs.getBoolean("aurora_stat_utils");
        LOGGER.finest(() -> String.format("auroraUtils: %b", auroraUtils));
        if (!auroraUtils) {
          return false;
        }
      }
      rs.close();
      stmt.close();

      stmt = connection.createStatement();
      rs = stmt.executeQuery(this.globalDbStatusFuncExistQuery);

      if (rs.next()) {
        rs.close();
        stmt.close();

        stmt = connection.createStatement();
        rs = stmt.executeQuery(this.globalDbInstanceStatusFuncExistQuery);

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
