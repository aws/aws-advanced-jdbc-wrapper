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
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;

public class AuroraMysqlDialect extends MysqlDialect {

  protected final String topologyQuery =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP "
          + "FROM information_schema.replica_host_status "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' ";

  protected final String isWriterQuery =
      "SELECT SERVER_ID FROM information_schema.replica_host_status "
      + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id";

  protected final String nodeIdQuery = "SELECT @@aurora_server_id";
  protected final String isReaderQuery = "SELECT @@innodb_read_only";

  @Override
  public boolean isDialect(final Connection connection) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery("SHOW VARIABLES LIKE 'aurora_version'");
      if (rs.next()) {
        // If variable with such name is presented then it means it's an Aurora cluster
        return true;
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
    return false;
  }

  @Override
  public List</* dialect code */ String> getDialectUpdateCandidates() {
    return Arrays.asList(
        DialectCodes.GLOBAL_AURORA_MYSQL,
        DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER);
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
            this.topologyQuery,
            this.nodeIdQuery,
            this.isReaderQuery,
            this.isWriterQuery,
            pluginService);
      }
      return new AuroraHostListProvider(
          properties,
          initialUrl,
          hostListProviderService,
          this.topologyQuery,
          this.nodeIdQuery,
          this.isReaderQuery);
    };
  }
}

