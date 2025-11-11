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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import software.amazon.jdbc.hostlistprovider.AuroraTopologyUtils;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.hostlistprovider.TopologyUtils;
import software.amazon.jdbc.util.FullServicesContainer;

public class AuroraMysqlDialect extends MysqlDialect implements TopologyDialect, BlueGreenDialect {

  protected static final String AURORA_VERSION_EXISTS_QUERY = "SHOW VARIABLES LIKE 'aurora_version'";
  protected static final String TOPOLOGY_QUERY =
      "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
          + "CPU, REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP "
          + "FROM information_schema.replica_host_status "
          // filter out instances that have not been updated in the last 5 minutes
          + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' ";

  protected static final String INSTANCE_ID_QUERY = "SELECT @@aurora_server_id, @@aurora_server_id";
  protected static final String WRITER_ID_QUERY =
      "SELECT SERVER_ID FROM information_schema.replica_host_status "
          + "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id";
  protected static final String IS_READER_QUERY = "SELECT @@innodb_read_only";

  protected static final String BG_TOPOLOGY_EXISTS_QUERY =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
          + " table_schema = 'mysql' AND table_name = 'rds_topology'";
  protected static final String BG_STATUS_QUERY = "SELECT * FROM mysql.rds_topology";

  @Override
  public boolean isDialect(final Connection connection) {
    return dialectUtils.checkExistenceQueries(connection, AURORA_VERSION_EXISTS_QUERY);
  }

  @Override
  public List</* dialect code */ String> getDialectUpdateCandidates() {
    return Collections.singletonList(DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER);
  }

  @Override
  public HostListProvider createHostListProvider(
      FullServicesContainer servicesContainer, Properties props, String initialUrl) throws SQLException {
    final TopologyUtils topologyUtils =
        new AuroraTopologyUtils(this, servicesContainer.getPluginService().getHostSpecBuilder());
    return new RdsHostListProvider(topologyUtils, props, initialUrl, servicesContainer);
  }

  @Override
  public String getTopologyQuery() {
    return TOPOLOGY_QUERY;
  }

  @Override
  public String getInstanceIdQuery() {
    return INSTANCE_ID_QUERY;
  }

  @Override
  public String getWriterIdQuery() {
    return WRITER_ID_QUERY;
  }

  @Override
  public String getIsReaderQuery() {
    return IS_READER_QUERY;
  }

  @Override
  public boolean isBlueGreenStatusAvailable(final Connection connection) {
    return dialectUtils.checkExistenceQueries(connection, BG_TOPOLOGY_EXISTS_QUERY);
  }

  @Override
  public String getBlueGreenStatusQuery() {
    return BG_STATUS_QUERY;
  }
}
