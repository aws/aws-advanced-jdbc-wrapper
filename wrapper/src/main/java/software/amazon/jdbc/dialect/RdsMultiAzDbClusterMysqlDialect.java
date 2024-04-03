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
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.hostlistprovider.RdsMultiAzDbClusterListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsMultiAzHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class RdsMultiAzDbClusterMysqlDialect extends MysqlDialect {

  private static final String TOPOLOGY_QUERY = "SELECT id, endpoint, port FROM mysql.rds_topology";

  private static final String TOPOLOGY_TABLE_EXIST_QUERY =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
      + " table_schema = 'mysql' AND table_name = 'rds_topology'";

  // For reader nodes, the query returns a writer node ID. For a writer node, the query returns no data.
  private static final String FETCH_WRITER_NODE_QUERY = "SHOW REPLICA STATUS";

  private static final String FETCH_WRITER_NODE_QUERY_COLUMN_NAME = "Source_Server_Id";

  private static final String NODE_ID_QUERY = "SELECT @@server_id";
  private static final String IS_READER_QUERY = "SELECT @@read_only";

  private static final EnumSet<FailoverRestriction> RDS_MULTI_AZ_RESTRICTIONS =
      EnumSet.of(FailoverRestriction.DISABLE_TASK_A, FailoverRestriction.ENABLE_WRITER_IN_TASK_B);

  protected final RdsUtils rdsUtils = new RdsUtils();

  @Override
  public boolean isDialect(final Connection connection) {
    try {
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(TOPOLOGY_TABLE_EXIST_QUERY)) {
        if (!rs.next()) {
          return false;
        }
      }

      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(TOPOLOGY_QUERY)) {
        if (!rs.next()) {
          return false;
        }
      }

      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'report_host'")) {
        if (!rs.next()) {
          return false;
        }
        final String reportHost = rs.getString(2); // get variable value; expected value is IP address
        return !StringUtils.isNullOrEmpty(reportHost);
      }

    } catch (final SQLException ex) {
      // ignore
    }
    return false;
  }

  @Override
  public List</* dialect code */ String> getDialectUpdateCandidates() {
    return null;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, hostListProviderService, pluginService) -> {

      final FailoverConnectionPlugin failover2Plugin = pluginService.getPlugin(FailoverConnectionPlugin.class);

      if (failover2Plugin != null) {
        return new MonitoringRdsMultiAzHostListProvider(
            properties,
            initialUrl,
            hostListProviderService,
            TOPOLOGY_QUERY,
            NODE_ID_QUERY,
            IS_READER_QUERY,
            pluginService,
            FETCH_WRITER_NODE_QUERY,
            FETCH_WRITER_NODE_QUERY_COLUMN_NAME);

      } else {
        return new RdsMultiAzDbClusterListProvider(
            properties,
            initialUrl,
            hostListProviderService,
            TOPOLOGY_QUERY,
            NODE_ID_QUERY,
            IS_READER_QUERY,
            FETCH_WRITER_NODE_QUERY,
            FETCH_WRITER_NODE_QUERY_COLUMN_NAME);
      }
    };
  }

  @Override
  public void prepareConnectProperties(
      final @NonNull Properties connectProperties, final @NonNull String protocol, final @NonNull HostSpec hostSpec) {
    final String connectionAttributes =
        "_jdbc_wrapper_name:aws_jdbc_driver,_jdbc_wrapper_version:" + DriverInfo.DRIVER_VERSION;
    connectProperties.setProperty("connectionAttributes",
        connectProperties.getProperty("connectionAttributes") == null
            ? connectionAttributes
            : connectProperties.getProperty("connectionAttributes") + "," + connectionAttributes);
  }

  @Override
  public EnumSet<FailoverRestriction> getFailoverRestrictions() {
    return RDS_MULTI_AZ_RESTRICTIONS;
  }
}
