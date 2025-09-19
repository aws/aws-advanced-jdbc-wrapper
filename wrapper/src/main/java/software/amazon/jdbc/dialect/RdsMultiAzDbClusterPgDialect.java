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
import java.util.List;
import java.util.logging.Logger;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MultiAzDbClusterPgExceptionHandler;
import software.amazon.jdbc.hostlistprovider.RdsMultiAzDbClusterListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsMultiAzHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.util.DriverInfo;

public class RdsMultiAzDbClusterPgDialect extends PgDialect {

  private static final Logger LOGGER = Logger.getLogger(RdsMultiAzDbClusterPgDialect.class.getName());

  private static MultiAzDbClusterPgExceptionHandler exceptionHandler;

  private static final String TOPOLOGY_QUERY =
      "SELECT id, endpoint, port FROM rds_tools.show_topology('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')";

  // For reader nodes, the query should return a writer node ID. For a writer node, the query should return no data.
  private static final String FETCH_WRITER_NODE_QUERY =
      "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"
          + " WHERE multi_az_db_cluster_source_dbi_resource_id !="
          + " (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())";

  private static final String IS_RDS_CLUSTER_QUERY =
      "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()";

  private static final String FETCH_WRITER_NODE_QUERY_COLUMN_NAME = "multi_az_db_cluster_source_dbi_resource_id";

  private static final String NODE_ID_QUERY = "SELECT dbi_resource_id FROM rds_tools.dbi_resource_id()";

  private static final String IS_READER_QUERY = "SELECT pg_is_in_recovery()";

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (exceptionHandler == null) {
      exceptionHandler = new MultiAzDbClusterPgExceptionHandler();
    }
    return exceptionHandler;
  }

  @Override
  public boolean isDialect(final Connection connection) {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(IS_RDS_CLUSTER_QUERY)) {
      return rs.next() && rs.getString(1) != null;
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
    return (properties, initialUrl, servicesContainer) -> {
      final PluginService pluginService = servicesContainer.getPluginService();
      if (pluginService.isPluginInUse(FailoverConnectionPlugin.class)) {
        return new MonitoringRdsMultiAzHostListProvider(
            properties,
            initialUrl,
            servicesContainer,
            TOPOLOGY_QUERY,
            NODE_ID_QUERY,
            IS_READER_QUERY,
            FETCH_WRITER_NODE_QUERY,
            FETCH_WRITER_NODE_QUERY_COLUMN_NAME);

      } else {

        return new RdsMultiAzDbClusterListProvider(
            properties,
            initialUrl,
            servicesContainer,
            TOPOLOGY_QUERY,
            NODE_ID_QUERY,
            IS_READER_QUERY,
            FETCH_WRITER_NODE_QUERY,
            FETCH_WRITER_NODE_QUERY_COLUMN_NAME);
      }
    };
  }
}
