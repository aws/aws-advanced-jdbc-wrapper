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
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MultiAzDbClusterPgExceptionHandler;
import software.amazon.jdbc.hostlistprovider.RdsMultiAzDbClusterListProvider;
import software.amazon.jdbc.util.DriverInfo;

public class RdsMultiAzDbClusterPgDialect extends PgDialect {

  private static MultiAzDbClusterPgExceptionHandler exceptionHandler;

  private static final String TOPOLOGY_QUERY =
      "SELECT id, endpoint, port FROM rds_tools.show_topology('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')";

  private static final String WRITER_NODE_FUNC_EXIST_QUERY =
      "SELECT 1 AS tmp FROM information_schema.routines"
          + " WHERE routine_schema='rds_tools' AND routine_name='multi_az_db_cluster_source_dbi_resource_id'";

  private static final String FETCH_WRITER_NODE_QUERY =
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
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(WRITER_NODE_FUNC_EXIST_QUERY);

      if (rs.next()) {
        rs.close();
        stmt.close();

        stmt = connection.createStatement();
        rs = stmt.executeQuery(FETCH_WRITER_NODE_QUERY);

        return rs.next();
      }
      return false;
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
    return null;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, hostListProviderService) -> new RdsMultiAzDbClusterListProvider(
        properties,
        initialUrl,
        hostListProviderService,
        TOPOLOGY_QUERY,
        NODE_ID_QUERY,
        IS_READER_QUERY,
        FETCH_WRITER_NODE_QUERY,
        FETCH_WRITER_NODE_QUERY_COLUMN_NAME);
  }

  public String getTopologyQuery() {
    return TOPOLOGY_QUERY;
  }
}
