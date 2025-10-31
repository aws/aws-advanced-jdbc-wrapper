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
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MultiAzDbClusterPgExceptionHandler;
import software.amazon.jdbc.util.DriverInfo;

public class RdsMultiAzDbClusterPgDialect extends PgDialect implements TopologyDialect {

  protected static final String IS_RDS_CLUSTER_QUERY =
      "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()";
  protected static final String TOPOLOGY_QUERY =
      "SELECT id, endpoint, port FROM rds_tools.show_topology('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')";

  protected static final String INSTANCE_ID_QUERY = "SELECT dbi_resource_id FROM rds_tools.dbi_resource_id()";
  // For reader instances, the query should return a writer instance ID.
  // For a writer instance, the query should return no data.
  protected static final String WRITER_ID_QUERY =
      "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"
          + " WHERE multi_az_db_cluster_source_dbi_resource_id OPERATOR(pg_catalog.!=)"
          + " (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())";
  protected static final String WRITER_ID_QUERY_COLUMN = "multi_az_db_cluster_source_dbi_resource_id";
  protected static final String IS_READER_QUERY = "SELECT pg_catalog.pg_is_in_recovery()";

  private static MultiAzDbClusterPgExceptionHandler exceptionHandler;

  protected final MultiAzDialectUtils dialectUtils = new MultiAzDialectUtils(
      INSTANCE_ID_QUERY, WRITER_ID_QUERY, WRITER_ID_QUERY_COLUMN);

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
  public ExceptionHandler getExceptionHandler() {
    if (exceptionHandler == null) {
      exceptionHandler = new MultiAzDbClusterPgExceptionHandler();
    }
    return exceptionHandler;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return this.dialectUtils.getHostListProviderSupplier(this);
  }

  @Override
  public String getTopologyQuery() {
    return TOPOLOGY_QUERY;
  }

  @Override
  public @Nullable List<TopologyQueryHostSpec> processTopologyResults(Connection conn, ResultSet rs)
      throws SQLException {
    return this.dialectUtils.processTopologyResults(conn, rs);
  }

  @Override
  public String getInstanceIdQuery() {
    return INSTANCE_ID_QUERY;
  }

  @Override
  public boolean isWriterInstance(Connection connection) throws SQLException {
    return dialectUtils.isWriterInstance(connection);
  }

  @Override
  public String getIsReaderQuery() {
    return IS_READER_QUERY;
  }
}
