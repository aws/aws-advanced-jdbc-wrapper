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
import software.amazon.jdbc.plugin.failover.FailoverRestriction;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class MultiAzClusterMysqlDialect extends MysqlDialect implements TopologyDialect {

  protected static final String REPORT_HOST_EXISTS_QUERY = "SHOW VARIABLES LIKE 'report_host'";
  protected static final String TOPOLOGY_TABLE_EXISTS_QUERY =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
          + " table_schema = 'mysql' AND table_name = 'rds_topology'";
  protected static final String TOPOLOGY_QUERY = "SELECT id, endpoint, port FROM mysql.rds_topology";

  // The query return nodeId and nodeName.
  // For example: "1845128080", "test-multiaz-instance-1"
  private static final String INSTANCE_ID_QUERY = "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1)"
      + " FROM mysql.rds_topology"
      + " WHERE id = @@server_id";
  // For reader instances, the query returns a writer instance ID. For a writer instance, the query returns no data.
  protected static final String WRITER_ID_QUERY = "SHOW REPLICA STATUS";
  protected static final String WRITER_ID_QUERY_COLUMN = "Source_Server_Id";
  protected static final String IS_READER_QUERY = "SELECT @@read_only";

  private static final EnumSet<FailoverRestriction> FAILOVER_RESTRICTIONS =
      EnumSet.of(FailoverRestriction.DISABLE_TASK_A, FailoverRestriction.ENABLE_WRITER_IN_TASK_B);

  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final MultiAzDialectUtils dialectUtils = new MultiAzDialectUtils(
      INSTANCE_ID_QUERY, WRITER_ID_QUERY, WRITER_ID_QUERY_COLUMN);

  @Override
  public boolean isDialect(final Connection connection) {
    try {
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(TOPOLOGY_TABLE_EXISTS_QUERY)) {
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
          ResultSet rs = stmt.executeQuery(REPORT_HOST_EXISTS_QUERY)) {
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
  public HostListProviderSupplier getHostListProviderSupplier() {
    return this.dialectUtils.getHostListProviderSupplier(this);
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
    return FAILOVER_RESTRICTIONS;
  }

  @Override
  public String getTopologyQuery() {
    return TOPOLOGY_QUERY;
  }

  @Override
  public List<TopologyQueryHostSpec> processTopologyResults(Connection conn, ResultSet rs)
      throws SQLException {
    return dialectUtils.processTopologyResults(conn, rs);
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
