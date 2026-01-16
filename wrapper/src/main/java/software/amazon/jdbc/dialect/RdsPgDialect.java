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
import java.util.logging.Logger;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.Messages;

/**
 * Suitable for the following AWS PG configurations. - Multi-AZ DB Cluster - Multi-AZ DB Instance -
 * Single DB Instance
 */
public class RdsPgDialect extends PgDialect implements BlueGreenDialect {

  protected static final String EXTENSIONS_EXIST_SQL =
      "SELECT (setting LIKE '%rds_tools%') AS rds_tools, "
          + "(setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_catalog.pg_settings "
          + "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'";
  protected static final String TOPOLOGY_TABLE_EXISTS_QUERY =
      "SELECT 'rds_tools.show_topology'::regproc";

  protected static final String BG_STATUS_QUERY =
      "SELECT * FROM rds_tools.show_topology('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')";

  private static final Logger LOGGER = Logger.getLogger(RdsPgDialect.class.getName());
  private static final List<String> dialectUpdateCandidates =
      Arrays.asList(
          DialectCodes.RDS_MULTI_AZ_PG_CLUSTER,
          DialectCodes.GLOBAL_AURORA_PG,
          DialectCodes.AURORA_PG);

  @Override
  public boolean isDialect(final Connection connection) {
    if (!super.isDialect(connection)) {
      return false;
    }

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(EXTENSIONS_EXIST_SQL)) {
      while (rs.next()) {
        final boolean rdsTools = rs.getBoolean("rds_tools");
        final boolean auroraUtils = rs.getBoolean("aurora_stat_utils");
        LOGGER.finest(
            Messages.get("RdsPgDialect.rdsToolsAuroraUtils", new Object[] {rdsTools, auroraUtils}));
        if (rdsTools && !auroraUtils) {
          return true;
        }
      }
    } catch (final SQLException ex) {
      return false;
    }

    return false;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }

  @Override
  public boolean isBlueGreenStatusAvailable(final Connection connection) {
    return dialectUtils.checkExistenceQueries(connection, TOPOLOGY_TABLE_EXISTS_QUERY);
  }

  @Override
  public String getBlueGreenStatusQuery() {
    return BG_STATUS_QUERY;
  }
}
