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
import java.util.logging.Logger;

/**
 * Suitable for the following AWS PG configurations.
 * - Regional Cluster
 */
public class AuroraPgDialect extends PgDialect implements TopologyAwareDatabaseCluster {

  private static final Logger LOGGER = Logger.getLogger(AuroraPgDialect.class.getName());

  private static final String extensionsSql =
      "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
          + "FROM pg_settings "
          + "WHERE name='rds.extensions'";

  private static final String topologySql = "SELECT 1 FROM aurora_replica_status() LIMIT 1";

  @Override
  public String getTopologyQuery() {
    return "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
        + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
        + "FROM aurora_replica_status() "
        // filter out nodes that haven't been updated in the last 5 minutes
        + "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' ";
  }

  @Override
  public String getNodeIdQuery() {
    return "SELECT aurora_db_instance_identifier()";
  }

  @Override
  public String getIsReaderQuery() {
    return "SELECT pg_is_in_recovery()";
  }

  @Override
  public boolean isDialect(final Connection connection) {
    if (!super.isDialect(connection)) {
      return false;
    }

    boolean hasExtensions = false;
    boolean hasTopology = false;
    try {
      try (final Statement stmt = connection.createStatement();
          final ResultSet rs = stmt.executeQuery(extensionsSql)) {
        if (rs.next()) {
          final boolean auroraUtils = rs.getBoolean("aurora_stat_utils");
          LOGGER.finest(() -> String.format("auroraUtils: %b", auroraUtils));
          if (auroraUtils) {
            hasExtensions = true;
          }
        }
      }

      try (final Statement stmt = connection.createStatement();
          final ResultSet rs = stmt.executeQuery(topologySql)) {
        if (rs.next()) {
          LOGGER.finest(() -> "hasTopology: true");
          hasTopology = true;
        }
      }

      return hasExtensions && hasTopology;

    } catch (final SQLException ex) {
      // ignore
    }
    return false;
  }
}
