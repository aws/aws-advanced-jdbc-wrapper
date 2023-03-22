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

/**
 * Suitable for the following AWS PG configurations.
 * - Multi-AZ DB Cluster
 * - Multi-AZ DB Instance
 * - Single DB Instance
 */
public class RdsPgDialect extends PgDialect {

  private static final List<String> dialectUpdateCandidates = Collections.singletonList(
      DialectCodes.AURORA_PG);

  private final static String extensionsSql = "SELECT (setting LIKE '%rds_tools%') AS rds_tools, "
      + "(setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
      + "FROM pg_settings "
      + "WHERE name='rds.extensions'";

  @Override
  public boolean isDialect(final Connection connection) {
    if (!super.isDialect(connection)) {
      return false;
    }

    try {
      try (final Statement stmt = connection.createStatement();
          final ResultSet rs = stmt.executeQuery(extensionsSql)) {
        while (rs.next()) {
          final boolean rdsTools = rs.getBoolean("rds_tools");
          final boolean auroraUtils = rs.getBoolean("aurora_stat_utils");
          if (rdsTools && !auroraUtils) {
            return true;
          }
        }
      }
    } catch (final SQLException ex) {
      // ignore
    }
    return false;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }
}
