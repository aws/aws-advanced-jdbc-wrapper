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
import software.amazon.jdbc.util.StringUtils;

public class RdsMysqlDialect extends MysqlDialect implements BlueGreenDialect {

  protected static final String REPORT_HOST_EXISTS_QUERY = "SHOW VARIABLES LIKE 'report_host'";
  protected static final String TOPOLOGY_TABLE_EXISTS_QUERY =
      "SELECT 1 AS tmp FROM information_schema.tables WHERE"
          + " table_schema = 'mysql' AND table_name = 'rds_topology'";

  protected static final String BG_STATUS_QUERY =
      "SELECT * FROM mysql.rds_topology";

  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.AURORA_MYSQL,
      DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER);

  @Override
  public boolean isDialect(final Connection connection) {
    if (super.isDialect(connection)) {
      // MysqlDialect and RdsMysqlDialect use the same server version query to determine the dialect.
      //
      // For community Mysql:
      // SHOW VARIABLES LIKE 'version_comment'
      // | Variable_name   | value                                            |
      // |-----------------|--------------------------------------------------|
      // | version_comment | MySQL Community Server (GPL) |
      //
      // For RDS MySQL:
      // SHOW VARIABLES LIKE 'version_comment'
      // | Variable_name   | value               |
      // |-----------------|---------------------|
      // | version_comment | Source distribution |
      // If super.isDialect returns true there is no need to check for RdsMysqlDialect.
      return false;
    }

    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(VERSION_QUERY)) {
        if (!rs.next()) {
          return false;
        }

        final String columnValue = rs.getString(2);
        if (!"Source distribution".equalsIgnoreCase(columnValue)) {
          return false;
        }
      }

      try (ResultSet rs = stmt.executeQuery(REPORT_HOST_EXISTS_QUERY)) {
        if (!rs.next()) {
          return false;
        }

        final String reportHost = rs.getString(2); // get variable value; expected empty value
        return StringUtils.isNullOrEmpty(reportHost);
      }

    } catch (final SQLException ex) {
      return false;
    }
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }

  @Override
  public boolean isBlueGreenStatusAvailable(final Connection connection) {
    try {
      try (Statement statement = connection.createStatement();
           ResultSet rs = statement.executeQuery(TOPOLOGY_TABLE_EXISTS_QUERY)) {
        return rs.next();
      }
    } catch (SQLException ex) {
      return false;
    }
  }

  @Override
  public String getBlueGreenStatusQuery() {
    return BG_STATUS_QUERY;
  }
}
