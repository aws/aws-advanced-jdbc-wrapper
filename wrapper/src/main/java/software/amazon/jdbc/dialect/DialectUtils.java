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
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;

public class DialectUtils {
  /**
   * Given a series of existence queries, returns true if they all execute successfully and contain at least one record.
   * Otherwise, returns false.
   *
   * @param conn             the connection to use for executing the queries.
   * @param existenceQueries the queries to check for existing records.
   * @return true if all queries execute successfully and return at least one record, false otherwise.
   */
  public boolean checkExistenceQueries(Connection conn, String... existenceQueries) {
    for (String existenceQuery : existenceQueries) {
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(existenceQuery)) {
        if (!rs.next()) {
          return false;
        }
      } catch (SQLException e) {
        return false;
      }
    }

    return true;
  }

  /**
   * Evaluate the database role of the given connection, either {@link HostRole#WRITER} or {@link HostRole#READER}.
   *
   * @param conn the connection to evaluate.
   * @param isReaderQuery it's a query to check if the connection is to a reader instance.
   * @return the database role of the given connection.
   * @throws SQLException if an exception occurs when querying the database or processing the database response.
   */
  public HostRole getHostRole(Connection conn, String isReaderQuery) throws SQLException {
    try (final Statement stmt = conn.createStatement();
         final ResultSet rs = stmt.executeQuery(isReaderQuery)) {
      if (rs.next()) {
        boolean isReader = rs.getBoolean(1);
        return isReader ? HostRole.READER : HostRole.WRITER;
      }
    } catch (SQLException e) {
      throw new SQLException(Messages.get("HostRoleUtils.errorGettingHostRole"), e);
    }

    throw new SQLException(Messages.get("HostRoleUtils.errorGettingHostRole"));
  }

  /**
   * Identifies instances across different database types using instanceId and instanceName values.
   *
   * <p>Database types handle these identifiers differently:
   * - Aurora: Uses the instance name as both instanceId and instanceName
   * Example: "test-instance-1" for both values
   * - RDS Cluster: Uses distinct values for instanceId and instanceName
   * Example:
   * instanceId: "db-WQFQKBTL2LQUPIEFIFBGENS4ZQ"
   * instanceName: "test-multiaz-instance-1"
   *
   * @param connection the connection to use for executing the query.
   * @param instanceIdQuery the query to execute to retrieve the instance identifiers.

   * @return a pair of instanceId and instanceName if successful, null otherwise.
   */
  public @Nullable Pair<String /* instanceId */, String /* instanceName */> getInstanceId(
      final Connection connection, String instanceIdQuery) {
    try {
      try (final Statement stmt = connection.createStatement();
           final ResultSet rs = stmt.executeQuery(instanceIdQuery)) {
        if (rs.next()) {
          return Pair.create(rs.getString(1), rs.getString(2));
        }
      }
    } catch (SQLException ex) {
      return null;
    }

    return null;
  }
}
