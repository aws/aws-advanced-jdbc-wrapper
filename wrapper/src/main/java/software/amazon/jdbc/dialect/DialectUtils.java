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
}
