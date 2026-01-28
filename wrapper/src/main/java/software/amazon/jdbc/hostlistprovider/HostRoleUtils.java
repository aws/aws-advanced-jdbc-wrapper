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


package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.util.Messages;

/**
 * A class for assisting in retrieving the host role.
 */
public class HostRoleUtils {
  private final String isReaderQuery;

  public HostRoleUtils(String isReaderQuery) {
    this.isReaderQuery = isReaderQuery;
  }

  /**
   * Evaluate the database role of the given connection, either {@link HostRole#WRITER} or {@link HostRole#READER}.
   *
   * @param conn the connection to evaluate.
   * @return the database role of the given connection.
   * @throws SQLException if an exception occurs when querying the database or processing the database response.
   */
  public HostRole getHostRole(Connection conn) throws SQLException {
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
}
