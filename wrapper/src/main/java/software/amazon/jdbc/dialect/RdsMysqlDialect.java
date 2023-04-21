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

public class RdsMysqlDialect extends MysqlDialect {

  private static final List<String> dialectUpdateCandidates = Collections.singletonList(
      DialectCodes.AURORA_MYSQL);

  @Override
  public boolean isDialect(final Connection connection) {
    if (!super.isDialect(connection)) {
      return false;
    }
    try (final Statement stmt = connection.createStatement();
        final ResultSet rs = stmt.executeQuery(this.getServerVersionQuery())) {
      while (rs.next()) {
        final int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          final String columnValue = rs.getString(i);
          if ("Source distribution".equalsIgnoreCase(columnValue)) {
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
