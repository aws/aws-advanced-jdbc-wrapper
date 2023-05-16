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
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.PgExceptionHandler;

/**
 * Generic dialect for any Postgresql database.
 */
public class PgDialect implements Dialect {

  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.AURORA_PG, DialectCodes.RDS_PG);

  private static PgExceptionHandler pgExceptionHandler;

  @Override
  public int getDefaultPort() {
    return 5432;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (pgExceptionHandler == null) {
      pgExceptionHandler = new PgExceptionHandler();
    }
    return pgExceptionHandler;
  }

  @Override
  public String getHostAliasQuery() {
    return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())";
  }

  @Override
  public String getServerVersionQuery() {
    return "SELECT 'version', VERSION()";
  }

  @Override
  public boolean isDialect(final Connection connection) {
    try {
      try (final Statement stmt = connection.createStatement();
          final ResultSet rs = stmt.executeQuery("SELECT 1 FROM pg_proc LIMIT 1")) {
        if (rs.next()) {
          return true;
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
