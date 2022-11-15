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

package software.amazon.jdbc.util;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlState {

  public static final SqlState UNKNOWN_STATE = new SqlState("");
  public static final SqlState CONNECTION_UNABLE_TO_CONNECT = new SqlState("08001");
  public static final SqlState CONNECTION_NOT_OPEN = new SqlState("08003");
  public static final SqlState CONNECTION_FAILURE = new SqlState("08006");
  public static final SqlState CONNECTION_FAILURE_DURING_TRANSACTION = new SqlState("08007");
  public static final SqlState COMMUNICATION_ERROR = new SqlState("08S01");
  public static final SqlState COMMUNICATION_LINK_CHANGED = new SqlState("08S02");
  public static final SqlState ACTIVE_SQL_TRANSACTION = new SqlState("25001");

  // TODO: add custom error codes support

  // The following SQL States for Postgresql are considered as "communication" errors
  private static final List<String> PG_SQLSTATES = Arrays.asList(
      "53", // insufficient resources
      "57P01", // admin shutdown
      "57P02", // crash shutdown
      "57P03", // cannot connect now
      "58", // system error (backend)
      "99", // unexpected error
      "F0", // configuration file error (backend)
      "XX" // internal error (backend)
  );

  private final String sqlState;

  SqlState(final String sqlState) {
    this.sqlState = sqlState;
  }

  public String getState() {
    return this.sqlState;
  }

  public static boolean isConnectionError(final SQLException sqlException) {
    return isConnectionError(sqlException.getSQLState());
  }

  public static boolean isConnectionError(@Nullable final String sqlState) {
    // TODO: should be user configurable

    if (sqlState == null) {
      return false;
    }

    if (sqlState.startsWith("08")) {
      return true;
    }

    for (final String pgSqlState : PG_SQLSTATES) {
      if (sqlState.startsWith(pgSqlState)) {
        return true;
      }
    }
    return false;
  }
}
