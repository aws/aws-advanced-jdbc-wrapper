/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlState {

  public static final SqlState UNKNOWN_STATE = new SqlState("");
  public static final SqlState CONNECTION_FAILURE_DURING_TRANSACTION = new SqlState("08007");
  public static final SqlState COMMUNICATION_LINK_CHANGED = new SqlState("08S02");
  public static final SqlState CONNECTION_UNABLE_TO_CONNECT = new SqlState("08001");
  public static final SqlState CONNECTION_NOT_OPEN = new SqlState("08003");

  // TODO: add custom error codes support

  private final String sqlState;

  SqlState(String sqlState) {
    this.sqlState = sqlState;
  }

  public String getState() {
    return this.sqlState;
  }

  public static boolean isConnectionError(SQLException sqlException) {
    return isConnectionError(sqlException.getSQLState());
  }

  public static boolean isConnectionError(@Nullable String sqlState) {
    // TODO

    if (sqlState == null) {
      return false;
    }

    return sqlState.startsWith("08");
  }
}
