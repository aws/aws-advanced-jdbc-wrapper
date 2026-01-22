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

package software.amazon.jdbc.exceptions;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.StringUtils;

public class MySQLExceptionHandler implements ExceptionHandler {
  public static final String SQLSTATE_ACCESS_ERROR = "28000";
  public static final String SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
  public static final String SET_NETWORK_TIMEOUT_ON_CLOSED_CONNECTION =
      "setNetworkTimeout cannot be called on a closed connection";

  private static final Set<Integer> SQLSTATE_READ_ONLY_CONNECTION = new HashSet<>(Arrays.asList(
      1290, // The MySQL server is running with the --read-only option, so it cannot execute this statement
      1836 // Running in read-only mode
  ));

  @Override
  public boolean isNetworkException(final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLException) {
        SQLException sqlException = (SQLException) exception;

        // Hikari throws a network exception with SQL state 42000 if all the following points are true:
        // - HikariDataSource#getConnection is called and the cached connection that was grabbed is broken due to server
        // failover.
        // - the MariaDB driver is being used (the underlying driver determines the SQL state of the Hikari exception).
        //
        // The check for the Hikari MariaDB exception is added here because the exception handler is determined by the
        // database dialect. Consequently, this exception handler is used when using the MariaDB driver against a MySQL
        // database engine.
        if (isNetworkException(sqlException.getSQLState()) || isHikariMariaDbNetworkException(sqlException)) {
          return true;
        }
      } else if (targetDriverDialect != null) {
        String sqlState = targetDriverDialect.getSQLState(exception);
        if (!StringUtils.isNullOrEmpty(sqlState)) {
          return isNetworkException(sqlState);
        }
      }

      exception = exception.getCause();
    }

    return false;
  }

  @Override
  public boolean isNetworkException(final String sqlState) {
    if (sqlState == null) {
      return false;
    }

    // 08004 -  "Server rejected the connection" or "Connection refused by server"
    return sqlState.startsWith("08") && !"08004".equals(sqlState);
  }

  @Override
  public boolean isLoginException(final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLLoginException) {
        return true;
      }

      String sqlState = null;
      if (exception instanceof SQLException) {
        sqlState = ((SQLException) exception).getSQLState();
      } else if (targetDriverDialect != null) {
        sqlState = targetDriverDialect.getSQLState(exception);
      }

      if (isLoginException(sqlState)) {
        return true;
      }

      exception = exception.getCause();
    }

    return false;
  }

  @Override
  public boolean isLoginException(final String sqlState) {
    if (sqlState == null) {
      return false;
    }

    return SQLSTATE_ACCESS_ERROR.equals(sqlState);
  }

  @Override
  public boolean isReadOnlyConnectionException(
      final @Nullable String sqlState, final @Nullable Integer errorCode) {
    // HY000 - generic SQL state; use error code for more specific information
    return "HY000".equals(sqlState) && errorCode != null && (SQLSTATE_READ_ONLY_CONNECTION.contains(errorCode));
  }

  @Override
  public boolean isReadOnlyConnectionException(
      final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {

    Throwable exception = throwable;

    while (exception != null) {
      String sqlState = null;
      Integer errorCode = null;
      if (exception instanceof SQLException) {
        sqlState = ((SQLException) exception).getSQLState();
        errorCode = ((SQLException) exception).getErrorCode();
      } else if (targetDriverDialect != null) {
        sqlState = targetDriverDialect.getSQLState(exception);
      }

      if (isReadOnlyConnectionException(sqlState, errorCode)) {
        return true;
      }

      exception = exception.getCause();
    }

    return false;
  }

  private boolean isHikariMariaDbNetworkException(final SQLException sqlException) {
    if (sqlException.getSQLState() == null || sqlException.getMessage() == null) {
      return false;
    }

    return sqlException.getSQLState().equals(SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION)
        && sqlException.getMessage().contains(SET_NETWORK_TIMEOUT_ON_CLOSED_CONNECTION);
  }
}
