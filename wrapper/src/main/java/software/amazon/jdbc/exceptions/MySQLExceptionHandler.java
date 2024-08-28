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

import com.mysql.cj.exceptions.CJException;
import java.sql.SQLException;

public class MySQLExceptionHandler implements ExceptionHandler {
  public static final String SQLSTATE_ACCESS_ERROR = "28000";
  public static final String SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
  public static final String SET_NETWORK_TIMEOUT_ON_CLOSED_CONNECTION =
      "setNetworkTimeout cannot be called on a closed connection";

  @Override
  public boolean isNetworkException(final Throwable throwable) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLException) {
        SQLException sqlException = (SQLException) exception;

        // Hikari throws a network exception with SQL state 42000 if:
        // - the MariaDB driver is being used (the underlying driver determines the SQL state of the Hikari exception)
        // - HikariDataSource#getConnection is called and the cached connection is broken due to server failover.
        //
        // The check for the Hikari MariaDB exception is added here because the exception handler is determined by the
        // database dialect. Consequently, this exception handler is used when using the MariaDB driver against a MySQL
        // database engine.
        if (isNetworkException(sqlException.getSQLState()) || isHikariMariaDbNetworkException(sqlException)) {
          return true;
        }
      } else if (exception instanceof CJException) {
        return isNetworkException(((CJException) exception).getSQLState());
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

    return sqlState.startsWith("08");
  }

  @Override
  public boolean isLoginException(final Throwable throwable) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLLoginException) {
        return true;
      }
      if (exception instanceof SQLException) {
        return isLoginException(((SQLException) exception).getSQLState());
      } else if (exception instanceof CJException) {
        return isLoginException(((CJException) exception).getSQLState());
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

  private boolean isHikariMariaDbNetworkException(final SQLException sqlException) {
    return sqlException.getSQLState().equals(SYNTAX_ERROR_OR_ACCESS_VIOLATION)
        && sqlException.getMessage().contains(SET_NETWORK_TIMEOUT_ON_CLOSED_CONNECTION);
  }
}
