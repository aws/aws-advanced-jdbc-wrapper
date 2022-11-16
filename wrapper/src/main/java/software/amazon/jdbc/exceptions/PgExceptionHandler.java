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
import java.util.List;

public class PgExceptionHandler implements ExceptionHandler {

  // The following SQL States for Postgresql are considered as "communication" errors
  public static final List<String> NETWORK_ERRORS = Arrays.asList(
      "53", // insufficient resources
      "57P01", // admin shutdown
      "57P02", // crash shutdown
      "57P03", // cannot connect now
      "58", // system error (backend)
      "99", // unexpected error
      "F0", // configuration file error (backend)
      "XX" // internal error (backend)
  );

  public static final String ACCESS_ERROR = "28P01";

  @Override
  public boolean isNetworkException(Throwable throwable) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLException) {
        return isNetworkException(((SQLException) exception).getSQLState());
      }

      exception = exception.getCause();
    }

    return false;
  }

  @Override
  public boolean isNetworkException(String sqlState) {
    if (sqlState == null) {
      return false;
    }

    for (final String pgSqlState : NETWORK_ERRORS) {
      if (sqlState.startsWith(pgSqlState)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean isLoginException(Throwable throwable) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLException) {
        return isLoginException(((SQLException) exception).getSQLState());
      }

      exception = exception.getCause();
    }

    return false;
  }

  @Override
  public boolean isLoginException(String sqlState) {
    if (sqlState == null) {
      return false;
    }
    return ACCESS_ERROR.contains(sqlState);
  }
}
