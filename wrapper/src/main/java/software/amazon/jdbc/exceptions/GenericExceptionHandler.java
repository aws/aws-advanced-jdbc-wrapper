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
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.StringUtils;

public class GenericExceptionHandler implements ExceptionHandler {

  static final List<String> ACCESS_ERRORS = Arrays.asList("28000", "28P01");
  static final List<String> NETWORK_ERRORS = Arrays.asList(
      "53", // insufficient resources
      "57P01", // admin shutdown
      "57P02", // crash shutdown
      "57P03", // cannot connect now
      "58", // system error (backend)
      "99", // unexpected error
      "F0", // configuration file error (backend)
      "XX", // internal error (backend)
      "08"
  );

  @Override
  public boolean isNetworkException(Throwable throwable) {
    return this.isNetworkException(throwable, null);
  }

  @Override
  public boolean isNetworkException(final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLException) {
        return isNetworkException(((SQLException) exception).getSQLState());
      } else if (targetDriverDialect != null) {
        String sqlState = targetDriverDialect.getSQLState(throwable);
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

    for (final String errors : NETWORK_ERRORS) {
      if (sqlState.startsWith(errors)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean isLoginException(Throwable throwable) {
    return this.isLoginException(throwable, null);
  }

  @Override
  public boolean isLoginException(final Throwable throwable, TargetDriverDialect targetDriverDialect) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLLoginException) {
        return true;
      }

      String sqlState = null;
      if (exception instanceof SQLException) {
        sqlState = ((SQLException) exception).getSQLState();
      } else if (targetDriverDialect != null) {
        sqlState = targetDriverDialect.getSQLState(throwable);
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
    return ACCESS_ERRORS.contains(sqlState);
  }
}
