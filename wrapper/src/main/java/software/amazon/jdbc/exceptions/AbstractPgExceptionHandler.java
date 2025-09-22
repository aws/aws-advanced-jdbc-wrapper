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
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.StringUtils;

public abstract class AbstractPgExceptionHandler implements ExceptionHandler {

  protected static final String READ_ONLY_CONNECTION_SQLSTATE = "25006";

  public abstract List<String> getNetworkErrors();

  public abstract List<String> getAccessErrors();

  @Override
  public boolean isNetworkException(
      final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    Throwable exception = throwable;

    while (exception != null) {
      if (exception instanceof SQLException) {
        return isNetworkException(((SQLException) exception).getSQLState());
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

    for (final String pgSqlState : getNetworkErrors()) {
      if (sqlState.startsWith(pgSqlState)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean isLoginException(
      final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
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
    return getAccessErrors().contains(sqlState);
  }

  @Override
  public boolean isReadOnlyConnectionException(
      final @Nullable String sqlState, final @Nullable Integer errorCode) {
    return READ_ONLY_CONNECTION_SQLSTATE.equals(sqlState);
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
}
