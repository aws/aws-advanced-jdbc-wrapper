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

import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public class ExceptionManager {

  public boolean isLoginException(
      final Dialect dialect, final Throwable throwable, final TargetDriverDialect targetDriverDialect) {
    final ExceptionHandler handler = getHandler(dialect);
    return handler.isLoginException(throwable, targetDriverDialect);
  }

  public boolean isLoginException(final Dialect dialect, final String sqlState) {
    final ExceptionHandler handler = getHandler(dialect);
    return handler.isLoginException(sqlState);
  }

  public boolean isNetworkException(
      final Dialect dialect, final Throwable throwable, final TargetDriverDialect targetDriverDialect) {
    final ExceptionHandler handler = getHandler(dialect);
    return handler.isNetworkException(throwable, targetDriverDialect);
  }

  public boolean isNetworkException(final Dialect dialect, final String sqlState) {
    final ExceptionHandler handler = getHandler(dialect);
    return handler.isNetworkException(sqlState);
  }

  public boolean isReadOnlyConnectionException(
      final Dialect dialect, final Throwable throwable, final TargetDriverDialect targetDriverDialect) {
    final ExceptionHandler handler = getHandler(dialect);
    return handler.isReadOnlyConnectionException(throwable, targetDriverDialect);
  }

  public boolean isReadOnlyConnectionException(
      final Dialect dialect, final @Nullable String sqlState, final @Nullable Integer errorCode) {
    final ExceptionHandler handler = getHandler(dialect);
    return handler.isReadOnlyConnectionException(sqlState, errorCode);
  }

  private ExceptionHandler getHandler(final Dialect dialect) {
    final ExceptionHandler customHandler = Driver.getCustomExceptionHandler();
    return customHandler != null ? customHandler : dialect.getExceptionHandler();
  }
}
