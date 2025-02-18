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

import software.amazon.jdbc.Driver;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public class ExceptionManager {

  /**
   * Sets a custom exception handler.
   *
   * @deprecated Use software.amazon.jdbc.Driver instead
   */
  @Deprecated
  public static void setCustomHandler(final ExceptionHandler exceptionHandler) {
    Driver.setCustomExceptionHandler(exceptionHandler);
  }

  /**
   * Resets a custom exception handler.
   *
   * @deprecated Use software.amazon.jdbc.Driver instead
   */
  @Deprecated
  public static void resetCustomHandler() {
    Driver.resetCustomExceptionHandler();
  }

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

  private ExceptionHandler getHandler(final Dialect dialect) {
    final ExceptionHandler customHandler = Driver.getCustomExceptionHandler();
    return customHandler != null ? customHandler : dialect.getExceptionHandler();
  }
}
