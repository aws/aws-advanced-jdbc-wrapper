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

public class ExceptionManager {

  protected static ExceptionHandler customHandler;

  private static GenericExceptionHandler genericExceptionHandler;
  private static MySQLExceptionHandler mySQLExceptionHandler;
  private static PgExceptionHandler pgExceptionHandler;
  private static MariaDBExceptionHandler mariaDBExceptionHandler;

  public static void setCustomHandler(final ExceptionHandler exceptionHandler) {
    customHandler = exceptionHandler;
  }

  public static void resetCustomHandler() {
    customHandler = null;
  }

  public boolean isLoginException(final String driverProtocol, final Throwable throwable) {
    final ExceptionHandler handler = getHandler(driverProtocol);
    return handler.isLoginException(throwable);
  }

  public boolean isLoginException(final String driverProtocol, final String sqlState) {
    final ExceptionHandler handler = getHandler(driverProtocol);
    return handler.isLoginException(sqlState);
  }

  public boolean isNetworkException(final String driverProtocol, final Throwable throwable) {
    final ExceptionHandler handler = getHandler(driverProtocol);
    return handler.isNetworkException(throwable);
  }

  public boolean isNetworkException(final String driverProtocol, final String sqlState) {
    final ExceptionHandler handler = getHandler(driverProtocol);
    return handler.isNetworkException(sqlState);
  }

  private ExceptionHandler getHandler(final String driverProtocol) {
    if (customHandler != null) {
      return customHandler;
    }

    if (driverProtocol == null) {
      return getGenericExceptionHandler();
    }

    if (driverProtocol.contains(":mysql:")) {
      return getMySQLExceptionHandler();
    }

    if (driverProtocol.contains(":mariadb:")) {
      return getMariaDBExceptionHandler();
    }

    if (driverProtocol.contains(":postgresql:")) {
      return getPgExceptionHandler();
    }

    return getGenericExceptionHandler();
  }

  private GenericExceptionHandler getGenericExceptionHandler() {
    if (genericExceptionHandler == null) {
      genericExceptionHandler = new GenericExceptionHandler();
    }
    return genericExceptionHandler;
  }

  private MySQLExceptionHandler getMySQLExceptionHandler() {
    if (mySQLExceptionHandler == null) {
      mySQLExceptionHandler = new MySQLExceptionHandler();
    }
    return mySQLExceptionHandler;
  }

  private PgExceptionHandler getPgExceptionHandler() {
    if (pgExceptionHandler == null) {
      pgExceptionHandler = new PgExceptionHandler();
    }
    return pgExceptionHandler;
  }

  private MariaDBExceptionHandler getMariaDBExceptionHandler() {
    return mariaDBExceptionHandler;
  }
}
