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

package software.amazon.jdbc.dialect;

import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MariaDBExceptionHandler;

public class MariaDBDialect extends MySQLDialect implements DatabaseDialect {
  static final ExceptionHandler exceptionHandler = new MariaDBExceptionHandler();
  private static final String URL_SCHEME = "jdbc:mariadb://";

  @Override
  public ExceptionHandler getExceptionHandler() {
    return exceptionHandler;
  }

  public String getURLScheme() {
    return URL_SCHEME;
  }

}
