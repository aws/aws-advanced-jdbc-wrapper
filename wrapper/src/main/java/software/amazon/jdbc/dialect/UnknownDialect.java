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

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.GenericExceptionHandler;

public class UnknownDialect implements Dialect {

  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.AURORA_PG,
      DialectCodes.AURORA_MYSQL,
      DialectCodes.RDS_PG,
      DialectCodes.RDS_MYSQL,
      DialectCodes.PG,
      DialectCodes.MYSQL,
      DialectCodes.MARIADB
  );

  private static GenericExceptionHandler genericExceptionHandler;

  @Override
  public int getDefaultPort() {
    return HostSpec.NO_PORT;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (genericExceptionHandler == null) {
      genericExceptionHandler = new GenericExceptionHandler();
    }
    return genericExceptionHandler;
  }

  @Override
  public String getHostAliasQuery() {
    return null;
  }

  @Override
  public String getServerVersionQuery() {
    return null;
  }

  @Override
  public boolean isDialect(final Connection connection) {
    return false;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }
}
