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
import software.amazon.jdbc.exceptions.MySQLExceptionHandler;

public class MySQLDialect implements DatabaseDialect{
  private static final ExceptionHandler exceptionHandler = new MySQLExceptionHandler();
  private static final String MYSQL_RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID "
          + "FROM information_schema.replica_host_status "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "ORDER BY LAST_UPDATE_TIMESTAMP";
  private static final String MYSQL_RETRIEVE_HOST_PORT_SQL =
      "SELECT CONCAT(@@hostname, ':', @@port)";

  private static final String MYSQL_READONLY_QUERY = "SELECT @@innodb_read_only AS is_reader";

  private static final String MYSQL_GET_INSTANCE_NAME_SQL = "SELECT @@aurora_server_id";
  private static final String MYSQL_INSTANCE_NAME_COL = "@@aurora_server_id";
  private static final int MYSQL_PORT = 3306;
  private static final String READ_ONLY_COLUMN_NAME = "is_reader";
  private static final String URL_SCHEME = "jdbc:mysql://";

  @Override
  public String getInstanceNameQuery() {
    return MYSQL_GET_INSTANCE_NAME_SQL;
  }

  @Override
  public String getInstanceNameColumn() {
    return MYSQL_INSTANCE_NAME_COL;
  }
  public boolean isSupported() {
    return true;
  }
  public int getDefaultPort() {
    return MYSQL_PORT;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    return exceptionHandler;
  }
  @Override
  public String getTopologyQuery() {
    return MYSQL_RETRIEVE_TOPOLOGY_SQL;
  }

  @Override
  public String getReadOnlyQuery() {
    return MYSQL_READONLY_QUERY;
  }
  public String getReadOnlyColumnName(){
    return READ_ONLY_COLUMN_NAME;
  }

  @Override
  public String getHostPortQuery() {
    return MYSQL_RETRIEVE_HOST_PORT_SQL;
  }
  public String getURLScheme() {
    return URL_SCHEME;
  }

}
