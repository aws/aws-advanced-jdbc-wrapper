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

package software.amazon.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface HostListProvider {

  List<HostSpec> refresh() throws SQLException;

  List<HostSpec> refresh(Connection connection) throws SQLException;

  List<HostSpec> forceRefresh() throws SQLException;

  List<HostSpec> forceRefresh(Connection connection) throws SQLException;

  /**
   * Evaluates the host role of the given connection - either a writer or a reader.
   *
   * @param connection a connection to the database instance whose role should be determined
   * @return the role of the given connection - either a writer or a reader
   * @throws SQLException if there is a problem executing or processing the SQL query used to
   *                      determine the host role
   */
  HostRole getHostRole(Connection connection) throws SQLException;

  HostSpec identifyConnection(Connection connection) throws SQLException;

  String getClusterId() throws UnsupportedOperationException, SQLException;
}
