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

package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;

public interface HostListProvider {

  /**
   * Get current topology information for the connected database. The returned topology should be the static host list
   * if `this` is a {@link StaticHostListProvider}. The returned topology should be fresh info returned from a topology
   * query if `this` is a {@link DynamicHostListProvider},
   *
   * @param conn the connection to use to query the database for topology information, if needed.
   * @param initialHostSpec the host details of the initial connection.
   * @return the topology information for the connected database.
   * @throws SQLException if an error occurs while attempting to acquire the database topology information.
   */
  List<HostSpec> getCurrentTopology(Connection conn, HostSpec initialHostSpec) throws SQLException;

  List<HostSpec> refresh() throws SQLException;

  /**
   * Force a host list provider to update its topology information. Results will be returned when the topology is
   * updated or the writer is verified, unless the default timeout is hit. It the caller needs topology from a verified
   * writer or with a different timeout value, they should call {@link #forceRefresh(boolean, long)} instead.
   *
   * @return a list of host details representing a cluster topology
   * @throws SQLException if there's errors updating topology
   * @throws TimeoutException if topology update takes longer time than expected
   */
  List<HostSpec> forceRefresh() throws SQLException, TimeoutException;

  /**
   * Force a host list provider to update its topology information. Results will be returned when the topology is
   * updated or the writer is verified, unless the timeout is hit.
   *
   * @param shouldVerifyWriter a flag indicating that the provider should verify the writer before
   *                           returning the updated topology.
   * @param timeoutMs timeout in msec to wait until topology is updated or the writer is verified.
   *                  If a timeout of 0 is provided, a topology update will be initiated but cached topology
   *                  will be returned. If a non-zero timeout is provided and the timeout is hit,
   *                  a TimeoutException will be thrown.
   * @return a list of host details representing a cluster topology
   * @throws SQLException if there's errors updating topology
   * @throws TimeoutException if topology update takes longer time than expected
   */
  List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException;

  /**
   * Evaluates the host role of the given connection - either a writer or a reader.
   *
   * @param connection a connection to the database instance whose role should be determined
   * @return the role of the given connection - either a writer or a reader
   * @throws SQLException if there is a problem executing or processing the SQL query used to
   *                      determine the host role
   */
  HostRole getHostRole(Connection connection) throws SQLException;

  @Nullable
  HostSpec identifyConnection(Connection connection) throws SQLException;

  String getClusterId() throws UnsupportedOperationException, SQLException;

  void stopMonitor();
}
