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
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.StateSnapshotProvider;

/**
 * Handles monitoring connection lifecycle: accepting connections offered by the monitor,
 * and asynchronously upgrading to a higher-priority connection when possible.
 */
public interface MonitoringConnectionHandler extends StateSnapshotProvider {

  /**
   * Called when a connection is offered to the handler (e.g., a writer connection found during panic mode).
   * The handler decides whether to accept it as the monitoring connection or open a different one
   * that better matches the configured priority.
   *
   * @param conn the offered connection
   * @param isWriter true if the connection is to a writer instance
   * @param hostSpec the host spec of the connection
   * @return true if the connection was accepted and set as the monitoring connection,
   *         false if the connection was rejected (caller should close it)
   */
  boolean acceptConnection(Connection conn, boolean isWriter, HostSpec hostSpec);

  /**
   * Offers a batch of harvested connections (e.g., from node monitoring threads after panic mode resolves)
   * to the handler. The handler picks the best one according to its priority list and sets it as the
   * monitoring connection. Connections that were not selected are returned in the result so the caller
   * can dispose of them appropriately.
   *
   * <p>The handler determines each connection's role from the topology (the writer host is known) — readers
   * are everything else. The handler may also use the host's region for region-aware priorities.
   *
   * @param connections     a map of {@code host -> connection} harvested from node threads
   * @param writerHostSpec  the host spec of the writer (if known)
   * @param topology        the current topology, used for region/role context
   * @return the host of the connection that was selected and set as the monitoring connection,
   *         or null if no connection was selected
   */
  @Nullable HostSpec acceptConnections(
      Map<HostSpec, AtomicConnection> connections,
      @Nullable HostSpec writerHostSpec,
      @Nullable List<HostSpec> topology);

  /**
   * Non-blocking attempt to upgrade the monitoring connection to a higher-priority node.
   * If the current connection already satisfies the highest priority, this is a no-op.
   * Otherwise, it checks for completed async upgrade attempts and submits new ones as needed.
   *
   * @param currentTopology the current cluster topology
   */
  void attemptConnectionUpgrade(List<HostSpec> currentTopology);

  /**
   * Cleans up resources: cancels pending upgrade attempts and closes any connections held by the handler.
   */
  void close();
}
