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

package software.amazon.jdbc.plugin.failover;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import software.amazon.jdbc.HostSpec;

/** This class holds results of Writer Failover Process. */
public class WriterFailoverResult {

  private final boolean isConnected;
  private final boolean isNewHost;
  private final List<HostSpec> topology;
  private final Connection newConnection;
  private final String taskName;
  private final SQLException exception;

  public WriterFailoverResult(
      final boolean isConnected,
      final boolean isNewHost,
      final List<HostSpec> topology,
      final Connection newConnection,
      final String taskName) {
    this(isConnected, isNewHost, topology, newConnection, taskName, null);
  }

  public WriterFailoverResult(
      final boolean isConnected,
      final boolean isNewHost,
      final List<HostSpec> topology,
      final Connection newConnection,
      final String taskName,
      final SQLException exception) {
    this.isConnected = isConnected;
    this.isNewHost = isNewHost;
    this.topology = topology;
    this.newConnection = newConnection;
    this.taskName = taskName;
    this.exception = exception;
  }

  /**
   * Checks if process result is successful and new connection to host is established.
   *
   * @return True, if process successfully connected to a host.
   */
  public boolean isConnected() {
    return this.isConnected;
  }

  /**
   * Checks if process successfully connected to a new host.
   *
   * @return True, if process successfully connected to a new host. False, if process successfully
   *     re-connected to the same host.
   */
  public boolean isNewHost() {
    return this.isNewHost;
  }

  /**
   * Get the latest topology.
   *
   * @return List of hosts that represent the latest topology. Returns null if no connection is
   *     established.
   */
  public List<HostSpec> getTopology() {
    return this.topology;
  }

  /**
   * Get the new connection established by the failover procedure if successful.
   *
   * @return {@link Connection} New connection to a host. Returns null if the failover procedure was
   *     unsuccessful.
   */
  public Connection getNewConnection() {
    return this.newConnection;
  }

  /**
   * Get the name of the writer failover task that created this result.
   *
   * @return The name of the writer failover task that created this result.
   */
  public String getTaskName() {
    return this.taskName;
  }

  /**
   * Get the exception raised during failover.
   *
   * @return a {@link SQLException}.
   */
  public SQLException getException() {
    return exception;
  }
}
