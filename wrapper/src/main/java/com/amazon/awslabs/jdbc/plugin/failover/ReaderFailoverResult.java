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

package com.amazon.awslabs.jdbc.plugin.failover;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This class holds results of Reader Failover Process.
 */

public class ReaderFailoverResult {

  private final Connection newConnection;
  private final int newConnectionIndex;
  private final boolean isConnected;
  private final SQLException exception;

  public ReaderFailoverResult(
      Connection newConnection, int newConnectionIndex, boolean isConnected) {
    this(newConnection, newConnectionIndex, isConnected, null);
  }

  public ReaderFailoverResult(
      Connection newConnection,
      int newConnectionIndex,
      boolean isConnected,
      SQLException exception) {
    this.newConnection = newConnection;
    this.newConnectionIndex = newConnectionIndex;
    this.isConnected = isConnected;
    this.exception = exception;
  }

  /**
   * Get new connection to a host.
   *
   * @return {@link Connection} New connection to a host. Returns null if no connection is established.
   */
  public Connection getConnection() {
    return newConnection;
  }

  /**
   * Get index of newly connected host.
   *
   * @return Index of connected host in topology Returns -1 (NO_CONNECTION_INDEX) if no connection is established.
   */
  public int getConnectionIndex() {
    return newConnectionIndex;
  }

  /**
   * Checks if process result is successful and new connection to host is established.
   *
   * @return True, if process successfully connected to a host.
   */
  public boolean isConnected() {
    return isConnected;
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
