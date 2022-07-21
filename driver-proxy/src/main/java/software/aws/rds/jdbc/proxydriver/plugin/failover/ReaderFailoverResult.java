/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.failover;

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

  /**
   * ReaderFailoverResult constructor.
   */
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
