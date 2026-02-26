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

package software.amazon.jdbc.plugin.encryption.wrapper;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import software.amazon.jdbc.plugin.encryption.KmsEncryptionPlugin;

/**
 * A DataSource wrapper that integrates encryption capabilities with the AWS Advanced JDBC Wrapper.
 * This DataSource wraps connections to provide transparent encryption/decryption functionality.
 */
public class EncryptingDataSource implements DataSource {

  private static final Logger LOGGER = Logger.getLogger(EncryptingDataSource.class.getName());

  private final DataSource delegate;
  private final KmsEncryptionPlugin encryptionPlugin;
  private final Properties encryptionProperties;
  private volatile boolean closed = false;

  /**
   * Creates an encrypting DataSource that wraps the provided DataSource.
   *
   * @param delegate The underlying DataSource to wrap
   * @param encryptionProperties Properties for configuring encryption
   * @throws SQLException if encryption plugin initialization fails
   */
  public EncryptingDataSource(DataSource delegate, Properties encryptionProperties)
      throws SQLException {
    this.delegate = delegate;
    this.encryptionProperties = new Properties();
    this.encryptionProperties.putAll(encryptionProperties);

    // Initialize the encryption plugin
    this.encryptionPlugin = new KmsEncryptionPlugin();
    this.encryptionPlugin.initialize(encryptionProperties);

    LOGGER.info("EncryptingDataSource initialized with encryption plugin");
  }

  @Override
  public Connection getConnection() throws SQLException {
    checkNotClosed();

    Connection connection = null;
    try {
      connection = delegate.getConnection();
      validateConnection(connection);
      return new EncryptingConnection(connection, encryptionPlugin);
    } catch (SQLException e) {
      // Close the connection if we got one but failed to wrap it
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException closeEx) {
          LOGGER.warning(
              () ->
                  String.format(
                      "Failed to close connection after wrapping failure %s",
                      closeEx.getMessage()));
        }
      }

      LOGGER.severe(
          () ->
              String.format(
                  "Failed to get connection from delegate DataSource %s", e.getMessage()));
      throw new SQLException("Failed to obtain encrypted connection: " + e.getMessage(), e);
    }
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    checkNotClosed();

    Connection connection = null;
    try {
      connection = delegate.getConnection(username, password);
      validateConnection(connection);
      return new EncryptingConnection(connection, encryptionPlugin);
    } catch (SQLException e) {
      // Close the connection if we got one but failed to wrap it
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException closeEx) {
          LOGGER.warning(
              () ->
                  String.format(
                      "Failed to close connection after wrapping failure %s",
                      closeEx.getMessage()));
        }
      }

      LOGGER.severe(
          () ->
              String.format(
                  "Failed to get connection from delegate DataSource with credentials %s",
                  e.getMessage()));
      throw new SQLException("Failed to obtain encrypted connection: " + e.getMessage(), e);
    }
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return delegate.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    delegate.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    delegate.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return delegate.getLoginTimeout();
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    return delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass()) || delegate.isWrapperFor(iface);
  }

  /**
   * Gets the underlying DataSource.
   *
   * @return The wrapped DataSource
   */
  public DataSource getDelegate() {
    return delegate;
  }

  /**
   * Gets the encryption plugin instance.
   *
   * @return The KmsEncryptionPlugin instance
   */
  public KmsEncryptionPlugin getEncryptionPlugin() {
    return encryptionPlugin;
  }

  /**
   * Tests if the DataSource can provide a valid connection. This method attempts to get a
   * connection and immediately closes it.
   *
   * @return true if a valid connection can be obtained, false otherwise
   */
  public boolean isConnectionAvailable() {
    if (closed) {
      return false;
    }

    Connection testConnection = null;
    try {
      testConnection = delegate.getConnection();
      return testConnection != null && !testConnection.isClosed() && testConnection.isValid(5);
    } catch (SQLException e) {
      LOGGER.finest(() -> String.format("Connection availability test failed %s", e.getMessage()));
      return false;
    } finally {
      if (testConnection != null) {
        try {
          testConnection.close();
        } catch (SQLException e) {
          LOGGER.finest(() -> String.format("Failed to close test connection %s", e.getMessage()));
        }
      }
    }
  }

  /** Closes the encryption plugin and releases resources. */
  public void close() {
    if (closed) {
      return;
    }

    LOGGER.info(() -> "Closing EncryptingDataSource");
    closed = true;

    if (encryptionPlugin != null) {
      try {
        encryptionPlugin.cleanup();
      } catch (Exception e) {
        LOGGER.warning(
            () -> String.format("Error during encryption plugin cleanup %s", e.getMessage()));
      }
    }

    // If the delegate DataSource has a close method, call it
    if (delegate != null) {
      try {
        // Try to close the delegate if it's closeable (e.g., HikariDataSource, etc.)
        if (delegate instanceof AutoCloseable) {
          ((AutoCloseable) delegate).close();
          LOGGER.finest(() -> "Closed delegate DataSource");
        }
      } catch (Exception e) {
        LOGGER.warning(() -> String.format("Error closing delegate DataSource %s", e.getMessage()));
      }
    }

    LOGGER.info("EncryptingDataSource closed");
  }

  /**
   * Checks if this DataSource has been closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Validates that the DataSource is not closed.
   *
   * @throws SQLException if the DataSource is closed
   */
  private void checkNotClosed() throws SQLException {
    if (closed) {
      throw new SQLException("EncryptingDataSource has been closed");
    }
  }

  /**
   * Validates that a connection is valid and not closed.
   *
   * @param connection the connection to validate
   * @throws SQLException if the connection is invalid
   */
  private void validateConnection(Connection connection) throws SQLException {
    if (connection == null) {
      throw new SQLException("Delegate DataSource returned null connection");
    }

    if (connection.isClosed()) {
      throw new SQLException("Delegate DataSource returned a closed connection");
    }

    // Test the connection with a short timeout
    try {
      if (!connection.isValid(5)) { // 5 second timeout
        throw new SQLException("Delegate DataSource returned an invalid connection");
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> String.format("Connection validation failed %s", e.getMessage()));
      throw new SQLException("Connection validation failed: " + e.getMessage(), e);
    }
  }
}
