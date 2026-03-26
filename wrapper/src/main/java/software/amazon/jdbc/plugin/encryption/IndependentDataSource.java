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

package software.amazon.jdbc.plugin.encryption;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.sql.DataSource;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.logging.ErrorContext;
import software.amazon.jdbc.util.Messages;

/**
 * DataSource implementation that creates independent connections using PluginService. This ensures
 * that MetadataManager gets its own connections and doesn't share with client applications.
 */
public class IndependentDataSource implements DataSource {

  private static final Logger LOGGER = Logger.getLogger(IndependentDataSource.class.getName());

  private final PluginService pluginService;
  private final Properties connectionProperties;
  private int loginTimeout = 0;
  private PrintWriter logWriter;

  // Connection monitoring metrics
  private final AtomicLong connectionRequestCount = new AtomicLong(0);
  private final AtomicLong successfulConnectionCount = new AtomicLong(0);
  private final AtomicLong failedConnectionCount = new AtomicLong(0);
  private volatile long lastSuccessfulConnectionTime = 0;
  private volatile long lastFailedConnectionTime = 0;

  /**
   * Creates an IndependentDataSource with the given PluginService.
   *
   * @param pluginService the PluginService to use for creating connections
   * @throws IllegalArgumentException if pluginService is null
   */
  public IndependentDataSource(PluginService pluginService) {
    this(pluginService, new Properties());
  }

  /**
   * Creates an IndependentDataSource with PluginService and connection properties.
   *
   * @param pluginService the PluginService to use for creating connections
   * @param connectionProperties additional connection properties
   * @throws IllegalArgumentException if pluginService is null
   */
  public IndependentDataSource(PluginService pluginService, Properties connectionProperties) {
    if (pluginService == null) {
      throw new IllegalArgumentException(Messages.get("IndependentDataSource.pluginServiceNull"));
    }

    this.pluginService = pluginService;
    this.connectionProperties =
        connectionProperties != null ? connectionProperties : new Properties();

    LOGGER.info(() -> Messages.get("IndependentDataSource.created"));
    LOGGER.finest(
        () -> Messages.get("IndependentDataSource.config",
            new Object[]{this.connectionProperties.size()}));
  }

  @Override
  public Connection getConnection() throws SQLException {
    long requestId = connectionRequestCount.incrementAndGet();

    LOGGER.finest(
        () -> Messages.get("IndependentDataSource.connectionRequest", new Object[]{requestId}));
    return createNewConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    long requestId = connectionRequestCount.incrementAndGet();

    LOGGER.finest(
        () -> Messages.get("IndependentDataSource.connectionRequestWithCreds", new Object[]{requestId}));

    // Create modified properties with the provided credentials
    Properties modifiedProps = new Properties(connectionProperties);
    modifiedProps.setProperty("user", username);
    modifiedProps.setProperty("password", password);

    return createNewConnection(modifiedProps);
  }

  /**
   * Creates a new independent connection using the PluginService.
   *
   * @return a new database connection
   * @throws SQLException if connection creation fails
   */
  private Connection createNewConnection() throws SQLException {
    return createNewConnection(connectionProperties);
  }

  /**
   * Creates a new independent connection using the PluginService with specified properties.
   *
   * @param props the connection properties to use
   * @return a new database connection
   * @throws SQLException if connection creation fails
   */
  private Connection createNewConnection(Properties props) throws SQLException {
    long startTime = System.currentTimeMillis();

    LOGGER.finest(() -> Messages.get("IndependentDataSource.creatingConnection"));

    try {
      // Get current host spec from PluginService
      HostSpec hostSpec = pluginService.getCurrentHostSpec();

      // Create connection using PluginService
      final Connection connection = pluginService.forceConnect(hostSpec, props);

      long duration = System.currentTimeMillis() - startTime;
      successfulConnectionCount.incrementAndGet();
      lastSuccessfulConnectionTime = System.currentTimeMillis();

      LOGGER.info(
          () -> Messages.get("IndependentDataSource.connectionCreated",
              new Object[]{duration, successfulConnectionCount.get(), failedConnectionCount.get()}));

      return connection;

    } catch (SQLException e) {
      long duration = System.currentTimeMillis() - startTime;
      failedConnectionCount.incrementAndGet();
      lastFailedConnectionTime = System.currentTimeMillis();

      LOGGER.severe(
          () -> Messages.get("IndependentDataSource.connectionFailed",
              new Object[]{duration, e.getMessage(),
                  successfulConnectionCount.get(), failedConnectionCount.get()}));

      // Create detailed error context for troubleshooting
      String errorDetails =
          ErrorContext.builder()
              .operation("CREATE_INDEPENDENT_CONNECTION_VIA_PLUGIN_SERVICE")
              .buildMessage("Connection creation failed: " + e.getMessage());

      LOGGER.severe(() -> Messages.get("IndependentDataSource.connectionErrorDetails", new Object[]{errorDetails}));

      throw new SQLException(
          Messages.get("IndependentDataSource.connectionCreationFailed",
              new Object[]{e.getMessage()}), e);
    }
  }

  /**
   * Validates that a connection can be created with the current PluginService.
   *
   * @return true if a connection can be created, false otherwise
   */
  public boolean validateConnection() {
    try (Connection conn = getConnection()) {
      return conn != null && !conn.isClosed();
    } catch (SQLException e) {
      LOGGER.finest(() -> Messages.get("IndependentDataSource.validationFailed"));
      return false;
    }
  }

  /**
   * Gets the PluginService used by this DataSource.
   *
   * @return the PluginService
   */
  public PluginService getPluginService() {
    return pluginService;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException(
        Messages.get("IndependentDataSource.cannotUnwrap", new Object[]{iface.getName()}));
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    this.logWriter = out;
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    this.loginTimeout = seconds;
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return loginTimeout;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(Messages.get("IndependentDataSource.parentLoggerNotSupported"));
  }

  // Connection monitoring and metrics methods

  /**
   * Gets the total number of connection requests made to this DataSource.
   *
   * @return the total connection request count
   */
  public long getConnectionRequestCount() {
    return connectionRequestCount.get();
  }

  /**
   * Gets the number of successful connection creations.
   *
   * @return the successful connection count
   */
  public long getSuccessfulConnectionCount() {
    return successfulConnectionCount.get();
  }

  /**
   * Gets the number of failed connection creation attempts.
   *
   * @return the failed connection count
   */
  public long getFailedConnectionCount() {
    return failedConnectionCount.get();
  }

  /**
   * Gets the timestamp of the last successful connection creation.
   *
   * @return the timestamp in milliseconds, or 0 if no successful connections
   */
  public long getLastSuccessfulConnectionTime() {
    return lastSuccessfulConnectionTime;
  }

  /**
   * Gets the timestamp of the last failed connection attempt.
   *
   * @return the timestamp in milliseconds, or 0 if no failed connections
   */
  public long getLastFailedConnectionTime() {
    return lastFailedConnectionTime;
  }

  /**
   * Calculates the connection success rate as a percentage.
   *
   * @return the success rate (0.0 to 1.0), or 1.0 if no attempts have been made
   */
  public double getConnectionSuccessRate() {
    long total = connectionRequestCount.get();
    if (total == 0) {
      return 1.0;
    }

    return (double) successfulConnectionCount.get() / total;
  }

  /**
   * Checks if the DataSource is currently healthy based on recent connection attempts.
   *
   * @return true if the DataSource appears healthy, false otherwise
   */
  public boolean isHealthy() {
    // Consider healthy if success rate is above 80% or if we haven't had failures recently
    double successRate = getConnectionSuccessRate();
    long timeSinceLastFailure = System.currentTimeMillis() - lastFailedConnectionTime;

    return successRate >= 0.8
        || (lastFailedConnectionTime == 0)
        || (timeSinceLastFailure > 300000); // 5 minutes
  }

  /**
   * Gets a comprehensive status message about the DataSource health and metrics.
   *
   * @return a detailed status message
   */
  public String getHealthStatus() {
    StringBuilder sb = new StringBuilder();

    sb.append("IndependentDataSource Status: ");
    sb.append("Healthy=").append(isHealthy()).append(", ");
    sb.append("Requests=").append(connectionRequestCount.get()).append(", ");
    sb.append("Successful=").append(successfulConnectionCount.get()).append(", ");
    sb.append("Failed=").append(failedConnectionCount.get()).append(", ");
    sb.append("SuccessRate=").append(String.format("%.2f%%", getConnectionSuccessRate() * 100));

    if (lastSuccessfulConnectionTime > 0) {
      long timeSinceSuccess = System.currentTimeMillis() - lastSuccessfulConnectionTime;
      sb.append(", LastSuccess=").append(timeSinceSuccess).append("ms ago");
    }

    if (lastFailedConnectionTime > 0) {
      long timeSinceFailure = System.currentTimeMillis() - lastFailedConnectionTime;
      sb.append(", LastFailure=").append(timeSinceFailure).append("ms ago");
    }

    return sb.toString();
  }

  /** Logs the current health status and metrics. */
  public void logHealthStatus() {
    String status = getHealthStatus();

    if (isHealthy()) {
      LOGGER.info(() -> Messages.get("IndependentDataSource.healthCheck", new Object[]{status}));
    } else {
      LOGGER.warning(
          () -> Messages.get("IndependentDataSource.healthCheckUnhealthy", new Object[]{status}));
    }
  }
}
