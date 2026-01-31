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

package software.amazon.jdbc.plugin.encryption.example;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;
import software.amazon.jdbc.factory.EncryptingDataSourceFactory;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingDataSource;

/**
 * Example demonstrating proper DataSource lifecycle management with encryption. Shows how to handle
 * connection failures and DataSource state management.
 */
public class DataSourceLifecycleExample {

  private static final Logger LOGGER = Logger.getLogger(DataSourceLifecycleExample.class.getName());

  public static void main(String[] args) {
    EncryptingDataSource dataSource = null;

    try {
      // Create the DataSource
      dataSource = createDataSource();

      // Demonstrate proper usage patterns
      demonstrateHealthyUsage(dataSource);

      // Demonstrate error handling
      demonstrateErrorHandling(dataSource);

      // Demonstrate lifecycle management
      demonstrateLifecycleManagement(dataSource);

    } catch (Exception e) {
      LOGGER.severe(() -> String.format("Example execution failed %s", e.getMessage()));
    } finally {
      // Always clean up resources
      if (dataSource != null) {
        dataSource.close();
        LOGGER.info("DataSource closed in finally block");
      }
    }
  }

  /** Creates an EncryptingDataSource for demonstration. */
  private static EncryptingDataSource createDataSource() throws SQLException {
    LOGGER.info("=== Creating EncryptingDataSource ===");

    EncryptingDataSource dataSource =
        new EncryptingDataSourceFactory.Builder()
            .jdbcUrl("jdbc:postgresql://localhost:5432/mydb")
            .username("myuser")
            .password("mypassword")
            .kmsKeyArn(
                "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
            .region("us-east-1")
            .cacheEnabled(true)
            .build();

    LOGGER.info("EncryptingDataSource created successfully");
    return dataSource;
  }

  /** Demonstrates healthy DataSource usage patterns. */
  private static void demonstrateHealthyUsage(EncryptingDataSource dataSource) {
    LOGGER.info("=== Demonstrating Healthy Usage ===");

    // Check if DataSource is available before using
    if (!dataSource.isConnectionAvailable()) {
      LOGGER.warning("DataSource is not available - skipping operations");
      return;
    }

    // Use try-with-resources for proper connection management
    try (Connection connection = dataSource.getConnection()) {
      LOGGER.info(
          () ->
              String.format(
                  "Successfully obtained connection: %s", connection.getClass().getSimpleName()));

      // Verify connection is valid
      if (connection.isValid(5)) {
        LOGGER.info(() -> "Connection is valid");
      } else {
        LOGGER.warning(() -> "Connection is not valid");
      }

    } catch (SQLException e) {
      LOGGER.severe(() -> String.format("Failed to get or use connection %s", e.getMessage()));
    }
  }

  /** Demonstrates error handling patterns. */
  private static void demonstrateErrorHandling(EncryptingDataSource dataSource) {
    LOGGER.info(() -> "=== Demonstrating Error Handling ===");

    // Attempt to get multiple connections to test resilience
    for (int i = 0; i < 3; i++) {
      try (Connection connection = dataSource.getConnection()) {
        int finalI = i;
        LOGGER.info(() -> String.format("Connection attempt %d: Success", finalI + 1));

        // Simulate some work
        Thread.sleep(100);

      } catch (SQLException e) {
        int finalI1 = i;
        LOGGER.severe(
            () -> String.format("Connection attempt %s failed: %s", finalI1 + 1, e.getMessage()));

        // Check if DataSource is still healthy
        if (!dataSource.isConnectionAvailable()) {
          LOGGER.severe("DataSource is no longer available - stopping attempts");
          break;
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  /** Demonstrates DataSource lifecycle management. */
  private static void demonstrateLifecycleManagement(EncryptingDataSource dataSource) {
    LOGGER.info("=== Demonstrating Lifecycle Management ===");

    // Check initial state
    LOGGER.info(() -> String.format("DataSource closed: %s", dataSource.isClosed()));
    LOGGER.info(
        () -> String.format("Connection available: %s", dataSource.isConnectionAvailable()));

    // Get a connection before closing
    try (Connection connection = dataSource.getConnection()) {
      LOGGER.info(
          () ->
              String.format(
                  "Got connection before close: %s", connection.getClass().getSimpleName()));
    } catch (SQLException e) {
      LOGGER.severe(
          () -> String.format("Failed to get connection before close %s", e.getMessage()));
    }

    // Close the DataSource
    dataSource.close();
    LOGGER.info(() -> String.format("DataSource closed: %s", dataSource.isClosed()));
    LOGGER.info(
        () ->
            String.format(
                "Connection available after close: %s", dataSource.isConnectionAvailable()));

    // Try to get connection after close (should fail)
    try (Connection connection = dataSource.getConnection()) {
      LOGGER.severe(() -> "Unexpectedly got connection after close!");
    } catch (SQLException e) {
      LOGGER.info(
          () ->
              String.format("Expected failure getting connection after close: %s", e.getMessage()));
    }

    // Multiple close calls should be safe
    dataSource.close();
    dataSource.close();
    LOGGER.info(() -> "Multiple close calls completed safely");
  }

  /**
   * Demonstrates connection validation and recovery patterns.
   *
   * @param originalDataSource Original data source to wrap
   */
  public static void demonstrateConnectionRecovery(DataSource originalDataSource) {
    LOGGER.info(() -> "=== Demonstrating Connection Recovery ===");

    EncryptingDataSource dataSource = null;

    try {
      // Wrap the original DataSource
      dataSource =
          EncryptingDataSourceFactory.createWithDefaults(
              originalDataSource,
              "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
              "us-east-1");

      // Implement retry logic for connection failures
      Connection connection = getConnectionWithRetry(dataSource, 3, 1000);

      if (connection != null) {
        try (Connection conn = connection) {
          LOGGER.info(() -> "Successfully recovered connection");
        }
      } else {
        LOGGER.severe(() -> "Failed to recover connection after retries");
      }

    } catch (SQLException e) {
      LOGGER.severe(
          () -> String.format("Connection recovery demonstration failed %s", e.getMessage()));
    } finally {
      if (dataSource != null) {
        dataSource.close();
      }
    }
  }

  /** Attempts to get a connection with retry logic. */
  private static Connection getConnectionWithRetry(
      EncryptingDataSource dataSource, int maxRetries, long delayMs) {
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      int finalAttempt = attempt;
      try {
        LOGGER.info(() -> String.format("Connection attempt %s of %s", finalAttempt, maxRetries));

        if (!dataSource.isConnectionAvailable()) {
          LOGGER.warning(
              () -> String.format("DataSource not available on attempt %s", finalAttempt));
          Thread.sleep(delayMs);
          continue;
        }

        Connection connection = dataSource.getConnection();
        LOGGER.info(() -> String.format("Successfully got connection on attempt %s", finalAttempt));
        return connection;

      } catch (SQLException e) {
        LOGGER.warning(
            () -> String.format("Connection attempt %s failed: %s", finalAttempt, e.getMessage()));

        if (attempt < maxRetries) {
          try {
            Thread.sleep(delayMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    return null;
  }
}
