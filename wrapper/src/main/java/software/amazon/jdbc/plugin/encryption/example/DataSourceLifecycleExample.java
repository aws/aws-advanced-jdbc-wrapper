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

import software.amazon.jdbc.factory.EncryptingDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Example demonstrating proper DataSource lifecycle management with encryption.
 * Shows how to handle connection failures and DataSource state management.
 */
public class DataSourceLifecycleExample {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceLifecycleExample.class);

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
            logger.error("Example execution failed", e);
        } finally {
            // Always clean up resources
            if (dataSource != null) {
                dataSource.close();
                logger.info("DataSource closed in finally block");
            }
        }
    }

    /**
     * Creates an EncryptingDataSource for demonstration.
     */
    private static EncryptingDataSource createDataSource() throws SQLException {
        logger.info("=== Creating EncryptingDataSource ===");

        EncryptingDataSource dataSource = new EncryptingDataSourceFactory.Builder()
            .jdbcUrl("jdbc:postgresql://localhost:5432/mydb")
            .username("myuser")
            .password("mypassword")
            .kmsKeyArn("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
            .region("us-east-1")
            .cacheEnabled(true)
            .build();

        logger.info("EncryptingDataSource created successfully");
        return dataSource;
    }

    /**
     * Demonstrates healthy DataSource usage patterns.
     */
    private static void demonstrateHealthyUsage(EncryptingDataSource dataSource) {
        logger.info("=== Demonstrating Healthy Usage ===");

        // Check if DataSource is available before using
        if (!dataSource.isConnectionAvailable()) {
            logger.warn("DataSource is not available - skipping operations");
            return;
        }

        // Use try-with-resources for proper connection management
        try (Connection connection = dataSource.getConnection()) {
            logger.info("Successfully obtained connection: {}", connection.getClass().getSimpleName());

            // Verify connection is valid
            if (connection.isValid(5)) {
                logger.info("Connection is valid");
            } else {
                logger.warn("Connection is not valid");
            }

        } catch (SQLException e) {
            logger.error("Failed to get or use connection", e);
        }
    }

    /**
     * Demonstrates error handling patterns.
     */
    private static void demonstrateErrorHandling(EncryptingDataSource dataSource) {
        logger.info("=== Demonstrating Error Handling ===");

        // Attempt to get multiple connections to test resilience
        for (int i = 0; i < 3; i++) {
            try (Connection connection = dataSource.getConnection()) {
                logger.info("Connection attempt {}: Success", i + 1);

                // Simulate some work
                Thread.sleep(100);

            } catch (SQLException e) {
                logger.error("Connection attempt {} failed: {}", i + 1, e.getMessage());

                // Check if DataSource is still healthy
                if (!dataSource.isConnectionAvailable()) {
                    logger.error("DataSource is no longer available - stopping attempts");
                    break;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Demonstrates DataSource lifecycle management.
     */
    private static void demonstrateLifecycleManagement(EncryptingDataSource dataSource) {
        logger.info("=== Demonstrating Lifecycle Management ===");

        // Check initial state
        logger.info("DataSource closed: {}", dataSource.isClosed());
        logger.info("Connection available: {}", dataSource.isConnectionAvailable());

        // Get a connection before closing
        try (Connection connection = dataSource.getConnection()) {
            logger.info("Got connection before close: {}", connection.getClass().getSimpleName());
        } catch (SQLException e) {
            logger.error("Failed to get connection before close", e);
        }

        // Close the DataSource
        dataSource.close();
        logger.info("DataSource closed: {}", dataSource.isClosed());
        logger.info("Connection available after close: {}", dataSource.isConnectionAvailable());

        // Try to get connection after close (should fail)
        try (Connection connection = dataSource.getConnection()) {
            logger.error("Unexpectedly got connection after close!");
        } catch (SQLException e) {
            logger.info("Expected failure getting connection after close: {}", e.getMessage());
        }

        // Multiple close calls should be safe
        dataSource.close();
        dataSource.close();
        logger.info("Multiple close calls completed safely");
    }

    /**
     * Demonstrates connection validation and recovery patterns.
     */
    public static void demonstrateConnectionRecovery(DataSource originalDataSource) {
        logger.info("=== Demonstrating Connection Recovery ===");

        EncryptingDataSource dataSource = null;

        try {
            // Wrap the original DataSource
            dataSource = EncryptingDataSourceFactory.createWithDefaults(
                originalDataSource,
                "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
                "us-east-1"
            );

            // Implement retry logic for connection failures
            Connection connection = getConnectionWithRetry(dataSource, 3, 1000);

            if (connection != null) {
                try (Connection conn = connection) {
                    logger.info("Successfully recovered connection");
                }
            } else {
                logger.error("Failed to recover connection after retries");
            }

        } catch (SQLException e) {
            logger.error("Connection recovery demonstration failed", e);
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    /**
     * Attempts to get a connection with retry logic.
     */
    private static Connection getConnectionWithRetry(EncryptingDataSource dataSource, int maxRetries, long delayMs) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                logger.info("Connection attempt {} of {}", attempt, maxRetries);

                if (!dataSource.isConnectionAvailable()) {
                    logger.warn("DataSource not available on attempt {}", attempt);
                    Thread.sleep(delayMs);
                    continue;
                }

                Connection connection = dataSource.getConnection();
                logger.info("Successfully got connection on attempt {}", attempt);
                return connection;

            } catch (SQLException e) {
                logger.warn("Connection attempt {} failed: {}", attempt, e.getMessage());

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
