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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import software.amazon.jdbc.plugin.encryption.factory.EncryptingDataSourceFactory;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingDataSource;

/**
 * Example demonstrating how to use the encryption functionality with AWS Advanced JDBC Wrapper.
 * This example shows different ways to configure and use encrypted database connections.
 */
public class AwsWrapperEncryptionExample {

  private static final Logger LOGGER =
      Logger.getLogger(AwsWrapperEncryptionExample.class.getName());

  public static void main(String[] args) {
    try {
      // Example 1: Using builder pattern
      demonstrateBuilderPattern();

      // Example 2: Using factory with properties
      demonstrateFactoryWithProperties();

      // Example 3: Using existing DataSource
      demonstrateWrappingExistingDataSource();

    } catch (Exception e) {
      LOGGER.severe(() -> String.format("Example execution failed", e));
    }
  }

  /** Demonstrates using the builder pattern to create an encrypted DataSource. */
  private static void demonstrateBuilderPattern() throws SQLException {
    LOGGER.info("=== Builder Pattern Example ===");

    EncryptingDataSource dataSource =
        new EncryptingDataSourceFactory.Builder()
            .jdbcUrl("jdbc:postgresql://localhost:5432/mydb")
            .username("myuser")
            .password("mypassword")
            .kmsKeyArn(
                "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
            .region("us-east-1")
            .cacheEnabled(true)
            .cacheExpirationMinutes(30)
            .cacheMaxSize(1000)
            .build();

    // Use the DataSource
    performDatabaseOperations(dataSource, "Builder Pattern");

    // Clean up
    dataSource.close();
  }

  /** Demonstrates using the factory with explicit properties. */
  private static void demonstrateFactoryWithProperties() throws SQLException {
    LOGGER.info("=== Factory with Properties Example ===");

    Properties encryptionProperties = new Properties();

    // KMS configuration
    encryptionProperties.setProperty(
        "kms.keyArn",
        "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012");
    encryptionProperties.setProperty("kms.region", "us-east-1");

    // Cache configuration
    encryptionProperties.setProperty("cache.enabled", "true");
    encryptionProperties.setProperty("cache.expirationMinutes", "30");
    encryptionProperties.setProperty("cache.maxSize", "1000");

    // Retry configuration
    encryptionProperties.setProperty("kms.maxRetries", "3");
    encryptionProperties.setProperty("kms.retryBackoffBaseMs", "100");

    // AWS Wrapper configuration (optional)
    encryptionProperties.setProperty("wrapperLogUnclosedConnections", "true");
    encryptionProperties.setProperty("wrapperLoggerLevel", "INFO");

    EncryptingDataSource dataSource =
        EncryptingDataSourceFactory.createWithAwsWrapper(
            "jdbc:postgresql://localhost:5432/mydb", "myuser", "mypassword", encryptionProperties);

    // Use the DataSource
    performDatabaseOperations(dataSource, "Factory with Properties");

    // Clean up
    dataSource.close();
  }

  /** Demonstrates wrapping an existing DataSource with encryption. */
  private static void demonstrateWrappingExistingDataSource() throws SQLException {
    LOGGER.info("=== Wrapping Existing DataSource Example ===");

    // Create an existing DataSource (this could be from a connection pool, etc.)
    DataSource existingDataSource = createExistingDataSource();

    // Wrap it with encryption
    EncryptingDataSource encryptingDataSource =
        EncryptingDataSourceFactory.createWithDefaults(
            existingDataSource,
            "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            "us-east-1");

    // Use the encrypted DataSource
    performDatabaseOperations(encryptingDataSource, "Wrapped Existing DataSource");

    // Clean up
    encryptingDataSource.close();
  }

  /** Performs sample database operations to demonstrate encryption/decryption. */
  private static void performDatabaseOperations(DataSource dataSource, String exampleName) {
    LOGGER.info(() -> String.format("Performing database operations for: %s", exampleName));

    try (Connection connection = dataSource.getConnection()) {

      // Create test table (if not exists)
      createTestTable(connection);

      // Insert encrypted data
      insertTestData(connection);

      // Query and decrypt data
      queryTestData(connection);

      LOGGER.info(
          () -> String.format("Database operations completed successfully for: %s", exampleName));

    } catch (SQLException e) {
      LOGGER.severe(() -> String.format("Database operations failed for: " + exampleName, e));
    }
  }

  /** Creates a test table for demonstration. */
  private static void createTestTable(Connection connection) throws SQLException {
    String createTableSql =
        "CREATE TABLE IF NOT EXISTS test_users ("
            + "id SERIAL PRIMARY KEY, "
            + "name VARCHAR(100) NOT NULL, "
            + "email VARCHAR(100), "
            + "ssn VARCHAR(20), "
            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            + ")";

    try (PreparedStatement stmt = connection.prepareStatement(createTableSql)) {
      stmt.executeUpdate();
      LOGGER.finest(() -> "Test table created or already exists");
    }
  }

  /** Inserts test data that will be automatically encrypted for configured columns. */
  private static void insertTestData(Connection connection) throws SQLException {
    String insertSql = "INSERT INTO test_users (name, email, ssn) VALUES (?, ?, ?)";

    try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
      // Insert first user
      stmt.setString(1, "John Doe");
      stmt.setString(2, "john.doe@example.com"); // Will be encrypted if configured
      stmt.setString(3, "123-45-6789"); // Will be encrypted if configured
      stmt.executeUpdate();

      // Insert second user
      stmt.setString(1, "Jane Smith");
      stmt.setString(2, "jane.smith@example.com"); // Will be encrypted if configured
      stmt.setString(3, "987-65-4321"); // Will be encrypted if configured
      stmt.executeUpdate();

      LOGGER.info("Inserted test data with automatic encryption");
    }
  }

  /** Queries test data that will be automatically decrypted for configured columns. */
  private static void queryTestData(Connection connection) throws SQLException {
    String selectSql = "SELECT id, name, email, ssn FROM test_users ORDER BY id";

    try (PreparedStatement stmt = connection.prepareStatement(selectSql);
        ResultSet rs = stmt.executeQuery()) {

      LOGGER.info("Querying test data with automatic decryption:");

      while (rs.next()) {
        int id = rs.getInt("id");
        String name = rs.getString("name");
        String email = rs.getString("email"); // Will be decrypted if configured
        String ssn = rs.getString("ssn"); // Will be decrypted if configured

        LOGGER.info(
            () -> String.format("User %s: Name=%s, Email=%s, SSN=%s", id, name, email, ssn));
      }
    }
  }

  /**
   * Creates a sample existing DataSource for demonstration. In a real application, this might come
   * from a connection pool or dependency injection.
   */
  private static DataSource createExistingDataSource() {
    // This is a simplified example - in practice you might use HikariCP, etc.
    return new DataSource() {
      @Override
      public Connection getConnection() throws SQLException {
        return java.sql.DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/mydb", "myuser", "mypassword");
      }

      @Override
      public Connection getConnection(String username, String password) throws SQLException {
        return java.sql.DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/mydb", username, password);
      }

      // Other DataSource methods with default implementations
      @Override
      public java.io.PrintWriter getLogWriter() throws SQLException {
        return null;
      }

      @Override
      public void setLogWriter(java.io.PrintWriter out) throws SQLException {
        // No-op
      }

      @Override
      public void setLoginTimeout(int seconds) throws SQLException {
        // No-op
      }

      @Override
      public int getLoginTimeout() throws SQLException {
        return 0;
      }

      @Override
      public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("javax.sql.DataSource");
      }

      @Override
      public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Cannot unwrap to " + iface.getName());
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
      }
    };
  }
}
