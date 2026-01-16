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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.factory.EncryptingDataSourceFactory;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingDataSource;

/** Example demonstrating how to use the encryption functionality with a properties file. */
public class PropertiesFileExample {

  private static final Logger LOGGER = Logger.getLogger(PropertiesFileExample.class.getName());

  public static void main(String[] args) {
    try {
      // Load properties from file
      Properties properties = loadPropertiesFromFile("example-jdbc-wrapper.properties");

      // Create EncryptingDataSource using the properties
      EncryptingDataSource dataSource = createDataSourceFromProperties(properties);

      // Use the DataSource
      demonstrateEncryptedOperations(dataSource);

      // Clean up
      dataSource.close();

    } catch (Exception e) {
      LOGGER.severe(() -> String.format("Example execution failed %s", e.getMessage()));
    }
  }

  /** Loads properties from a file in the classpath. */
  private static Properties loadPropertiesFromFile(String filename) throws IOException {
    Properties properties = new Properties();

    try (InputStream inputStream =
        PropertiesFileExample.class.getClassLoader().getResourceAsStream(filename)) {

      if (inputStream == null) {
        throw new IOException("Properties file not found: " + filename);
      }

      properties.load(inputStream);
      LOGGER.info(() -> String.format("Loaded properties from file: %s", filename));
    }

    return properties;
  }

  /** Creates an EncryptingDataSource from properties. */
  private static EncryptingDataSource createDataSourceFromProperties(Properties properties)
      throws SQLException {
    String jdbcUrl = properties.getProperty("jdbcUrl");
    String username = properties.getProperty("username");
    String password = properties.getProperty("password");

    if (jdbcUrl == null || username == null || password == null) {
      throw new SQLException("Missing required database connection properties");
    }

    LOGGER.info(() -> String.format("Creating EncryptingDataSource for URL: %s", jdbcUrl));

    return EncryptingDataSourceFactory.createWithAwsWrapper(
        jdbcUrl, username, password, properties);
  }

  /** Demonstrates encrypted database operations. */
  private static void demonstrateEncryptedOperations(EncryptingDataSource dataSource)
      throws SQLException {
    LOGGER.info(() -> "Demonstrating encrypted database operations");

    try (Connection connection = dataSource.getConnection()) {

      // Create test table
      createTestTable(connection);

      // Insert encrypted data
      insertTestData(connection);

      // Query and decrypt data
      queryTestData(connection);

      LOGGER.info("Encrypted operations completed successfully");
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
      // Insert test user
      stmt.setString(1, "Jane Doe");
      stmt.setString(2, "jane.doe@example.com"); // Will be encrypted if configured
      stmt.setString(3, "987-65-4321"); // Will be encrypted if configured
      stmt.executeUpdate();

      LOGGER.info("Inserted test data with automatic encryption");
    }
  }

  /** Queries test data that will be automatically decrypted for configured columns. */
  private static void queryTestData(Connection connection) throws SQLException {
    String selectSql = "SELECT id, name, email, ssn FROM test_users ORDER BY id DESC LIMIT 1";

    try (PreparedStatement stmt = connection.prepareStatement(selectSql);
        ResultSet rs = stmt.executeQuery()) {

      if (rs.next()) {
        int id = rs.getInt("id");
        String name = rs.getString("name");
        String email = rs.getString("email"); // Will be decrypted if configured
        String ssn = rs.getString("ssn"); // Will be decrypted if configured

        LOGGER.info(
            () ->
                String.format(
                    "Retrieved user %s: Name=%s, Email=%s, SSN=%s", id, name, email, ssn));
      }
    }
  }
}
