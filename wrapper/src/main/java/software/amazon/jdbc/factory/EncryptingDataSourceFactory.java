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

package software.amazon.jdbc.factory;

import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingDataSource;

/**
 * Factory for creating EncryptingDataSource instances that integrate with the AWS Advanced JDBC
 * Wrapper. This factory provides convenient methods to wrap existing DataSources with encryption
 * capabilities.
 */
public class EncryptingDataSourceFactory {

  private static final Logger logger = LoggerFactory.getLogger(EncryptingDataSourceFactory.class);

  /**
   * Creates an EncryptingDataSource that wraps the provided DataSource with encryption
   * capabilities.
   *
   * @param dataSource The underlying DataSource to wrap
   * @param encryptionProperties Properties for configuring encryption
   * @return An EncryptingDataSource instance
   * @throws SQLException if encryption initialization fails
   */
  public static EncryptingDataSource create(DataSource dataSource, Properties encryptionProperties)
      throws SQLException {
    logger.info("Creating EncryptingDataSource with encryption properties");

    // Validate required properties
    validateEncryptionProperties(encryptionProperties);

    return new EncryptingDataSource(dataSource, encryptionProperties);
  }

  /**
   * Creates an EncryptingDataSource using AWS JDBC Wrapper with encryption. This method creates an
   * AWS Wrapper DataSource and then wraps it with encryption.
   *
   * @param jdbcUrl The JDBC URL for the database
   * @param username Database username
   * @param password Database password
   * @param encryptionProperties Properties for configuring encryption
   * @return An EncryptingDataSource instance
   * @throws SQLException if DataSource creation or encryption initialization fails
   */
  public static EncryptingDataSource createWithAwsWrapper(
      String jdbcUrl, String username, String password, Properties encryptionProperties)
      throws SQLException {
    logger.info("Creating EncryptingDataSource with AWS JDBC Wrapper for URL: {}", jdbcUrl);

    try {
      // Create properties for AWS JDBC Wrapper
      Properties awsWrapperProperties = new Properties();
      awsWrapperProperties.setProperty("jdbcUrl", jdbcUrl);
      awsWrapperProperties.setProperty("username", username);
      awsWrapperProperties.setProperty("password", password);

      // Add any additional AWS wrapper properties from encryption properties
      copyAwsWrapperProperties(encryptionProperties, awsWrapperProperties);

      // Create AWS Wrapper DataSource using reflection to avoid compile-time dependency
      DataSource awsDataSource = createAwsWrapperDataSource(awsWrapperProperties);

      // Wrap with encryption
      return create(awsDataSource, encryptionProperties);

    } catch (Exception e) {
      logger.error("Failed to create EncryptingDataSource with AWS Wrapper", e);
      throw new SQLException("Failed to create encrypted DataSource: " + e.getMessage(), e);
    }
  }

  /**
   * Creates an EncryptingDataSource with default encryption properties.
   *
   * @param dataSource The underlying DataSource to wrap
   * @param kmsKeyArn The KMS key ARN for encryption
   * @param region The AWS region
   * @return An EncryptingDataSource instance
   * @throws SQLException if encryption initialization fails
   */
  public static EncryptingDataSource createWithDefaults(
      DataSource dataSource, String kmsKeyArn, String region) throws SQLException {
    Properties encryptionProperties = createDefaultEncryptionProperties(kmsKeyArn, region);
    return create(dataSource, encryptionProperties);
  }

  /**
   * Validates that required encryption properties are present.
   *
   * @param properties The properties to validate
   * @throws SQLException if required properties are missing
   */
  private static void validateEncryptionProperties(Properties properties) throws SQLException {
    if (properties == null) {
      throw new SQLException("Encryption properties cannot be null");
    }

    // Check for required properties (these will be validated by EncryptionConfig)
    logger.debug("Validating encryption properties");

    // The actual validation is done by EncryptionConfig.validate() in the plugin
    // We just do basic null checks here
  }

  /**
   * Copies AWS Wrapper specific properties from encryption properties.
   *
   * @param encryptionProperties Source properties
   * @param awsWrapperProperties Target properties
   */
  private static void copyAwsWrapperProperties(
      Properties encryptionProperties, Properties awsWrapperProperties) {
    // Copy AWS wrapper specific properties
    String[] awsWrapperKeys = {
      "wrapperPlugins", "wrapperLogUnclosedConnections", "wrapperLoggerLevel", "aws.region"
    };

    for (String key : awsWrapperKeys) {
      String value = encryptionProperties.getProperty(key);
      if (value != null) {
        awsWrapperProperties.setProperty(key, value);
      }
    }
  }

  /**
   * Creates an AWS Wrapper DataSource using reflection to avoid compile-time dependency issues.
   *
   * @param properties Properties for the AWS Wrapper DataSource
   * @return DataSource instance
   * @throws Exception if DataSource creation fails
   */
  private static DataSource createAwsWrapperDataSource(Properties properties) throws Exception {
    try {
      // Try to create AWS Wrapper DataSource using reflection
      Class<?> awsDataSourceClass = Class.forName("software.amazon.jdbc.AwsWrapperDataSource");
      return (DataSource)
          awsDataSourceClass.getConstructor(Properties.class).newInstance(properties);
    } catch (ClassNotFoundException e) {
      logger.warn("AWS JDBC Wrapper not found, falling back to direct PostgreSQL DataSource");
      return createPostgreSqlDataSource(properties);
    }
  }

  /**
   * Creates a PostgreSQL DataSource as fallback when AWS Wrapper is not available.
   *
   * @param properties Properties for the DataSource
   * @return DataSource instance
   * @throws Exception if DataSource creation fails
   */
  private static DataSource createPostgreSqlDataSource(Properties properties) throws Exception {
    // Create a basic PostgreSQL DataSource
    Class<?> pgDataSourceClass = Class.forName("org.postgresql.ds.PGSimpleDataSource");
    DataSource dataSource = (DataSource) pgDataSourceClass.getDeclaredConstructor().newInstance();

    // Set properties using reflection
    String jdbcUrl = properties.getProperty("jdbcUrl");
    String username = properties.getProperty("username");
    String password = properties.getProperty("password");

    if (jdbcUrl != null) {
      // Parse URL to extract host, port, database
      // This is a simplified implementation
      pgDataSourceClass.getMethod("setUrl", String.class).invoke(dataSource, jdbcUrl);
    }

    if (username != null) {
      pgDataSourceClass.getMethod("setUser", String.class).invoke(dataSource, username);
    }

    if (password != null) {
      pgDataSourceClass.getMethod("setPassword", String.class).invoke(dataSource, password);
    }

    return dataSource;
  }

  /**
   * Creates default encryption properties.
   *
   * @param kmsKeyArn The KMS key ARN
   * @param region The AWS region
   * @return Properties with default encryption settings
   */
  private static Properties createDefaultEncryptionProperties(String kmsKeyArn, String region) {
    Properties properties = new Properties();

    // KMS configuration
    properties.setProperty("kms.region", region != null ? region : "us-east-1");
    properties.setProperty("kms.keyArn", kmsKeyArn);

    // Cache configuration
    properties.setProperty("cache.enabled", "true");
    properties.setProperty("cache.expirationMinutes", "30");
    properties.setProperty("cache.maxSize", "1000");

    // Retry configuration
    properties.setProperty("kms.maxRetries", "3");
    properties.setProperty("kms.retryBackoffBaseMs", "100");

    // Metadata configuration
    properties.setProperty("metadata.refreshIntervalMinutes", "5");

    logger.debug(
        "Created default encryption properties for KMS key: {}, region: {}", kmsKeyArn, region);

    return properties;
  }

  /** Builder class for creating EncryptingDataSource with fluent API. */
  public static class Builder {
    private DataSource dataSource;
    private String jdbcUrl;
    private String username;
    private String password;
    private final Properties encryptionProperties = new Properties();

    public Builder dataSource(DataSource dataSource) {
      this.dataSource = dataSource;
      return this;
    }

    public Builder jdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder kmsKeyArn(String kmsKeyArn) {
      encryptionProperties.setProperty("kms.keyArn", kmsKeyArn);
      return this;
    }

    public Builder region(String region) {
      encryptionProperties.setProperty("kms.region", region);
      return this;
    }

    public Builder cacheEnabled(boolean enabled) {
      encryptionProperties.setProperty("cache.enabled", String.valueOf(enabled));
      return this;
    }

    public Builder cacheExpirationMinutes(int minutes) {
      encryptionProperties.setProperty("cache.expirationMinutes", String.valueOf(minutes));
      return this;
    }

    public Builder cacheMaxSize(int maxSize) {
      encryptionProperties.setProperty("cache.maxSize", String.valueOf(maxSize));
      return this;
    }

    public Builder property(String key, String value) {
      encryptionProperties.setProperty(key, value);
      return this;
    }

    public EncryptingDataSource build() throws SQLException {
      if (dataSource != null) {
        return create(dataSource, encryptionProperties);
      } else if (jdbcUrl != null && username != null && password != null) {
        return createWithAwsWrapper(jdbcUrl, username, password, encryptionProperties);
      } else {
        throw new SQLException(
            "Either dataSource or (jdbcUrl, username, password) must be provided");
      }
    }
  }
}
