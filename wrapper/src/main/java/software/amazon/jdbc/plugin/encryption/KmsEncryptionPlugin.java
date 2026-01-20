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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.factory.IndependentDataSource;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.logging.AuditLogger;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.encryption.sql.SqlAnalysisService;
import software.amazon.jdbc.plugin.encryption.wrapper.DecryptingResultSet;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingPreparedStatement;

/**
 * Main encryption plugin that integrates with the AWS Advanced JDBC Wrapper to provide transparent
 * client-side encryption using AWS KMS.
 *
 * <p>This plugin intercepts JDBC operations to automatically encrypt data before storage and
 * decrypt data upon retrieval based on metadata configuration.
 */
public class KmsEncryptionPlugin {

  private static final Logger LOGGER = Logger.getLogger(KmsEncryptionPlugin.class.getName());

  // Plugin configuration
  private EncryptionConfig config;
  private MetadataManager metadataManager;
  private KeyManager keyManager;
  private EncryptionService encryptionService;
  private KmsClient kmsClient;

  // Plugin services
  private PluginService pluginService;
  private IndependentDataSource independentDataSource;

  // SQL Analysis
  private SqlAnalysisService sqlAnalysisService;

  // Monitoring and metrics
  private AuditLogger auditLogger;

  // Plugin lifecycle state
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  // Track connections where custom types have been registered
  private final java.util.Map<java.sql.Connection, Boolean> registeredConnections =
      new java.util.WeakHashMap<>();

  // Plugin properties
  private Properties pluginProperties;

  /**
   * Constructor that accepts PluginService for integration with AWS JDBC Wrapper.
   *
   * @param pluginService The PluginService instance from AWS JDBC Wrapper
   */
  public KmsEncryptionPlugin(PluginService pluginService) {
    this.pluginService = pluginService;
    LOGGER.fine(
        () ->
            String.format(
                "KmsEncryptionPlugin created with PluginService: %s",
                pluginService != null ? "available" : "null"));
  }

  /** Default constructor for backward compatibility. */
  public KmsEncryptionPlugin() {
    this.pluginService = null;
    LOGGER.warning(
        "KmsEncryptionPlugin created without PluginService - connection parameter extraction may fail");
  }

  /**
   * Sets the PluginService instance. This method can be called to provide the PluginService after
   * construction if it wasn't available during construction.
   *
   * @param pluginService The PluginService instance from AWS JDBC Wrapper
   */
  public void setPluginService(PluginService pluginService) {
    if (this.pluginService == null) {
      this.pluginService = pluginService;
      LOGGER.info(
          () ->
              String.format(
                  "PluginService set after construction: %s",
                  pluginService != null ? "available" : "null"));
    } else {
      LOGGER.warning("PluginService already set, ignoring new instance");
    }
  }

  /**
   * Initializes the plugin with the provided configuration. This method is called by the AWS JDBC
   * Wrapper during plugin loading.
   *
   * @param properties Configuration properties for the plugin
   * @throws SQLException if initialization fails
   */
  public void initialize(Properties properties) throws SQLException {
    if (initialized.get()) {
      LOGGER.warning("Plugin already initialized, skipping re-initialization");
      return;
    }

    LOGGER.info("Initializing KmsEncryptionPlugin");

    try {
      // Store properties for later use
      this.pluginProperties = new Properties();
      this.pluginProperties.putAll(properties);

      // Load and validate configuration
      this.config = loadConfiguration(properties);
      config.validate();

      // Initialize AWS KMS client
      this.kmsClient = createKmsClient(config);

      // Initialize core services
      this.encryptionService = new EncryptionService();

      // Initialize audit LOGGER
      this.auditLogger = new AuditLogger(config.isAuditLoggingEnabled());

      LOGGER.info("KmsEncryptionPlugin initialized successfully");
      initialized.set(true);

    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to initialize KmsEncryptionPlugin %s", e.getMessage()));
      throw new SQLException("Plugin initialization failed: " + e.getMessage(), e);
    }
  }

  /**
   * Initializes plugin components that require a database connection. This method uses
   * PluginService to get connection parameters instead of extraction.
   *
   * @throws SQLException if initialization fails
   */
  private void initializeWithDataSource() throws SQLException {
    if (metadataManager != null) {
      return; // Already initialized
    }

    try {
      if (pluginService != null) {
        // Create independent DataSource using PluginService
        this.independentDataSource = new IndependentDataSource(pluginService, pluginProperties);

        // Log success
        auditLogger.logConnectionParameterExtraction("PluginService", "PLUGIN_SERVICE", true, null);

        // Initialize managers with PluginService
        this.keyManager = new KeyManager(kmsClient, pluginService, config);
        this.metadataManager = new MetadataManager(pluginService, config);
        metadataManager.initialize();

        // Initialize SQL analysis service
        this.sqlAnalysisService = new SqlAnalysisService(pluginService, metadataManager);

        LOGGER.info("Plugin initialized with PluginService connection parameters");

      } else {
        LOGGER.severe("PluginService not available - cannot create independent connections");

        auditLogger.logConnectionParameterExtraction(
            "PluginService", "PLUGIN_SERVICE", false, "PluginService not available");

        throw new SQLException(
            "PluginService not available - cannot create independent connections");
      }

    } catch (MetadataException e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Failed to initialize plugin components with database %s", e.getMessage()));
      throw new SQLException("Failed to initialize plugin with database: " + e.getMessage(), e);
    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to initialize plugin with PluginService %s", e.getMessage()));
      throw new SQLException("Failed to initialize plugin: " + e.getMessage(), e);
    }
  }

  /**
   * Registers custom PostgreSQL types with the JDBC driver for a specific connection. Only
   * registers once per connection.
   */
  private void registerPostgresTypesForConnection(java.sql.Connection conn) {
    if (conn == null) {
      return;
    }

    synchronized (registeredConnections) {
      if (registeredConnections.containsKey(conn)) {
        return; // Already registered for this connection
      }

      try {
        org.postgresql.PGConnection pgConn = conn.unwrap(org.postgresql.PGConnection.class);
        pgConn.addDataType(
            "encrypted_data", software.amazon.jdbc.plugin.encryption.wrapper.EncryptedData.class);
        registeredConnections.put(conn, Boolean.TRUE);
        LOGGER.fine("Registered encrypted_data type for connection");
      } catch (Exception e) {
        LOGGER.fine(() -> "Failed to register PostgreSQL custom types: " + e.getMessage());
      }
    }
  }

  /**
   * Wraps a PreparedStatement to add encryption capabilities.
   *
   * @param statement The original PreparedStatement
   * @param sql The SQL statement
   * @return Wrapped PreparedStatement with encryption support
   * @throws SQLException if wrapping fails
   */
  public PreparedStatement wrapPreparedStatement(PreparedStatement statement, String sql)
      throws SQLException {
    if (!initialized.get()) {
      throw new SQLException("Plugin not initialized");
    }

    // Initialize with DataSource if needed (lazy initialization)
    if (metadataManager == null) {
      try {
        initializeWithDataSource();
      } catch (Exception e) {
        LOGGER.severe(
            () -> String.format("Failed to initialize plugin with connection %s", e.getMessage()));
        throw new SQLException("Failed to initialize plugin: " + e.getMessage(), e);
      }
    }

    // Register custom types for this connection
    registerPostgresTypesForConnection(statement.getConnection());

    LOGGER.fine(() -> String.format("Wrapping PreparedStatement for SQL: %s", sql));

    // Analyze SQL to determine if encryption is needed
    SqlAnalysisService.SqlAnalysisResult analysisResult = SqlAnalysisService.analyzeSql(sql);
    LOGGER.fine(() -> String.format("SQL analysis result: %s", analysisResult));

    return new EncryptingPreparedStatement(
        statement, metadataManager, encryptionService, keyManager, sqlAnalysisService, sql);
  }

  /**
   * Wraps a ResultSet to add decryption capabilities.
   *
   * @param resultSet The original ResultSet
   * @return Wrapped ResultSet with decryption support
   * @throws SQLException if wrapping fails
   */
  public ResultSet wrapResultSet(ResultSet resultSet) throws SQLException {
    if (!initialized.get()) {
      throw new SQLException("Plugin not initialized");
    }

    // Initialize with DataSource if needed (lazy initialization)
    if (metadataManager == null) {
      try {
        initializeWithDataSource();
      } catch (Exception e) {
        LOGGER.severe(
            () -> String.format("Failed to initialize plugin with connection %s", e.getMessage()));
        throw new SQLException("Failed to initialize plugin: " + e.getMessage(), e);
      }
    }

    // Register custom types for this connection
    try {
      registerPostgresTypesForConnection(resultSet.getStatement().getConnection());
    } catch (Exception e) {
      LOGGER.fine(() -> "Could not register types for ResultSet connection: " + e.getMessage());
    }

    LOGGER.finest(() -> "Wrapping ResultSet");

    return new DecryptingResultSet(resultSet, metadataManager, encryptionService, keyManager);
  }

  /**
   * Returns the plugin name for identification.
   *
   * @return Plugin name
   */
  public String getPluginName() {
    return "KmsEncryptionPlugin";
  }

  /** Cleans up plugin resources. This method is called when the plugin is being unloaded. */
  public void cleanup() {
    if (closed.get()) {
      return;
    }

    LOGGER.info("Cleaning up KmsEncryptionPlugin resources");

    // Log final connection status
    if (independentDataSource != null) {
      try {
        independentDataSource.logHealthStatus();
      } catch (Exception e) {
        LOGGER.warning(
            () -> String.format("Error logging final DataSource health status %s", e.getMessage()));
      }
    }

    try {
      if (kmsClient != null) {
        kmsClient.close();
      }
    } catch (Exception e) {
      LOGGER.warning(() -> String.format("Error closing KMS client %s", e.getMessage()));
    }

    closed.set(true);
    LOGGER.info("KmsEncryptionPlugin cleanup completed");
  }

  /**
   * Loads configuration from properties.
   *
   * @param properties Configuration properties
   * @return EncryptionConfig instance
   * @throws SQLException if configuration is invalid
   */
  private EncryptionConfig loadConfiguration(Properties properties) throws SQLException {
    try {
      // Set default region if not provided
      if (!properties.containsKey("kms.region")) {
        properties.setProperty("kms.region", "us-east-1");
      }

      EncryptionConfig config = EncryptionConfig.fromProperties(properties);

      LOGGER.info(
          () ->
              String.format(
                  "Loaded encryption configuration: region=%s, cacheEnabled=%s, maxRetries=%s",
                  config.getKmsRegion(), config.isCacheEnabled(), config.getMaxRetries()));

      return config;

    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to load configuration from properties %s", e.getMessage()));
      throw new SQLException("Invalid configuration: " + e.getMessage(), e);
    }
  }

  /**
   * Creates a KMS client with the specified configuration.
   *
   * @param config Encryption configuration
   * @return Configured KMS client
   */
  private KmsClient createKmsClient(EncryptionConfig config) {
    LOGGER.fine(() -> String.format("Creating KMS client for region: %s", config.getKmsRegion()));

    return KmsClient.builder().region(Region.of(config.getKmsRegion())).build();
  }

  // Getters for testing and monitoring

  /**
   * Returns the current configuration.
   *
   * @return EncryptionConfig instance
   */
  public EncryptionConfig getConfig() {
    return config;
  }

  /**
   * Returns the metadata manager.
   *
   * @return MetadataManager instance
   */
  public MetadataManager getMetadataManager() {
    return metadataManager;
  }

  /**
   * Returns the key manager.
   *
   * @return KeyManager instance
   */
  public KeyManager getKeyManager() {
    return keyManager;
  }

  /**
   * Returns the encryption service.
   *
   * @return EncryptionService instance
   */
  public EncryptionService getEncryptionService() {
    return encryptionService;
  }

  /**
   * Checks if the plugin is initialized.
   *
   * @return true if initialized, false otherwise
   */
  public boolean isInitialized() {
    return initialized.get();
  }

  /**
   * Checks if the plugin is closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Returns the plugin service.
   *
   * @return PluginService instance
   */
  public PluginService getPluginService() {
    return pluginService;
  }

  /**
   * Returns the independent DataSource used by MetadataManager.
   *
   * @return IndependentDataSource instance, or null if not initialized
   */
  public IndependentDataSource getIndependentDataSource() {
    return independentDataSource;
  }

  /**
   * Checks if the plugin is using independent connections.
   *
   * @return true if using independent connections, false otherwise
   */
  public boolean isUsingIndependentConnections() {
    return independentDataSource != null;
  }

  /**
   * Creates a detailed status message about the current connection mode.
   *
   * @return a comprehensive status message
   */
  public String getConnectionModeStatus() {
    if (isUsingIndependentConnections()) {
      return "Plugin is using independent connections via PluginService";
    } else {
      return "Plugin connection mode is not yet determined";
    }
  }

  /**
   * Logs the current connection status and performance metrics. This method can be called for
   * troubleshooting purposes.
   */
  public void logCurrentStatus() {
    LOGGER.info("=== KmsEncryptionPlugin Status Report ===");

    // Log connection mode status
    LOGGER.info(() -> String.format("Connection Mode: %s", getConnectionModeStatus()));

    // Log DataSource health
    if (independentDataSource != null) {
      independentDataSource.logHealthStatus();
    } else {
      LOGGER.info("Independent DataSource: Not configured");
    }

    LOGGER.info("=== End Status Report ===");
  }
}
