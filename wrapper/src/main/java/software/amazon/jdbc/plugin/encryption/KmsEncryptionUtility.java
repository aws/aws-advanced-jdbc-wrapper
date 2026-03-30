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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.logging.AuditLogger;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.encryption.sql.SqlAnalysisService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ResourceLock;

/**
 * Main encryption plugin that integrates with the AWS Advanced JDBC Wrapper to provide transparent
 * client-side encryption using AWS KMS.
 *
 * <p>This plugin intercepts JDBC operations to automatically encrypt data before storage and
 * decrypt data upon retrieval based on metadata configuration.
 */
public class KmsEncryptionUtility {

  private static final Logger LOGGER = Logger.getLogger(KmsEncryptionUtility.class.getName());
  private final ResourceLock resourceLock = new ResourceLock();

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
  public KmsEncryptionUtility(PluginService pluginService) {
    this.pluginService = pluginService;
    LOGGER.fine(() -> Messages.get(
        "KmsEncryptionUtility.createdWithPluginService", new Object[]{pluginService != null ? "available" : "null"}));
  }

  /** Default constructor for backward compatibility. */
  public KmsEncryptionUtility() {
    this.pluginService = null;
    LOGGER.warning(Messages.get("KmsEncryptionUtility.createdWithoutPluginService"));
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
      LOGGER.info(() -> Messages.get(
          "KmsEncryptionUtility.pluginServiceSetAfterConstruction",
          new Object[]{pluginService != null ? "available" : "null"}));
    } else {
      LOGGER.warning(Messages.get("KmsEncryptionUtility.pluginServiceAlreadySet"));
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
      LOGGER.warning(Messages.get("KmsEncryptionUtility.alreadyInitialized"));
      return;
    }

    LOGGER.info(Messages.get("KmsEncryptionUtility.initializing"));

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

      LOGGER.info(Messages.get("KmsEncryptionUtility.initialized"));
      initialized.set(true);

    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get("KmsEncryptionUtility.initFailed", new Object[]{e.getMessage()}));
      throw new SQLException(
          Messages.get("KmsEncryptionUtility.pluginInitFailed", new Object[]{e.getMessage(), e}));
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

        LOGGER.info(Messages.get("KmsEncryptionUtility.initWithPluginService"));

      } else {
        LOGGER.severe(Messages.get("KmsEncryptionUtility.pluginServiceNotAvailable"));

        auditLogger.logConnectionParameterExtraction(
            "PluginService", "PLUGIN_SERVICE", false, "PluginService not available");

        throw new SQLException(Messages.get("KmsEncryptionUtility.pluginServiceNotAvailableExc"));
      }

    } catch (MetadataException e) {
      LOGGER.severe(() -> Messages.get("KmsEncryptionUtility.initWithDatabaseFailed", new Object[]{e.getMessage()}));
      throw new SQLException(
          Messages.get("KmsEncryptionUtility.initWithDatabaseFailedExc", new Object[]{e.getMessage(), e}));
    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get(
          "KmsEncryptionUtility.initWithPluginServiceFailed", new Object[]{e.getMessage()}));
      throw new SQLException(
          Messages.get("KmsEncryptionUtility.initPluginFailed", new Object[]{e.getMessage(), e}));
    }
  }

  /**
   * Registers custom database types with the JDBC driver for a specific connection.
   * Only registers once per connection. PostgreSQL requires custom type registration,
   * MySQL does not.
   */
  private void registerCustomTypesForConnection(java.sql.Connection conn) {
    if (conn == null) {
      return;
    }

    try (ResourceLock lock = resourceLock.obtain()) {
      if (registeredConnections.containsKey(conn)) {
        return; // Already registered for this connection
      }
      TargetDriverDialect targetDriverDialect = pluginService.getTargetDriverDialect();
      try {
        targetDriverDialect.registerDataType(conn, "encrypted_data",
            "software.amazon.jdbc.plugin.encryption.wrapper.EncryptedData");
        LOGGER.fine(Messages.get("KmsEncryptionUtility.registeredType"));
      } catch (SQLException e) {
        LOGGER.fine(() -> Messages.get("KmsEncryptionUtility.registerTypeFailed", new Object[]{e.getMessage()}));
      }
      registeredConnections.put(conn, Boolean.TRUE);
    }
  }


  public SqlAnalysisService getSqlAnalysisService() {
    return sqlAnalysisService;
  }

  /**
   * Removes the type registration entry for a connection.
   * Called on connection close so types are re-registered on the next connection.
   */
  public void onConnectionClosed(Connection conn) {
    registeredConnections.remove(conn);
  }

  /**
   * Ensures the plugin is initialized with a database connection.
   * Called lazily when the first PreparedStatement is created.
   */
  public void ensureInitializedWithConnection(Connection conn) throws SQLException {
    if (!initialized.get()) {
      throw new SQLException(Messages.get("KmsEncryptionUtility.pluginNotInitialized"));
    }
    if (metadataManager == null) {
      initializeWithDataSource();
    }
    registerCustomTypesForConnection(conn);
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

    LOGGER.info(Messages.get("KmsEncryptionUtility.cleaningUp"));

    // Log final connection status
    if (independentDataSource != null) {
      try {
        independentDataSource.logHealthStatus();
      } catch (Exception e) {
        LOGGER.warning(() -> Messages.get("KmsEncryptionUtility.healthStatusLogError", new Object[]{e.getMessage()}));
      }
    }

    try {
      if (kmsClient != null) {
        kmsClient.close();
      }
    } catch (Exception e) {
      LOGGER.warning(() -> Messages.get("KmsEncryptionUtility.kmsClientCloseError", new Object[]{e.getMessage()}));
    }

    closed.set(true);
    LOGGER.info(Messages.get("KmsEncryptionUtility.cleanupCompleted"));
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

      LOGGER.info(() -> Messages.get("KmsEncryptionUtility.configLoaded", new Object[]{config.getKmsRegion(),
          config.isCacheEnabled(), config.getMaxRetries()}));

      return config;

    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get("KmsEncryptionUtility.configLoadFailed", new Object[]{e.getMessage()}));
      throw new SQLException(
          Messages.get("KmsEncryptionUtility.invalidConfig", new Object[]{e.getMessage(), e}));
    }
  }

  /**
   * Creates a KMS client with the specified configuration.
   *
   * @param config Encryption configuration
   * @return Configured KMS client
   */
  private KmsClient createKmsClient(EncryptionConfig config) {
    LOGGER.fine(() -> Messages.get("KmsEncryptionUtility.creatingKmsClient", new Object[]{config.getKmsRegion()}));

    // Get AWS credentials provider using the awsProfile property if specified
    software.amazon.awssdk.auth.credentials.AwsCredentialsProvider credentialsProvider =
        software.amazon.jdbc.authentication.AwsCredentialsManager.getProvider(
            null, // HostSpec not needed for KMS
            this.pluginProperties);

    return KmsClient.builder()
        .region(Region.of(config.getKmsRegion()))
        .credentialsProvider(credentialsProvider)
        .build();
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
    LOGGER.info(Messages.get("KmsEncryptionUtility.statusReport"));

    // Log connection mode status
    LOGGER.info(() -> Messages.get("KmsEncryptionUtility.connectionMode", new Object[]{getConnectionModeStatus()}));

    // Log DataSource health
    if (independentDataSource != null) {
      independentDataSource.logHealthStatus();
    } else {
      LOGGER.info(Messages.get("KmsEncryptionUtility.noIndependentDataSource"));
    }

    LOGGER.info(Messages.get("KmsEncryptionUtility.endStatusReport"));
  }
}
