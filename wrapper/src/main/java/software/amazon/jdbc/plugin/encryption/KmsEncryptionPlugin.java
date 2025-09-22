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


import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.factory.IndependentDataSource;
import software.amazon.jdbc.plugin.encryption.logging.AuditLogger;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.sql.SqlAnalysisService;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingPreparedStatement;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.encryption.wrapper.DecryptingResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main encryption plugin that integrates with the AWS Advanced JDBC Wrapper
 * to provide transparent client-side encryption using AWS KMS.
 *
 * This plugin intercepts JDBC operations to automatically encrypt data before storage
 * and decrypt data upon retrieval based on metadata configuration.
 */
public class KmsEncryptionPlugin {

    private static final Logger logger = LoggerFactory.getLogger(KmsEncryptionPlugin.class);

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

    // Plugin properties
    private Properties pluginProperties;

    /**
     * Constructor that accepts PluginService for integration with AWS JDBC Wrapper.
     *
     * @param pluginService The PluginService instance from AWS JDBC Wrapper
     */
    public KmsEncryptionPlugin(PluginService pluginService) {
        this.pluginService = pluginService;
        logger.debug("KmsEncryptionPlugin created with PluginService: {}", pluginService != null ? "available" : "null");
    }

    /**
     * Default constructor for backward compatibility.
     */
    public KmsEncryptionPlugin() {
        this.pluginService = null;
        logger.warn("KmsEncryptionPlugin created without PluginService - connection parameter extraction may fail");
    }

    /**
     * Sets the PluginService instance. This method can be called to provide
     * the PluginService after construction if it wasn't available during construction.
     *
     * @param pluginService The PluginService instance from AWS JDBC Wrapper
     */
    public void setPluginService(PluginService pluginService) {
        if (this.pluginService == null) {
            this.pluginService = pluginService;
            logger.info("PluginService set after construction: {}", pluginService != null ? "available" : "null");
        } else {
            logger.warn("PluginService already set, ignoring new instance");
        }
    }

    /**
     * Initializes the plugin with the provided configuration.
     * This method is called by the AWS JDBC Wrapper during plugin loading.
     *
     * @param properties Configuration properties for the plugin
     * @throws SQLException if initialization fails
     */
    public void initialize(Properties properties) throws SQLException {
        if (initialized.get()) {
            logger.warn("Plugin already initialized, skipping re-initialization");
            return;
        }

        logger.info("Initializing KmsEncryptionPlugin");

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

            // Initialize audit logger
            this.auditLogger = new AuditLogger(config.isAuditLoggingEnabled());

            logger.info("KmsEncryptionPlugin initialized successfully");
            initialized.set(true);

        } catch (Exception e) {
            logger.error("Failed to initialize KmsEncryptionPlugin", e);
            throw new SQLException("Plugin initialization failed: " + e.getMessage(), e);
        }
    }

    /**
     * Initializes plugin components that require a database connection.
     * This method uses PluginService to get connection parameters instead of extraction.
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

                logger.info("Plugin initialized with PluginService connection parameters");

            } else {
                logger.error("PluginService not available - cannot create independent connections");

                auditLogger.logConnectionParameterExtraction("PluginService", "PLUGIN_SERVICE", false, "PluginService not available");

                throw new SQLException("PluginService not available - cannot create independent connections");
            }

        } catch (MetadataException e) {
            logger.error("Failed to initialize plugin components with database", e);
            throw new SQLException("Failed to initialize plugin with database: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Failed to initialize plugin with PluginService", e);
            throw new SQLException("Failed to initialize plugin: " + e.getMessage(), e);
        }
    }

    /**
     * Wraps a PreparedStatement to add encryption capabilities.
     *
     * @param statement The original PreparedStatement
     * @param sql       The SQL statement
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
                logger.error("Failed to initialize plugin with connection", e);
                throw new SQLException("Failed to initialize plugin: " + e.getMessage(), e);
            }
        }

        logger.debug("Wrapping PreparedStatement for SQL: {}", sql);

        // Analyze SQL to determine if encryption is needed
        SqlAnalysisService.SqlAnalysisResult analysisResult = null;
        if (sqlAnalysisService != null) {
            analysisResult = sqlAnalysisService.analyzeSql(sql);
            logger.debug("SQL analysis result: {}", analysisResult);
        }

        return new EncryptingPreparedStatement(
            statement,
            metadataManager,
            encryptionService,
            keyManager,
            sqlAnalysisService,
            sql
        );
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
                logger.error("Failed to initialize plugin with connection", e);
                throw new SQLException("Failed to initialize plugin: " + e.getMessage(), e);
            }
        }

        logger.debug("Wrapping ResultSet");

        return new DecryptingResultSet(
            resultSet,
            metadataManager,
            encryptionService,
            keyManager
        );
    }

    /**
     * Returns the plugin name for identification.
     *
     * @return Plugin name
     */
    public String getPluginName() {
        return "KmsEncryptionPlugin";
    }

    /**
     * Cleans up plugin resources.
     * This method is called when the plugin is being unloaded.
     */
    public void cleanup() {
        if (closed.get()) {
            return;
        }

        logger.info("Cleaning up KmsEncryptionPlugin resources");

        // Log final connection status
        if (independentDataSource != null) {
            try {
                independentDataSource.logHealthStatus();
            } catch (Exception e) {
                logger.warn("Error logging final DataSource health status", e);
            }
        }

        try {
            if (kmsClient != null) {
                kmsClient.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing KMS client", e);
        }

        closed.set(true);
        logger.info("KmsEncryptionPlugin cleanup completed");
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

            logger.info("Loaded encryption configuration: region={}, cacheEnabled={}, maxRetries={}",
                       config.getKmsRegion(), config.isCacheEnabled(), config.getMaxRetries());

            return config;

        } catch (Exception e) {
            logger.error("Failed to load configuration from properties", e);
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
        logger.debug("Creating KMS client for region: {}", config.getKmsRegion());

        return KmsClient.builder()
                .region(Region.of(config.getKmsRegion()))
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
     * Logs the current connection status and performance metrics.
     * This method can be called for troubleshooting purposes.
     */
    public void logCurrentStatus() {
        logger.info("=== KmsEncryptionPlugin Status Report ===");

        // Log connection mode status
        logger.info("Connection Mode: {}", getConnectionModeStatus());

        // Log DataSource health
        if (independentDataSource != null) {
            independentDataSource.logHealthStatus();
        } else {
            logger.info("Independent DataSource: Not configured");
        }

        logger.info("=== End Status Report ===");
    }
}
