package software.amazon.jdbc.metadata;

import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.model.ColumnEncryptionConfig;
import software.amazon.jdbc.model.EncryptionConfig;
import software.amazon.jdbc.model.KeyMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages encryption metadata by loading configuration from database tables,
 * providing caching mechanisms, and offering lookup methods for column encryption settings.
 */
public class MetadataManager {
    
    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);
    
    private final PluginService pluginService;
    private volatile EncryptionConfig config;
    private final Map<String, ColumnEncryptionConfig> metadataCache;
    private final ReadWriteLock cacheLock;
    private volatile Instant lastRefreshTime;
    private volatile ScheduledExecutorService refreshExecutor;
    
    // SQL queries for metadata operations
    private static final String LOAD_ENCRYPTION_METADATA_SQL = 
        "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, " +
        "       em.created_at, em.updated_at, " +
        "       ks.master_key_arn, ks.encrypted_data_key, ks.key_spec, " +
        "       ks.created_at as key_created_at, ks.last_used_at " +
        "FROM encryption_metadata em " +
        "JOIN key_storage ks ON em.key_id = ks.key_id " +
        "ORDER BY em.table_name, em.column_name";
    
    private static final String CHECK_COLUMN_ENCRYPTED_SQL = 
        "SELECT 1 FROM encryption_metadata " +
        "WHERE table_name = ? AND column_name = ?";
    
    private static final String GET_COLUMN_CONFIG_SQL = 
        "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, " +
        "       em.created_at, em.updated_at, " +
        "       ks.master_key_arn, ks.encrypted_data_key, ks.key_spec, " +
        "       ks.created_at as key_created_at, ks.last_used_at " +
        "FROM encryption_metadata em " +
        "JOIN key_storage ks ON em.key_id = ks.key_id " +
        "WHERE em.table_name = ? AND em.column_name = ?";

    public MetadataManager(PluginService pluginService, EncryptionConfig config) {
        this.pluginService = pluginService;
        this.config = config;
        this.metadataCache = new ConcurrentHashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
        this.lastRefreshTime = Instant.EPOCH;
        this.refreshExecutor = createRefreshExecutor();
    }

    /**
     * Loads encryption metadata from database tables and returns a map of column configurations.
     * 
     * @return Map of column identifiers to ColumnEncryptionConfig objects
     * @throws MetadataException if database operations fail
     */
    public Map<String, ColumnEncryptionConfig> loadEncryptionMetadata() throws MetadataException {
        logger.debug("Loading encryption metadata from database");
        
        Map<String, ColumnEncryptionConfig> metadata = new ConcurrentHashMap<>();
        
        try (Connection connection = pluginService.forceConnect(pluginService.getCurrentHostSpec(), pluginService.getProperties());
             PreparedStatement stmt = connection.prepareStatement(LOAD_ENCRYPTION_METADATA_SQL);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                ColumnEncryptionConfig columnConfig = buildColumnConfigFromResultSet(rs);
                String columnIdentifier = columnConfig.getColumnIdentifier();
                metadata.put(columnIdentifier, columnConfig);
                
                logger.trace("Loaded encryption config for column: {}", columnIdentifier);
            }
            
            logger.info("Successfully loaded {} encryption configurations", metadata.size());
            
        } catch (SQLException e) {
            String errorMsg = "Failed to load encryption metadata from database";
            logger.error(errorMsg, e);
            throw new MetadataException(errorMsg, e);
        }
        
        return metadata;
    }

    /**
     * Refreshes the metadata cache by reloading from the database.
     * This method is thread-safe and can be called without application restart.
     * 
     * @throws MetadataException if refresh operation fails
     */
    public void refreshMetadata() throws MetadataException {
        logger.info("Refreshing encryption metadata cache");
        
        cacheLock.writeLock().lock();
        try {
            Map<String, ColumnEncryptionConfig> newMetadata = loadEncryptionMetadata();
            
            // Clear existing cache and populate with new data
            metadataCache.clear();
            metadataCache.putAll(newMetadata);
            lastRefreshTime = Instant.now();
            
            logger.info("Metadata cache refreshed successfully with {} configurations", 
                       metadataCache.size());
            
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Checks if a specific column is configured for encryption.
     * Uses cache if available and valid, otherwise queries database directly.
     * 
     * @param tableName the table name
     * @param columnName the column name
     * @return true if column is encrypted, false otherwise
     * @throws MetadataException if database operations fail
     */
    public boolean isColumnEncrypted(String tableName, String columnName) throws MetadataException {
        if (tableName == null || columnName == null) {
            return false;
        }
        
        String columnIdentifier = tableName + "." + columnName;
        
        // Try cache first if caching is enabled
        if (config.isCacheEnabled() && isCacheValid()) {
            cacheLock.readLock().lock();
            try {
                boolean result = metadataCache.containsKey(columnIdentifier);
                logger.trace("Cache lookup for column {}: {}", columnIdentifier, result);
                return result;
            } finally {
                cacheLock.readLock().unlock();
            }
        }
        
        // Fallback to database query
        return isColumnEncryptedFromDatabase(tableName, columnName);
    }

    /**
     * Retrieves the encryption configuration for a specific column.
     * Uses cache if available and valid, otherwise queries database directly.
     * 
     * @param tableName the table name
     * @param columnName the column name
     * @return ColumnEncryptionConfig if found, null otherwise
     * @throws MetadataException if database operations fail
     */
    public ColumnEncryptionConfig getColumnConfig(String tableName, String columnName) 
            throws MetadataException {
        if (tableName == null || columnName == null) {
            return null;
        }
        
        String columnIdentifier = tableName + "." + columnName;
        
        // Try cache first if caching is enabled
        if (config.isCacheEnabled() && isCacheValid()) {
            cacheLock.readLock().lock();
            try {
                ColumnEncryptionConfig result = metadataCache.get(columnIdentifier);
                logger.trace("Cache lookup for column config {}: {}", 
                           columnIdentifier, result != null ? "found" : "not found");
                return result;
            } finally {
                cacheLock.readLock().unlock();
            }
        }
        
        // Fallback to database query
        return getColumnConfigFromDatabase(tableName, columnName);
    }

    /**
     * Initializes the metadata cache by loading all configurations from database.
     * Should be called during plugin initialization.
     * 
     * @throws MetadataException if initialization fails
     */
    public void initialize() throws MetadataException {
        logger.info("Initializing MetadataManager");
        
        if (config.isCacheEnabled()) {
            refreshMetadata();
        }
        
        // Start automatic refresh if configured
        startAutomaticRefresh();
        
        logger.info("MetadataManager initialized successfully");
    }

    /**
     * Updates the configuration and adjusts refresh behavior accordingly.
     */
    public void updateConfig(EncryptionConfig newConfig) {
        EncryptionConfig oldConfig = this.config;
        this.config = newConfig;
        
        // Restart automatic refresh if interval changed
        if (!oldConfig.getMetadataRefreshInterval().equals(newConfig.getMetadataRefreshInterval())) {
            stopAutomaticRefresh();
            startAutomaticRefresh();
        }
        
        logger.info("MetadataManager configuration updated");
    }

    /**
     * Shuts down the metadata manager and cleans up resources.
     */
    public void shutdown() {
        logger.info("Shutting down MetadataManager");
        
        stopAutomaticRefresh();
        
        // Clear cache
        cacheLock.writeLock().lock();
        try {
            metadataCache.clear();
        } finally {
            cacheLock.writeLock().unlock();
        }
        
        logger.info("MetadataManager shutdown completed");
    }

    /**
     * Returns the timestamp of the last cache refresh.
     * 
     * @return Instant of last refresh, or Instant.EPOCH if never refreshed
     */
    public Instant getLastRefreshTime() {
        return lastRefreshTime;
    }

    /**
     * Returns the current size of the metadata cache.
     * 
     * @return number of cached configurations
     */
    public int getCacheSize() {
        cacheLock.readLock().lock();
        try {
            return metadataCache.size();
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Checks if the cache is valid based on expiration time.
     * 
     * @return true if cache is valid, false if expired or never initialized
     */
    private boolean isCacheValid() {
        if (lastRefreshTime.equals(Instant.EPOCH)) {
            return false;
        }
        
        Instant expirationTime = lastRefreshTime.plusSeconds(config.getCacheExpirationMinutes() * 60L);
        return Instant.now().isBefore(expirationTime);
    }

    /**
     * Queries database directly to check if column is encrypted.
     */
    private boolean isColumnEncryptedFromDatabase(String tableName, String columnName) 
            throws MetadataException {
        logger.trace("Checking encryption status for column {}.{} from database", tableName, columnName);
        
        try (Connection connection = pluginService.forceConnect(pluginService.getCurrentHostSpec(), pluginService.getProperties());
             PreparedStatement stmt = connection.prepareStatement(CHECK_COLUMN_ENCRYPTED_SQL)) {
            
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                boolean result = rs.next();
                logger.trace("Database lookup for column {}.{}: {}", tableName, columnName, result);
                return result;
            }
            
        } catch (SQLException e) {
            String errorMsg = String.format("Failed to check encryption status for column %s.%s", 
                                          tableName, columnName);
            logger.error(errorMsg, e);
            throw new MetadataException(errorMsg, e);
        }
    }

    /**
     * Queries database directly to get column configuration.
     */
    private ColumnEncryptionConfig getColumnConfigFromDatabase(String tableName, String columnName) 
            throws MetadataException {
        logger.trace("Loading encryption config for column {}.{} from database", tableName, columnName);
        
        try (Connection connection = pluginService.forceConnect(pluginService.getCurrentHostSpec(), pluginService.getProperties());
             PreparedStatement stmt = connection.prepareStatement(GET_COLUMN_CONFIG_SQL)) {
            
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    ColumnEncryptionConfig result = buildColumnConfigFromResultSet(rs);
                    logger.trace("Database lookup for column config {}.{}: found", tableName, columnName);
                    return result;
                } else {
                    logger.trace("Database lookup for column config {}.{}: not found", tableName, columnName);
                    return null;
                }
            }
            
        } catch (SQLException e) {
            String errorMsg = String.format("Failed to load encryption config for column %s.%s", 
                                          tableName, columnName);
            logger.error(errorMsg, e);
            throw new MetadataException(errorMsg, e);
        }
    }

    /**
     * Builds a ColumnEncryptionConfig from a ResultSet row.
     */
    private ColumnEncryptionConfig buildColumnConfigFromResultSet(ResultSet rs) throws SQLException {
        // Build KeyMetadata
        KeyMetadata keyMetadata = KeyMetadata.builder()
                .keyId(rs.getString("key_id"))
                .masterKeyArn(rs.getString("master_key_arn"))
                .encryptedDataKey(rs.getString("encrypted_data_key"))
                .keySpec(rs.getString("key_spec"))
                .createdAt(convertTimestampToInstant(rs.getTimestamp("key_created_at")))
                .lastUsedAt(convertTimestampToInstant(rs.getTimestamp("last_used_at")))
                .build();

        // Build ColumnEncryptionConfig
        return ColumnEncryptionConfig.builder()
                .tableName(rs.getString("table_name"))
                .columnName(rs.getString("column_name"))
                .algorithm(rs.getString("encryption_algorithm"))
                .keyId(rs.getString("key_id"))
                .keyMetadata(keyMetadata)
                .createdAt(convertTimestampToInstant(rs.getTimestamp("created_at")))
                .updatedAt(convertTimestampToInstant(rs.getTimestamp("updated_at")))
                .build();
    }

    /**
     * Converts SQL Timestamp to Instant, handling null values.
     */
    private Instant convertTimestampToInstant(Timestamp timestamp) {
        return timestamp != null ? timestamp.toInstant() : Instant.now();
    }

    /**
     * Creates a new refresh executor.
     */
    private ScheduledExecutorService createRefreshExecutor() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MetadataManager-Refresh");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Stops automatic metadata refresh.
     */
    private void stopAutomaticRefresh() {
        if (refreshExecutor != null && !refreshExecutor.isShutdown()) {
            logger.debug("Stopping automatic metadata refresh");
            refreshExecutor.shutdown();
            try {
                if (!refreshExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    refreshExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                refreshExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * Starts automatic metadata refresh based on configuration.
     */
    private void startAutomaticRefresh() {
        Duration refreshInterval = config.getMetadataRefreshInterval();
        
        if (refreshInterval.isZero() || refreshInterval.isNegative()) {
            logger.info("Automatic metadata refresh disabled (interval: {})", refreshInterval);
            return;
        }
        
        // Create new executor if current one is shut down
        if (refreshExecutor == null || refreshExecutor.isShutdown()) {
            refreshExecutor = createRefreshExecutor();
        }
        
        long intervalMs = refreshInterval.toMillis();
        refreshExecutor.scheduleAtFixedRate(() -> {
            try {
                logger.debug("Performing automatic metadata refresh");
                refreshMetadata();
            } catch (Exception e) {
                logger.warn("Automatic metadata refresh failed", e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        
        logger.info("Started automatic metadata refresh every {}ms", intervalMs);
    }
}