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

package software.amazon.jdbc.plugin.encryption.metadata;

import java.sql.Connection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.util.Messages;

/**
 * Manages encryption metadata by loading configuration from database tables, providing caching
 * mechanisms, and offering lookup methods for column encryption settings.
 */
public class MetadataManager {

  private static final Logger LOGGER = Logger.getLogger(MetadataManager.class.getName());

  private final @Nullable PluginService pluginService;
  private final @Nullable DataSource dataSource;
  private final @Nullable Connection connection;
  private volatile EncryptionConfig config;
  private final Map<String, ColumnEncryptionConfig> metadataCache;
  private final ReadWriteLock cacheLock;
  private volatile Instant lastRefreshTime;
  private volatile ScheduledExecutorService refreshExecutor;

  public MetadataManager(PluginService pluginService, EncryptionConfig config) {
    this.pluginService = pluginService;
    this.dataSource = null;
    this.connection = null;
    this.config = config;
    this.metadataCache = new ConcurrentHashMap<>();
    this.cacheLock = new ReentrantReadWriteLock();
    this.lastRefreshTime = Instant.EPOCH;
    this.refreshExecutor = createRefreshExecutor();
  }

  public MetadataManager(DataSource dataSource, EncryptionConfig config) {
    this.pluginService = null;
    this.dataSource = dataSource;
    this.connection = null;
    this.config = config;
    this.metadataCache = new ConcurrentHashMap<>();
    this.cacheLock = new ReentrantReadWriteLock();
    this.lastRefreshTime = Instant.EPOCH;
    this.refreshExecutor = createRefreshExecutor();
  }

  public MetadataManager(Connection connection, EncryptionConfig config) {
    this.pluginService = null;
    this.dataSource = null;
    this.connection = connection;
    this.config = config;
    this.metadataCache = new ConcurrentHashMap<>();
    this.cacheLock = new ReentrantReadWriteLock();
    this.lastRefreshTime = Instant.EPOCH;
    this.refreshExecutor = createRefreshExecutor();
  }

  private Connection getConnection() throws SQLException {
    if (connection != null) {
      return connection;
    }
    if (dataSource != null) {
      return dataSource.getConnection();
    }
    if (pluginService != null) {
      return pluginService.forceConnect(
          pluginService.getCurrentHostSpec(), pluginService.getProperties());
    }
    throw new SQLException(Messages.get("MetadataManager.noConnectionAvailable"));
  }

  private void closeConnection(Connection conn) throws SQLException {
    if ((dataSource != null || pluginService != null) && conn != null) {
      conn.close();
    }
    // Don't close if using provided connection
  }

  private String getLoadEncryptionMetadataSql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, "
        + "       em.created_at, em.updated_at, "
        + "       ks.name, ks.master_key_arn, ks.encrypted_data_key, ks.hmac_key, ks.key_spec, "
        + "       ks.created_at as key_created_at, ks.last_used_at "
        + "FROM "
        + schema + ".encryption_metadata em "
        + "JOIN "
        + schema + ".key_storage ks ON em.key_id = ks.id "
        + "ORDER BY em.table_name, em.column_name";
  }

  private String getCheckColumnEncryptedSql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "SELECT 1 FROM " + schema + ".encryption_metadata WHERE table_name = ? AND column_name = ?";
  }

  private String getColumnConfigSql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, "
        + "       em.created_at, em.updated_at, "
        + "       ks.master_key_arn, ks.encrypted_data_key, ks.hmac_key, ks.key_spec, "
        + "       ks.created_at as key_created_at, ks.last_used_at "
        + "FROM "
        + schema + ".encryption_metadata em "
        + "JOIN "
        + schema + ".key_storage ks ON em.key_id = ks.id "
        + "WHERE em.table_name = ? AND em.column_name = ?";
  }

  /**
   * Loads encryption metadata from database tables and returns a map of column configurations.
   *
   * @return Map of column identifiers to ColumnEncryptionConfig objects
   * @throws MetadataException if database operations fail
   */
  public Map<String, ColumnEncryptionConfig> loadEncryptionMetadata() throws MetadataException {
    LOGGER.finest(() -> Messages.get("MetadataManager.loadingMetadata"));

    Map<String, ColumnEncryptionConfig> metadata = new ConcurrentHashMap<>();

    Connection conn = null;
    try {
      conn = getConnection();
      try (PreparedStatement stmt = conn.prepareStatement(getLoadEncryptionMetadataSql());
          ResultSet rs = stmt.executeQuery()) {

        while (rs.next()) {
          ColumnEncryptionConfig columnConfig = buildColumnConfigFromResultSet(rs);
          String columnIdentifier = columnConfig.getColumnIdentifier();
          metadata.put(columnIdentifier, columnConfig);

          LOGGER.finest(() -> Messages.get("MetadataManager.loadedColumnConfig", new Object[]{columnIdentifier}));
        }

        LOGGER.info(() -> Messages.get("MetadataManager.loadedConfigs", new Object[]{metadata.size()}));
      }

    } catch (SQLException e) {
      String errorMsg = "Failed to load encryption metadata from database";
      LOGGER.severe(() -> errorMsg + e.getMessage());
      throw new MetadataException(errorMsg, e);
    } finally {
      try {
        closeConnection(conn);
      } catch (SQLException e) {
        LOGGER.warning(() -> Messages.get("MetadataManager.closeConnectionFailed", new Object[]{e.getMessage()}));
      }
    }

    return metadata;
  }

  /**
   * Refreshes the metadata cache by reloading from the database. This method is thread-safe and can
   * be called without application restart.
   *
   * @throws MetadataException if refresh operation fails
   */
  public void refreshMetadata() throws MetadataException {
    LOGGER.info(Messages.get("MetadataManager.refreshingCache"));

    cacheLock.writeLock().lock();
    try {
      Map<String, ColumnEncryptionConfig> newMetadata = loadEncryptionMetadata();

      // Clear existing cache and populate with new data
      metadataCache.clear();
      metadataCache.putAll(newMetadata);
      lastRefreshTime = Instant.now();

      LOGGER.info(() -> Messages.get("MetadataManager.cacheRefreshed", new Object[]{metadataCache.size()}));

    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  /**
   * Checks if a specific column is configured for encryption. Uses cache if available and valid,
   * otherwise queries database directly.
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
        LOGGER.finest(() -> Messages.get("MetadataManager.cacheLookup", new Object[]{columnIdentifier, result}));
        return result;
      } finally {
        cacheLock.readLock().unlock();
      }
    }

    // Fallback to database query
    return isColumnEncryptedFromDatabase(tableName, columnName);
  }

  /**
   * Retrieves the encryption configuration for a specific column. Uses cache if available and
   * valid, otherwise queries database directly.
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
        LOGGER.finest(() -> Messages.get(
            "MetadataManager.cacheConfigLookup",
            new Object[]{columnIdentifier, result != null ? "found" : "not found"}));
        return result;
      } finally {
        cacheLock.readLock().unlock();
      }
    }

    // Fallback to database query
    return getColumnConfigFromDatabase(tableName, columnName);
  }

  /**
   * Initializes the metadata cache by loading all configurations from database. Should be called
   * during plugin initialization.
   *
   * @throws MetadataException if initialization fails
   */
  public void initialize() throws MetadataException {
    LOGGER.info(Messages.get("MetadataManager.initializing"));

    if (config.isCacheEnabled()) {
      refreshMetadata();
    }

    // Start automatic refresh if configured
    startAutomaticRefresh();

    LOGGER.info(Messages.get("MetadataManager.initialized"));
  }

  /**
   * Updates the configuration and adjusts refresh behavior accordingly.
   *
   * @param newConfig New encryption configuration
   */
  public void updateConfig(EncryptionConfig newConfig) {
    EncryptionConfig oldConfig = this.config;
    this.config = newConfig;

    // Restart automatic refresh if interval changed
    if (!oldConfig.getMetadataRefreshInterval().equals(newConfig.getMetadataRefreshInterval())) {
      stopAutomaticRefresh();
      startAutomaticRefresh();
    }

    LOGGER.info(Messages.get("MetadataManager.configUpdated"));
  }

  /** Shuts down the metadata manager and cleans up resources. */
  public void shutdown() {
    LOGGER.info(Messages.get("MetadataManager.shuttingDown"));

    stopAutomaticRefresh();

    // Clear cache
    cacheLock.writeLock().lock();
    try {
      metadataCache.clear();
    } finally {
      cacheLock.writeLock().unlock();
    }

    LOGGER.info(Messages.get("MetadataManager.shutdownCompleted"));
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

  /** Queries database directly to check if column is encrypted. */
  private boolean isColumnEncryptedFromDatabase(String tableName, String columnName)
      throws MetadataException {
    LOGGER.finest(() -> Messages.get("MetadataManager.checkingColumnFromDb", new Object[]{tableName, columnName}));

    Connection conn = null;
    try {
      conn = getConnection();
      try (PreparedStatement stmt = conn.prepareStatement(getCheckColumnEncryptedSql())) {

        stmt.setString(1, tableName);
        stmt.setString(2, columnName);

        try (ResultSet rs = stmt.executeQuery()) {
          boolean result = rs.next();
          LOGGER.finest(() -> Messages.get(
              "MetadataManager.dbLookupResult", new Object[]{tableName, columnName, result}));
          return result;
        }
      }

    } catch (SQLException e) {
      String errorMsg =
          String.format(
              "Failed to check encryption status for column %s.%s", tableName, columnName);
      LOGGER.severe(() -> errorMsg + e);
      throw new MetadataException(errorMsg, e);
    } finally {
      try {
        closeConnection(conn);
      } catch (SQLException e) {
        LOGGER.warning(() -> Messages.get("MetadataManager.closeConnectionFailed2", new Object[]{e.getMessage()}));
      }
    }
  }

  /** Queries database directly to get column configuration. */
  private ColumnEncryptionConfig getColumnConfigFromDatabase(String tableName, String columnName)
      throws MetadataException {
    LOGGER.finest(() -> Messages.get("MetadataManager.loadingColumnConfig", new Object[]{tableName, columnName}));

    Connection conn = null;
    try {
      conn = getConnection();
      try (PreparedStatement stmt = conn.prepareStatement(getColumnConfigSql())) {

        stmt.setString(1, tableName);
        stmt.setString(2, columnName);

        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            ColumnEncryptionConfig result = buildColumnConfigFromResultSet(rs);
            LOGGER.finest(() -> Messages.get("MetadataManager.dbConfigFound", new Object[]{tableName, columnName}));
            return result;
          } else {
            LOGGER.finest(() -> Messages.get("MetadataManager.dbConfigNotFound", new Object[]{tableName, columnName}));
            return null;
          }
        }
      }

    } catch (SQLException e) {
      String errorMsg =
          String.format("Failed to load encryption config for column %s.%s", tableName, columnName);
      LOGGER.severe(() -> errorMsg + " " + e.getMessage());
      throw new MetadataException(errorMsg, e);
    } finally {
      try {
        closeConnection(conn);
      } catch (SQLException e) {
        LOGGER.warning(() -> Messages.get("MetadataManager.closeConnectionFailed3", new Object[]{e.getMessage()}));
      }
    }
  }

  /** Builds a ColumnEncryptionConfig from a ResultSet row. */
  private ColumnEncryptionConfig buildColumnConfigFromResultSet(ResultSet rs) throws SQLException {
    // Build KeyMetadata
    KeyMetadata keyMetadata =
        KeyMetadata.builder()
            .keyId(rs.getInt("key_id"))
            .keyName(rs.getString("name"))
            .masterKeyArn(rs.getString("master_key_arn"))
            .encryptedDataKey(rs.getString("encrypted_data_key"))
            .hmacKey(rs.getBytes("hmac_key"))
            .keySpec(rs.getString("key_spec"))
            .createdAt(convertTimestampToInstant(rs.getTimestamp("key_created_at")))
            .lastUsedAt(convertTimestampToInstant(rs.getTimestamp("last_used_at")))
            .build();

    // Build ColumnEncryptionConfig
    return ColumnEncryptionConfig.builder()
        .tableName(rs.getString("table_name"))
        .columnName(rs.getString("column_name"))
        .algorithm(rs.getString("encryption_algorithm"))
        .keyId(rs.getInt("key_id"))
        .keyMetadata(keyMetadata)
        .createdAt(convertTimestampToInstant(rs.getTimestamp("created_at")))
        .updatedAt(convertTimestampToInstant(rs.getTimestamp("updated_at")))
        .build();
  }

  /** Converts SQL Timestamp to Instant, handling null values. */
  private Instant convertTimestampToInstant(Timestamp timestamp) {
    return timestamp != null ? timestamp.toInstant() : Instant.now();
  }

  /** Creates a new refresh executor. */
  private ScheduledExecutorService createRefreshExecutor() {
    return Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = new Thread(r, "MetadataManager-Refresh");
          t.setDaemon(true);
          return t;
        });
  }

  /** Stops automatic metadata refresh. */
  private void stopAutomaticRefresh() {
    if (refreshExecutor != null && !refreshExecutor.isShutdown()) {
      LOGGER.finest(() -> Messages.get("MetadataManager.stoppingRefresh"));
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

  /** Starts automatic metadata refresh based on configuration. */
  private void startAutomaticRefresh() {
    Duration refreshInterval = config.getMetadataRefreshInterval();

    if (refreshInterval.isZero() || refreshInterval.isNegative()) {
      LOGGER.info(() -> Messages.get("MetadataManager.refreshDisabled", new Object[]{refreshInterval}));
      return;
    }

    // Create new executor if current one is shut down
    if (refreshExecutor == null || refreshExecutor.isShutdown()) {
      refreshExecutor = createRefreshExecutor();
    }

    long intervalMs = refreshInterval.toMillis();
    refreshExecutor.scheduleAtFixedRate(
        () -> {
          try {
            LOGGER.finest(() -> Messages.get("MetadataManager.performingRefresh"));
            refreshMetadata();
          } catch (Exception e) {
            LOGGER.warning(() -> Messages.get("MetadataManager.refreshFailed", new Object[]{e.getMessage()}));
          }
        },
        intervalMs,
        intervalMs,
        TimeUnit.MILLISECONDS);

    LOGGER.info(() -> Messages.get("MetadataManager.refreshStarted", new Object[]{intervalMs}));
  }
}
