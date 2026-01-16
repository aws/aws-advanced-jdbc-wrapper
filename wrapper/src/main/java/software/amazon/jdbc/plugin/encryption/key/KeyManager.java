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

package software.amazon.jdbc.plugin.encryption.key;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.cache.DataKeyCache;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;

/**
 * Manages KMS operations and data key lifecycle for the encryption plugin. Handles key creation,
 * data key generation/decryption, and database storage of key metadata.
 */
public class KeyManager {

  private static final Logger LOGGER = Logger.getLogger(KeyManager.class.getName());

  private final KmsClient kmsClient;
  private final PluginService pluginService;
  private final EncryptionConfig config;
  private final DataKeyCache dataKeyCache;

  public KeyManager(KmsClient kmsClient, PluginService pluginService, EncryptionConfig config) {
    this.kmsClient = Objects.requireNonNull(kmsClient, "KmsClient cannot be null");
    this.pluginService = Objects.requireNonNull(pluginService, "DataSource cannot be null");
    this.config = Objects.requireNonNull(config, "EncryptionConfig cannot be null");
    this.dataKeyCache = new DataKeyCache(config);
  }

  private String getInsertKeyMetadataSql() {
    String schema = config.getEncryptionMetadataSchema();
    return "INSERT INTO "
        + schema
        + ".key_storage (name, master_key_arn, encrypted_data_key, key_spec, created_at, last_used_at) "
        + "VALUES (?, ?, ?, ?, ?, ?) "
        + "RETURNING id";
  }

  private String getSelectKeyMetadataSql() {
    return "SELECT id, name, master_key_arn, encrypted_data_key, key_spec, created_at, last_used_at "
        + "FROM "
        + config.getEncryptionMetadataSchema()
        + ".key_storage WHERE id = ?";
  }

  private String getUpdateLastUsedSql() {
    return "UPDATE "
        + config.getEncryptionMetadataSchema()
        + ".key_storage SET last_used_at = ? WHERE key_id = ?";
  }

  /**
   * Creates a new KMS master key with the specified description.
   *
   * @param description Description for the master key
   * @return The ARN of the created master key
   * @throws KeyManagementException if key creation fails
   */
  public String createMasterKey(String description) throws KeyManagementException {
    Objects.requireNonNull(description, "Description cannot be null");

    LOGGER.info(() -> String.format("Creating KMS master key with description: %s", description));

    try {
      CreateKeyRequest request =
          CreateKeyRequest.builder()
              .description(description)
              .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
              .keySpec(KeySpec.SYMMETRIC_DEFAULT)
              .build();

      CreateKeyResponse response = executeWithRetry(() -> kmsClient.createKey(request));
      String keyArn = response.keyMetadata().arn();

      LOGGER.info(() -> String.format("Successfully created KMS master key: %s", keyArn));
      return keyArn;

    } catch (Exception e) {
      LOGGER.severe(() -> String.format("Failed to create KMS master key", e));
      throw new KeyManagementException("Failed to create KMS master key: " + e.getMessage(), e);
    }
  }

  /**
   * Generates a new data key using the specified master key.
   *
   * @param masterKeyArn ARN of the master key to use for data key generation
   * @return DataKeyResult containing both plaintext and encrypted data keys
   * @throws KeyManagementException if data key generation fails
   */
  public DataKeyResult generateDataKey(String masterKeyArn) throws KeyManagementException {
    Objects.requireNonNull(masterKeyArn, "Master key ARN cannot be null");

    LOGGER.finest(() -> String.format("Generating data key using master key: %s", masterKeyArn));

    try {
      GenerateDataKeyRequest request =
          GenerateDataKeyRequest.builder().keyId(masterKeyArn).keySpec(DataKeySpec.AES_256).build();

      GenerateDataKeyResponse response = executeWithRetry(() -> kmsClient.generateDataKey(request));

      byte[] plaintextKey = response.plaintext().asByteArray();
      String encryptedKey =
          Base64.getEncoder().encodeToString(response.ciphertextBlob().asByteArray());

      LOGGER.finest(
          () -> String.format("Successfully generated data key for master key: %s", masterKeyArn));
      return new DataKeyResult(plaintextKey, encryptedKey);

    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to generate data key for master key: %s", masterKeyArn, e));
      throw new KeyManagementException("Failed to generate data key: " + e.getMessage(), e);
    }
  }

  /**
   * Decrypts an encrypted data key using KMS with caching support.
   *
   * @param encryptedDataKey Base64-encoded encrypted data key
   * @param masterKeyArn ARN of the master key used for encryption
   * @return Decrypted data key as byte array
   * @throws KeyManagementException if decryption fails
   */
  public byte[] decryptDataKey(String encryptedDataKey, String masterKeyArn)
      throws KeyManagementException {
    Objects.requireNonNull(encryptedDataKey, "Encrypted data key cannot be null");
    Objects.requireNonNull(masterKeyArn, "Master key ARN cannot be null");

    // Create cache key from encrypted data key hash
    String cacheKey = createCacheKey(encryptedDataKey);

    // Try cache first if enabled
    if (config.isDataKeyCacheEnabled()) {
      byte[] cachedKey = dataKeyCache.get(cacheKey);
      if (cachedKey != null) {
        LOGGER.finest(() -> "Cache hit for data key decryption");
        return cachedKey;
      }
    }

    LOGGER.finest(() -> String.format("Decrypting data key using master key: %s", masterKeyArn));

    try {
      byte[] encryptedKeyBytes = Base64.getDecoder().decode(encryptedDataKey);

      DecryptRequest request =
          DecryptRequest.builder()
              .ciphertextBlob(SdkBytes.fromByteArray(encryptedKeyBytes))
              .keyId(masterKeyArn)
              .build();

      DecryptResponse response = executeWithRetry(() -> kmsClient.decrypt(request));
      byte[] plaintextKey = response.plaintext().asByteArray();

      // Cache the decrypted key if caching is enabled
      if (config.isDataKeyCacheEnabled()) {
        dataKeyCache.put(cacheKey, plaintextKey);
      }

      LOGGER.finest(
          () -> String.format("Successfully decrypted data key for master key: %s", masterKeyArn));
      return plaintextKey;

    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to decrypt data key for master key: %s", masterKeyArn, e));
      throw new KeyManagementException("Failed to decrypt data key: " + e.getMessage(), e);
    }
  }

  /**
   * Stores key metadata in the database for the specified table and column.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param keyMetadata Key metadata to store
   * @return the generated integer ID
   * @throws KeyManagementException if storage fails
   */
  public int storeKeyMetadata(String tableName, String columnName, KeyMetadata keyMetadata)
      throws KeyManagementException {
    Objects.requireNonNull(tableName, "Table name cannot be null");
    Objects.requireNonNull(columnName, "Column name cannot be null");
    Objects.requireNonNull(keyMetadata, "Key metadata cannot be null");

    if (!keyMetadata.isValid()) {
      throw new KeyManagementException("Invalid key metadata provided");
    }

    LOGGER.finest(() -> String.format("Storing key metadata for %s.%s", tableName, columnName));

    try (Connection conn =
            pluginService.forceConnect(
                pluginService.getCurrentHostSpec(), pluginService.getProperties());
        PreparedStatement stmt = conn.prepareStatement(getInsertKeyMetadataSql())) {

      stmt.setString(1, keyMetadata.getKeyName());
      stmt.setString(2, keyMetadata.getMasterKeyArn());
      stmt.setString(3, keyMetadata.getEncryptedDataKey());
      stmt.setString(4, keyMetadata.getKeySpec());
      stmt.setTimestamp(5, Timestamp.from(keyMetadata.getCreatedAt()));
      stmt.setTimestamp(6, Timestamp.from(keyMetadata.getLastUsedAt()));

      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        int generatedId = rs.getInt(1);
        LOGGER.finest(
            () ->
                String.format(
                    "Successfully stored key metadata for %s.%s with ID: %s",
                    tableName, columnName, generatedId));
        return generatedId;
      } else {
        throw new KeyManagementException("Failed to get generated key ID");
      }

    } catch (SQLException e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Database error storing key metadata for %s.%s %s",
                  tableName, columnName, e.getMessage()));
      throw new KeyManagementException("Failed to store key metadata: " + e.getMessage(), e);
    }
  }

  /**
   * Retrieves key metadata from the database for the specified key ID.
   *
   * @param keyId Key ID to retrieve metadata for
   * @return Optional containing key metadata if found
   * @throws KeyManagementException if retrieval fails
   */
  public Optional<KeyMetadata> getKeyMetadata(String keyId) throws KeyManagementException {
    Objects.requireNonNull(keyId, "Key ID cannot be null");

    LOGGER.finest(() -> String.format("Retrieving key metadata for key ID: %s", keyId));

    try (Connection conn =
            pluginService.forceConnect(
                pluginService.getCurrentHostSpec(), pluginService.getProperties());
        PreparedStatement stmt = conn.prepareStatement(getSelectKeyMetadataSql())) {

      stmt.setString(1, keyId);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          KeyMetadata metadata =
              KeyMetadata.builder()
                  .keyId(rs.getString("key_id"))
                  .masterKeyArn(rs.getString("master_key_arn"))
                  .encryptedDataKey(rs.getString("encrypted_data_key"))
                  .keySpec(rs.getString("key_spec"))
                  .createdAt(rs.getTimestamp("created_at").toInstant())
                  .lastUsedAt(rs.getTimestamp("last_used_at").toInstant())
                  .build();

          LOGGER.finest(
              () -> String.format("Successfully retrieved key metadata for key ID: %s", keyId));
          return Optional.of(metadata);
        } else {
          LOGGER.finest(() -> String.format("No key metadata found for key ID: %s", keyId));
          return Optional.empty();
        }
      }

    } catch (SQLException e) {
      LOGGER.severe(
          () -> String.format("Database error retrieving key metadata for key ID: %s", keyId, e));
      throw new KeyManagementException("Failed to retrieve key metadata: " + e.getMessage(), e);
    }
  }

  /**
   * Updates the last used timestamp for the specified key.
   *
   * @param keyId Key ID to update
   * @throws KeyManagementException if update fails
   */
  public void updateLastUsed(String keyId) throws KeyManagementException {
    Objects.requireNonNull(keyId, "Key ID cannot be null");

    try (Connection conn =
            pluginService.forceConnect(
                pluginService.getCurrentHostSpec(), pluginService.getProperties());
        PreparedStatement stmt = conn.prepareStatement(getUpdateLastUsedSql())) {

      stmt.setTimestamp(1, Timestamp.from(Instant.now()));
      stmt.setString(2, keyId);

      stmt.executeUpdate();

    } catch (SQLException e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Database error updating last used timestamp for key ID: %s %s",
                  keyId, e.getMessage()));
      throw new KeyManagementException(
          "Failed to update last used timestamp: " + e.getMessage(), e);
    }
  }

  /**
   * Generates a unique key ID for new keys to store in the database.
   *
   * @return Unique key ID
   */
  public String generateKeyId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Returns the data key cache for metrics and management.
   *
   * @return Data key cache instance
   */
  public DataKeyCache getDataKeyCache() {
    return dataKeyCache;
  }

  /** Clears the data key cache. */
  public void clearCache() {
    dataKeyCache.clear();
    LOGGER.info(() -> "Data key cache cleared");
  }

  /** Shuts down the key manager and cleans up resources. */
  public void shutdown() {
    LOGGER.info(() -> "Shutting down KeyManager");
    dataKeyCache.shutdown();
  }

  /** Executes a KMS operation with retry logic and exponential backoff. */
  private <T> T executeWithRetry(KmsOperation<T> operation) throws Exception {
    Exception lastException = null;
    int maxRetries = config.getMaxRetries();

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return operation.execute();
      } catch (Exception e) {
        lastException = e;

        if (attempt == maxRetries) {
          break;
        }

        if (isRetryableException(e)) {
          long backoffMs = calculateBackoff(attempt);
          int finalAttempt = attempt;
          LOGGER.warning(
              () ->
                  String.format(
                      "KMS operation failed (attempt %s/%s), retrying in %sms: %s",
                      finalAttempt + 1, maxRetries + 1, backoffMs, e.getMessage()));

          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new KeyManagementException("Operation interrupted during retry", ie);
          }
        } else {
          // Non-retryable exception, fail immediately
          break;
        }
      }
    }

    throw lastException;
  }

  /** Determines if an exception is retryable. */
  private boolean isRetryableException(Exception e) {
    if (e instanceof KmsException) {
      KmsException kmsException = (KmsException) e;
      // Retry on throttling, service unavailable, and internal errors
      boolean isServerError = kmsException.statusCode() >= 500;
      boolean isThrottling = kmsException.statusCode() == 429;

      // Check error code if available
      boolean isThrottlingError = false;
      if (kmsException.awsErrorDetails() != null
          && kmsException.awsErrorDetails().errorCode() != null) {
        isThrottlingError =
            "ThrottlingException".equals(kmsException.awsErrorDetails().errorCode());
      }

      return isServerError || isThrottling || isThrottlingError;
    }

    // Retry on general network/connection issues
    return e instanceof java.net.ConnectException
        || e instanceof java.net.SocketTimeoutException
        || e instanceof java.io.IOException;
  }

  /** Calculates exponential backoff with jitter. */
  private long calculateBackoff(int attempt) {
    long baseMs = config.getRetryBackoffBase().toMillis();
    long exponentialBackoff = baseMs * (1L << attempt);

    // Add jitter (±25% of the calculated backoff)
    long jitter =
        (long) (exponentialBackoff * 0.25 * (ThreadLocalRandom.current().nextDouble() - 0.5) * 2);

    return Math.max(baseMs, exponentialBackoff + jitter);
  }

  /** Creates a cache key from an encrypted data key. */
  private String createCacheKey(String encryptedDataKey) {
    // Use a hash of the encrypted data key as cache key for security
    return "datakey_" + Math.abs(encryptedDataKey.hashCode());
  }

  /** Functional interface for KMS operations that can be retried. */
  @FunctionalInterface
  private interface KmsOperation<T> {
    T execute() throws Exception;
  }

  /** Result class for data key generation operations. */
  public static class DataKeyResult {
    private final byte[] plaintextKey;
    private final String encryptedKey;

    public DataKeyResult(byte[] plaintextKey, String encryptedKey) {
      this.plaintextKey = Objects.requireNonNull(plaintextKey, "Plaintext key cannot be null");
      this.encryptedKey = Objects.requireNonNull(encryptedKey, "Encrypted key cannot be null");
    }

    public byte[] getPlaintextKey() {
      return plaintextKey.clone(); // Return copy for security
    }

    public String getEncryptedKey() {
      return encryptedKey;
    }

    /** Clears the plaintext key from memory for security. */
    public void clearPlaintextKey() {
      if (plaintextKey != null) {
        java.util.Arrays.fill(plaintextKey, (byte) 0);
      }
    }
  }
}
