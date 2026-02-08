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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import javax.sql.DataSource;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;

/**
 * Utility class providing administrative functions for key management operations. This class offers
 * high-level methods for creating master keys, setting up encryption for tables/columns, rotating
 * keys, and managing the encryption lifecycle.
 */
public class KeyManagementUtility {

  private static final Logger LOGGER = Logger.getLogger(KeyManagementUtility.class.getName());

  private final KeyManager keyManager;
  private final MetadataManager metadataManager;
  private final DataSource dataSource;
  private final KmsClient kmsClient;
  private final EncryptionConfig config;

  public KeyManagementUtility(
      KeyManager keyManager,
      MetadataManager metadataManager,
      DataSource dataSource,
      KmsClient kmsClient,
      EncryptionConfig config) {
    this.keyManager = Objects.requireNonNull(keyManager, "KeyManager cannot be null");
    this.metadataManager =
        Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
    this.dataSource = Objects.requireNonNull(dataSource, "DataSource cannot be null");
    this.kmsClient = Objects.requireNonNull(kmsClient, "KmsClient cannot be null");
    this.config = Objects.requireNonNull(config, "EncryptionConfig cannot be null");
  }

  private String getInsertEncryptionMetadataSql() {
    String schema = config.getEncryptionMetadataSchema();
    return "INSERT INTO "
        + schema
        + ".encryption_metadata (table_name, column_name, encryption_algorithm, key_id, created_at, updated_at) "
        + "VALUES (?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (table_name, column_name) DO UPDATE SET "
        + "encryption_algorithm = EXCLUDED.encryption_algorithm, "
        + "key_id = EXCLUDED.key_id, "
        + "updated_at = EXCLUDED.updated_at";
  }

  private String getUpdateEncryptionMetadataKeySql() {
    return "UPDATE "
        + config.getEncryptionMetadataSchema()
        + ".encryption_metadata SET key_id = ?, updated_at = ? "
        + "WHERE table_name = ? AND column_name = ?";
  }

  private String getSelectColumnsWithKeySql() {
    return "SELECT table_name, column_name FROM "
        + config.getEncryptionMetadataSchema()
        + ".encryption_metadata WHERE key_id = ?";
  }

  private String getDeleteEncryptionMetadataSql() {
    return "DELETE FROM "
        + config.getEncryptionMetadataSchema()
        + ".encryption_metadata WHERE table_name = ? AND column_name = ?";
  }

  /**
   * Creates a new KMS master key with proper permissions for encryption operations.
   *
   * @param description Description for the master key
   * @param keyPolicy Optional key policy JSON string. If null, uses default policy
   * @return The ARN of the created master key
   * @throws KeyManagementException if key creation fails
   */
  public String createMasterKeyWithPermissions(String description, String keyPolicy)
      throws KeyManagementException {
    Objects.requireNonNull(description, "Description cannot be null");

    LOGGER.info(() -> String.format("Creating KMS master key with permissions: %s", description));

    try {
      CreateKeyRequest.Builder requestBuilder =
          CreateKeyRequest.builder()
              .description(description)
              .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
              .keySpec(KeySpec.SYMMETRIC_DEFAULT);

      // Add key policy if provided
      if (keyPolicy != null && !keyPolicy.trim().isEmpty()) {
        requestBuilder.policy(keyPolicy);
        LOGGER.finest(() -> "Using custom key policy for master key creation");
      }

      CreateKeyResponse response = kmsClient.createKey(requestBuilder.build());
      String keyArn = response.keyMetadata().arn();

      // Create an alias for easier management
      String aliasName = "alias/jdbc-encryption-" + System.currentTimeMillis();
      CreateAliasRequest aliasRequest =
          CreateAliasRequest.builder().aliasName(aliasName).targetKeyId(keyArn).build();

      kmsClient.createAlias(aliasRequest);

      LOGGER.info(
          () ->
              String.format(
                  "Successfully created KMS master key: %s with alias: %s", keyArn, aliasName));
      return keyArn;

    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to create KMS master key with permissions", e.getMessage()));
      throw new KeyManagementException("Failed to create KMS master key: " + e.getMessage(), e);
    }
  }

  /**
   * Creates a master key with default permissions suitable for JDBC encryption.
   *
   * @param description Description for the master key
   * @return The ARN of the created master key
   * @throws KeyManagementException if key creation fails
   */
  public String createMasterKeyWithPermissions(String description) throws KeyManagementException {
    return createMasterKeyWithPermissions(description, null);
  }

  /**
   * Generates and stores a data key for the specified table and column. This method creates the
   * complete encryption setup for a column.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param masterKeyArn ARN of the master key to use
   * @param algorithm Encryption algorithm (defaults to AES-256-GCM if null)
   * @return The generated key ID
   * @throws KeyManagementException if key generation or storage fails
   */
  public String generateAndStoreDataKey(
      String tableName, String columnName, String masterKeyArn, String algorithm)
      throws KeyManagementException {
    Objects.requireNonNull(tableName, "Table name cannot be null");
    Objects.requireNonNull(columnName, "Column name cannot be null");
    Objects.requireNonNull(masterKeyArn, "Master key ARN cannot be null");

    if (algorithm == null || algorithm.trim().isEmpty()) {
      algorithm = "AES-256-GCM";
    }

    LOGGER.info(
        () ->
            String.format(
                "Generating and storing data key for %s.%s using master key: %s",
                tableName, columnName, masterKeyArn));

    try {

      // Generate the data key using KMS
      KeyManager.DataKeyResult dataKeyResult = keyManager.generateDataKey(masterKeyArn);

      try {
        // Generate a unique key name
        String keyName = "key-" + tableName + "-" + columnName + "-" + System.currentTimeMillis();

        // Create key metadata
        KeyMetadata keyMetadata =
            KeyMetadata.builder()
                .keyId("dummy") // Not used anymore but required by builder
                .keyName(keyName)
                .masterKeyArn(masterKeyArn)
                .encryptedDataKey(dataKeyResult.getEncryptedKey())
                .keySpec("AES_256")
                .createdAt(Instant.now())
                .lastUsedAt(Instant.now())
                .build();

        // Store key metadata in database and get the generated integer ID
        int generatedKeyId = keyManager.storeKeyMetadata(tableName, columnName, keyMetadata);

        // Store encryption metadata using the generated integer key ID
        storeEncryptionMetadata(tableName, columnName, algorithm, generatedKeyId);

        // Refresh metadata cache
        metadataManager.refreshMetadata();

        LOGGER.info(
            () ->
                String.format(
                    "Successfully generated and stored data key for %s.%s with key ID: %s",
                    tableName, columnName, generatedKeyId));

        return String.valueOf(generatedKeyId);

      } finally {
        // Clear sensitive data from memory
        dataKeyResult.clearPlaintextKey();
      }

    } catch (Exception e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Failed to generate and store data key for %s.%s",
                  tableName, columnName, e.getMessage()));
      throw new KeyManagementException(
          "Failed to generate and store data key: " + e.getMessage(), e);
    }
  }

  /**
   * Rotates the data key for an existing encrypted column. This creates a new data key while
   * preserving the existing encryption metadata.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param newMasterKeyArn Optional new master key ARN. If null, uses existing master key
   * @return The new key ID
   * @throws KeyManagementException if key rotation fails
   */
  public String rotateDataKey(String tableName, String columnName, String newMasterKeyArn)
      throws KeyManagementException {
    Objects.requireNonNull(tableName, "Table name cannot be null");
    Objects.requireNonNull(columnName, "Column name cannot be null");

    LOGGER.info(() -> String.format("Rotating data key for %s.%s", tableName, columnName));

    try {
      // Get current encryption configuration
      ColumnEncryptionConfig currentConfig = metadataManager.getColumnConfig(tableName, columnName);
      if (currentConfig == null) {
        throw new KeyManagementException(
            "No encryption configuration found for " + tableName + "." + columnName);
      }

      // Use existing master key if new one not provided
      String masterKeyArn =
          newMasterKeyArn != null
              ? newMasterKeyArn
              : currentConfig.getKeyMetadata().getMasterKeyArn();

      // Generate new data key
      String newKeyId = keyManager.generateKeyId();
      KeyManager.DataKeyResult dataKeyResult = keyManager.generateDataKey(masterKeyArn);

      try {
        // Create new key metadata
        KeyMetadata newKeyMetadata =
            KeyMetadata.builder()
                .keyId(newKeyId)
                .masterKeyArn(masterKeyArn)
                .encryptedDataKey(dataKeyResult.getEncryptedKey())
                .keySpec("AES_256")
                .createdAt(Instant.now())
                .lastUsedAt(Instant.now())
                .build();

        // Store new key metadata
        keyManager.storeKeyMetadata(tableName, columnName, newKeyMetadata);

        // Update encryption metadata to use new key
        updateEncryptionMetadataKey(tableName, columnName, newKeyId);

        // Refresh metadata cache
        metadataManager.refreshMetadata();

        LOGGER.info(
            () ->
                String.format(
                    "Successfully rotated data key for %s.%s from %s to %s",
                    tableName, columnName, currentConfig.getKeyId(), newKeyId));

        return newKeyId;

      } finally {
        dataKeyResult.clearPlaintextKey();
      }

    } catch (Exception e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Failed to rotate data key for %s.%s", tableName, columnName, e.getMessage()));
      throw new KeyManagementException("Failed to rotate data key: " + e.getMessage(), e);
    }
  }

  /**
   * Initializes encryption for a new table and column combination. This is a convenience method
   * that creates everything needed for encryption.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param masterKeyArn ARN of the master key to use
   * @return The generated key ID
   * @throws KeyManagementException if initialization fails
   */
  public String initializeEncryptionForColumn(
      String tableName, String columnName, String masterKeyArn) throws KeyManagementException {
    return initializeEncryptionForColumn(tableName, columnName, masterKeyArn, "AES-256-GCM");
  }

  /**
   * Initializes encryption for a new table and column combination with specified algorithm.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param masterKeyArn ARN of the master key to use
   * @param algorithm Encryption algorithm to use
   * @return The generated key ID
   * @throws KeyManagementException if initialization fails
   */
  public String initializeEncryptionForColumn(
      String tableName, String columnName, String masterKeyArn, String algorithm)
      throws KeyManagementException {
    LOGGER.info(
        () -> String.format("Initializing encryption for column %s.%s", tableName, columnName));

    // Check if column is already encrypted
    try {
      if (metadataManager.isColumnEncrypted(tableName, columnName)) {
        throw new KeyManagementException(
            "Column " + tableName + "." + columnName + " is already encrypted");
      }
    } catch (MetadataException e) {
      throw new KeyManagementException("Failed to check existing encryption status", e);
    }

    // Generate and store the data key
    return generateAndStoreDataKey(tableName, columnName, masterKeyArn, algorithm);
  }

  /**
   * Removes encryption configuration for a table and column. This does not delete the actual key
   * data for security reasons.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @throws KeyManagementException if removal fails
   */
  public void removeEncryptionForColumn(String tableName, String columnName)
      throws KeyManagementException {
    Objects.requireNonNull(tableName, "Table name cannot be null");
    Objects.requireNonNull(columnName, "Column name cannot be null");

    LOGGER.info(
        () -> String.format("Removing encryption configuration for %s.%s", tableName, columnName));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(getDeleteEncryptionMetadataSql())) {

      stmt.setString(1, tableName);
      stmt.setString(2, columnName);

      int rowsAffected = stmt.executeUpdate();
      if (rowsAffected == 0) {
        LOGGER.warning(
            () ->
                String.format(
                    "No encryption configuration found for %s.%s", tableName, columnName));
      } else {
        LOGGER.info(
            () ->
                String.format(
                    "Successfully removed encryption configuration for %s.%s",
                    tableName, columnName));
      }

      // Refresh metadata cache
      metadataManager.refreshMetadata();

    } catch (MetadataException e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Failed to refresh metadata after removing encryption configuration", e));
      throw new KeyManagementException("Failed to refresh metadata: " + e.getMessage(), e);
    } catch (SQLException e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Failed to remove encryption configuration for %s.%s", tableName, columnName, e));
      throw new KeyManagementException(
          "Failed to remove encryption configuration: " + e.getMessage(), e);
    }
  }

  /**
   * Lists all columns that use a specific key ID. Useful for understanding the impact of key
   * operations.
   *
   * @param keyId The key ID to search for
   * @return List of table.column identifiers using the key
   * @throws KeyManagementException if query fails
   */
  public List<String> getColumnsUsingKey(String keyId) throws KeyManagementException {
    Objects.requireNonNull(keyId, "Key ID cannot be null");

    LOGGER.finest(() -> String.format("Finding columns using key ID: %s", keyId));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(getSelectColumnsWithKeySql())) {

      stmt.setString(1, keyId);

      try (ResultSet rs = stmt.executeQuery()) {
        List<String> columns = new ArrayList<>();
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          String columnName = rs.getString("column_name");
          columns.add(tableName + "." + columnName);
        }
        return columns;
      }

    } catch (SQLException e) {
      LOGGER.severe(
          () -> String.format("Failed to find columns using key ID: %s", keyId, e.getMessage()));
      throw new KeyManagementException("Failed to find columns using key: " + e.getMessage(), e);
    }
  }

  /**
   * Validates that a master key exists and is accessible.
   *
   * @param masterKeyArn ARN of the master key to validate
   * @return true if key is valid and accessible
   * @throws KeyManagementException if validation fails
   */
  public boolean validateMasterKey(String masterKeyArn) throws KeyManagementException {
    Objects.requireNonNull(masterKeyArn, "Master key ARN cannot be null");

    LOGGER.finest(() -> String.format("Validating master key: %s", masterKeyArn));

    try {
      DescribeKeyRequest request = DescribeKeyRequest.builder().keyId(masterKeyArn).build();

      DescribeKeyResponse response = kmsClient.describeKey(request);
      software.amazon.awssdk.services.kms.model.KeyMetadata keyMetadata = response.keyMetadata();

      boolean isValid =
          keyMetadata.enabled()
              && keyMetadata.keyState() == KeyState.ENABLED
              && keyMetadata.keyUsage() == KeyUsageType.ENCRYPT_DECRYPT;

      LOGGER.finest(
          () -> String.format("Master key %s validation result: %s", masterKeyArn, isValid));
      return isValid;

    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to validate master key: %s", masterKeyArn, e.getMessage()));
      throw new KeyManagementException("Failed to validate master key: " + e.getMessage(), e);
    }
  }

  /** Stores encryption metadata in the database. */
  private void storeEncryptionMetadata(
      String tableName, String columnName, String algorithm, int keyId) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(getInsertEncryptionMetadataSql())) {

      Timestamp now = Timestamp.from(Instant.now());

      stmt.setString(1, tableName);
      stmt.setString(2, columnName);
      stmt.setString(3, algorithm);
      stmt.setInt(4, keyId);
      stmt.setTimestamp(5, now);
      stmt.setTimestamp(6, now);

      int rowsAffected = stmt.executeUpdate();
      if (rowsAffected == 0) {
        throw new SQLException("Failed to store encryption metadata - no rows affected");
      }

      LOGGER.finest(
          () ->
              String.format(
                  "Successfully stored encryption metadata for %s.%s", tableName, columnName));
    }
  }

  /** Updates the key ID for existing encryption metadata. */
  private void updateEncryptionMetadataKey(String tableName, String columnName, String newKeyId)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(getUpdateEncryptionMetadataKeySql())) {

      stmt.setString(1, newKeyId);
      stmt.setTimestamp(2, Timestamp.from(Instant.now()));
      stmt.setString(3, tableName);
      stmt.setString(4, columnName);

      int rowsAffected = stmt.executeUpdate();
      if (rowsAffected == 0) {
        throw new SQLException("Failed to update encryption metadata key - no rows affected");
      }

      LOGGER.finest(
          () ->
              String.format(
                  "Successfully updated encryption metadata key for %s.%s to %s",
                  tableName, columnName, newKeyId));
    }
  }
}
