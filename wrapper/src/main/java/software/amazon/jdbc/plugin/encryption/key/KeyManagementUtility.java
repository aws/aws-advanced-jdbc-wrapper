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

import java.security.SecureRandom;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.plugin.encryption.service.EncryptionAlgorithm;
import software.amazon.jdbc.plugin.encryption.service.EncryptionAlgorithm;
import software.amazon.jdbc.util.Messages;

/**
 * Utility class providing administrative functions for key management operations. This class offers
 * high-level methods for creating master keys, setting up encryption for tables/columns, rotating
 * keys, and managing the encryption lifecycle.
 */
public class KeyManagementUtility {

  private static final Logger LOGGER = Logger.getLogger(KeyManagementUtility.class.getName());

  private final KeyManager keyManager;
  private final MetadataManager metadataManager;
  private final @Nullable DataSource dataSource;
  private final @Nullable Connection connection;
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
    this.connection = null;
    this.kmsClient = Objects.requireNonNull(kmsClient, "KmsClient cannot be null");
    this.config = Objects.requireNonNull(config, "EncryptionConfig cannot be null");
  }

  public KeyManagementUtility(
      KeyManager keyManager,
      MetadataManager metadataManager,
      Connection connection,
      KmsClient kmsClient,
      EncryptionConfig config) {
    this.keyManager = Objects.requireNonNull(keyManager, "KeyManager cannot be null");
    this.metadataManager =
        Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
    this.dataSource = null;
    this.connection = Objects.requireNonNull(connection, "Connection cannot be null");
    this.kmsClient = Objects.requireNonNull(kmsClient, "KmsClient cannot be null");
    this.config = Objects.requireNonNull(config, "EncryptionConfig cannot be null");
  }

  private Connection getConnection() throws SQLException {
    if (connection != null) {
      return connection;
    }
    if (dataSource != null) {
      return dataSource.getConnection();
    }
    throw new SQLException(Messages.get("KeyManagementUtility.noConnectionAvailable"));
  }

  private void closeConnection(Connection conn) throws SQLException {
    if (dataSource != null && conn != null) {
      conn.close();
    }
    // Don't close if using provided connection
  }

  private String getInsertEncryptionMetadataSql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    String baseSql = "INSERT INTO "
        + schema + ".encryption_metadata"
        + " (table_name, column_name, encryption_algorithm, key_id, created_at, updated_at) "
        + "VALUES (?, ?, ?, ?, ?, ?)";

    if (keyManager.isPostgreSQL()) {
      return baseSql + " ON CONFLICT (table_name, column_name) DO UPDATE SET "
          + "encryption_algorithm = EXCLUDED.encryption_algorithm, "
          + "key_id = EXCLUDED.key_id, "
          + "updated_at = EXCLUDED.updated_at";
    } else {
      return baseSql + " ON DUPLICATE KEY UPDATE "
          + "encryption_algorithm = VALUES(encryption_algorithm), "
          + "key_id = VALUES(key_id), "
          + "updated_at = VALUES(updated_at)";
    }
  }

  private String getUpdateEncryptionMetadataKeySql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "UPDATE " + schema + ".encryption_metadata SET key_id = ?, updated_at = ? "
        + "WHERE table_name = ? AND column_name = ?";
  }

  private String getSelectColumnsWithKeySql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "SELECT table_name, column_name FROM " + schema + ".encryption_metadata WHERE key_id = ?";
  }

  private String getDeleteEncryptionMetadataSql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "DELETE FROM " + schema + ".encryption_metadata WHERE table_name = ? AND column_name = ?";
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

    LOGGER.info(() -> Messages.get("KeyManagementUtility.creatingMasterKey", new Object[]{description}));

    try {
      CreateKeyRequest.Builder requestBuilder =
          CreateKeyRequest.builder()
              .description(description)
              .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
              .keySpec(KeySpec.SYMMETRIC_DEFAULT);

      // Add key policy if provided
      if (keyPolicy != null && !keyPolicy.trim().isEmpty()) {
        requestBuilder.policy(keyPolicy);
        LOGGER.finest(() -> Messages.get("KeyManagementUtility.usingCustomPolicy"));
      }

      CreateKeyResponse response = kmsClient.createKey(requestBuilder.build());
      String keyArn = response.keyMetadata().arn();

      // Create an alias for easier management
      String aliasName = "alias/jdbc-encryption-" + System.currentTimeMillis();
      CreateAliasRequest aliasRequest =
          CreateAliasRequest.builder().aliasName(aliasName).targetKeyId(keyArn).build();

      kmsClient.createAlias(aliasRequest);

      LOGGER.info(() -> Messages.get("KeyManagementUtility.masterKeyCreated", new Object[]{keyArn, aliasName}));
      return keyArn;

    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get("KeyManagementUtility.masterKeyFailed", new Object[]{e.getMessage()}));
      throw new KeyManagementException(
          Messages.get("KeyManagementUtility.createMasterKeyFailed", new Object[]{e.getMessage(), e}));
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
   * @throws KeyManagementException if key generation or storage fails
   */
  public void generateAndStoreDataKey(
      String tableName, String columnName, String masterKeyArn, String algorithm)
      throws KeyManagementException {
    Objects.requireNonNull(tableName, "Table name cannot be null");
    Objects.requireNonNull(columnName, "Column name cannot be null");
    Objects.requireNonNull(masterKeyArn, "Master key ARN cannot be null");

    if (algorithm == null || algorithm.trim().isEmpty()) {
      algorithm = EncryptionAlgorithm.AES_256_GCM;
    }

    LOGGER.info(() -> Messages.get(
        "KeyManagementUtility.generatingDataKey", new Object[]{tableName, columnName, masterKeyArn}));

    try {

      // Generate the data key using KMS
      KeyManager.DataKeyResult dataKeyResult = keyManager.generateDataKey(masterKeyArn);

      // Generate a unique key name
      String keyName = "key-" + tableName + "-" + columnName + "-" + System.currentTimeMillis();

      // Generate HMAC key for data integrity verification
      byte[] hmacKey = new byte[32];
      new SecureRandom().nextBytes(hmacKey);

      // Create key metadata
      KeyMetadata keyMetadata =
          KeyMetadata.builder()
              .keyName(keyName)
              .masterKeyArn(masterKeyArn)
              .encryptedDataKey(dataKeyResult.getEncryptedKey())
              .hmacKey(hmacKey)
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

      LOGGER.info(() -> Messages.get(
          "KeyManagementUtility.dataKeyStored", new Object[]{tableName, columnName, generatedKeyId}));


    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get(
          "KeyManagementUtility.dataKeyStoreFailed", new Object[]{tableName, columnName, e.getMessage()}));
      throw new KeyManagementException(
          Messages.get("KeyManagementUtility.generateDataKeyFailed",
              new Object[]{e.getMessage()}), e);
    }
  }

  /**
   * Rotates the data key for an existing encrypted column. This creates a new data key while
   * preserving the existing encryption metadata.
   *
   * @param tableName       Name of the table
   * @param columnName      Name of the column
   * @param newMasterKeyArn Optional new master key ARN. If null, uses existing master key
   * @throws KeyManagementException if key rotation fails
   */
  public void rotateDataKey(String tableName, String columnName, @Nullable String newMasterKeyArn)
      throws KeyManagementException {
    Objects.requireNonNull(tableName, "Table name cannot be null");
    Objects.requireNonNull(columnName, "Column name cannot be null");

    LOGGER.info(() -> Messages.get("KeyManagementUtility.rotatingKey", new Object[]{tableName, columnName}));

    try {
      // Get current encryption configuration
      ColumnEncryptionConfig currentConfig = metadataManager.getColumnConfig(tableName, columnName);
      if (currentConfig == null) {
        throw new KeyManagementException(
            Messages.get("KeyManagementUtility.noEncryptionConfigExc", new Object[]{tableName + "." + columnName}));
      }

      // Use existing master key if new one not provided
      String masterKeyArn =
          newMasterKeyArn != null
              ? newMasterKeyArn
              : currentConfig.getKeyMetadata().getMasterKeyArn();

      // Generate new data key
      KeyManager.DataKeyResult dataKeyResult = keyManager.generateDataKey(masterKeyArn);

      // Generate a unique key name
      String keyName = "key-" + tableName + "-" + columnName + "-" + System.currentTimeMillis();

      // Generate HMAC key for data integrity verification
      byte[] hmacKey = new byte[32];
      new SecureRandom().nextBytes(hmacKey);

      // Create new key metadata
      KeyMetadata newKeyMetadata =
          KeyMetadata.builder()
              .keyName(keyName)
              .masterKeyArn(masterKeyArn)
              .encryptedDataKey(dataKeyResult.getEncryptedKey())
              .hmacKey(hmacKey)
              .keySpec("AES_256")
              .createdAt(Instant.now())
              .lastUsedAt(Instant.now())
              .build();

      // Store new key metadata and get generated ID
      int newKeyId = keyManager.storeKeyMetadata(tableName, columnName, newKeyMetadata);

      // Update encryption metadata to use new key
      updateEncryptionMetadataKey(tableName, columnName, newKeyId);


      LOGGER.info(() -> Messages.get(
          "KeyManagementUtility.keyRotated", new Object[]{tableName, columnName, currentConfig.getKeyId(), newKeyId}));

    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get(
          "KeyManagementUtility.keyRotationFailed", new Object[]{tableName, columnName, e.getMessage()}));
      throw new KeyManagementException(
          Messages.get("KeyManagementUtility.rotateDataKeyFailed", new Object[]{e.getMessage(), e}));
    }
  }

  /**
   * Initializes encryption for a new table and column combination. This is a convenience method
   * that creates everything needed for encryption.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param masterKeyArn ARN of the master key to use
   * @throws KeyManagementException if initialization fails
   */
  public void initializeEncryptionForColumn(
      String tableName, String columnName, String masterKeyArn) throws KeyManagementException {
    initializeEncryptionForColumn(tableName, columnName, masterKeyArn, EncryptionAlgorithm.AES_256_GCM);
  }

  /**
   * Initializes encryption for a new table and column combination with specified algorithm.
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param masterKeyArn ARN of the master key to use
   * @param algorithm Encryption algorithm to use
   * @throws KeyManagementException if initialization fails
   */
  public void initializeEncryptionForColumn(
      String tableName, String columnName, String masterKeyArn, String algorithm)
      throws KeyManagementException {
    LOGGER.info(() -> Messages.get("KeyManagementUtility.initColumnEncryption", new Object[]{tableName, columnName}));

    // Check if column is already encrypted
    try {
      if (metadataManager.isColumnEncrypted(tableName, columnName)) {
        throw new KeyManagementException(Messages.get(
            "KeyManagementUtility.columnAlreadyEncrypted",
            new Object[]{tableName + "." + columnName
                + " is already encrypted"}));
      }
    } catch (MetadataException e) {
      throw new KeyManagementException(Messages.get("KeyManagementUtility.checkEncryptionStatusFailed"), e);
    }

    // Generate and store the data key
    generateAndStoreDataKey(tableName, columnName, masterKeyArn, algorithm);
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

    LOGGER.info(() -> Messages.get("KeyManagementUtility.removingEncryption", new Object[]{tableName, columnName}));

    Connection conn = null;
    try {
      conn = getConnection();
      try (PreparedStatement stmt = conn.prepareStatement(getDeleteEncryptionMetadataSql())) {

        stmt.setString(1, tableName);
        stmt.setString(2, columnName);

        int rowsAffected = stmt.executeUpdate();
        if (rowsAffected == 0) {
          LOGGER.warning(() -> Messages.get(
              "KeyManagementUtility.noEncryptionConfig", new Object[]{tableName, columnName}));
        } else {
          LOGGER.info(() -> Messages.get(
              "KeyManagementUtility.encryptionRemoved", new Object[]{tableName, columnName}));
        }

        // Refresh metadata cache
        metadataManager.refreshMetadata();
      }
    } catch (MetadataException e) {
      LOGGER.severe(() -> Messages.get("KeyManagementUtility.refreshAfterRemoveFailed", new Object[]{e}));
      throw new KeyManagementException(
          Messages.get("KeyManagementUtility.refreshMetadataFailed", new Object[]{e.getMessage(), e}));
    } catch (SQLException e) {
      LOGGER.severe(() -> Messages.get(
          "KeyManagementUtility.removeEncryptionFailed", new Object[]{tableName, columnName, e}));
      throw new KeyManagementException(
          Messages.get("KeyManagementUtility.removeEncryptionConfigFailed",
              new Object[]{e.getMessage()}), e);
    } finally {
      try {
        closeConnection(conn);
      } catch (SQLException e) {
        LOGGER.warning(() -> Messages.get("KeyManagementUtility.closeConnectionFailed", new Object[]{e.getMessage()}));
      }
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

    LOGGER.finest(() -> Messages.get("KeyManagementUtility.findingColumns", new Object[]{keyId}));

    Connection conn = null;
    try {
      conn = getConnection();
      try (PreparedStatement stmt = conn.prepareStatement(getSelectColumnsWithKeySql())) {

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
      }

    } catch (SQLException e) {
      LOGGER.severe(() -> Messages.get("KeyManagementUtility.findColumnsFailed", new Object[]{keyId, e.getMessage()}));
      throw new KeyManagementException(
          Messages.get("KeyManagementUtility.findColumnsForKeyFailed", new Object[]{e.getMessage(), e}));
    } finally {
      try {
        closeConnection(conn);
      } catch (SQLException e) {
        LOGGER.warning(() -> Messages.get("KeyManagementUtility.closeConnectionFailed2", new Object[]{e.getMessage()}));
      }
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

    LOGGER.finest(() -> Messages.get("KeyManagementUtility.validatingMasterKey", new Object[]{masterKeyArn}));

    try {
      DescribeKeyRequest request = DescribeKeyRequest.builder().keyId(masterKeyArn).build();

      DescribeKeyResponse response = kmsClient.describeKey(request);
      software.amazon.awssdk.services.kms.model.KeyMetadata keyMetadata = response.keyMetadata();

      boolean isValid =
          keyMetadata.enabled()
              && keyMetadata.keyState() == KeyState.ENABLED
              && keyMetadata.keyUsage() == KeyUsageType.ENCRYPT_DECRYPT;

      LOGGER.finest(() -> Messages.get("KeyManagementUtility.masterKeyValidated", new Object[]{masterKeyArn, isValid}));
      return isValid;

    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get(
          "KeyManagementUtility.masterKeyValidationFailed", new Object[]{masterKeyArn, e.getMessage()}));
      throw new KeyManagementException(
          Messages.get("KeyManagementUtility.validateMasterKeyFailed", new Object[]{e.getMessage(), e}));
    }
  }

  /** Stores encryption metadata in the database. */
  private void storeEncryptionMetadata(
      String tableName, String columnName, String algorithm, int keyId) throws SQLException {
    Connection conn = null;
    try {
      conn = getConnection();
      try (PreparedStatement stmt = conn.prepareStatement(getInsertEncryptionMetadataSql())) {

        Timestamp now = Timestamp.from(Instant.now());

        stmt.setString(1, tableName);
        stmt.setString(2, columnName);
        stmt.setString(3, algorithm);
        stmt.setInt(4, keyId);
        stmt.setTimestamp(5, now);
        stmt.setTimestamp(6, now);

        int rowsAffected = stmt.executeUpdate();
        if (rowsAffected == 0) {
          throw new SQLException(Messages.get("KeyManagementUtility.storeMetadataNoRows"));
        }

        LOGGER.finest(() -> Messages.get("KeyManagementUtility.metadataStored", new Object[]{tableName, columnName}));
      }
    } finally {
      try {
        closeConnection(conn);
      } catch (SQLException e) {
        LOGGER.warning(() -> Messages.get("KeyManagementUtility.closeConnectionFailed3", new Object[]{e.getMessage()}));
      }
    }
  }

  /** Updates the key ID for existing encryption metadata. */
  private void updateEncryptionMetadataKey(String tableName, String columnName, int newKeyId)
      throws SQLException {
    Connection conn = null;
    try {
      conn = getConnection();
      try (PreparedStatement stmt = conn.prepareStatement(getUpdateEncryptionMetadataKeySql())) {

        stmt.setInt(1, newKeyId);
        stmt.setTimestamp(2, Timestamp.from(Instant.now()));
        stmt.setString(3, tableName);
        stmt.setString(4, columnName);

        int rowsAffected = stmt.executeUpdate();
        if (rowsAffected == 0) {
          throw new SQLException(Messages.get("KeyManagementUtility.updateMetadataKeyNoRows"));
        }

        LOGGER.finest(() -> Messages.get(
            "KeyManagementUtility.metadataKeyUpdated", new Object[]{tableName, columnName, newKeyId}));
      }
    } finally {
      try {
        closeConnection(conn);
      } catch (SQLException e) {
        LOGGER.warning(() -> Messages.get("KeyManagementUtility.closeConnectionFailed4", new Object[]{e.getMessage()}));
      }
    }
  }
}
