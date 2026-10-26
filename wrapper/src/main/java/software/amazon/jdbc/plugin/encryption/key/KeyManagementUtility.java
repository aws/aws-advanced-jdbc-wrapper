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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.jdbc.plugin.encryption.KmsEncryptionConnectionPlugin;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.plugin.encryption.service.EncryptionAlgorithm;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.util.Messages;

/**
 * Utility class providing administrative functions for key management operations. This class offers
 * high-level methods for creating master keys, setting up encryption for tables/columns, rotating
 * keys, and managing the encryption lifecycle.
 */
public class KeyManagementUtility {

  private static final Logger LOGGER = Logger.getLogger(KeyManagementUtility.class.getName());

  /** Default number of rows per transaction for data key re-encryption. */
  public static final int DEFAULT_REENCRYPTION_BATCH_SIZE = 1000;

  /** Maximum number of rows per transaction for data key re-encryption. */
  public static final int MAX_REENCRYPTION_BATCH_SIZE = 100_000;

  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");

  private final EncryptionService encryptionService = new EncryptionService();
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

  private void closeConnection(final @Nullable Connection conn) throws SQLException {
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

  private String getSelectCurrentKeySql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "SELECT em.key_id, em.encryption_algorithm, ks.master_key_arn, ks.encrypted_data_key, ks.hmac_key "
        + "FROM " + schema + ".encryption_metadata em "
        + "JOIN " + schema + ".key_storage ks ON em.key_id = ks.id "
        + "WHERE em.table_name = ? AND em.column_name = ?";
  }

  private String getSelectKeyByIdSql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "SELECT id, master_key_arn, encrypted_data_key, hmac_key FROM " + schema + ".key_storage WHERE id = ?";
  }

  private String getSwitchEncryptionMetadataKeySql() {
    SchemaName schema = config.getEncryptionMetadataSchema();
    return "UPDATE " + schema + ".encryption_metadata SET key_id = ?, updated_at = ? "
        + "WHERE table_name = ? AND column_name = ? AND key_id = ?";
  }

  /** Keyset page of (pk, value) rows. Identifiers must be validated before calling this. */
  private String getSelectPageSql(
      String tableName, String columnName, String primaryKeyColumn, boolean firstPage, boolean forUpdate) {
    final String pk = quoteIdentifier(primaryKeyColumn);
    final String column = quoteIdentifier(columnName);
    return "SELECT " + pk + ", " + column + " FROM " + quoteTableName(tableName)
        + " WHERE " + column + " IS NOT NULL AND " + pk + " IS NOT NULL"
        + (firstPage ? "" : " AND " + pk + " > ?")
        + " ORDER BY " + pk + " LIMIT ?"
        + (forUpdate ? " FOR UPDATE" : "");
  }

  /** Identifiers must be validated before calling this. */
  private String getUpdateEncryptedValueSql(String tableName, String columnName, String primaryKeyColumn) {
    final String column = quoteIdentifier(columnName);
    return "UPDATE " + quoteTableName(tableName) + " SET " + column + " = ?"
        + " WHERE " + quoteIdentifier(primaryKeyColumn) + " = ? AND " + column + " = ?";
  }

  private String getSetTriggerHmacCacheSql() {
    return "SELECT set_config(?, ?, false)";
  }

  /**
   * Creates a new KMS master key with proper permissions for encryption operations.
   *
   * @param description Description for the master key
   * @param keyPolicy Optional key policy JSON string. If null, uses default policy
   * @return The ARN of the created master key
   * @throws KeyManagementException if key creation fails
   */
  public String createMasterKeyWithPermissions(String description, final @Nullable String keyPolicy)
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
   * Creates a new data key for the column and makes it the column's active key, without
   * re-encrypting existing rows.
   *
   * <p><b>Warning:</b> this method only inserts a new {@code key_storage} row and repoints
   * {@code encryption_metadata.key_id}. It does not re-encrypt the values already stored in the
   * column:
   *
   * <ul>
   *   <li>Every value encrypted under the previous data key fails to decrypt through the plugin
   *       after this call ({@code HMAC verification failed}).
   *   <li>Running plugin instances keep encrypting new values with the previous key until their
   *       metadata cache refreshes ({@code metadataCacheRefreshIntervalMs}, 5 minutes by default).
   *   <li>The previous {@code key_storage} row is kept but no longer referenced. Do not delete it,
   *       and do not disable or delete the KMS key that wraps it, while any row or backup still
   *       holds data encrypted under it. Without them that data cannot be recovered.
   * </ul>
   *
   * <p>This method is safe only for a column that holds no encrypted data. To rotate the data key
   * of a column that holds data, use {@link #rotateDataKeyAndReencrypt(String, String, String,
   * String, int)}. If this method was already called on a column with data, re-encrypt the
   * remaining rows with {@link #reencryptRemainingRows(String, String, String, List, int)}, passing
   * the previous key id. To rotate the KMS key instead, enable KMS automatic key rotation; it is
   * transparent to the plugin.
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
   * Rotates the column's data key and re-encrypts every stored value under the new key.
   *
   * <p>Steps, in this order:
   *
   * <ol>
   *   <li>Pre-check (read only): every non-null value must verify against the current HMAC key.
   *       Otherwise the method throws and nothing is changed.
   *   <li>Creates a new data key and HMAC key (under {@code newMasterKeyArn}, or under the current
   *       master key if it is null) and stores them in {@code key_storage}.
   *   <li>Switches {@code encryption_metadata.key_id} to the new key and commits.
   *   <li>Re-encrypts the rows in primary key order, {@code batchSize} rows per transaction.
   * </ol>
   *
   * <p>The previous {@code key_storage} row and its KMS key are never deleted or changed. Keep them
   * while any row or backup holds data encrypted under them.
   *
   * <p>Run this method on a plain JDBC connection without the KMS encryption plugin, during a
   * maintenance window. The connection's auto-commit mode is changed while the method runs and
   * restored afterwards; pass a dedicated connection with no open transaction. While the batches
   * run, reads through the plugin fail for rows that are not converted yet. If a batch fails, the
   * committed batches are kept and the thrown exception names the previous key id; finish with
   * {@link #reencryptRemainingRows(String, String, String, List, int)}. Running plugin instances
   * keep writing with the previous key until their metadata cache refreshes, so run
   * {@code reencryptRemainingRows} again after {@code metadataCacheRefreshIntervalMs}.
   *
   * @param tableName Name of the table, optionally schema-qualified ({@code schema.table}). Letters,
   *     digits and underscores only.
   * @param columnName Name of the encrypted column
   * @param primaryKeyColumn Name of a single, unique, NOT NULL column used to page through the
   *     table. Rows with a NULL value in this column are not processed.
   * @param newMasterKeyArn Optional new master key ARN. If null, uses the current master key
   * @param batchSize Rows per transaction, 1 to {@link #MAX_REENCRYPTION_BATCH_SIZE}
   * @return the counts of re-encrypted, already-current and unmatched rows
   * @throws KeyManagementException if the pre-check, the key creation, the switch or a batch fails
   * @throws IllegalArgumentException if an identifier or the batch size is invalid
   */
  public DataKeyReencryptionResult rotateDataKeyAndReencrypt(
      String tableName, String columnName, String primaryKeyColumn,
      @Nullable String newMasterKeyArn, int batchSize) throws KeyManagementException {
    validateReencryptionArguments(tableName, columnName, primaryKeyColumn, batchSize);

    Connection conn = null;
    boolean originalAutoCommit = true;
    boolean autoCommitChanged = false;
    final List<StoredKey> keysToClear = new ArrayList<>();
    try {
      conn = getConnection();
      ensurePlainConnection(conn);
      originalAutoCommit = conn.getAutoCommit();

      final CurrentColumnKey current = loadCurrentKey(conn, tableName, columnName);
      final StoredKey oldKey = current.key;
      keysToClear.add(oldKey);
      LOGGER.info(() -> Messages.get("KeyManagementUtility.rotationStarting",
          new Object[]{tableName, columnName, String.valueOf(oldKey.id), String.valueOf(batchSize)}));

      precheckRowsUnderKey(conn, tableName, columnName, primaryKeyColumn, oldKey, batchSize);

      final String masterKeyArn = newMasterKeyArn != null ? newMasterKeyArn : oldKey.masterKeyArn;
      final KeyManager.DataKeyResult dataKeyResult = keyManager.generateDataKey(masterKeyArn);
      final byte[] hmacKey = new byte[32];
      new SecureRandom().nextBytes(hmacKey);
      final String keyName = "key-" + tableName + "-" + columnName + "-" + System.currentTimeMillis();
      final KeyMetadata newKeyMetadata =
          KeyMetadata.builder()
              .keyName(keyName)
              .masterKeyArn(masterKeyArn)
              .encryptedDataKey(dataKeyResult.getEncryptedKey())
              .hmacKey(hmacKey)
              .keySpec("AES_256")
              .createdAt(Instant.now())
              .lastUsedAt(Instant.now())
              .build();
      final int newKeyId = keyManager.storeKeyMetadata(tableName, columnName, newKeyMetadata);
      final StoredKey newKey = new StoredKey(newKeyId, masterKeyArn, dataKeyResult.getEncryptedKey(), hmacKey);
      keysToClear.add(newKey);

      conn.setAutoCommit(false);
      autoCommitChanged = true;
      final int switched;
      try (PreparedStatement stmt = conn.prepareStatement(getSwitchEncryptionMetadataKeySql())) {
        stmt.setInt(1, newKeyId);
        stmt.setTimestamp(2, Timestamp.from(Instant.now()));
        stmt.setString(3, tableName);
        stmt.setString(4, columnName);
        stmt.setInt(5, oldKey.id);
        switched = stmt.executeUpdate();
      }
      if (switched == 0) {
        rollbackQuietly(conn);
        throw new KeyManagementException(Messages.get("KeyManagementUtility.rotationSwitchConflict",
            new Object[]{tableName, columnName, String.valueOf(oldKey.id), String.valueOf(newKeyId)}));
      }
      conn.commit();
      LOGGER.info(() -> Messages.get("KeyManagementUtility.rotationKeySwitched",
          new Object[]{tableName, columnName, String.valueOf(newKeyId), String.valueOf(oldKey.id)}));

      try {
        metadataManager.refreshMetadata();
      } catch (MetadataException e) {
        LOGGER.warning(() -> Messages.get("KeyManagementUtility.refreshAfterRotationFailed",
            new Object[]{tableName, columnName, e.getMessage()}));
      }

      return reencryptRows(conn, tableName, columnName, primaryKeyColumn, newKey, current.algorithm,
          Collections.singletonList(oldKey), batchSize);

    } catch (KeyManagementException e) {
      throw e;
    } catch (SQLException | RuntimeException e) {
      if (autoCommitChanged) {
        rollbackQuietly(conn);
      }
      throw new KeyManagementException(Messages.get("KeyManagementUtility.rotateAndReencryptFailed",
          new Object[]{tableName, columnName, e.getMessage()}), e);
    } finally {
      restoreAutoCommit(conn, originalAutoCommit, autoCommitChanged);
      closeConnectionQuietly(conn);
      for (StoredKey key : keysToClear) {
        key.clear();
      }
    }
  }

  /**
   * Re-encrypts the values that are still encrypted under one of {@code oldKeyIds} into the
   * column's current key. The current key is read from the database, not from the metadata cache.
   * Uses the same batching, ordering and HMAC matching as {@link #rotateDataKeyAndReencrypt}. The
   * method is idempotent and safe to rerun: values already under the current key are skipped, and
   * values that match neither the current key nor any old key are counted, reported (first
   * {@link DataKeyReencryptionResult#MAX_REPORTED_UNMATCHED_KEYS} primary keys) and left unchanged.
   *
   * <p>Use it to resume a failed {@code rotateDataKeyAndReencrypt}, to convert rows written by
   * stale plugin instances after a rotation, and to recover a column after
   * {@link #rotateDataKey(String, String, String)} was called on it. Run it on a plain JDBC
   * connection without the KMS encryption plugin. Old key rows and KMS keys are never deleted.
   *
   * @param tableName Name of the table, optionally schema-qualified ({@code schema.table})
   * @param columnName Name of the encrypted column
   * @param primaryKeyColumn Name of a single, unique, NOT NULL column used to page through the table
   * @param oldKeyIds Ids of the previous {@code key_storage} rows the remaining values are encrypted
   *     with. Must not contain the current key id.
   * @param batchSize Rows per transaction, 1 to {@link #MAX_REENCRYPTION_BATCH_SIZE}
   * @return the counts of re-encrypted, already-current and unmatched rows
   * @throws KeyManagementException if a key cannot be loaded or a batch fails
   * @throws IllegalArgumentException if an identifier, the key id list or the batch size is invalid
   */
  public DataKeyReencryptionResult reencryptRemainingRows(
      String tableName, String columnName, String primaryKeyColumn,
      List<Integer> oldKeyIds, int batchSize) throws KeyManagementException {
    validateReencryptionArguments(tableName, columnName, primaryKeyColumn, batchSize);
    Objects.requireNonNull(oldKeyIds, Messages.get("KeyManagementUtility.oldKeyIdsNull"));
    final Set<Integer> uniqueOldKeyIds = new LinkedHashSet<>();
    for (Integer oldKeyId : oldKeyIds) {
      if (oldKeyId == null) {
        throw new IllegalArgumentException(Messages.get("KeyManagementUtility.oldKeyIdsEmpty"));
      }
      uniqueOldKeyIds.add(oldKeyId);
    }
    if (uniqueOldKeyIds.isEmpty()) {
      throw new IllegalArgumentException(Messages.get("KeyManagementUtility.oldKeyIdsEmpty"));
    }

    Connection conn = null;
    boolean originalAutoCommit = true;
    boolean autoCommitChanged = false;
    final List<StoredKey> keysToClear = new ArrayList<>();
    try {
      conn = getConnection();
      ensurePlainConnection(conn);
      originalAutoCommit = conn.getAutoCommit();

      final CurrentColumnKey current = loadCurrentKey(conn, tableName, columnName);
      keysToClear.add(current.key);
      final List<StoredKey> oldKeys = new ArrayList<>();
      for (Integer oldKeyId : uniqueOldKeyIds) {
        if (oldKeyId == current.key.id) {
          throw new KeyManagementException(Messages.get("KeyManagementUtility.oldKeyIsCurrent",
              new Object[]{String.valueOf(oldKeyId), tableName, columnName}));
        }
        final StoredKey oldKey = loadKeyById(conn, oldKeyId);
        if (oldKey == null) {
          throw new KeyManagementException(Messages.get("KeyManagementUtility.oldKeyNotFound",
              new Object[]{String.valueOf(oldKeyId)}));
        }
        oldKeys.add(oldKey);
        keysToClear.add(oldKey);
      }

      conn.setAutoCommit(false);
      autoCommitChanged = true;
      return reencryptRows(conn, tableName, columnName, primaryKeyColumn, current.key, current.algorithm,
          oldKeys, batchSize);

    } catch (KeyManagementException e) {
      throw e;
    } catch (SQLException | RuntimeException e) {
      if (autoCommitChanged) {
        rollbackQuietly(conn);
      }
      throw new KeyManagementException(Messages.get("KeyManagementUtility.reencryptRemainingRowsFailed",
          new Object[]{tableName, columnName, e.getMessage()}), e);
    } finally {
      restoreAutoCommit(conn, originalAutoCommit, autoCommitChanged);
      closeConnectionQuietly(conn);
      for (StoredKey key : keysToClear) {
        key.clear();
      }
    }
  }

  /**
   * Batch loop shared by {@link #rotateDataKeyAndReencrypt} and {@link #reencryptRemainingRows}.
   * The connection must be in manual-commit mode. Each batch is one transaction.
   */
  private DataKeyReencryptionResult reencryptRows(
      Connection conn, String tableName, String columnName, String primaryKeyColumn,
      StoredKey targetKey, String algorithm, List<StoredKey> sourceKeys, int batchSize)
      throws KeyManagementException {
    final List<Integer> sourceKeyIds = new ArrayList<>();
    for (StoredKey sourceKey : sourceKeys) {
      sourceKeyIds.add(sourceKey.id);
    }
    LOGGER.info(() -> Messages.get("KeyManagementUtility.reencryptStarting",
        new Object[]{tableName, columnName, String.valueOf(targetKey.id), sourceKeyIds.toString(),
            String.valueOf(batchSize)}));

    long reencryptedCount = 0;
    long alreadyCurrentCount = 0;
    long unmatchedCount = 0;
    int batchCount = 0;
    int batchNumber = 0;
    final List<Object> unmatchedPrimaryKeys = new ArrayList<>();
    @Nullable Object lastPk = null;

    while (true) {
      batchNumber++;
      long batchReencrypted = 0;
      long batchAlreadyCurrent = 0;
      final List<Object> batchUnmatched = new ArrayList<>();
      int rowsRead = 0;
      try {
        final byte[] targetDataKey = targetKey.getDataKey(keyManager);
        if (keyManager.isPostgreSQL()) {
          setPostgresTriggerHmacCache(conn, tableName, columnName, targetKey.hmacKey);
        }
        try (PreparedStatement select =
                 conn.prepareStatement(getSelectPageSql(tableName, columnName, primaryKeyColumn,
                     lastPk == null, true));
             PreparedStatement update =
                 conn.prepareStatement(getUpdateEncryptedValueSql(tableName, columnName, primaryKeyColumn))) {
          int paramIndex = 1;
          if (lastPk != null) {
            select.setObject(paramIndex++, lastPk);
          }
          select.setInt(paramIndex, batchSize);

          final List<Object> pendingPks = new ArrayList<>();
          try (ResultSet rs = select.executeQuery()) {
            while (rs.next()) {
              rowsRead++;
              final @Nullable Object pk = rs.getObject(1);
              final byte @Nullable [] value = rs.getBytes(2);
              if (pk == null) {
                continue;
              }
              lastPk = pk;
              if (value == null) {
                continue;
              }
              if (encryptionService.verifyEncryptedData(value, targetKey.hmacKey)) {
                batchAlreadyCurrent++;
                continue;
              }
              final @Nullable StoredKey sourceKey = findMatchingKey(value, sourceKeys);
              if (sourceKey == null) {
                batchUnmatched.add(pk);
                continue;
              }
              final byte[] newValue = encryptionService.reencrypt(value, sourceKey.getDataKey(keyManager),
                  sourceKey.hmacKey, targetDataKey, targetKey.hmacKey, algorithm);
              update.setBytes(1, newValue);
              update.setObject(2, pk);
              update.setBytes(3, value);
              update.addBatch();
              pendingPks.add(pk);
            }
          }

          if (!pendingPks.isEmpty()) {
            final int[] updateCounts = update.executeBatch();
            for (int i = 0; i < pendingPks.size(); i++) {
              final int count = i < updateCounts.length ? updateCounts[i] : 0;
              if (count != Statement.SUCCESS_NO_INFO && count < 1) {
                throw new SQLException(Messages.get("KeyManagementUtility.rowUpdateMissed",
                    new Object[]{String.valueOf(pendingPks.get(i))}));
              }
            }
            batchReencrypted = pendingPks.size();
          }
        }
        conn.commit();
      } catch (SQLException | RuntimeException e) {
        rollbackQuietly(conn);
        throw new KeyManagementException(Messages.get("KeyManagementUtility.reencryptFailed",
            new Object[]{tableName, columnName, String.valueOf(batchNumber), String.valueOf(reencryptedCount),
                e.getMessage(), String.valueOf(targetKey.id), sourceKeyIds.toString()}), e);
      }

      if (rowsRead > 0) {
        batchCount++;
      }
      reencryptedCount += batchReencrypted;
      alreadyCurrentCount += batchAlreadyCurrent;
      unmatchedCount += batchUnmatched.size();
      for (Object pk : batchUnmatched) {
        if (unmatchedPrimaryKeys.size() >= DataKeyReencryptionResult.MAX_REPORTED_UNMATCHED_KEYS) {
          break;
        }
        unmatchedPrimaryKeys.add(pk);
      }
      final int committedBatch = batchNumber;
      final long loggedReencrypted = batchReencrypted;
      final long loggedAlreadyCurrent = batchAlreadyCurrent;
      final int loggedUnmatched = batchUnmatched.size();
      LOGGER.fine(() -> Messages.get("KeyManagementUtility.batchCommitted",
          new Object[]{tableName, columnName, String.valueOf(committedBatch), String.valueOf(loggedReencrypted),
              String.valueOf(loggedAlreadyCurrent), String.valueOf(loggedUnmatched)}));

      if (rowsRead < batchSize) {
        break;
      }
    }

    if (unmatchedCount > 0) {
      final long loggedUnmatchedCount = unmatchedCount;
      LOGGER.warning(() -> Messages.get("KeyManagementUtility.unmatchedRows",
          new Object[]{tableName, columnName, String.valueOf(loggedUnmatchedCount), String.valueOf(targetKey.id),
              sourceKeyIds.toString(), unmatchedPrimaryKeys.toString()}));
    }
    final DataKeyReencryptionResult result = new DataKeyReencryptionResult(tableName, columnName, targetKey.id,
        sourceKeyIds, reencryptedCount, alreadyCurrentCount, unmatchedCount, batchCount, unmatchedPrimaryKeys);
    LOGGER.info(() -> Messages.get("KeyManagementUtility.reencryptCompleted",
        new Object[]{tableName, columnName, String.valueOf(result.getReencryptedCount()),
            String.valueOf(result.getAlreadyCurrentCount()), String.valueOf(result.getUnmatchedCount()),
            String.valueOf(result.getBatchCount())}));
    return result;
  }

  /**
   * Read-only pass of the rotation: every non-null value must verify against the current HMAC key.
   * Throws before any change otherwise.
   */
  private void precheckRowsUnderKey(
      Connection conn, String tableName, String columnName, String primaryKeyColumn,
      StoredKey currentKey, int batchSize) throws SQLException {
    long mismatchCount = 0;
    final List<Object> mismatchedPks = new ArrayList<>();
    @Nullable Object lastPk = null;
    while (true) {
      int rowsRead = 0;
      try (PreparedStatement select =
               conn.prepareStatement(getSelectPageSql(tableName, columnName, primaryKeyColumn,
                   lastPk == null, false))) {
        int paramIndex = 1;
        if (lastPk != null) {
          select.setObject(paramIndex++, lastPk);
        }
        select.setInt(paramIndex, batchSize);
        try (ResultSet rs = select.executeQuery()) {
          while (rs.next()) {
            rowsRead++;
            final @Nullable Object pk = rs.getObject(1);
            final byte @Nullable [] value = rs.getBytes(2);
            if (pk == null) {
              continue;
            }
            lastPk = pk;
            if (value != null && !encryptionService.verifyEncryptedData(value, currentKey.hmacKey)) {
              mismatchCount++;
              if (mismatchedPks.size() < DataKeyReencryptionResult.MAX_REPORTED_UNMATCHED_KEYS) {
                mismatchedPks.add(pk);
              }
            }
          }
        }
      }
      if (rowsRead < batchSize) {
        break;
      }
    }
    if (mismatchCount > 0) {
      throw new KeyManagementException(Messages.get("KeyManagementUtility.rotationPrecheckFailed",
          new Object[]{tableName, columnName, String.valueOf(mismatchCount), String.valueOf(currentKey.id),
              mismatchedPks.toString()}));
    }
  }

  private @Nullable StoredKey findMatchingKey(byte[] value, List<StoredKey> keys) {
    for (StoredKey key : keys) {
      if (encryptionService.verifyEncryptedData(value, key.hmacKey)) {
        return key;
      }
    }
    return null;
  }

  /** Reads the column's current key and algorithm from the database, bypassing the metadata cache. */
  private CurrentColumnKey loadCurrentKey(Connection conn, String tableName, String columnName)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(getSelectCurrentKeySql())) {
      stmt.setString(1, tableName);
      stmt.setString(2, columnName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          throw new KeyManagementException(Messages.get(
              "KeyManagementUtility.noEncryptionConfigExc", new Object[]{tableName + "." + columnName}));
        }
        final StoredKey key = readStoredKey(rs, rs.getInt("key_id"));
        final @Nullable String algorithm = rs.getString("encryption_algorithm");
        return new CurrentColumnKey(key,
            algorithm == null || algorithm.trim().isEmpty() ? EncryptionAlgorithm.AES_256_GCM : algorithm);
      }
    }
  }

  private @Nullable StoredKey loadKeyById(Connection conn, int keyId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(getSelectKeyByIdSql())) {
      stmt.setInt(1, keyId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        return readStoredKey(rs, rs.getInt("id"));
      }
    }
  }

  private StoredKey readStoredKey(ResultSet rs, int keyId) throws SQLException {
    final @Nullable String masterKeyArn = rs.getString("master_key_arn");
    final @Nullable String encryptedDataKey = rs.getString("encrypted_data_key");
    final byte @Nullable [] hmacKey = rs.getBytes("hmac_key");
    if (masterKeyArn == null || encryptedDataKey == null || hmacKey == null) {
      throw new KeyManagementException(Messages.get("KeyManagementUtility.incompleteKeyRow",
          new Object[]{String.valueOf(keyId)}));
    }
    return new StoredKey(keyId, masterKeyArn, encryptedDataKey, hmacKey);
  }

  /**
   * Sets the PostgreSQL HMAC trigger's session cache to the target HMAC key, inside the current
   * batch transaction. Harmless when the trigger is not installed.
   */
  private void setPostgresTriggerHmacCache(
      Connection conn, String tableName, String columnName, byte[] hmacKey) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(getSetTriggerHmacCacheSql())) {
      stmt.setString(1, "hmac_key." + unqualifiedTableName(tableName) + "." + columnName);
      stmt.setString(2, toHex(hmacKey));
      try (ResultSet ignored = stmt.executeQuery()) {
        // set_config returns the new value; nothing to read.
      }
    }
  }

  private void ensurePlainConnection(Connection conn) throws SQLException {
    if (conn.isWrapperFor(KmsEncryptionConnectionPlugin.class)) {
      throw new KeyManagementException(Messages.get("KeyManagementUtility.pluginConnectionNotAllowed"));
    }
  }

  private static void validateReencryptionArguments(
      String tableName, String columnName, String primaryKeyColumn, int batchSize) {
    Objects.requireNonNull(tableName, Messages.get("KeyManagementUtility.tableNameNull"));
    Objects.requireNonNull(columnName, Messages.get("KeyManagementUtility.columnNameNull"));
    Objects.requireNonNull(primaryKeyColumn, Messages.get("KeyManagementUtility.primaryKeyColumnNull"));

    final String[] tableParts = tableName.split("\\.", -1);
    boolean validTable = tableParts.length <= 2;
    for (String part : tableParts) {
      validTable = validTable && isValidIdentifier(part);
    }
    if (!validTable) {
      throw new IllegalArgumentException(
          Messages.get("KeyManagementUtility.invalidTableName", new Object[]{tableName}));
    }
    if (!isValidIdentifier(columnName)) {
      throw new IllegalArgumentException(
          Messages.get("KeyManagementUtility.invalidColumnName", new Object[]{columnName}));
    }
    if (!isValidIdentifier(primaryKeyColumn)) {
      throw new IllegalArgumentException(
          Messages.get("KeyManagementUtility.invalidPrimaryKeyColumn", new Object[]{primaryKeyColumn}));
    }
    if (primaryKeyColumn.equalsIgnoreCase(columnName)) {
      throw new IllegalArgumentException(
          Messages.get("KeyManagementUtility.primaryKeySameAsColumn", new Object[]{primaryKeyColumn}));
    }
    if (batchSize < 1 || batchSize > MAX_REENCRYPTION_BATCH_SIZE) {
      throw new IllegalArgumentException(Messages.get("KeyManagementUtility.invalidBatchSize",
          new Object[]{String.valueOf(MAX_REENCRYPTION_BATCH_SIZE), String.valueOf(batchSize)}));
    }
  }

  private static boolean isValidIdentifier(String identifier) {
    return IDENTIFIER_PATTERN.matcher(identifier).matches();
  }

  private String quoteIdentifier(String identifier) {
    return keyManager.isPostgreSQL() ? "\"" + identifier + "\"" : "`" + identifier + "`";
  }

  private String quoteTableName(String tableName) {
    final int dot = tableName.indexOf('.');
    if (dot < 0) {
      return quoteIdentifier(tableName);
    }
    return quoteIdentifier(tableName.substring(0, dot)) + "." + quoteIdentifier(tableName.substring(dot + 1));
  }

  private static String unqualifiedTableName(String tableName) {
    return tableName.substring(tableName.lastIndexOf('.') + 1);
  }

  private static String toHex(byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16));
      sb.append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }

  private void rollbackQuietly(@Nullable Connection conn) {
    if (conn == null) {
      return;
    }
    try {
      conn.rollback();
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("KeyManagementUtility.rollbackFailed", new Object[]{e.getMessage()}));
    }
  }

  private void restoreAutoCommit(@Nullable Connection conn, boolean originalAutoCommit, boolean changed) {
    if (conn == null || !changed) {
      return;
    }
    try {
      conn.setAutoCommit(originalAutoCommit);
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("KeyManagementUtility.restoreAutoCommitFailed",
          new Object[]{e.getMessage()}));
    }
  }

  private void closeConnectionQuietly(@Nullable Connection conn) {
    try {
      closeConnection(conn);
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("KeyManagementUtility.closeConnectionFailed", new Object[]{e.getMessage()}));
    }
  }

  /** A key_storage row, with its plaintext data key decrypted on first use. */
  private static final class StoredKey {
    private final int id;
    private final String masterKeyArn;
    private final String encryptedDataKey;
    private final byte[] hmacKey;
    private byte @Nullable [] dataKey;

    private StoredKey(int id, String masterKeyArn, String encryptedDataKey, byte[] hmacKey) {
      this.id = id;
      this.masterKeyArn = masterKeyArn;
      this.encryptedDataKey = encryptedDataKey;
      this.hmacKey = hmacKey;
    }

    private byte[] getDataKey(KeyManager keyManager) throws KeyManagementException {
      byte @Nullable [] key = dataKey;
      if (key == null) {
        key = keyManager.decryptDataKey(encryptedDataKey, masterKeyArn);
        dataKey = key;
      }
      return key;
    }

    private void clear() {
      final byte @Nullable [] key = dataKey;
      if (key != null) {
        Arrays.fill(key, (byte) 0);
      }
      dataKey = null;
    }
  }

  /** The current key of a column and the column's algorithm. */
  private static final class CurrentColumnKey {
    private final StoredKey key;
    private final String algorithm;

    private CurrentColumnKey(StoredKey key, String algorithm) {
      this.key = key;
      this.algorithm = algorithm;
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
