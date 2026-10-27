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

package software.amazon;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import javax.sql.DataSource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.key.DataKeyReencryptionResult;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementException;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementUtility;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.service.EncryptionAlgorithm;

/**
 * Example demonstrating how to use the KeyManagementUtility for administrative tasks. This class
 * shows typical workflows for setting up and managing encryption keys.
 */
public class KeyManagementExample {

  private static final Logger LOGGER = Logger.getLogger(KeyManagementExample.class.getName());

  private final KeyManagementUtility keyManagementUtility;

  public KeyManagementExample(DataSource dataSource, KmsClient kmsClient) throws Exception {
    // Create encryption configuration
    EncryptionConfig config =
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .cacheEnabled(true)
            .cacheExpirationMinutes(30)
            .maxRetries(3)
            .retryBackoffBase(Duration.ofMillis(100))
            .build();

    // Create managers using a DataSource connection
    Connection connection = dataSource.getConnection();
    boolean isPostgreSQL = connection.getMetaData().getDatabaseProductName()
        .toLowerCase().contains("postgresql");
    KeyManager keyManager = new KeyManager(kmsClient, connection, isPostgreSQL, config);
    MetadataManager metadataManager = new MetadataManager(dataSource, config);

    // Create utility
    this.keyManagementUtility =
        new KeyManagementUtility(keyManager, metadataManager, dataSource, kmsClient, config);
  }

  /**
   * Example: Setting up encryption for a new application.
   *
   * @throws KeyManagementException if key management operations fail
   */
  public void setupNewApplication() throws KeyManagementException {
    LOGGER.info("Setting up encryption for new application");

    // 1. Create a master key for the application
    String masterKeyArn =
        keyManagementUtility.createMasterKeyWithPermissions("JDBC Encryption Master Key for MyApp");

    LOGGER.info(() -> String.format("Created master key: %s", masterKeyArn));

    // 2. Initialize encryption for sensitive columns
    keyManagementUtility.initializeEncryptionForColumn("users", "email", masterKeyArn);
    keyManagementUtility.initializeEncryptionForColumn("users", "ssn", masterKeyArn);
    keyManagementUtility.initializeEncryptionForColumn(
        "orders", "credit_card_number", masterKeyArn);

    LOGGER.info("Initialized encryption for users.email");
    LOGGER.info("Initialized encryption for users.ssn");
    LOGGER.info("Initialized encryption for orders.credit_card_number");
  }

  /**
   * Example: Adding encryption to an existing column.
   *
   * @throws KeyManagementException if key management operations fail
   */
  public void addEncryptionToExistingColumn() throws KeyManagementException {
    LOGGER.info("Adding encryption to existing column");

    String masterKeyArn = "arn:aws:kms:us-east-1:123456789012:key/existing-master-key";

    // Validate the master key first
    if (!keyManagementUtility.validateMasterKey(masterKeyArn)) {
      throw new KeyManagementException("Master key is not valid or accessible: " + masterKeyArn);
    }

    // Initialize encryption for the column
    keyManagementUtility.initializeEncryptionForColumn(
        "customers", "phone_number", masterKeyArn, EncryptionAlgorithm.AES_256_GCM);

    LOGGER.info("Added encryption to customers.phone_number");
  }

  /**
   * Example: Rotating a column data key with re-encryption.
   *
   * <p>For routine rotation, prefer enabling automatic rotation on the KMS key; it needs no data
   * changes. Rotate a column's data key only when required. Run this during a maintenance window
   * on a plain JDBC connection or DataSource (without the kmsEncryption plugin): reads through the
   * plugin fail for rows that are not converted yet while the batches run.
   *
   * <p>Application instances that still cache the old key keep writing rows under it until their
   * metadata cache refreshes. Convert those rows later with {@link #repairAfterCacheRefresh(int)},
   * passing the returned old key id.
   *
   * @return the previous key id of users.ssn, needed by the later repair pass
   * @throws KeyManagementException if key management operations fail
   */
  public int performKeyRotation() throws KeyManagementException {
    LOGGER.info("Rotating column data keys with re-encryption");

    // Rotate the data key of users.ssn under the same master key. "id" is the primary key column.
    DataKeyReencryptionResult ssnResult = keyManagementUtility.rotateDataKeyAndReencrypt(
        "users", "ssn", "id", null, KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
    LOGGER.info(() -> String.format("Rotated key for users.ssn: %s", ssnResult));

    // Rotate with a new master key
    String newMasterKeyArn =
        keyManagementUtility.createMasterKeyWithPermissions("New Master Key for Enhanced Security");

    DataKeyReencryptionResult cardResult = keyManagementUtility.rotateDataKeyAndReencrypt(
        "orders", "credit_card_number", "id", newMasterKeyArn,
        KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
    LOGGER.info(() -> String.format("Rotated key for orders.credit_card_number: %s", cardResult));

    // Keep the old key_storage rows and KMS keys while backups hold data under them.
    return ssnResult.getSourceKeyIds().get(0);
  }

  /**
   * Example: Converting rows that stale application instances wrote under the old key.
   *
   * <p>This belongs in a separate, later job: run it after {@code metadataCacheRefreshIntervalMs}
   * (5 minutes by default) has passed since {@link #performKeyRotation()}, or after restarting the
   * applications. Running it right after the rotation misses the rows written in that interval.
   *
   * @param oldKeyId the previous key id of users.ssn, returned by {@link #performKeyRotation()}
   * @throws KeyManagementException if key management operations fail
   */
  public void repairAfterCacheRefresh(int oldKeyId) throws KeyManagementException {
    DataKeyReencryptionResult repairResult = keyManagementUtility.reencryptRemainingRows(
        "users", "ssn", "id", Collections.singletonList(oldKeyId),
        KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
    LOGGER.info(() -> String.format("Repair pass for users.ssn: %s", repairResult));
  }

  /**
   * Example: Auditing and managing existing keys.
   *
   * @throws KeyManagementException if key management operations fail
   */
  public void auditExistingKeys() throws KeyManagementException {
    LOGGER.info("Auditing existing encryption keys");

    // Find all columns using a specific key
    String keyIdToAudit = "some-existing-key-id";
    List<String> columnsUsingKey = keyManagementUtility.getColumnsUsingKey(keyIdToAudit);

    LOGGER.info(
        () ->
            String.format(
                "Key %s is used by %s columns: %s",
                keyIdToAudit, columnsUsingKey.size(), columnsUsingKey));

    // Validate all master keys are still accessible
    String[] masterKeysToValidate = {
      "arn:aws:kms:us-east-1:123456789012:key/key1",
      "arn:aws:kms:us-east-1:123456789012:key/key2",
      "arn:aws:kms:us-east-1:123456789012:key/key3"
    };

    for (String masterKeyArn : masterKeysToValidate) {
      boolean isValid = keyManagementUtility.validateMasterKey(masterKeyArn);
      LOGGER.info(
          () ->
              String.format(
                  "Master key %s validation: %s", masterKeyArn, isValid ? "VALID" : "INVALID"));
    }
  }

  /**
   * Example: Removing encryption from a column (for decommissioning).
   *
   * @throws KeyManagementException if key management operations fail
   */
  public void removeEncryptionFromColumn() throws KeyManagementException {
    LOGGER.info("Removing encryption from decommissioned column");

    // Remove encryption configuration (keys remain for data recovery)
    keyManagementUtility.removeEncryptionForColumn("old_table", "deprecated_column");

    LOGGER.info("Removed encryption configuration for old_table.deprecated_column");
  }

  /**
   * Main method demonstrating the complete workflow.
   *
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    try {
      // In a real application, you would configure these properly
      DataSource dataSource = null; // Configure your DataSource
      KmsClient kmsClient = KmsClient.builder().region(Region.US_EAST_1).build();

      KeyManagementExample example = new KeyManagementExample(dataSource, kmsClient);

      // Run examples (commented out since we don't have real connections)
      // example.setupNewApplication();
      // example.addEncryptionToExistingColumn();
      // int oldSsnKeyId = example.performKeyRotation();
      // The repair pass is a separate, later invocation: run it after metadataCacheRefreshIntervalMs
      // (or after restarting the applications), not right after the rotation.
      // example.repairAfterCacheRefresh(oldSsnKeyId);
      // example.auditExistingKeys();
      // example.removeEncryptionFromColumn();

      LOGGER.info("Key management examples completed successfully");

    } catch (Exception e) {
      LOGGER.severe(() -> String.format("Error running key management examples", e));
    }
  }
}
