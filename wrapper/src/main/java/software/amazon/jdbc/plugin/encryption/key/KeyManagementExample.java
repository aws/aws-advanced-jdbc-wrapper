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

import java.time.Duration;
import java.util.List;
import java.util.logging.Logger;
import javax.sql.DataSource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

/**
 * Example demonstrating how to use the KeyManagementUtility for administrative tasks. This class
 * shows typical workflows for setting up and managing encryption keys.
 */
public class KeyManagementExample {

  private static final Logger LOGGER = Logger.getLogger(KeyManagementExample.class.getName());

  private final KeyManagementUtility keyManagementUtility;

  public KeyManagementExample(DataSource dataSource, KmsClient kmsClient) {
    // Create encryption configuration
    EncryptionConfig config =
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .defaultMasterKeyArn("arn:aws:kms:us-east-1:123456789012:key/default-key")
            .cacheEnabled(true)
            .cacheExpirationMinutes(30)
            .maxRetries(3)
            .retryBackoffBase(Duration.ofMillis(100))
            .build();

    // Create managers
    KeyManager keyManager = null; // new KeyManager(kmsClient, dataSource, config);
    MetadataManager metadataManager = null; // new MetadataManager(dataSource, config);

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
    String userEmailKeyId =
        keyManagementUtility.initializeEncryptionForColumn("users", "email", masterKeyArn);

    String userSsnKeyId =
        keyManagementUtility.initializeEncryptionForColumn("users", "ssn", masterKeyArn);

    String orderCreditCardKeyId =
        keyManagementUtility.initializeEncryptionForColumn(
            "orders", "credit_card_number", masterKeyArn);

    LOGGER.info(
        () -> String.format("Initialized encryption for users.email with key: %s", userEmailKeyId));
    LOGGER.info(
        () -> String.format("Initialized encryption for users.ssn with key: %s", userSsnKeyId));
    LOGGER.info(
        () ->
            String.format(
                "Initialized encryption for orders.credit_card_number with key: %s",
                orderCreditCardKeyId));
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
    String keyId =
        keyManagementUtility.initializeEncryptionForColumn(
            "customers", "phone_number", masterKeyArn, "AES-256-GCM");

    LOGGER.info(
        () -> String.format("Added encryption to customers.phone_number with key: %s", keyId));
  }

  /**
   * Example: Rotating keys for security compliance.
   *
   * @throws KeyManagementException if key management operations fail
   */
  public void performKeyRotation() throws KeyManagementException {
    LOGGER.info("Performing key rotation for security compliance");

    // Rotate key for a specific column
    String newKeyId = keyManagementUtility.rotateDataKey("users", "ssn", null);
    LOGGER.info(() -> String.format("Rotated key for users.ssn, new key ID: %s", newKeyId));

    // Rotate with a new master key
    String newMasterKeyArn =
        keyManagementUtility.createMasterKeyWithPermissions("New Master Key for Enhanced Security");

    String newKeyIdWithNewMaster =
        keyManagementUtility.rotateDataKey("orders", "credit_card_number", newMasterKeyArn);

    LOGGER.info(
        () ->
            String.format(
                "Rotated key for orders.credit_card_number with new master key, new key ID: %s",
                newKeyIdWithNewMaster));
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
      // example.performKeyRotation();
      // example.auditExistingKeys();
      // example.removeEncryptionFromColumn();

      LOGGER.info("Key management examples completed successfully");

    } catch (Exception e) {
      LOGGER.severe(() -> String.format("Error running key management examples", e));
    }
  }
}
