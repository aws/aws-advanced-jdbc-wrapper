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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementException;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

/** Integration test for KeyManagementUtility functionality. */

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature({
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY
})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@MakeSureFirstInstanceWriter
@Order(18)
public class KeyManagementUtilityIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(KeyManagementUtilityIntegrationTest.class.getName());

  private static final String KMS_KEY_ARN_ENV = "AWS_KMS_KEY_ARN";
  private static final String TEST_TABLE = "users";
  private static final String TEST_COLUMN = "ssn";
  private static final String TEST_ALGORITHM = "AES-256-GCM";

  private Connection connection;
  private KmsClient kmsClient;
  private String masterKeyArn;
  private boolean createdKey = false;

  @BeforeEach
  void setUp() throws Exception {
    // Get or create master key
    final String kmsRegion = TestEnvironment.getCurrent().getInfo().getRegion();
    kmsClient = KmsClient.builder().region(Region.of(kmsRegion)).build();

    masterKeyArn = System.getenv(KMS_KEY_ARN_ENV);
    if (masterKeyArn == null || masterKeyArn.isEmpty()) {
      LOGGER.info("No AWS_KMS_KEY_ARN environment variable found, creating new master key");
      masterKeyArn = createTestMasterKey();
      createdKey = true;
    } else {
      LOGGER.info("Using existing master key from environment: " + masterKeyArn);
    }

    assumeTrue(
        masterKeyArn != null && !masterKeyArn.isEmpty(),
        "KMS Key ARN must be provided via " + KMS_KEY_ARN_ENV + " environment variable");

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_MASTER_KEY_ARN.name, masterKeyArn);
    props.setProperty(EncryptionConfig.KMS_REGION.name, kmsRegion);

    final String url = ConnectionStringHelper.getWrapperUrl();

    connection = DriverManager.getConnection(url, props);

    // Setup test database schema
    setupTestSchema();
    EncryptedDataTypeInstaller.installEncryptedDataType(connection);
    LOGGER.info("Test setup completed with master key: " + masterKeyArn);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (connection != null) {
      try (Statement stmt = connection.createStatement()) {
        // Clean up test data
        stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
        stmt.execute(
            "DELETE FROM encrypt.encryption_metadata WHERE table_name = '" + TEST_TABLE + "'");
        stmt.execute("DELETE FROM encrypt.key_storage WHERE key_id LIKE 'test-%'");
      }
      connection.close();
    }

    if (kmsClient != null) {
      kmsClient.close();
    }
  }

  @TestTemplate
  void testCreateDataKeyAndPopulateMetadata() throws Exception {
    LOGGER.info("Testing data key creation and metadata population for " + TEST_TABLE + "." + TEST_COLUMN);

    // Clean up any existing metadata for this column
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DELETE FROM encrypt.encryption_metadata WHERE table_name = '" + TEST_TABLE + "' AND column_name = '" + TEST_COLUMN + "'");
    }

    // Step 1: Generate a data key using KMS
    GenerateDataKeyResponse dataKeyResponse = executeWithRetry(() ->
        kmsClient.generateDataKey(
            GenerateDataKeyRequest.builder()
                .keyId(masterKeyArn)
                .keySpec(DataKeySpec.AES_256)
                .build()
        )
    );

    byte[] plaintextKey = dataKeyResponse.plaintext().asByteArray();
    String encryptedDataKey = Base64.getEncoder().encodeToString(
        dataKeyResponse.ciphertextBlob().asByteArray()
    );

    // Generate HMAC key (32 bytes for SHA-256)
    byte[] hmacKey = new byte[32];
    new java.security.SecureRandom().nextBytes(hmacKey);

    LOGGER.info("Generated data key using master key: " + masterKeyArn);

    // Step 2: Store the key in key_storage table
    int keyId;
    try (PreparedStatement stmt =
        connection.prepareStatement(
            "INSERT INTO encrypt.key_storage (name, master_key_arn, encrypted_data_key, hmac_key, key_spec) VALUES (?, ?, ?, ?, ?) RETURNING id")) {
      stmt.setString(1, TEST_TABLE + "." + TEST_COLUMN);
      stmt.setString(2, masterKeyArn);
      stmt.setString(3, encryptedDataKey);
      stmt.setBytes(4, hmacKey);
      stmt.setString(5, "AES_256");
      ResultSet rs = stmt.executeQuery();
      rs.next();
      keyId = rs.getInt(1);
      LOGGER.info("Stored key in key_storage with ID: " + keyId);
    }

    // Step 3: Store the encryption metadata referencing the key
    try (PreparedStatement stmt =
        connection.prepareStatement(
            "INSERT INTO encrypt.encryption_metadata (table_name, column_name, encryption_algorithm, key_id) VALUES (?, ?, ?, ?)")) {
      stmt.setString(1, TEST_TABLE);
      stmt.setString(2, TEST_COLUMN);
      stmt.setString(3, TEST_ALGORITHM);
      stmt.setInt(4, keyId);
      stmt.executeUpdate();
      LOGGER.info("Stored encryption metadata referencing key ID: " + keyId);
    }

    // Step 4: Verify the metadata was created correctly
    try (PreparedStatement checkStmt =
        connection.prepareStatement(
            "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, ks.master_key_arn, ks.encrypted_data_key " +
            "FROM encrypt.encryption_metadata em " +
            "JOIN encrypt.key_storage ks ON em.key_id = ks.id " +
            "WHERE em.table_name = ? AND em.column_name = ?")) {
      checkStmt.setString(1, TEST_TABLE);
      checkStmt.setString(2, TEST_COLUMN);
      ResultSet rs = checkStmt.executeQuery();

      assertTrue(rs.next(), "Should find encryption metadata");
      assertEquals(TEST_TABLE, rs.getString("table_name"));
      assertEquals(TEST_COLUMN, rs.getString("column_name"));
      assertEquals(TEST_ALGORITHM, rs.getString("encryption_algorithm"));
      assertEquals(keyId, rs.getInt("key_id"));
      assertEquals(masterKeyArn, rs.getString("master_key_arn"));
      assertNotNull(rs.getString("encrypted_data_key"), "Encrypted data key should be stored");
      LOGGER.info("Verified encryption metadata with key_storage reference");
    }

    // Step 5: Test that the encryption system works with the configured metadata
    String insertSql = "INSERT INTO " + TEST_TABLE + " (name, " + TEST_COLUMN + ") VALUES (?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, "Test User");
      pstmt.setString(2, "123-45-6789");
      int rowsInserted = pstmt.executeUpdate();
      assertEquals(1, rowsInserted, "Should insert one row");
      LOGGER.info("Successfully inserted encrypted data");
    }

    // Step 6: Verify data can be retrieved and decrypted
    String selectSql = "SELECT name, " + TEST_COLUMN + " FROM " + TEST_TABLE + " WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setString(1, "Test User");
      ResultSet rs = pstmt.executeQuery();

      assertTrue(rs.next(), "Should find inserted row");
      assertEquals("Test User", rs.getString("name"));
      assertEquals("123-45-6789", rs.getString(TEST_COLUMN));
      LOGGER.info("Successfully retrieved and decrypted data");
    }

    // Clean up plaintext key from memory
    Arrays.fill(plaintextKey, (byte) 0);
    Arrays.fill(hmacKey, (byte) 0);
  }

  @TestTemplate
  void testEncryptionWithDifferentValues() throws Exception {
    LOGGER.info("Testing encryption with different SSN values");

    // Clean up any existing metadata for this column
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DELETE FROM encrypt.encryption_metadata WHERE table_name = '" + TEST_TABLE + "' AND column_name = '" + TEST_COLUMN + "'");
      stmt.execute("TRUNCATE TABLE " + TEST_TABLE);
    }

    // Step 1: Generate a data key using KMS
    GenerateDataKeyResponse dataKeyResponse = executeWithRetry(() ->
        kmsClient.generateDataKey(
            GenerateDataKeyRequest.builder()
                .keyId(masterKeyArn)
                .keySpec(DataKeySpec.AES_256)
                .build()
        )
    );

    byte[] plaintextKey = dataKeyResponse.plaintext().asByteArray();
    String encryptedDataKey = Base64.getEncoder().encodeToString(
        dataKeyResponse.ciphertextBlob().asByteArray()
    );

    // Generate HMAC key (32 bytes for SHA-256)
    byte[] hmacKey = new byte[32];
    new java.security.SecureRandom().nextBytes(hmacKey);

    LOGGER.info("Generated data key for multiple value test");

    // Step 2: Store the key in key_storage table
    int keyId;
    try (PreparedStatement stmt =
        connection.prepareStatement(
            "INSERT INTO encrypt.key_storage (name, master_key_arn, encrypted_data_key, hmac_key, key_spec) VALUES (?, ?, ?, ?, ?) RETURNING id")) {
      stmt.setString(1, TEST_TABLE + "." + TEST_COLUMN + "_multi");
      stmt.setString(2, masterKeyArn);
      stmt.setString(3, encryptedDataKey);
      stmt.setBytes(4, hmacKey);
      stmt.setString(5, "AES_256");
      ResultSet rs = stmt.executeQuery();
      rs.next();
      keyId = rs.getInt(1);
      LOGGER.info("Stored key in key_storage with ID: " + keyId);
    }

    // Step 3: Setup encryption metadata referencing the key
    try (PreparedStatement stmt =
        connection.prepareStatement(
            "INSERT INTO encrypt.encryption_metadata (table_name, column_name, encryption_algorithm, key_id) VALUES (?, ?, ?, ?)")) {
      stmt.setString(1, TEST_TABLE);
      stmt.setString(2, TEST_COLUMN);
      stmt.setString(3, TEST_ALGORITHM);
      stmt.setInt(4, keyId);
      stmt.executeUpdate();
      LOGGER.info("Setup encryption metadata referencing key ID: " + keyId);
    }

    // Step 4: Test multiple SSN values with same data key
    String[] testSSNs = {"111-11-1111", "222-22-2222", "333-33-3333"};
    String[] testNames = {"Alice", "Bob", "Charlie"};

    // Insert test data using the configured encryption
    String insertSql = "INSERT INTO " + TEST_TABLE + " (name, " + TEST_COLUMN + ") VALUES (?, ?)";
    for (int i = 0; i < testSSNs.length; i++) {
      try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
        pstmt.setString(1, testNames[i]);
        pstmt.setString(2, testSSNs[i]);
        pstmt.executeUpdate();
        LOGGER.info("Inserted encrypted data for " + testNames[i]);
      }
    }

    // Step 5: Verify all data can be retrieved correctly
    String selectSql = "SELECT name, " + TEST_COLUMN + " FROM " + TEST_TABLE + " ORDER BY name";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      ResultSet rs = pstmt.executeQuery();

      int count = 0;
      while (rs.next()) {
        String name = rs.getString("name");
        String ssn = rs.getString(TEST_COLUMN);

        // Find matching test data
        for (int i = 0; i < testNames.length; i++) {
          if (testNames[i].equals(name)) {
            assertEquals(testSSNs[i], ssn, "SSN should match for " + name);
            count++;
            LOGGER.info("Successfully decrypted data for " + name);
            break;
          }
        }
      }

      assertEquals(testSSNs.length, count, "Should retrieve all inserted records");
      LOGGER.info("Successfully verified " + count + " encrypted records");
    }

    // Clean up plaintext key from memory
    Arrays.fill(plaintextKey, (byte) 0);
    Arrays.fill(hmacKey, (byte) 0);
  }

  private KeyManager.DataKeyResult generateDataKey(String masterKeyArn) throws KeyManagementException {
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
      return new KeyManager.DataKeyResult(plaintextKey, encryptedKey);

    } catch (Exception e) {
      LOGGER.severe(
          () -> String.format("Failed to generate data key for master key: %s", masterKeyArn, e));
      throw new KeyManagementException("Failed to generate data key: " + e.getMessage(), e);
    }
  }

  private String createTestMasterKey() throws Exception {
    LOGGER.info("Creating test master key");

    CreateKeyRequest request =
        CreateKeyRequest.builder()
            .description("Test master key for KeyManagementUtility integration test")
            .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
            .keySpec(KeySpec.SYMMETRIC_DEFAULT)
            .build();

    CreateKeyResponse response = kmsClient.createKey(request);
    String keyArn = response.keyMetadata().arn();
    LOGGER.info("Created test master key: " + keyArn);
    return keyArn;
  }


  /** Executes a KMS operation with retry logic and exponential backoff. */
  private <T> T executeWithRetry(KmsOperation<T> operation) throws Exception {
    Exception lastException = null;
    //TODO: get this out of config
    int maxRetries = 5;

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
    // TODO: this should come from the config
    long baseMs = 100;
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

  private void setupTestSchema() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Drop and recreate tables with correct schema
      stmt.execute("DROP SCHEMA IF EXISTS encrypt CASCADE");
      stmt.execute("CREATE SCHEMA encrypt");
      stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE + " CASCADE");

      // Create key storage table first (due to foreign key)
      stmt.execute(
          "CREATE TABLE encrypt.key_storage ("
              + "id SERIAL PRIMARY KEY, "
              + "name VARCHAR(255) NOT NULL, "
              + "master_key_arn VARCHAR(512) NOT NULL, "
              + "encrypted_data_key TEXT NOT NULL, "
              + "hmac_key BYTEA NOT NULL, "
              + "key_spec VARCHAR(50) DEFAULT 'AES_256', "
              + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
              + "last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)");

      // Create encryption metadata table
      stmt.execute(
          "CREATE TABLE encrypt.encryption_metadata ("
              + "table_name VARCHAR(255) NOT NULL, "
              + "column_name VARCHAR(255) NOT NULL, "
              + "encryption_algorithm VARCHAR(50) NOT NULL, "
              + "key_id INTEGER NOT NULL, "
              + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
              + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
              + "PRIMARY KEY (table_name, column_name), "
              + "FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)"
              + ")");

      // Create test users table
      stmt.execute(
          "CREATE TABLE "
              + TEST_TABLE
              + " ("
              + "id SERIAL PRIMARY KEY, "
              + "name VARCHAR(100), "
              + "ssn TEXT, "
              + "email VARCHAR(100)"
              + ")");

      LOGGER.info("Test database schema setup complete");
    }
  }
}
