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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import integration.DatabaseEngine;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

/** Integration test for KMS encryption functionality with JSqlParser. */

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnDatabaseEngine(DatabaseEngine.PG)
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
@Order(17)
public class KmsEncryptionIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(KmsEncryptionIntegrationTest.class.getName());
  private static final String KMS_KEY_ARN_ENV = "AWS_KMS_KEY_ARN";
  private static final String TEST_SSN_1 = "111-11-1111";
  private static final String TEST_NAME_1 = "Alice Test";
  private static final String TEST_EMAIL_1 = "alice@test.com";
  private static final String TEST_SSN_2 = "222-22-2222";
  private static final String TEST_NAME_2 = "Bob Test";
  private static final String TEST_EMAIL_2 = "bob@test.com";

  private static Connection connection;
  private static String kmsKeyArn;
  private static String region;

  @BeforeAll
  static void setUp() throws Exception {
    kmsKeyArn = System.getenv(KMS_KEY_ARN_ENV);
    assumeTrue(
        kmsKeyArn != null && !kmsKeyArn.isEmpty(),
        "KMS Key ARN must be provided via " + KMS_KEY_ARN_ENV + " environment variable");

    region = TestEnvironment.getCurrent().getInfo().getRegion();
    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_MASTER_KEY_ARN.name, kmsKeyArn);
    props.setProperty(EncryptionConfig.KMS_REGION.name, region);

    // Get the metadata schema from config (defaults to "encrypt")
    String metadataSchema = EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue;

    String url = ConnectionStringHelper.getWrapperUrl();
    // use a direct connection so that we setup all of the metadata before instantiating the
    // encrypted connection
    String directUrl = ConnectionStringHelper.getUrl();

    try (Connection directConnection = DriverManager.getConnection(directUrl, props)) {
      // Setup encryption metadata schema
      try (Statement stmt = directConnection.createStatement()) {
        // Drop and recreate tables with correct schema
        stmt.execute("DROP SCHEMA IF EXISTS " + metadataSchema + " CASCADE");
        stmt.execute("CREATE SCHEMA " + metadataSchema);
        stmt.execute("DROP TABLE IF EXISTS users CASCADE");

        // Install encrypted_data custom type
        LOGGER.finest("Installing encrypted_data custom type");
        stmt.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        EncryptedDataTypeInstaller.installEncryptedDataType(directConnection);

        // Create key_storage table first (referenced by encryption_metadata)
        stmt.execute(
            "CREATE TABLE if not exists "
                + metadataSchema
                + ".key_storage ("
                + "id SERIAL PRIMARY KEY, "
                + "name VARCHAR(255) NOT NULL, "
                + "master_key_arn VARCHAR(512) NOT NULL, "
                + "encrypted_data_key TEXT NOT NULL, "
                + "hmac_key BYTEA NOT NULL, "
                + "key_spec VARCHAR(50) DEFAULT 'AES_256', "
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
                + "last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)");

        // Create encryption_metadata table with correct schema
        stmt.execute(
            "CREATE TABLE if not exists "
                + metadataSchema
                + ".encryption_metadata ("
                + "table_name VARCHAR(255) NOT NULL, "
                + "column_name VARCHAR(255) NOT NULL, "
                + "encryption_algorithm VARCHAR(50) NOT NULL, "
                + "key_id INTEGER NOT NULL, "
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
                + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
                + "PRIMARY KEY (table_name, column_name), "
                + "FOREIGN KEY (key_id) REFERENCES "
                + metadataSchema
                + ".key_storage(id))");

        // Insert a key into key_storage with real KMS data key and separate HMAC key
        KmsClient kmsClient =
            KmsClient.builder().region(Region.of(region)).build();
        GenerateDataKeyRequest dataKeyRequest =
            GenerateDataKeyRequest.builder().keyId(kmsKeyArn).keySpec("AES_256").build();
        GenerateDataKeyResponse dataKeyResponse = kmsClient.generateDataKey(dataKeyRequest);
        String encryptedDataKeyBase64 =
            Base64.getEncoder().encodeToString(dataKeyResponse.ciphertextBlob().asByteArray());

        // Generate separate HMAC key (32 bytes for HMAC-SHA256)
        byte[] hmacKey = new byte[32];
        new java.security.SecureRandom().nextBytes(hmacKey);

        PreparedStatement keyStmt =
            directConnection.prepareStatement(
                "INSERT INTO "
                    + metadataSchema
                    + ".key_storage (name, master_key_arn, encrypted_data_key, hmac_key, key_spec) VALUES (?, ?, ?, ?, ?) RETURNING id");
        keyStmt.setString(1, "test-key-users-ssn");
        keyStmt.setString(2, kmsKeyArn);
        keyStmt.setString(3, encryptedDataKeyBase64);
        keyStmt.setBytes(4, hmacKey);
        keyStmt.setString(5, "AES_256");
        ResultSet keyRs = keyStmt.executeQuery();
        keyRs.next();
        int generatedKeyId = keyRs.getInt(1);
        keyStmt.close();

        // Use KeyManagementUtility approach to setup encryption metadata
        LOGGER.finest(
            "Setting up encryption metadata for users.ssn using KeyManagementUtility approach");

        try (PreparedStatement metaStmt =
            directConnection.prepareStatement(
                "INSERT INTO "
                    + metadataSchema
                    + ".encryption_metadata (table_name, column_name, encryption_algorithm, key_id) VALUES (?, ?, ?, ?)")) {
          metaStmt.setString(1, "users");
          metaStmt.setString(2, "ssn");
          metaStmt.setString(3, "AES-256-GCM");
          metaStmt.setInt(4, generatedKeyId);
          metaStmt.executeUpdate();
          LOGGER.finest("Encryption metadata configured for key: " + generatedKeyId);
        }

        // Verify the metadata was configured correctly
        try (PreparedStatement checkStmt =
            directConnection.prepareStatement(
                "SELECT table_name, column_name, encryption_algorithm, key_id FROM "
                    + EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue
                    + ".encryption_metadata WHERE table_name = ? AND column_name = ?")) {
          checkStmt.setString(1, "users");
          checkStmt.setString(2, "ssn");
          ResultSet rs = checkStmt.executeQuery();
          while (rs.next()) {
            LOGGER.finest(
                "Verified metadata: " + rs.getString("table_name") + "." + rs.getString("column_name") + " -> " + rs.getString("encryption_algorithm") + " (key: " + rs.getInt("key_id") + ")");
          }
        }

        // Create users table with encrypted_data type for SSN
        stmt.execute(
            "CREATE TABLE if not exists users ("
                + "id SERIAL PRIMARY KEY, "
                + "name VARCHAR(100), "
                + "ssn encrypted_data, "
                + "email VARCHAR(100))");

        // Add trigger to validate HMAC on ssn column
        stmt.execute(
            "CREATE TRIGGER validate_ssn_hmac "
                + "BEFORE INSERT OR UPDATE ON users "
                + "FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('ssn')");

        LOGGER.finest("Test setup completed");

        // Final verification that metadata exists
        try (PreparedStatement finalCheck =
            directConnection.prepareStatement(
                "SELECT COUNT(*) FROM "
                    + EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue
                    + ".encryption_metadata WHERE table_name = 'users' AND column_name = 'ssn'")) {
          ResultSet rs = finalCheck.executeQuery();
          rs.next();
          int count = rs.getInt(1);
          LOGGER.info("Final metadata verification: " + count + " rows found for users.ssn");
          if (count == 0) {
            throw new RuntimeException("Encryption metadata was not properly created!");
          }
        }
      }
    }
    connection = DriverManager.getConnection(url, props);
  }

  @AfterEach
  void cleanupTestData() throws Exception {
    // Clean up test data between tests without dropping schema
    /*
    if (connection != null && !connection.isClosed()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DELETE FROM users WHERE name LIKE '%Test'");
        logger.finest("Cleaned up test data");
      }
    }
     */
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @TestTemplate
  void testBasicEncryption() throws Exception {
    String insertSql = "INSERT INTO users (name, ssn, email) VALUES (?, ?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, TEST_NAME_1);
      pstmt.setString(2, TEST_SSN_1);
      pstmt.setString(3, TEST_EMAIL_1);
      pstmt.executeUpdate();
    }

    String selectSql = "SELECT name, ssn, email FROM users WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setString(1, TEST_NAME_1);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(TEST_NAME_1, rs.getString("name"));
        assertEquals(TEST_SSN_1, rs.getString("ssn"));
        assertEquals(TEST_EMAIL_1, rs.getString("email"));
      }
    }

    // Verify data is encrypted in storage
    Properties plainProps = ConnectionStringHelper.getDefaultProperties();
    String plainUrl = ConnectionStringHelper.getUrl();

    try (Connection plainConn = DriverManager.getConnection(plainUrl, plainProps);
        PreparedStatement pstmt = plainConn.prepareStatement(selectSql)) {
      pstmt.setString(1, TEST_NAME_1);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(TEST_NAME_1, rs.getString("name"));
        assertNotEquals(TEST_SSN_1, rs.getString("ssn")); // Should be encrypted
      }
    }
  }

  @TestTemplate
  void testUpdateEncryption() throws Exception {
    String insertSql = "INSERT INTO users (name, ssn,email) VALUES (?, ?, ?)";
    LOGGER.finest("testUpdateEncryption: INSERT SQL: " + insertSql);
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      LOGGER.finest(
          "Setting INSERT parameters: name=" + TEST_NAME_2 + ", ssn=" + TEST_SSN_1 + ", email=" + TEST_EMAIL_2);
      pstmt.setString(1, TEST_NAME_2);
      pstmt.setString(2, TEST_SSN_1);
      pstmt.setString(3, TEST_EMAIL_2);
      assertEquals(1, pstmt.executeUpdate());
    }

    // Check what was actually stored in the database
    LOGGER.finest("Checking what was stored in database...");
    try (PreparedStatement stmt =
        connection.prepareStatement(
            "SELECT name, ssn, pg_typeof(name) as name_type, pg_typeof(ssn) as ssn_type FROM users where name = ?")) {
      stmt.setString(1, TEST_NAME_2);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        assertEquals(TEST_NAME_2, rs.getString("name"));
        assertEquals(TEST_SSN_1, rs.getString("ssn"));
        assertEquals("character varying", rs.getString("name_type"));
        assertEquals("encrypted_data", rs.getString("ssn_type"));
      }
    }

    String updateSql = "UPDATE users SET ssn = ? WHERE name = ?";
    LOGGER.finest("testUpdateEncryption: UPDATE SQL: " + updateSql);
    try (PreparedStatement pstmt = connection.prepareStatement(updateSql)) {
      LOGGER.finest("Setting UPDATE parameters: ssn=" + TEST_SSN_2 + ", name=" + TEST_NAME_2);
      pstmt.setString(1, TEST_SSN_2);
      pstmt.setString(2, TEST_NAME_2);
      assertEquals(1, pstmt.executeUpdate());
    }

    String selectSql = "SELECT ssn FROM users WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setString(1, TEST_NAME_2);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(TEST_SSN_2, rs.getString("ssn"));
      }
    }
  }

  @TestTemplate
  void testEncryptionMetadataSetup() throws Exception {
    // Verify encryption metadata was created with master key ARN
    String metadataSql =
        "SELECT table_name, column_name, encryption_algorithm FROM "
            + EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue
            + ".encryption_metadata WHERE table_name = 'users'";
    try (PreparedStatement pstmt = connection.prepareStatement(metadataSql)) {
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("users", rs.getString("table_name"));
        assertEquals("ssn", rs.getString("column_name"));
        assertEquals("AES-256-GCM", rs.getString("encryption_algorithm"));
      }
    }

    // Verify key storage table exists and is ready for KMS key storage
    String keyStorageSql =
        "SELECT COUNT(*) FROM "
            + EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue
            + ".key_storage";
    try (PreparedStatement pstmt = connection.prepareStatement(keyStorageSql)) {
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertTrue(rs.getInt(1) >= 0);
      }
    }

    // Verify KMS master key ARN is configured
    LOGGER.info("kmsKeyArn:::" + kmsKeyArn);
    LOGGER.info("KMS_KEY_ARN_ENV:::" + KMS_KEY_ARN_ENV);
    assertEquals(kmsKeyArn, System.getenv(KMS_KEY_ARN_ENV));
    assertTrue(kmsKeyArn.startsWith("arn:aws:kms:"));
  }

  @TestTemplate
  void testEncryptedDataTypeHmacVerification() throws Exception {
    // Insert test data
    String insertSql = "INSERT INTO users (name, ssn, email) VALUES (?, ?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, "HMAC Test User");
      pstmt.setString(2, "999-99-9999");
      pstmt.setString(3, "hmac@test.com");
      assertEquals(1, pstmt.executeUpdate());
    }

    // Verify HMAC structure at database level (doesn't require key)
    String structureCheckSql =
        "SELECT name, has_valid_hmac_structure(ssn) as valid_structure FROM users WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(structureCheckSql)) {
      pstmt.setString(1, "HMAC Test User");
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertTrue(
            rs.getBoolean("valid_structure"), "Encrypted data should have valid HMAC structure");
        LOGGER.info("HMAC structure validation passed for encrypted SSN");
      }
    }

    // Verify we can still decrypt the data
    String selectSql = "SELECT ssn FROM users WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setString(1, "HMAC Test User");
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("999-99-9999", rs.getString("ssn"));
        LOGGER.info("Successfully decrypted SSN with HMAC verification");
      }
    }
  }

  @Test
  public void testPlainTextFails() {
    // make sure we cannot insert plain text into the ssn column
    assertThrows(SQLException.class,() -> {
      Statement stmt = connection.createStatement();
      stmt.execute("INSERT INTO users (name, ssn, email) VALUES ('Dave', '111', 'XXXXXXXXXXXXX')");
    });
  }
}
