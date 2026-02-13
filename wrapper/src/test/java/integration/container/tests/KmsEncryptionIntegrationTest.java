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

import integration.DatabaseEngine;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
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
@EnableOnDatabaseEngine({DatabaseEngine.PG, DatabaseEngine.MYSQL})
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
      DatabaseEngine dbEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();

      // Setup encryption metadata schema
      try (Statement stmt = directConnection.createStatement()) {
        // PostgreSQL supports CASCADE, MySQL doesn't
        if (dbEngine == DatabaseEngine.PG) {
          stmt.execute("DROP SCHEMA IF EXISTS " + metadataSchema + " CASCADE");
        } else {
          stmt.execute("DROP SCHEMA IF EXISTS " + metadataSchema);
        }
        stmt.execute("CREATE SCHEMA " + metadataSchema);
        stmt.execute("DROP TABLE IF EXISTS users");

        switch (dbEngine) {
          case PG:
            setupPostgreSQL(directConnection, stmt, metadataSchema);
            break;
          case MYSQL:
            setupMySQL(directConnection, stmt, metadataSchema);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported database: " + dbEngine);
        }

        LOGGER.finest("Test setup completed");

        // Final verification that metadata exists
        try (PreparedStatement finalCheck =
            directConnection.prepareStatement(
                "SELECT COUNT(*) FROM " + metadataSchema + "."
                    + "encryption_metadata WHERE table_name = 'users' AND column_name = 'ssn'")) {
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
    assertThrows(SQLException.class, () -> {
      Statement stmt = connection.createStatement();
      stmt.execute("INSERT INTO users (name, ssn, email) VALUES ('Dave', '111', 'XXXXXXXXXXXXX')");
    });
  }

  private static void setupPostgreSQL(Connection conn, Statement stmt, String metadataSchema)
      throws Exception {
    // Install encrypted_data custom type
    LOGGER.finest("Installing encrypted_data custom type for PostgreSQL");
    stmt.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
    EncryptedDataTypeInstaller.installEncryptedDataType(conn, metadataSchema);

    // Create key_storage table
    stmt.execute(
        "CREATE TABLE "
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

    // Create encryption_metadata table
    stmt.execute(
        "CREATE TABLE "
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

    insertKeyAndMetadata(conn, metadataSchema);

    // Create users table with encrypted_data type
    stmt.execute(
        "CREATE TABLE users ("
            + "id SERIAL PRIMARY KEY, "
            + "name VARCHAR(100), "
            + "ssn encrypted_data, "
            + "email VARCHAR(100))");

    // Add trigger to validate HMAC
    stmt.execute(
        "CREATE TRIGGER validate_ssn_hmac "
            + "BEFORE INSERT OR UPDATE ON users "
            + "FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('ssn')");
  }

  private static void setupMySQL(Connection conn, Statement stmt, String metadataSchema)
      throws Exception {
    LOGGER.finest("Setting up MySQL encryption schema");

    // MySQL supports CREATE SCHEMA (alias for CREATE DATABASE)
    stmt.execute(
        "CREATE TABLE encrypt.key_storage ("
            + "id INT AUTO_INCREMENT PRIMARY KEY, "
            + "name VARCHAR(255) NOT NULL, "
            + "master_key_arn VARCHAR(512) NOT NULL, "
            + "encrypted_data_key TEXT NOT NULL, "
            + "hmac_key VARBINARY(32) NOT NULL, "
            + "key_spec VARCHAR(50) DEFAULT 'AES_256', "
            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            + "last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

    stmt.execute(
        "CREATE TABLE encrypt.encryption_metadata ("
            + "table_name VARCHAR(255) NOT NULL, "
            + "column_name VARCHAR(255) NOT NULL, "
            + "encryption_algorithm VARCHAR(50) NOT NULL, "
            + "key_id INT NOT NULL, "
            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            + "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
            + "PRIMARY KEY (table_name, column_name), "
            + "FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id))");

    insertKeyAndMetadata(conn, "encrypt.");

    stmt.execute(
        "CREATE TABLE users ("
            + "id INT AUTO_INCREMENT PRIMARY KEY, "
            + "name VARCHAR(100), "
            + "ssn VARBINARY(256), "
            + "email VARCHAR(100))");
  }

  private static void insertKeyAndMetadata(Connection conn, String metadataSchema)
      throws Exception {
    // Generate KMS data key
    KmsClient kmsClient = KmsClient.builder().region(Region.of(region)).build();
    GenerateDataKeyRequest dataKeyRequest =
        GenerateDataKeyRequest.builder().keyId(kmsKeyArn).keySpec("AES_256").build();
    GenerateDataKeyResponse dataKeyResponse = kmsClient.generateDataKey(dataKeyRequest);
    String encryptedDataKeyBase64 =
        Base64.getEncoder().encodeToString(dataKeyResponse.ciphertextBlob().asByteArray());

    // Generate HMAC key
    byte[] hmacKey = new byte[32];
    new java.security.SecureRandom().nextBytes(hmacKey);

    // Insert key
    DatabaseEngine dbEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    String tablePrefix = (metadataSchema != null) ? metadataSchema + "." : "";
    String insertKeySql;

    if (dbEngine == DatabaseEngine.PG) {
      insertKeySql =
          "INSERT INTO "
              + tablePrefix
              + "key_storage (name, master_key_arn, encrypted_data_key, hmac_key, key_spec) "
              + "VALUES (?, ?, ?, ?, ?) RETURNING id";
    } else {
      insertKeySql =
          "INSERT INTO "
              + tablePrefix
              + "key_storage (name, master_key_arn, encrypted_data_key, hmac_key, key_spec) "
              + "VALUES (?, ?, ?, ?, ?)";
    }

    int generatedKeyId;
    try (PreparedStatement keyStmt = conn.prepareStatement(insertKeySql,
        dbEngine == DatabaseEngine.PG ? Statement.NO_GENERATED_KEYS : Statement.RETURN_GENERATED_KEYS)) {
      keyStmt.setString(1, "test-key-users-ssn");
      keyStmt.setString(2, kmsKeyArn);
      keyStmt.setString(3, encryptedDataKeyBase64);
      keyStmt.setBytes(4, hmacKey);
      keyStmt.setString(5, "AES_256");

      if (dbEngine == DatabaseEngine.PG) {
        ResultSet keyRs = keyStmt.executeQuery();
        keyRs.next();
        generatedKeyId = keyRs.getInt(1);
      } else {
        keyStmt.executeUpdate();
        ResultSet keyRs = keyStmt.getGeneratedKeys();
        keyRs.next();
        generatedKeyId = keyRs.getInt(1);
      }
    }

    // Insert metadata
    try (PreparedStatement metaStmt =
        conn.prepareStatement(
            "INSERT INTO "
                + tablePrefix
                + "encryption_metadata (table_name, column_name, encryption_algorithm, key_id) "
                + "VALUES (?, ?, ?, ?)")) {
      metaStmt.setString(1, "users");
      metaStmt.setString(2, "ssn");
      metaStmt.setString(3, "AES-256-GCM");
      metaStmt.setInt(4, generatedKeyId);
      metaStmt.executeUpdate();
      LOGGER.finest("Encryption metadata configured for key: " + generatedKeyId);
    }
  }

  @TestTemplate
  void testEncryptionWithAnnotations() throws Exception {
    LOGGER.info("Starting testEncryptionWithAnnotations");

    // Insert using annotation syntax
    String insertSql = "INSERT INTO users (name, ssn, email) VALUES (?, /*@encrypt:users.ssn*/ ?, ?)";
    try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
      stmt.setString(1, "Alice Annotation");
      stmt.setString(2, "111-22-3333"); // Will be encrypted via annotation
      stmt.setString(3, "alice@example.com");
      stmt.executeUpdate();
      LOGGER.info("Inserted user with annotation-based encryption");
    }

    // Query and verify decryption
    try (PreparedStatement stmt = connection.prepareStatement("SELECT name, ssn, email FROM users WHERE name = ?")) {
      stmt.setString(1, "Alice Annotation");
      ResultSet rs = stmt.executeQuery();

      assertTrue(rs.next(), "Should find inserted user");
      assertEquals("Alice Annotation", rs.getString("name"));
      assertEquals("111-22-3333", rs.getString("ssn"), "SSN should be decrypted");
      assertEquals("alice@example.com", rs.getString("email"));

      LOGGER.info("Successfully verified annotation-based encryption/decryption");
    }
  }

  @TestTemplate
  void testAnnotationOverridesMetadata() throws Exception {
    LOGGER.info("Starting testAnnotationOverridesMetadata");

    // Insert using annotation - this explicitly tells the plugin to encrypt
    // even if we're inserting into a different column position
    String insertSql = "INSERT INTO users (name, email, ssn) VALUES (?, ?, /*@encrypt:users.ssn*/ ?)";
    try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
      stmt.setString(1, "Bob Annotation");
      stmt.setString(2, "bob@example.com");
      stmt.setString(3, "222-33-4444"); // Encrypted via annotation
      stmt.executeUpdate();
      LOGGER.info("Inserted user with annotation in different column order");
    }

    // Verify decryption works
    try (PreparedStatement stmt = connection.prepareStatement("SELECT name, ssn FROM users WHERE name = ?")) {
      stmt.setString(1, "Bob Annotation");
      ResultSet rs = stmt.executeQuery();

      assertTrue(rs.next());
      assertEquals("Bob Annotation", rs.getString("name"));
      assertEquals("222-33-4444", rs.getString("ssn"), "SSN should be decrypted");

      LOGGER.info("Successfully verified annotation with different column order");
    }
  }

  @TestTemplate
  void testAnnotationWithUpdate() throws Exception {
    LOGGER.info("Starting testAnnotationWithUpdate");

    // Insert initial data
    try (PreparedStatement stmt = connection.prepareStatement(
        "INSERT INTO users (name, ssn, email) VALUES (?, /*@encrypt:users.ssn*/ ?, ?)")) {
      stmt.setString(1, "Dave Update");
      stmt.setString(2, "444-55-6666");
      stmt.setString(3, "dave@example.com");
      stmt.executeUpdate();
    }

    // Update using annotation
    String updateSql = "UPDATE users SET ssn = /*@encrypt:users.ssn*/ ? WHERE name = ?";
    try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
      stmt.setString(1, "555-66-7777"); // New encrypted value
      stmt.setString(2, "Dave Update");
      int updated = stmt.executeUpdate();
      assertEquals(1, updated, "Should update one row");
      LOGGER.info("Updated SSN with annotation");
    }

    // Verify updated value
    try (PreparedStatement stmt = connection.prepareStatement("SELECT ssn FROM users WHERE name = ?")) {
      stmt.setString(1, "Dave Update");
      ResultSet rs = stmt.executeQuery();

      assertTrue(rs.next());
      assertEquals("555-66-7777", rs.getString("ssn"), "Should have new encrypted value");

      LOGGER.info("Successfully verified annotation-based update");
    }
  }
}
