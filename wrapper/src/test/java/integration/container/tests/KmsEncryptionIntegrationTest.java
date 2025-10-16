package integration.container.tests;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import integration.container.ConnectionStringHelper;
import integration.container.TestEnvironment;
import java.sql.*;
import java.util.Base64;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

/**
 * Integration test for KMS encryption functionality with JSqlParser.
 */
public class KmsEncryptionIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(KmsEncryptionIntegrationTest.class);
  private static final String KMS_KEY_ARN_ENV = "AWS_KMS_KEY_ARN";
  private static final String TEST_SSN_1 = "111-11-1111";
  private static final String TEST_NAME_1 = "Alice Test";
  private static final String TEST_EMAIL_1 = "alice@test.com";
  private static final String TEST_SSN_2 = "222-22-2222";
  private static final String TEST_NAME_2 = "Bob Test";
  private static final String TEST_EMAIL_2 = "bob@test.com";

  private static Connection connection;
  private static String kmsKeyArn;

  @BeforeAll
  static void setUp() throws Exception {
    kmsKeyArn = System.getenv(KMS_KEY_ARN_ENV);
    assumeTrue(kmsKeyArn != null && !kmsKeyArn.isEmpty(),
        "KMS Key ARN must be provided via " + KMS_KEY_ARN_ENV + " environment variable");

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_MASTER_KEY_ARN.name, kmsKeyArn);
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");

    String url = String.format("jdbc:aws-wrapper:postgresql://%s:%d/%s",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    // use a direct connection so that we setup all of the metadata before instantiating the encrypted connection
    String directUrl = String.format("jdbc:postgresql://%s:%d/%s",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    try (Connection directConnection = DriverManager.getConnection(directUrl, props)){
      // Setup encryption metadata schema
      try (Statement stmt = directConnection.createStatement()) {
        // Drop and recreate tables with correct schema
        stmt.execute("DROP SCHEMA IF EXISTS encrypt CASCADE");
        stmt.execute("CREATE SCHEMA encrypt");
        stmt.execute("DROP TABLE IF EXISTS users CASCADE");

        // Create key_storage table first (referenced by encryption_metadata)
        stmt.execute("CREATE TABLE if not exists encrypt.key_storage ("
            + "id SERIAL PRIMARY KEY, "
            + "name VARCHAR(255) NOT NULL, "
            + "master_key_arn VARCHAR(512) NOT NULL, "
            + "encrypted_data_key TEXT NOT NULL, "
            + "key_spec VARCHAR(50) DEFAULT 'AES_256', "
            + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
            + "last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)");

        // Create encryption_metadata table with correct schema
        stmt.execute("CREATE TABLE if not exists encrypt.encryption_metadata ("
            + "table_name VARCHAR(255) NOT NULL, "
            + "column_name VARCHAR(255) NOT NULL, "
            + "encryption_algorithm VARCHAR(50) NOT NULL, "
            + "key_id INTEGER NOT NULL, "
            + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
            + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
            + "PRIMARY KEY (table_name, column_name), "
            + "FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id))");

        // Insert a key into key_storage with real KMS data key
        KmsClient kmsClient = KmsClient.builder().region(software.amazon.awssdk.regions.Region.US_EAST_1).build();
        GenerateDataKeyRequest dataKeyRequest = GenerateDataKeyRequest.builder()
            .keyId(kmsKeyArn)
            .keySpec("AES_256")
            .build();
        GenerateDataKeyResponse dataKeyResponse = kmsClient.generateDataKey(dataKeyRequest);
        String encryptedDataKeyBase64 = Base64.getEncoder().encodeToString(dataKeyResponse.ciphertextBlob().asByteArray());

        PreparedStatement keyStmt = directConnection.prepareStatement(
            "INSERT INTO encrypt.key_storage (name, master_key_arn, encrypted_data_key, key_spec) VALUES (?, ?, ?, ?) RETURNING id");
        keyStmt.setString(1, "test-key-users-ssn");
        keyStmt.setString(2, kmsKeyArn);
        keyStmt.setString(3, encryptedDataKeyBase64);
        keyStmt.setString(4, "AES_256");
        ResultSet keyRs = keyStmt.executeQuery();
        keyRs.next();
        int generatedKeyId = keyRs.getInt(1);
        keyStmt.close();

        // Use KeyManagementUtility approach to setup encryption metadata
        logger.trace("Setting up encryption metadata for users.ssn using KeyManagementUtility approach");

        try (PreparedStatement metaStmt = directConnection.prepareStatement(
            "INSERT INTO encrypt.encryption_metadata (table_name, column_name, encryption_algorithm, key_id) VALUES (?, ?, ?, ?)")) {
          metaStmt.setString(1, "users");
          metaStmt.setString(2, "ssn");
          metaStmt.setString(3, "AES-256-GCM");
          metaStmt.setInt(4, generatedKeyId);
          metaStmt.executeUpdate();
          logger.trace("Encryption metadata configured for key: {}", generatedKeyId);
        }

        // Verify the metadata was configured correctly
        try (PreparedStatement checkStmt = directConnection.prepareStatement(
            "SELECT table_name, column_name, encryption_algorithm, key_id FROM encrypt.encryption_metadata WHERE table_name = ? AND column_name = ?")) {
          checkStmt.setString(1, "users");
          checkStmt.setString(2, "ssn");
          ResultSet rs = checkStmt.executeQuery();
          while (rs.next()) {
            logger.trace("Verified metadata: {}.{} -> {} (key: {})",
                rs.getString("table_name"), rs.getString("column_name"),
                rs.getString("encryption_algorithm"), rs.getInt("key_id"));
          }
        }

        // Create users table with bytea for encrypted data
        stmt.execute("CREATE TABLE if not exists users ("
            + "id SERIAL PRIMARY KEY, "
            + "name VARCHAR(100), "
            + "ssn bytea, "
            + "email VARCHAR(100))");

        logger.trace("Test setup completed");

        // Final verification that metadata exists
        try (PreparedStatement finalCheck = directConnection.prepareStatement(
            "SELECT COUNT(*) FROM encrypt.encryption_metadata WHERE table_name = 'users' AND column_name = 'ssn'")) {
          ResultSet rs = finalCheck.executeQuery();
          rs.next();
          int count = rs.getInt(1);
          logger.info("Final metadata verification: {} rows found for users.ssn", count);
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
        logger.trace("Cleaned up test data");
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

  @Test
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
    String plainUrl = String.format("jdbc:postgresql://%s:%d/%s",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

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

  @Test
  void testUpdateEncryption() throws Exception {
    String insertSql = "INSERT INTO users (name, ssn,email) VALUES (?, ?, ?)";
    logger.trace("testUpdateEncryption: INSERT SQL: {}", insertSql);
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      logger.trace("Setting INSERT parameters: name={}, ssn={}, email={}", TEST_NAME_2, TEST_SSN_1, TEST_EMAIL_2);
      pstmt.setString(1, TEST_NAME_2);
      pstmt.setString(2, TEST_SSN_1);
      pstmt.setString(3, TEST_EMAIL_2);
      assertEquals(1,pstmt.executeUpdate());
    }

    // Check what was actually stored in the database
    logger.trace("Checking what was stored in database...");
    try (PreparedStatement stmt = connection.prepareStatement("SELECT name, ssn, pg_typeof(name) as name_type, pg_typeof(ssn) as ssn_type FROM users where name = ?")) {
      stmt.setString(1, TEST_NAME_2);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        assertEquals(TEST_NAME_2, rs.getString("name"));
        assertEquals(TEST_SSN_1, rs.getString("ssn"));
        assertEquals("character varying", rs.getString("name_type"));
        assertEquals("bytea", rs.getString("ssn_type"));
      }
    }

    String updateSql = "UPDATE users SET ssn = ? WHERE name = ?";
    logger.trace("testUpdateEncryption: UPDATE SQL: {}", updateSql);
    try (PreparedStatement pstmt = connection.prepareStatement(updateSql)) {
      logger.trace("Setting UPDATE parameters: ssn={}, name={}", TEST_SSN_2, TEST_NAME_2);
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

  @Test
  void testEncryptionMetadataSetup() throws Exception {
    // Verify encryption metadata was created with master key ARN
    String metadataSql = "SELECT table_name, column_name, encryption_algorithm FROM encrypt.encryption_metadata WHERE table_name = 'users'";
    try (PreparedStatement pstmt = connection.prepareStatement(metadataSql)) {
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("users", rs.getString("table_name"));
        assertEquals("ssn", rs.getString("column_name"));
        assertEquals("AES-256-GCM", rs.getString("encryption_algorithm"));
      }
    }

    // Verify key storage table exists and is ready for KMS key storage
    String keyStorageSql = "SELECT COUNT(*) FROM encrypt.key_storage";
    try (PreparedStatement pstmt = connection.prepareStatement(keyStorageSql)) {
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertTrue(rs.getInt(1) >= 0);
      }
    }

    // Verify KMS master key ARN is configured
    assertEquals(kmsKeyArn, System.getenv(KMS_KEY_ARN_ENV));
    assertTrue(kmsKeyArn.startsWith("arn:aws:kms:"));
  }
}
