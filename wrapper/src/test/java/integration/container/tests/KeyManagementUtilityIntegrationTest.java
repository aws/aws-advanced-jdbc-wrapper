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
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementUtility;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

/** Integration test for KeyManagementUtility functionality. */

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
@Order(18)
public class KeyManagementUtilityIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(KeyManagementUtilityIntegrationTest.class.getName());

  private static final String KMS_KEY_ARN_ENV = "AWS_KMS_KEY_ARN";
  private static final String TEST_TABLE = "users";
  private static final String TEST_COLUMN = "ssn";
  private static final String TEST_ALGORITHM = "AES-256-GCM";

  private Connection connection;
  private Connection wrappedConnection;
  private KmsClient kmsClient;
  private String masterKeyArn;
  private boolean createdKey = false;
  private DatabaseEngine dbEngine = null;

  private String getTablePrefix() {
    // Both databases use schema prefix
    return "encrypt.";
  }

  @BeforeEach
  void setUp() throws Exception {
    dbEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
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
    if (connection.isWrapperFor(Connection.class)) {
      wrappedConnection = connection.unwrap(Connection.class);
    } else {
      wrappedConnection = connection;
    }

    if (dbEngine == DatabaseEngine.PG) {
      EncryptedDataTypeInstaller.installEncryptedDataType(wrappedConnection, "encrypt");
    }
    // Setup test database schema
    setupTestSchema();
    LOGGER.info("Test setup completed with master key: " + masterKeyArn);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (connection != null) {
      String tablePrefix = getTablePrefix();
      try (Statement stmt = wrappedConnection.createStatement()) {
        // Clean up test data
        stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
        stmt.execute(
            "DELETE FROM " + tablePrefix + "encryption_metadata WHERE table_name = '" + TEST_TABLE + "'");
        String tableName = TEST_TABLE + '.' + TEST_COLUMN;
        stmt.execute("DELETE FROM " + tablePrefix + "key_storage WHERE name = '" + tableName + "'");
      }
      connection.close();
    }

    if (kmsClient != null) {
      kmsClient.close();
    }
  }

  @TestTemplate
  void testCreateDataKeyAndPopulateMetadata() throws Exception {
    LOGGER.info("Testing data key creation and metadata population using KeyManagementUtility for "
        + TEST_TABLE + "." + TEST_COLUMN);

    String tablePrefix = getTablePrefix();

    // Clean up any existing metadata for this column
    try (Statement stmt = wrappedConnection.createStatement()) {
      stmt.execute("DELETE FROM " + tablePrefix + "encryption_metadata WHERE table_name = '"
          + TEST_TABLE + "' AND column_name = '" + TEST_COLUMN + "'");
    }

    // Create components needed for KeyManagementUtility
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion(TestEnvironment.getCurrent().getInfo().getRegion())
        .defaultMasterKeyArn(masterKeyArn)
        .encryptionMetadataSchema("encrypt")
        .build();

    boolean isPostgreSQL = dbEngine == DatabaseEngine.PG;
    KeyManager keyManager = new KeyManager(kmsClient, wrappedConnection, isPostgreSQL, config);
    MetadataManager metadataManager = new MetadataManager(wrappedConnection, config);
    metadataManager.initialize();

    KeyManagementUtility keyManagementUtility =
        new KeyManagementUtility(keyManager, metadataManager, wrappedConnection, kmsClient, config);

    // Use KeyManagementUtility to initialize encryption for the column
    keyManagementUtility.initializeEncryptionForColumn(
        TEST_TABLE, TEST_COLUMN, masterKeyArn, TEST_ALGORITHM);

    LOGGER.info("KeyManagementUtility initialized encryption for " + TEST_TABLE + "." + TEST_COLUMN);

    // Verify the metadata was created correctly
    try (PreparedStatement checkStmt =
        wrappedConnection.prepareStatement(
            "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, "
            + "ks.master_key_arn, ks.encrypted_data_key "
            + "FROM " + tablePrefix + "encryption_metadata em "
            + "JOIN " + tablePrefix + "key_storage ks ON em.key_id = ks.id "
            + "WHERE em.table_name = ? AND em.column_name = ?")) {
      checkStmt.setString(1, TEST_TABLE);
      checkStmt.setString(2, TEST_COLUMN);
      ResultSet rs = checkStmt.executeQuery();

      assertTrue(rs.next(), "Should find encryption metadata");
      assertEquals(TEST_TABLE, rs.getString("table_name"));
      assertEquals(TEST_COLUMN, rs.getString("column_name"));
      assertEquals(TEST_ALGORITHM, rs.getString("encryption_algorithm"));
      assertNotNull(rs.getInt("key_id"), "Key ID should be set");
      assertEquals(masterKeyArn, rs.getString("master_key_arn"));
      assertNotNull(rs.getString("encrypted_data_key"), "Encrypted data key should be stored");
      LOGGER.info("Verified encryption metadata with key_storage reference");
    }

    // Test that the encryption system works with the configured metadata
    String insertSql = "INSERT INTO " + TEST_TABLE + " (name, " + TEST_COLUMN + ") VALUES (?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, "Test User");
      pstmt.setString(2, "123-45-6789");
      int rowsInserted = pstmt.executeUpdate();
      assertEquals(1, rowsInserted, "Should insert one row");
      LOGGER.info("Successfully inserted encrypted data");
    }

    // Verify data can be retrieved and decrypted
    String selectSql = "SELECT name, " + TEST_COLUMN + " FROM " + TEST_TABLE + " WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setString(1, "Test User");
      ResultSet rs = pstmt.executeQuery();

      assertTrue(rs.next(), "Should find inserted row");
      assertEquals("Test User", rs.getString("name"));
      assertEquals("123-45-6789", rs.getString(TEST_COLUMN));
      LOGGER.info("Successfully retrieved and decrypted data");
    }
  }

  @TestTemplate
  void testEncryptionWithDifferentValues() throws Exception {
    LOGGER.info("Testing encryption with different SSN values using KeyManagementUtility");

    String tablePrefix = getTablePrefix();

    // Clean up any existing metadata for this column
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DELETE FROM " + tablePrefix + "encryption_metadata WHERE table_name = '"
          + TEST_TABLE + "' AND column_name = '" + TEST_COLUMN + "'");
      stmt.execute("TRUNCATE TABLE " + TEST_TABLE);
    }

    // Create components for KeyManagementUtility
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion(TestEnvironment.getCurrent().getInfo().getRegion())
        .defaultMasterKeyArn(masterKeyArn)
        .encryptionMetadataSchema("encrypt")
        .build();

    boolean isPostgreSQL = dbEngine == DatabaseEngine.PG;
    KeyManager keyManager = new KeyManager(kmsClient, connection, isPostgreSQL, config);
    MetadataManager metadataManager = new MetadataManager(connection, config);
    metadataManager.initialize();

    KeyManagementUtility keyManagementUtility =
        new KeyManagementUtility(keyManager, metadataManager, connection, kmsClient, config);

    // Use KeyManagementUtility to initialize encryption
    keyManagementUtility.initializeEncryptionForColumn(
        TEST_TABLE, TEST_COLUMN, masterKeyArn, TEST_ALGORITHM);

    LOGGER.info("KeyManagementUtility initialized encryption for " + TEST_TABLE + "." + TEST_COLUMN);

    // Test multiple SSN values with same data key
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

    // Verify all data can be retrieved correctly
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

  @TestTemplate
  void testKeyRotation() throws Exception {
    LOGGER.info("Testing key rotation using KeyManagementUtility");

    String tablePrefix = getTablePrefix();

    // Create components for KeyManagementUtility
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion(TestEnvironment.getCurrent().getInfo().getRegion())
        .defaultMasterKeyArn(masterKeyArn)
        .encryptionMetadataSchema("encrypt")
        .build();

    boolean isPostgreSQL = dbEngine == DatabaseEngine.PG;
    KeyManager keyManager = new KeyManager(kmsClient, connection, isPostgreSQL, config);
    MetadataManager metadataManager = new MetadataManager(connection, config);
    metadataManager.initialize();

    KeyManagementUtility keyManagementUtility =
        new KeyManagementUtility(keyManager, metadataManager, connection, kmsClient, config);

    // Initialize encryption with first key
    keyManagementUtility.initializeEncryptionForColumn(
        TEST_TABLE, TEST_COLUMN, masterKeyArn, TEST_ALGORITHM);

    // Get initial key ID
    ColumnEncryptionConfig initialConfig = metadataManager.getColumnConfig(TEST_TABLE, TEST_COLUMN);
    assertNotNull(initialConfig, "Initial config should exist");
    Integer initialKeyId = initialConfig.getKeyId();
    assertNotNull(initialKeyId, "Initial key ID should exist");
    LOGGER.info("Initial key ID: " + initialKeyId);

    // Insert test data with first key
    String insertSql = "INSERT INTO " + TEST_TABLE + " (name, " + TEST_COLUMN + ") VALUES (?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, "TestUser");
      pstmt.setString(2, "123-45-6789");
      pstmt.executeUpdate();
      LOGGER.info("Inserted data with initial key");
    }

    // Rotate the key
    keyManagementUtility.rotateDataKey(TEST_TABLE, TEST_COLUMN, null);
    LOGGER.info("Rotated data key for " + TEST_TABLE + "." + TEST_COLUMN);

    // Get new key ID
    ColumnEncryptionConfig rotatedConfig = metadataManager.getColumnConfig(TEST_TABLE, TEST_COLUMN);
    assertNotNull(rotatedConfig, "Rotated config should exist");
    Integer rotatedKeyId = rotatedConfig.getKeyId();
    assertNotNull(rotatedKeyId, "Rotated key ID should exist");
    LOGGER.info("Rotated key ID: " + rotatedKeyId);

    // Verify key ID changed
    assertTrue(rotatedKeyId > initialKeyId, "Key ID should have changed after rotation");

    LOGGER.info("Key rotation test completed successfully");
  }


  private void setupTestSchema() throws SQLException {
    DatabaseEngine dbEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();

    try (Statement stmt = wrappedConnection.createStatement()) {
      // PostgreSQL supports CASCADE, MySQL doesn't
      if (dbEngine == DatabaseEngine.PG) {
        stmt.execute("DROP SCHEMA IF EXISTS encrypt CASCADE");
      } else {
        stmt.execute("DROP SCHEMA IF EXISTS encrypt");
      }
      stmt.execute("CREATE SCHEMA encrypt");
      stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);

      if (dbEngine == DatabaseEngine.PG) {
        setupPostgreSQLSchema(stmt);
      } else {
        setupMySQLSchema(stmt);
      }

      LOGGER.info("Test database schema setup complete");
    }
  }

  private void setupPostgreSQLSchema(Statement stmt) throws SQLException {
    // Create key storage table
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
            + "FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id))");

    // Create test users table with encrypted_data type
    stmt.execute(
        "CREATE TABLE "
            + TEST_TABLE
            + " ("
            + "id SERIAL PRIMARY KEY, "
            + "name VARCHAR(100), "
            + "ssn encrypted_data, "
            + "email VARCHAR(100))");

    // Add HMAC validation trigger
    stmt.execute(
        "CREATE TRIGGER validate_ssn_hmac "
            + "BEFORE INSERT OR UPDATE ON " + TEST_TABLE + " "
            + "FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('ssn')");
  }

  private void setupMySQLSchema(Statement stmt) throws SQLException {
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

    stmt.execute(
        "CREATE TABLE "
            + TEST_TABLE
            + " ("
            + "id INT AUTO_INCREMENT PRIMARY KEY, "
            + "name VARCHAR(100), "
            + "ssn VARBINARY(256), "
            + "email VARCHAR(100))");
  }
}
