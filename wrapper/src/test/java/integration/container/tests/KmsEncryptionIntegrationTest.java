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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import integration.DatabaseEngine;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.KmsTestUtility;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;

/**
 * Integration test for KMS encryption functionality with JSqlParser.
 *
 * <p>Requires {@code KMS_KEY_ID} (a key ID or ARN; {@code AWS_KMS_KEY_ARN} is used as a fallback)
 * and AWS credentials in the test container. The schema and the shared plugin connection are set up
 * in {@code @BeforeEach}, once per class, because the connection URL depends on the test driver,
 * which {@link TestDriverProvider} sets only after {@code @BeforeAll}.
 */

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
@Order(16)
public class KmsEncryptionIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(KmsEncryptionIntegrationTest.class.getName());
  private static final String METADATA_SCHEMA = EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue;
  // A test-specific table name, so that DROP TABLE never removes an unrelated table.
  private static final String TABLE = "kms_enc_users";
  private static final String KEY_NAME = "test-key-users-ssn";
  // Smallest possible ciphertext: HMAC (32) + type (1) + IV (12) bytes, before the GCM payload.
  private static final int MIN_ENCRYPTED_LENGTH = 45;
  private static final String TEST_SSN_1 = "111-11-1111";
  private static final String TEST_NAME_1 = "Alice Test";
  private static final String TEST_EMAIL_1 = "alice@test.com";
  private static final String TEST_SSN_2 = "222-22-2222";
  private static final String TEST_NAME_2 = "Bob Test";
  private static final String TEST_EMAIL_2 = "bob@test.com";

  private static Connection connection;
  private static String kmsKeyId;
  private static String region;
  private static boolean isPg;
  private static boolean mysqlTriggersInstalled;
  private static boolean schemaReady;

  @BeforeAll
  static void setUp() throws Exception {
    kmsKeyId = KmsTestUtility.resolveKmsKeyId();
    assumeTrue(kmsKeyId != null,
        "A KMS key must be provided via " + KmsTestUtility.KMS_KEY_ID_ENV
            + " or " + KmsTestUtility.KMS_KEY_ARN_ENV);

    region = KmsTestUtility.resolveRegion(kmsKeyId, TestEnvironment.getCurrent().getInfo().getRegion());
    assumeTrue(region != null,
        "The KMS region is unknown: pass " + KmsTestUtility.KMS_KEY_ID_ENV
            + " as a key ARN or run on an AWS deployment");

    isPg = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG;
  }

  /**
   * Creates the metadata schema, the test table and the shared plugin connection once per class.
   * This cannot run in {@code @BeforeAll}: the connection URL depends on the current test driver,
   * which {@link TestDriverProvider} sets for each test invocation, before {@code @BeforeEach}
   * methods run.
   */
  @BeforeEach
  void setUpSchemaOnce() throws Exception {
    if (schemaReady) {
      return;
    }
    final SchemaName metadataSchema = SchemaName.of(METADATA_SCHEMA);

    // use a direct connection so that we setup all of the metadata before instantiating the
    // encrypted connection
    try (Connection directConnection = DriverManager.getConnection(
        ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties())) {

      // Create the metadata schema and its key_storage/encryption_metadata tables.
      KmsTestUtility.createMetadataTables(directConnection, metadataSchema, isPg);

      // The test table lives in the default schema, which the metadata schema drop does not touch.
      try (Statement stmt = directConnection.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + TABLE);
      }

      // Insert the key_storage/encryption_metadata rows directly (not through KeyManagementUtility, so
      // a bug in the utility cannot mask a bug in the read/write path), create the test table, and
      // install the HMAC validation triggers.
      KmsTestUtility.configureColumn(
          directConnection, metadataSchema, isPg, region, kmsKeyId, KEY_NAME, TABLE, "ssn");
      KmsTestUtility.createAppTable(
          directConnection, isPg, TABLE,
          Collections.singletonList("ssn"), Collections.singletonList("email"));
      mysqlTriggersInstalled =
          KmsTestUtility.installTriggers(directConnection, metadataSchema, isPg, TABLE, "ssn");

      LOGGER.finest("Test setup completed");

      // Final verification that metadata exists
      try (PreparedStatement finalCheck =
          directConnection.prepareStatement(
              "SELECT COUNT(*) FROM " + metadataSchema + "."
                  + "encryption_metadata WHERE table_name = ? AND column_name = 'ssn'")) {
        finalCheck.setString(1, TABLE);
        try (ResultSet rs = finalCheck.executeQuery()) {
          rs.next();
          int count = rs.getInt(1);
          LOGGER.info("Final metadata verification: " + count + " rows found for " + TABLE + ".ssn");
          if (count == 0) {
            throw new RuntimeException("Encryption metadata was not properly created!");
          }
        }
      }
    }
    connection = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), pluginProps());
    schemaReady = true;
  }

  @AfterEach
  void cleanupTestData() throws SQLException {
    // Clean up test data between tests without dropping schema. DELETE fires no HMAC trigger.
    if (!schemaReady) {
      return;
    }
    try (Connection plainConn = DriverManager.getConnection(
        ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
         Statement stmt = plainConn.createStatement()) {
      stmt.execute("DELETE FROM " + TABLE);
      LOGGER.finest("Cleaned up test data");
    }
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
    connection = null;

    // Drop the test table and delete its metadata/key rows, leaving the metadata schema in place.
    if (schemaReady) {
      try (Connection directConnection = DriverManager.getConnection(
          ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties())) {
        KmsTestUtility.teardown(directConnection, SchemaName.of(METADATA_SCHEMA), TABLE);
      }
    }

    schemaReady = false;
    mysqlTriggersInstalled = false;
  }

  /** Connection properties with the KMS encryption plugin. */
  private static Properties pluginProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    // kmsEncryption needs the SQL parse results that the sqlParser plugin puts in the call context.
    props.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_REGION.name, region);
    return props;
  }

  @TestTemplate
  void testBasicEncryption() throws Exception {
    String insertSql = "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, ?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, TEST_NAME_1);
      pstmt.setString(2, TEST_SSN_1);
      pstmt.setString(3, TEST_EMAIL_1);
      pstmt.executeUpdate();
    }

    String selectSql = "SELECT name, ssn, email FROM " + TABLE + " WHERE name = ?";
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
    String insertSql = "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, ?, ?)";
    LOGGER.finest("testUpdateEncryption: INSERT SQL: " + insertSql);
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      LOGGER.finest(
          "Setting INSERT parameters: name=" + TEST_NAME_2 + ", ssn=" + TEST_SSN_1 + ", email=" + TEST_EMAIL_2);
      pstmt.setString(1, TEST_NAME_2);
      pstmt.setString(2, TEST_SSN_1);
      pstmt.setString(3, TEST_EMAIL_2);
      assertEquals(1, pstmt.executeUpdate());
    }

    // Check what was actually stored in the database (pg_typeof exists only on PostgreSQL)
    if (isPg) {
      LOGGER.finest("Checking what was stored in database...");
      try (PreparedStatement stmt =
          connection.prepareStatement(
              "SELECT name, ssn, pg_typeof(name) as name_type, pg_typeof(ssn) as ssn_type FROM "
                  + TABLE + " where name = ?")) {
        stmt.setString(1, TEST_NAME_2);
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(TEST_NAME_2, rs.getString("name"));
          assertEquals(TEST_SSN_1, rs.getString("ssn"));
          assertEquals("character varying", rs.getString("name_type"));
          assertEquals("encrypted_data", rs.getString("ssn_type"));
        }
      }
    }

    String updateSql = "UPDATE " + TABLE + " SET ssn = ? WHERE name = ?";
    LOGGER.finest("testUpdateEncryption: UPDATE SQL: " + updateSql);
    try (PreparedStatement pstmt = connection.prepareStatement(updateSql)) {
      LOGGER.finest("Setting UPDATE parameters: ssn=" + TEST_SSN_2 + ", name=" + TEST_NAME_2);
      pstmt.setString(1, TEST_SSN_2);
      pstmt.setString(2, TEST_NAME_2);
      assertEquals(1, pstmt.executeUpdate());
    }

    String selectSql = "SELECT ssn FROM " + TABLE + " WHERE name = ?";
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
    // Verify encryption metadata was created
    String metadataSql =
        "SELECT table_name, column_name, encryption_algorithm FROM "
            + METADATA_SCHEMA
            + ".encryption_metadata WHERE table_name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(metadataSql)) {
      pstmt.setString(1, TABLE);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(TABLE, rs.getString("table_name"));
        assertEquals("ssn", rs.getString("column_name"));
        assertEquals("AES-256-GCM", rs.getString("encryption_algorithm"));
      }
    }

    // Verify the data key row references the configured KMS master key
    String keyStorageSql =
        "SELECT COUNT(*), MAX(master_key_arn) FROM "
            + METADATA_SCHEMA
            + ".key_storage WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(keyStorageSql)) {
      pstmt.setString(1, KEY_NAME);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(kmsKeyId, rs.getString(2));
      }
    }
  }

  /**
   * Reporter statement 1: a literal before the encrypted column. The automatic mapping must map
   * parameter 1 to {@code ssn}, not to {@code name}, so the SSN is stored encrypted.
   */
  @TestTemplate
  void testReporterLiteralBeforeEncryptedColumnIsEncrypted() throws Exception {
    final String name = "Reporter A";
    final String ssn = "123-45-6789";
    final String email = "reporter.a@test.com";
    deleteUsersByName(name);

    try (PreparedStatement pstmt = connection.prepareStatement(
        "INSERT INTO " + TABLE + " (name, ssn, email) VALUES ('" + name + "', ?, ?)")) {
      pstmt.setString(1, ssn);
      pstmt.setString(2, email);
      assertEquals(1, pstmt.executeUpdate());
    }

    assertWrapperRoundTrip(name, ssn);
    assertStoredEncrypted(name, ssn);
  }

  /**
   * Reporter statement 2: a {@code ?} inside a string literal before an annotated parameter. The
   * annotation must be attached to the real placeholder, so the SSN is stored encrypted.
   */
  @TestTemplate
  void testReporterQuestionMarkLiteralWithAnnotationIsEncrypted() throws Exception {
    final String name = "Reporter B";
    final String ssn = "987-65-4321";
    deleteUsersByName(name);

    try (PreparedStatement pstmt = connection.prepareStatement(
        "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, ?, ?)")) {
      pstmt.setString(1, name);
      pstmt.setString(2, "000-00-0000");
      pstmt.setString(3, "reporter.b@test.com");
      assertEquals(1, pstmt.executeUpdate());
    }

    try (PreparedStatement pstmt = connection.prepareStatement(
        "UPDATE " + TABLE + " SET email = 'really?', ssn = /*@encrypt:" + TABLE + ".ssn*/ ? WHERE name = '"
            + name + "'")) {
      pstmt.setString(1, ssn);
      assertEquals(1, pstmt.executeUpdate());
    }

    assertWrapperRoundTrip(name, ssn);
    assertStoredEncrypted(name, ssn);
  }

  /**
   * An INSERT without a column list on a table with encrypted columns cannot be mapped, so the
   * plugin must reject it at prepare time (default encryptionFailOnUnmappedParameter=true).
   */
  @TestTemplate
  void testUnmappableInsertIsRejected() {
    final SQLException e = assertThrows(SQLException.class,
        () -> connection.prepareStatement("INSERT INTO " + TABLE + " VALUES (DEFAULT, ?, ?, ?)"));
    assertTrue(e.getMessage() != null
            && e.getMessage().contains(EncryptionConfig.FAIL_ON_UNMAPPED_PARAMETER.name),
        "Unexpected message: " + e.getMessage());
  }

  private static void deleteUsersByName(final String name) throws SQLException {
    try (Connection plainConn = DriverManager.getConnection(
            ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
        PreparedStatement pstmt = plainConn.prepareStatement("DELETE FROM " + TABLE + " WHERE name = ?")) {
      pstmt.setString(1, name);
      pstmt.executeUpdate();
    }
  }

  private static void assertWrapperRoundTrip(final String name, final String ssn)
      throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT ssn FROM " + TABLE + " WHERE name = ?")) {
      pstmt.setString(1, name);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next(), "Should find inserted user");
        assertEquals(ssn, rs.getString("ssn"), "SSN should be decrypted through the wrapper");
      }
    }
  }

  private static void assertStoredEncrypted(final String name, final String ssn)
      throws SQLException {
    final String sql = isPg
        ? "SELECT ssn, has_valid_hmac_structure(ssn) AS valid_structure FROM " + TABLE + " WHERE name = ?"
        : "SELECT ssn FROM " + TABLE + " WHERE name = ?";
    try (Connection plainConn = DriverManager.getConnection(
            ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
        PreparedStatement pstmt = plainConn.prepareStatement(sql)) {
      pstmt.setString(1, name);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next(), "Should find inserted user");
        final byte[] stored = rs.getBytes("ssn");
        assertNotNull(stored, "Stored SSN must not be null");
        assertTrue(stored.length >= MIN_ENCRYPTED_LENGTH,
            "Stored SSN is too short to be ciphertext: " + stored.length);
        assertFalse(Arrays.equals(ssn.getBytes(StandardCharsets.UTF_8), stored),
            "Stored SSN must not be the plaintext");
        if (isPg) {
          assertTrue(rs.getBoolean("valid_structure"),
              "Stored SSN should have a valid HMAC structure");
        }
      }
    }
  }

  @TestTemplate
  void testEncryptedDataTypeHmacVerification() throws Exception {
    // Insert test data
    String insertSql = "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, ?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, "HMAC Test User");
      pstmt.setString(2, "999-99-9999");
      pstmt.setString(3, "hmac@test.com");
      assertEquals(1, pstmt.executeUpdate());
    }

    if (isPg) {
      // Verify HMAC structure at database level (doesn't require key)
      String structureCheckSql =
          "SELECT name, has_valid_hmac_structure(ssn) as valid_structure FROM " + TABLE + " WHERE name = ?";
      try (PreparedStatement pstmt = connection.prepareStatement(structureCheckSql)) {
        pstmt.setString(1, "HMAC Test User");
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
          assertTrue(
              rs.getBoolean("valid_structure"), "Encrypted data should have valid HMAC structure");
          LOGGER.info("HMAC structure validation passed for encrypted SSN");
        }
      }
    } else {
      // MySQL has no has_valid_hmac_structure: check the stored length on a plain connection.
      // [HMAC 32][type 1][IV 12][ciphertext + GCM tag 16] is at least 61 bytes.
      try (Connection plainConn = DriverManager.getConnection(
          ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
           PreparedStatement pstmt = plainConn.prepareStatement(
               "SELECT LENGTH(ssn) FROM " + TABLE + " WHERE name = ?")) {
        pstmt.setString(1, "HMAC Test User");
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
          assertTrue(rs.getInt(1) >= 61, "Encrypted data should have valid HMAC structure");
        }
      }
    }

    // Verify we can still decrypt the data
    String selectSql = "SELECT ssn FROM " + TABLE + " WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setString(1, "HMAC Test User");
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("999-99-9999", rs.getString("ssn"));
        LOGGER.info("Successfully decrypted SSN with HMAC verification");
      }
    }
  }

  @TestTemplate
  void testPlainTextFails() {
    // On PostgreSQL the encrypted_data domain rejects the value; on MySQL the HMAC trigger does.
    if (!isPg) {
      assumeTrue(mysqlTriggersInstalled, "The MySQL HMAC validation triggers could not be installed");
    }
    // make sure we cannot insert plain text into the ssn column
    assertThrows(SQLException.class, () -> {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("INSERT INTO " + TABLE + " (name, ssn, email) VALUES ('Dave', '111', 'XXXXXXXXXXXXX')");
      }
    });
  }

  @TestTemplate
  void testDataSourceEncryption() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerName(TestEnvironment.getCurrent()
        .getInfo().getDatabaseInfo().getInstances().get(0).getHost());
    ds.setDatabase(TestEnvironment.getCurrent()
        .getInfo().getDatabaseInfo().getDefaultDbName());
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());
    ds.setTargetDataSourceProperties(pluginProps());

    try (Connection dsConn = ds.getConnection()) {
      // Insert via DataSource connection
      try (PreparedStatement pstmt = dsConn.prepareStatement(
          "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, ?, ?)")) {
        pstmt.setString(1, "DataSource Test");
        pstmt.setString(2, "444-44-4444");
        pstmt.setString(3, "ds@test.com");
        pstmt.executeUpdate();
      }

      // Read back and verify decryption
      try (PreparedStatement pstmt = dsConn.prepareStatement(
          "SELECT name, ssn, email FROM " + TABLE + " WHERE name = ?")) {
        pstmt.setString(1, "DataSource Test");
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("DataSource Test", rs.getString("name"));
          assertEquals("444-44-4444", rs.getString("ssn"));
          assertEquals("ds@test.com", rs.getString("email"));
        }
      }
    }
  }

  @TestTemplate
  void testDataSourceEncryptionVerifyStoredEncrypted() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerName(TestEnvironment.getCurrent()
        .getInfo().getDatabaseInfo().getInstances().get(0).getHost());
    ds.setDatabase(TestEnvironment.getCurrent()
        .getInfo().getDatabaseInfo().getDefaultDbName());
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());
    ds.setTargetDataSourceProperties(pluginProps());

    // Insert via encrypted DataSource
    try (Connection dsConn = ds.getConnection()) {
      try (PreparedStatement pstmt = dsConn.prepareStatement(
          "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, ?, ?)")) {
        pstmt.setString(1, "DS Verify Test");
        pstmt.setString(2, "555-55-5555");
        pstmt.setString(3, "dsverify@test.com");
        pstmt.executeUpdate();
      }
    }

    // Read via plain connection — SSN should be encrypted (not readable as plaintext)
    Properties plainProps = ConnectionStringHelper.getDefaultProperties();
    String plainUrl = ConnectionStringHelper.getUrl();
    try (Connection plainConn = DriverManager.getConnection(plainUrl, plainProps);
        PreparedStatement pstmt = plainConn.prepareStatement(
            "SELECT name, ssn FROM " + TABLE + " WHERE name = ?")) {
      pstmt.setString(1, "DS Verify Test");
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("DS Verify Test", rs.getString("name"));
        assertNotEquals("555-55-5555", rs.getString("ssn"));
      }
    }
  }

  @TestTemplate
  void testEncryptionWithAnnotations() throws Exception {
    LOGGER.info("Starting testEncryptionWithAnnotations");

    // Insert using annotation syntax
    String insertSql =
        "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, /*@encrypt:" + TABLE + ".ssn*/ ?, ?)";
    try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
      stmt.setString(1, "Alice Annotation");
      stmt.setString(2, "111-22-3333"); // Will be encrypted via annotation
      stmt.setString(3, "alice@example.com");
      stmt.executeUpdate();
      LOGGER.info("Inserted user with annotation-based encryption");
    }

    // Query and verify decryption
    try (PreparedStatement stmt = connection.prepareStatement(
        "SELECT name, ssn, email FROM " + TABLE + " WHERE name = ?")) {
      stmt.setString(1, "Alice Annotation");
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Should find inserted user");
        assertEquals("Alice Annotation", rs.getString("name"));
        assertEquals("111-22-3333", rs.getString("ssn"), "SSN should be decrypted");
        assertEquals("alice@example.com", rs.getString("email"));
      }

      LOGGER.info("Successfully verified annotation-based encryption/decryption");
    }
  }

  @TestTemplate
  void testAnnotationOverridesMetadata() throws Exception {
    LOGGER.info("Starting testAnnotationOverridesMetadata");

    // Insert using annotation - this explicitly tells the plugin to encrypt
    // even if we're inserting into a different column position
    String insertSql =
        "INSERT INTO " + TABLE + " (name, email, ssn) VALUES (?, ?, /*@encrypt:" + TABLE + ".ssn*/ ?)";
    try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
      stmt.setString(1, "Bob Annotation");
      stmt.setString(2, "bob@example.com");
      stmt.setString(3, "222-33-4444"); // Encrypted via annotation
      stmt.executeUpdate();
      LOGGER.info("Inserted user with annotation in different column order");
    }

    // Verify decryption works
    try (PreparedStatement stmt = connection.prepareStatement(
        "SELECT name, ssn FROM " + TABLE + " WHERE name = ?")) {
      stmt.setString(1, "Bob Annotation");
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Bob Annotation", rs.getString("name"));
        assertEquals("222-33-4444", rs.getString("ssn"), "SSN should be decrypted");
      }

      LOGGER.info("Successfully verified annotation with different column order");
    }
  }

  @TestTemplate
  void testAnnotationWithUpdate() throws Exception {
    LOGGER.info("Starting testAnnotationWithUpdate");

    // Insert initial data
    try (PreparedStatement stmt = connection.prepareStatement(
        "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, /*@encrypt:" + TABLE + ".ssn*/ ?, ?)")) {
      stmt.setString(1, "Dave Update");
      stmt.setString(2, "444-55-6666");
      stmt.setString(3, "dave@example.com");
      stmt.executeUpdate();
    }

    // Update using annotation
    String updateSql = "UPDATE " + TABLE + " SET ssn = /*@encrypt:" + TABLE + ".ssn*/ ? WHERE name = ?";
    try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
      stmt.setString(1, "555-66-7777"); // New encrypted value
      stmt.setString(2, "Dave Update");
      int updated = stmt.executeUpdate();
      assertEquals(1, updated, "Should update one row");
      LOGGER.info("Updated SSN with annotation");
    }

    // Verify updated value
    try (PreparedStatement stmt = connection.prepareStatement("SELECT ssn FROM " + TABLE + " WHERE name = ?")) {
      stmt.setString(1, "Dave Update");
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("555-66-7777", rs.getString("ssn"), "Should have new encrypted value");
      }

      LOGGER.info("Successfully verified annotation-based update");
    }
  }

  /**
   * Covers the case an annotation actually exists for: a parameter whose <em>position</em> does not
   * correspond to the annotated column.
   *
   * <p>The other annotation tests above all place the annotation on a parameter whose index already
   * lines up with the target column, so the positional column mapping derived from the parsed
   * statement resolves {@code ssn} on its own and the annotation makes no difference. That is why a
   * defect in the annotation path was able to survive them: the metadata lookup key was composed as
   * {@code tableName + "." + columnName} from an already-qualified annotation value, producing
   * {@code kms_enc_users.kms_enc_users.ssn}, missing, and silently skipping encryption — but the
   * positional mapping covered for it.
   *
   * <p>Here a literal in the {@code VALUES} list shifts the placeholder indices: the column list is
   * {@code (name, email, ssn)} so the positional mapping is {@code 1 -> name, 2 -> email,
   * 3 -> ssn}, while there are only two placeholders, parameter 1 being {@code email} and parameter
   * 2 being {@code ssn}. Nothing but the annotation can tell the plugin that parameter 2 belongs to
   * the encrypted column, so this test exercises the annotation path in isolation.
   *
   * <p>Verification reads the column back through a plain driver connection, bypassing the wrapper,
   * because a round trip through the wrapper alone cannot distinguish a value that was encrypted
   * from one that was stored as-is.
   */
  @TestTemplate
  void testAnnotationOnPositionallyMismatchedParameter() throws Exception {
    LOGGER.info("Starting testAnnotationOnPositionallyMismatchedParameter");

    final String name = "Positional Mismatch";
    final String ssn = "666-77-8888";
    final String email = "mismatch@example.com";

    final String insertSql = "INSERT INTO " + TABLE + " (name, email, ssn) VALUES ('" + name
        + "', ?, /*@encrypt:" + TABLE + ".ssn*/ ?)";
    try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
      stmt.setString(1, email);
      stmt.setString(2, ssn);
      stmt.executeUpdate();
      LOGGER.info("Inserted user with an annotation on a positionally mismatched parameter");
    }

    // The wrapper must still round-trip the value.
    try (PreparedStatement stmt = connection.prepareStatement(
        "SELECT name, ssn, email FROM " + TABLE + " WHERE name = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Should find inserted user");
        assertEquals(ssn, rs.getString("ssn"), "SSN should be decrypted through the wrapper");
        assertEquals(email, rs.getString("email"));
      }
    }

    // Read the stored column with a plain connection: it must not be the plaintext SSN.
    final Properties plainProps = ConnectionStringHelper.getDefaultProperties();
    final String plainUrl = ConnectionStringHelper.getUrl();
    try (Connection plainConn = DriverManager.getConnection(plainUrl, plainProps);
        PreparedStatement stmt = plainConn.prepareStatement(
            "SELECT ssn FROM " + TABLE + " WHERE name = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertNotEquals(ssn, rs.getString("ssn"),
            "The annotated parameter must have been encrypted before it reached the database");
      }
    }

    LOGGER.info("Successfully verified annotation on a positionally mismatched parameter");
  }

  @TestTemplate
  void testAwsProfileSupport() throws Exception {
    LOGGER.info("Starting testAwsProfileSupport");

    // Test that awsProfile property is accepted and doesn't cause errors
    // Note: We can't test actual profile switching in integration tests,
    // but we can verify the property is processed without errors

    Properties propsWithProfile = pluginProps();
    propsWithProfile.setProperty(EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.name, METADATA_SCHEMA);

    // Set awsProfile property (will use default credentials if profile doesn't exist)
    propsWithProfile.setProperty(PropertyDefinition.AWS_PROFILE.name, "default");

    String url = ConnectionStringHelper.getWrapperUrl();

    // Should not throw exception even with awsProfile set
    try (Connection connWithProfile = DriverManager.getConnection(url, propsWithProfile)) {
      LOGGER.info("Successfully created connection with awsProfile property");

      // Verify encryption still works with profile property set
      try (PreparedStatement stmt = connWithProfile.prepareStatement(
          "INSERT INTO " + TABLE + " (name, ssn, email) VALUES (?, ?, ?)")) {
        stmt.setString(1, "Profile Test");
        stmt.setString(2, "999-99-9999");
        stmt.setString(3, "profile@test.com");
        stmt.executeUpdate();
        LOGGER.info("Successfully inserted data with awsProfile property");
      }

      // Verify decryption works
      try (PreparedStatement stmt = connWithProfile.prepareStatement(
          "SELECT ssn FROM " + TABLE + " WHERE name = ?")) {
        stmt.setString(1, "Profile Test");
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Should find inserted row");
          assertEquals("999-99-9999", rs.getString("ssn"),
              "Should decrypt SSN correctly with awsProfile property");
        }

        LOGGER.info("Successfully verified encryption/decryption with awsProfile property");
      }
    }
  }
}
