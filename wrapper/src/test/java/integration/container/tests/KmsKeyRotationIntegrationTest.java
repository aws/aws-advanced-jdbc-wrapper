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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.KmsEncryptionConnectionPlugin;
import software.amazon.jdbc.plugin.encryption.key.DataKeyReencryptionResult;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementException;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementUtility;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;
import software.amazon.jdbc.util.Messages;

/**
 * Integration tests for KMS encryption plugin data key rotation against a real database and real
 * AWS KMS: the legacy {@link KeyManagementUtility#rotateDataKey}, {@link
 * KeyManagementUtility#rotateDataKeyAndReencrypt} and {@link
 * KeyManagementUtility#reencryptRemainingRows}.
 *
 * <p>Requires {@code KMS_KEY_ID} (preferably a key ARN; {@code AWS_KMS_KEY_ARN} is used as a
 * fallback) and AWS credentials in the test container. The optional {@code KMS_KEY_ID_2} enables
 * the rotation under a second master key. Each test uses its own table and key rows and removes
 * them afterwards. KMS keys are never created or deleted.
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
public class KmsKeyRotationIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(KmsKeyRotationIntegrationTest.class.getName());

  private static final String KMS_KEY_ID_ENV = "KMS_KEY_ID";
  private static final String KMS_KEY_ARN_ENV = "AWS_KMS_KEY_ARN";
  private static final String KMS_KEY_ID_2_ENV = "KMS_KEY_ID_2";
  private static final String METADATA_SCHEMA = "encrypt";
  private static final String HMAC_FAILURE = "HMAC verification failed";
  private static final String SSN = "ssn";
  private static final String SCORE = "score";
  private static final String PK = "id";
  private static final SecureRandom RANDOM = new SecureRandom();

  private static String kmsKeyId;
  private static String secondKmsKeyId;
  private static String region;
  private static boolean isPg;
  private static KmsClient kmsClient;
  private static EncryptionConfig encryptionConfig;

  private final List<String> tables = new ArrayList<>();
  private final List<Connection> connections = new ArrayList<>();
  private final List<MetadataManager> metadataManagers = new ArrayList<>();

  @BeforeAll
  static void setUp() throws Exception {
    kmsKeyId = firstNonEmpty(System.getenv(KMS_KEY_ID_ENV), System.getenv(KMS_KEY_ARN_ENV));
    assumeTrue(kmsKeyId != null,
        "A KMS key must be provided via " + KMS_KEY_ID_ENV + " or " + KMS_KEY_ARN_ENV);
    secondKmsKeyId = firstNonEmpty(System.getenv(KMS_KEY_ID_2_ENV));

    region = firstNonEmpty(regionFromArn(kmsKeyId), TestEnvironment.getCurrent().getInfo().getRegion());
    assumeTrue(region != null,
        "The KMS region is unknown: pass " + KMS_KEY_ID_ENV + " as a key ARN or run on an AWS deployment");

    isPg = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG;
    kmsClient = KmsClient.builder().region(Region.of(region)).build();
    encryptionConfig = EncryptionConfig.builder()
        .kmsRegion(region)
        .encryptionMetadataSchema(METADATA_SCHEMA)
        .build();

    final SchemaName metadataSchema = SchemaName.of(METADATA_SCHEMA);
    try (Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
         Statement stmt = conn.createStatement()) {
      if (isPg) {
        stmt.execute("DROP SCHEMA IF EXISTS " + metadataSchema + " CASCADE");
      } else {
        stmt.execute("DROP SCHEMA IF EXISTS " + metadataSchema);
      }
      stmt.execute("CREATE SCHEMA " + metadataSchema);

      if (isPg) {
        stmt.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        EncryptedDataTypeInstaller.installEncryptedDataType(conn, metadataSchema);
        stmt.execute(
            "CREATE TABLE " + metadataSchema + ".key_storage ("
                + "id SERIAL PRIMARY KEY, "
                + "name VARCHAR(255) NOT NULL, "
                + "master_key_arn VARCHAR(512) NOT NULL, "
                + "encrypted_data_key TEXT NOT NULL, "
                + "hmac_key BYTEA NOT NULL, "
                + "key_spec VARCHAR(50) DEFAULT 'AES_256', "
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
                + "last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)");
        stmt.execute(
            "CREATE TABLE " + metadataSchema + ".encryption_metadata ("
                + "table_name VARCHAR(255) NOT NULL, "
                + "column_name VARCHAR(255) NOT NULL, "
                + "encryption_algorithm VARCHAR(50) NOT NULL, "
                + "key_id INTEGER NOT NULL, "
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
                + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
                + "PRIMARY KEY (table_name, column_name), "
                + "FOREIGN KEY (key_id) REFERENCES " + metadataSchema + ".key_storage(id))");
      } else {
        stmt.execute(
            "CREATE TABLE " + metadataSchema + ".key_storage ("
                + "id INT AUTO_INCREMENT PRIMARY KEY, "
                + "name VARCHAR(255) NOT NULL, "
                + "master_key_arn VARCHAR(512) NOT NULL, "
                + "encrypted_data_key TEXT NOT NULL, "
                + "hmac_key VARBINARY(32) NOT NULL, "
                + "key_spec VARCHAR(50) DEFAULT 'AES_256', "
                + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
        stmt.execute(
            "CREATE TABLE " + metadataSchema + ".encryption_metadata ("
                + "table_name VARCHAR(255) NOT NULL, "
                + "column_name VARCHAR(255) NOT NULL, "
                + "encryption_algorithm VARCHAR(50) NOT NULL, "
                + "key_id INT NOT NULL, "
                + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                + "PRIMARY KEY (table_name, column_name), "
                + "FOREIGN KEY (key_id) REFERENCES " + metadataSchema + ".key_storage(id))");
      }
    }
    LOGGER.finest("Key rotation test setup completed");
  }

  @AfterAll
  static void tearDown() {
    if (kmsClient != null) {
      kmsClient.close();
      kmsClient = null;
    }
  }

  @AfterEach
  void cleanUp() throws SQLException {
    for (Connection conn : connections) {
      try {
        conn.close();
      } catch (SQLException e) {
        LOGGER.finest("Closing a test connection failed: " + e.getMessage());
      }
    }
    connections.clear();
    for (MetadataManager metadataManager : metadataManagers) {
      metadataManager.shutdown();
    }
    metadataManagers.clear();
    if (tables.isEmpty()) {
      return;
    }
    try (Connection conn = openPlainConnection()) {
      for (String table : tables) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("DROP TABLE IF EXISTS " + table);
        }
        try (PreparedStatement stmt = conn.prepareStatement(
            "DELETE FROM " + METADATA_SCHEMA + ".encryption_metadata WHERE table_name = ?")) {
          stmt.setString(1, table);
          stmt.executeUpdate();
        }
        // Only the database rows are removed. The KMS keys are never touched.
        try (PreparedStatement stmt = conn.prepareStatement(
            "DELETE FROM " + METADATA_SCHEMA + ".key_storage WHERE name LIKE ?")) {
          stmt.setString(1, "key-" + table + "-%");
          stmt.executeUpdate();
        }
      }
    }
    tables.clear();
  }

  /** Documents the reported behavior: the legacy method switches the key without re-encrypting. */
  @TestTemplate
  void test01LegacyRotateDataKeyLeavesOldRowsUndecryptable() throws Exception {
    final String table = createEncryptedTable("legacy", SSN);
    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 1, 5, false);
    }
    final int oldKeyId = currentKeyId(table, SSN);
    final Map<Integer, byte[]> rawBefore = readRawValues(table, SSN);
    assertEquals(5, rawBefore.size());

    try (Connection plain = openPlainConnection()) {
      newUtility(plain).rotateDataKey(table, SSN, null);
    }

    final int newKeyId = currentKeyId(table, SSN);
    assertNotEquals(oldKeyId, newKeyId);
    assertRawValuesUnchanged(rawBefore, readRawValues(table, SSN));

    try (Connection reader = openPluginConnection()) {
      for (int i = 1; i <= 5; i++) {
        final int row = i;
        assertHmacFailure(() -> readSsn(reader, table, row));
      }
    }

    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 6, 1, false);
    }
    try (Connection reader = openPluginConnection()) {
      assertEquals(ssnFor(6), readSsn(reader, table, 6));
    }
  }

  /** Several batches, two value types, null values left alone, every row readable afterwards. */
  @TestTemplate
  void test02RotateAndReencryptOverSeveralBatches() throws Exception {
    final String table = createEncryptedTable("batches", SSN, SCORE);
    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 1, 25, true);
      try (PreparedStatement stmt = writer.prepareStatement(
          "INSERT INTO " + table + " (name, ssn, score) VALUES (?, ?, ?)")) {
        stmt.setString(1, "row-null");
        stmt.setNull(2, Types.BINARY);
        stmt.setNull(3, Types.BINARY);
        assertEquals(1, stmt.executeUpdate());
      }
    }

    for (String column : new String[] {SSN, SCORE}) {
      final int oldKeyId = currentKeyId(table, column);
      final Map<Integer, byte[]> rawBefore = readRawValues(table, column);
      assertEquals(26, rawBefore.size());

      final DataKeyReencryptionResult result;
      try (Connection plain = openPlainConnection()) {
        result = newUtility(plain).rotateDataKeyAndReencrypt(table, column, PK, null, 10);
      }

      assertResult(result, 25, 0, 0, 3);
      assertEquals(Collections.singletonList(oldKeyId), result.getSourceKeyIds());
      final int newKeyId = currentKeyId(table, column);
      assertEquals(newKeyId, result.getCurrentKeyId());
      assertNotEquals(oldKeyId, newKeyId);

      final Map<Integer, byte[]> rawAfter = readRawValues(table, column);
      assertEquals(rawBefore.keySet(), rawAfter.keySet());
      int nullValues = 0;
      for (Map.Entry<Integer, byte[]> entry : rawBefore.entrySet()) {
        final byte[] after = rawAfter.get(entry.getKey());
        if (entry.getValue() == null) {
          nullValues++;
          assertNull(after, "Null value of row " + entry.getKey() + " must stay null");
        } else {
          assertNotNull(after);
          assertFalse(Arrays.equals(entry.getValue(), after),
              "Row " + entry.getKey() + " of " + column + " must be re-encrypted");
        }
      }
      assertEquals(1, nullValues);
    }

    assertAllRowsDecrypt(table, 25, true);
    try (Connection reader = openPluginConnection();
         PreparedStatement stmt = reader.prepareStatement(
             "SELECT ssn, score FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, "row-null");
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertNull(rs.getString(SSN));
        rs.getInt(SCORE);
        assertTrue(rs.wasNull());
      }
    }
  }

  /** Recovery of a column after the legacy method was used on it. */
  @TestTemplate
  void test03ReencryptRemainingRowsRecoversAfterLegacyRotation() throws Exception {
    final String table = createEncryptedTable("recover", SSN);
    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 1, 12, false);
    }
    final int oldKeyId = currentKeyId(table, SSN);
    try (Connection plain = openPlainConnection()) {
      newUtility(plain).rotateDataKey(table, SSN, null);
    }
    final int newKeyId = currentKeyId(table, SSN);
    assertNotEquals(oldKeyId, newKeyId);
    try (Connection reader = openPluginConnection()) {
      assertHmacFailure(() -> readSsn(reader, table, 1));
    }

    final DataKeyReencryptionResult result;
    try (Connection plain = openPlainConnection()) {
      result = newUtility(plain).reencryptRemainingRows(
          table, SSN, PK, Collections.singletonList(oldKeyId), 5);
    }

    assertResult(result, 12, 0, 0, 3);
    assertEquals(newKeyId, result.getCurrentKeyId());
    assertEquals(newKeyId, currentKeyId(table, SSN));
    assertAllRowsDecrypt(table, 12, false);
  }

  /**
   * A plugin connection that loaded the metadata before the rotation keeps writing with the old key.
   * Its rows are reported as needing conversion and {@code reencryptRemainingRows} converts them.
   */
  @TestTemplate
  void test04StaleWriterRowsAreRepaired() throws Exception {
    final String table = createEncryptedTable("stale", SSN);
    final Connection staleWriter = openPluginConnection();
    insertRows(staleWriter, table, 1, 10, false);
    final int oldKeyId = currentKeyId(table, SSN);

    final DataKeyReencryptionResult rotation;
    try (Connection plain = openPlainConnection()) {
      rotation = newUtility(plain).rotateDataKeyAndReencrypt(table, SSN, PK, null, 4);
    }
    assertResult(rotation, 10, 0, 0, 3);

    // The stale connection's metadata cache (and, on PostgreSQL, its trigger session cache) still
    // holds the old key, so these rows are encrypted under the old key.
    insertRows(staleWriter, table, 11, 2, false);

    try (Connection reader = openPluginConnection()) {
      for (int i = 1; i <= 10; i++) {
        assertEquals(ssnFor(i), readSsn(reader, table, i));
      }
      for (int i = 11; i <= 12; i++) {
        final int row = i;
        assertHmacFailure(() -> readSsn(reader, table, row));
      }
    }

    final DataKeyReencryptionResult repair;
    try (Connection plain = openPlainConnection()) {
      repair = newUtility(plain).reencryptRemainingRows(
          table, SSN, PK, Collections.singletonList(oldKeyId), 4);
    }
    assertEquals(2, repair.getReencryptedCount());
    assertEquals(10, repair.getAlreadyCurrentCount());
    assertEquals(0, repair.getUnmatchedCount());
    assertTrue(repair.isComplete());
    assertEquals(rotation.getCurrentKeyId(), repair.getCurrentKeyId());

    assertAllRowsDecrypt(table, 12, false);
  }

  /** Running the resume method on a fully converted column changes nothing. */
  @TestTemplate
  void test05ReencryptRemainingRowsIsIdempotent() throws Exception {
    final String table = createEncryptedTable("idem", SSN);
    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 1, 12, false);
    }
    final int oldKeyId = currentKeyId(table, SSN);
    try (Connection plain = openPlainConnection()) {
      assertResult(newUtility(plain).rotateDataKeyAndReencrypt(table, SSN, PK, null, 5), 12, 0, 0, 3);
    }
    final Map<Integer, byte[]> rawAfterRotation = readRawValues(table, SSN);

    for (int run = 1; run <= 2; run++) {
      final DataKeyReencryptionResult result;
      try (Connection plain = openPlainConnection()) {
        result = newUtility(plain).reencryptRemainingRows(
            table, SSN, PK, Collections.singletonList(oldKeyId), 5);
      }
      assertEquals(0, result.getReencryptedCount(), "run " + run);
      assertEquals(12, result.getAlreadyCurrentCount(), "run " + run);
      assertEquals(0, result.getUnmatchedCount(), "run " + run);
      assertTrue(result.isComplete(), "run " + run);
      assertRawValuesUnchanged(rawAfterRotation, readRawValues(table, SSN));
    }

    assertAllRowsDecrypt(table, 12, false);
  }

  /** The utility accepts a wrapper connection without the plugin and rejects one with the plugin. */
  @TestTemplate
  void test06UtilityRunsOnWrapperWithoutKmsPluginAndRejectsPluginConnection() throws Exception {
    final String table = createEncryptedTable("conn", SSN);
    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 1, 6, false);
    }
    final int oldKeyId = currentKeyId(table, SSN);
    final int keyRowsBefore = keyRowCount(table);
    final Map<Integer, byte[]> rawBefore = readRawValues(table, SSN);
    final String expectedMessage = Messages.get("KeyManagementUtility.pluginConnectionNotAllowed");

    try (Connection pluginConn = openPluginConnection()) {
      assertTrue(pluginConn.isWrapperFor(KmsEncryptionConnectionPlugin.class));
      final KeyManagementUtility utility = newUtility(pluginConn);

      final KeyManagementException rotateError = assertThrows(KeyManagementException.class,
          () -> utility.rotateDataKeyAndReencrypt(table, SSN, PK, null, 4));
      assertTrue(rotateError.getMessage().contains(expectedMessage), rotateError.getMessage());

      final KeyManagementException resumeError = assertThrows(KeyManagementException.class,
          () -> utility.reencryptRemainingRows(table, SSN, PK, Collections.singletonList(oldKeyId), 4));
      assertTrue(resumeError.getMessage().contains(expectedMessage), resumeError.getMessage());
    }
    assertEquals(oldKeyId, currentKeyId(table, SSN));
    assertEquals(keyRowsBefore, keyRowCount(table));
    assertRawValuesUnchanged(rawBefore, readRawValues(table, SSN));

    final DataKeyReencryptionResult result;
    try (Connection wrapperConn = openWrapperConnection("executionTime")) {
      assertFalse(wrapperConn.isWrapperFor(KmsEncryptionConnectionPlugin.class));
      result = newUtility(wrapperConn).rotateDataKeyAndReencrypt(table, SSN, PK, null, 4);
    }
    assertResult(result, 6, 0, 0, 2);
    assertNotEquals(oldKeyId, currentKeyId(table, SSN));
    assertEquals(keyRowsBefore + 1, keyRowCount(table));

    assertAllRowsDecrypt(table, 6, false);
  }

  /**
   * The PostgreSQL HMAC trigger caches the HMAC key per session. The rotation must succeed on a
   * session that cached the old key (the utility refreshes the cache with set_config), and a value
   * bound as bytea must compare against the encrypted_data domain. Another session that cached the
   * old key keeps rejecting writes until it is recycled.
   */
  @TestTemplate
  @EnableOnDatabaseEngine(DatabaseEngine.PG)
  void test07PostgresTriggerSessionWithCachedOldHmacKey() throws Exception {
    final String table = createEncryptedTable("trigger", SSN);
    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 1, 15, false);
    }
    final int oldKeyId = currentKeyId(table, SSN);
    final String settingName = "hmac_key." + table + "." + SSN;

    final Connection sessionP = openPlainConnection();
    connections.add(sessionP);
    final Connection sessionQ = openPlainConnection();
    connections.add(sessionQ);
    final int minId = minId(sessionP, table);
    touchRow(sessionP, table, minId);
    assertEquals(hmacKeyHex(oldKeyId), currentSetting(sessionP, settingName));
    touchRow(sessionQ, table, minId);
    assertEquals(hmacKeyHex(oldKeyId), currentSetting(sessionQ, settingName));

    final DataKeyReencryptionResult result =
        newUtility(sessionP).rotateDataKeyAndReencrypt(table, SSN, PK, null, 4);
    assertResult(result, 15, 0, 0, 4);
    final int newKeyId = currentKeyId(table, SSN);
    assertNotEquals(oldKeyId, newKeyId);

    assertEquals(hmacKeyHex(newKeyId), currentSetting(sessionP, settingName));
    touchRow(sessionP, table, minId);

    final byte[] rawValue = readRawValues(table, SSN).get(minId);
    assertNotNull(rawValue);
    try (PreparedStatement stmt = sessionP.prepareStatement(
        "SELECT count(*) FROM " + table + " WHERE ssn = ?")) {
      stmt.setBytes(1, rawValue);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }

    // Session Q still caches the old HMAC key: a write of a value under the new key is rejected.
    final SQLException staleSessionError = assertThrows(SQLException.class, () -> touchRow(sessionQ, table, minId));
    assertTrue(causeChainContains(staleSessionError, HMAC_FAILURE), staleSessionError.getMessage());

    assertAllRowsDecrypt(table, 15, false);
  }

  /** Rotation that moves the column to a second KMS master key. */
  @TestTemplate
  void test08RotateAndReencryptUnderNewMasterKey() throws Exception {
    assumeTrue(secondKmsKeyId != null, KMS_KEY_ID_2_ENV + " is not set; skipping the new master key rotation");
    final String table = createEncryptedTable("newcmk", SSN);
    try (Connection writer = openPluginConnection()) {
      insertRows(writer, table, 1, 8, false);
    }
    final int oldKeyId = currentKeyId(table, SSN);

    final DataKeyReencryptionResult result;
    try (Connection plain = openPlainConnection()) {
      result = newUtility(plain).rotateDataKeyAndReencrypt(table, SSN, PK, secondKmsKeyId, 3);
    }
    assertResult(result, 8, 0, 0, 3);
    final int newKeyId = currentKeyId(table, SSN);
    assertNotEquals(oldKeyId, newKeyId);
    assertEquals(secondKmsKeyId, masterKeyArn(newKeyId));
    assertEquals(kmsKeyId, masterKeyArn(oldKeyId));

    assertAllRowsDecrypt(table, 8, false);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Connection openPlainConnection() throws SQLException {
    return DriverManager.getConnection(ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
  }

  private Connection openWrapperConnection(String plugins) throws SQLException {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, plugins);
    final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    connections.add(conn);
    return conn;
  }

  /**
   * Opens a connection with the KMS encryption plugin. Its metadata cache is loaded on the first
   * prepareStatement and is not refreshed for the rest of the test.
   */
  private Connection openPluginConnection() throws SQLException {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_REGION.name, region);
    props.setProperty(EncryptionConfig.METADATA_CACHE_REFRESH_INTERVAL_MS.name, "3600000");
    final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    connections.add(conn);
    return conn;
  }

  /** Builds the utility the way the plugin guide's code sample does. */
  private KeyManagementUtility newUtility(Connection conn) {
    final KeyManager keyManager = new KeyManager(kmsClient, conn, isPg, encryptionConfig);
    final MetadataManager metadataManager = new MetadataManager(conn, encryptionConfig);
    metadataManagers.add(metadataManager);
    return new KeyManagementUtility(keyManager, metadataManager, conn, kmsClient, encryptionConfig);
  }

  /** Creates a table with a unique name and one data key per encrypted column. */
  private String createEncryptedTable(String scenario, String... encryptedColumns) throws Exception {
    final String table = "kms_rot_" + scenario + "_" + randomHex(6);
    tables.add(table);
    final StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(table).append(" (")
        .append(isPg ? "id SERIAL PRIMARY KEY, " : "id INT AUTO_INCREMENT PRIMARY KEY, ")
        .append("name VARCHAR(100)");
    for (String column : encryptedColumns) {
      ddl.append(", ").append(column).append(isPg ? " encrypted_data" : " VARBINARY(256)");
    }
    ddl.append(")");

    try (Connection conn = openPlainConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl.toString());
        if (isPg) {
          for (String column : encryptedColumns) {
            stmt.execute("CREATE TRIGGER " + table + "_" + column + "_hmac "
                + "BEFORE INSERT OR UPDATE ON " + table + " "
                + "FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('" + column + "')");
          }
        }
      }
      final KeyManagementUtility utility = newUtility(conn);
      for (String column : encryptedColumns) {
        utility.generateAndStoreDataKey(table, column, kmsKeyId, null);
      }
    }
    return table;
  }

  private static void insertRows(Connection pluginConn, String table, int from, int count, boolean withScore)
      throws SQLException {
    final String sql = withScore
        ? "INSERT INTO " + table + " (name, ssn, score) VALUES (?, ?, ?)"
        : "INSERT INTO " + table + " (name, ssn) VALUES (?, ?)";
    try (PreparedStatement stmt = pluginConn.prepareStatement(sql)) {
      for (int i = from; i < from + count; i++) {
        stmt.setString(1, nameFor(i));
        stmt.setString(2, ssnFor(i));
        if (withScore) {
          stmt.setInt(3, scoreFor(i));
        }
        assertEquals(1, stmt.executeUpdate());
      }
    }
  }

  private static String nameFor(int row) {
    return "row-" + row;
  }

  private static String ssnFor(int row) {
    return String.format("%03d-00-%04d", row, row);
  }

  private static int scoreFor(int row) {
    return 1000 + row;
  }

  private static String readSsn(Connection pluginConn, String table, int row) throws SQLException {
    try (PreparedStatement stmt = pluginConn.prepareStatement("SELECT ssn FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, nameFor(row));
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Row " + row + " not found");
        return rs.getString(SSN);
      }
    }
  }

  /** Reads every non-null row through a new plugin connection, i.e. with a fresh metadata cache. */
  private void assertAllRowsDecrypt(String table, int expectedRows, boolean withScore) throws SQLException {
    final String sql = "SELECT id, name, ssn" + (withScore ? ", score" : "") + " FROM " + table
        + " WHERE ssn IS NOT NULL ORDER BY id";
    int rows = 0;
    try (Connection reader = openPluginConnection();
         PreparedStatement stmt = reader.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        rows++;
        final String name = rs.getString("name");
        final int row = Integer.parseInt(name.substring("row-".length()));
        assertEquals(ssnFor(row), rs.getString(SSN), "ssn of " + name);
        if (withScore) {
          assertEquals(scoreFor(row), rs.getInt(SCORE), "score of " + name);
        }
      }
    }
    assertEquals(expectedRows, rows);
  }

  private static void assertHmacFailure(Executable executable) {
    final SQLException e = assertThrows(SQLException.class, executable);
    assertTrue(causeChainContains(e, HMAC_FAILURE), "Expected an HMAC failure, got: " + e);
  }

  private static boolean causeChainContains(Throwable error, String text) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(text)) {
        return true;
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return false;
  }

  private static void assertResult(
      DataKeyReencryptionResult result, long reencrypted, long alreadyCurrent, long unmatched, int batches) {
    assertEquals(reencrypted, result.getReencryptedCount(), "reencrypted: " + result);
    assertEquals(alreadyCurrent, result.getAlreadyCurrentCount(), "alreadyCurrent: " + result);
    assertEquals(unmatched, result.getUnmatchedCount(), "unmatched: " + result);
    assertEquals(batches, result.getBatchCount(), "batches: " + result);
    assertEquals(unmatched == 0, result.isComplete(), "complete: " + result);
  }

  private static void assertRawValuesUnchanged(Map<Integer, byte[]> before, Map<Integer, byte[]> after) {
    assertEquals(before.keySet(), after.keySet());
    for (Map.Entry<Integer, byte[]> entry : before.entrySet()) {
      assertArrayEquals(entry.getValue(), after.get(entry.getKey()), "Row " + entry.getKey() + " changed");
    }
  }

  /** Stored bytes of a column by primary key, read on a plain connection. */
  private static Map<Integer, byte[]> readRawValues(String table, String column) throws SQLException {
    final Map<Integer, byte[]> values = new LinkedHashMap<>();
    try (Connection conn = openPlainConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT id, " + column + " FROM " + table + " ORDER BY id")) {
      while (rs.next()) {
        values.put(rs.getInt(1), rs.getBytes(2));
      }
    }
    return values;
  }

  private static int currentKeyId(String table, String column) throws SQLException {
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement(
             "SELECT key_id FROM " + METADATA_SCHEMA + ".encryption_metadata "
                 + "WHERE table_name = ? AND column_name = ?")) {
      stmt.setString(1, table);
      stmt.setString(2, column);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "No encryption metadata for " + table + "." + column);
        return rs.getInt(1);
      }
    }
  }

  private static String hmacKeyHex(int keyId) throws SQLException {
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement(
             "SELECT hmac_key FROM " + METADATA_SCHEMA + ".key_storage WHERE id = ?")) {
      stmt.setInt(1, keyId);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Key " + keyId + " not found");
        final StringBuilder hex = new StringBuilder();
        for (byte b : rs.getBytes(1)) {
          hex.append(String.format("%02x", b));
        }
        return hex.toString();
      }
    }
  }

  private static String masterKeyArn(int keyId) throws SQLException {
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement(
             "SELECT master_key_arn FROM " + METADATA_SCHEMA + ".key_storage WHERE id = ?")) {
      stmt.setInt(1, keyId);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Key " + keyId + " not found");
        return rs.getString(1);
      }
    }
  }

  private static int keyRowCount(String table) throws SQLException {
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement(
             "SELECT count(*) FROM " + METADATA_SCHEMA + ".key_storage WHERE name LIKE ?")) {
      stmt.setString(1, "key-" + table + "-%");
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getInt(1);
      }
    }
  }

  private static int minId(Connection conn, String table) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT MIN(id) FROM " + table)) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }

  /** Rewrites the row's own value, which fires the PostgreSQL HMAC trigger in this session. */
  private static void touchRow(Connection conn, String table, int id) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("UPDATE " + table + " SET ssn = ssn WHERE id = ?")) {
      stmt.setInt(1, id);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  private static String currentSetting(Connection conn, String name) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("SELECT current_setting(?, true)")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString(1);
      }
    }
  }

  private static String randomHex(int length) {
    final StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(Character.forDigit(RANDOM.nextInt(16), 16));
    }
    return sb.toString();
  }

  private static String regionFromArn(String keyId) {
    // arn:aws:kms:<region>:<account>:key/<id>
    if (keyId == null || !keyId.startsWith("arn:")) {
      return null;
    }
    final String[] parts = keyId.split(":");
    return parts.length > 3 ? parts[3] : null;
  }

  private static String firstNonEmpty(String... values) {
    for (String value : values) {
      if (value != null && !value.trim().isEmpty()) {
        return value.trim();
      }
    }
    return null;
  }
}
