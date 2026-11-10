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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import integration.DatabaseEngine;
import integration.TestEnvironmentFeatures;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.KmsTestUtility;
import integration.util.KmsTestUtility.ColumnKey;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementUtility;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.plugin.encryption.service.EncryptionException;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;

/**
 * Integration tests for the server-side HMAC validation triggers against a real database and real
 * AWS KMS. The plugin encrypts and decrypts on the client, but only a database trigger can stop a
 * plaintext or tampered value from being stored in an encrypted column: it verifies each value's
 * HMAC tag as the value goes in, using the HMAC key in {@code key_storage}, so the database checks
 * integrity without any data key. These tests install the trigger SQL the driver ships in
 * {@code wrapper/src/test/resources/sql} on both engines and confirm that a value in the plugin's
 * encrypted format is accepted while a plaintext or an invalid payload is rejected, including across
 * a data key rotation.
 *
 * <p>Requires {@code KMS_KEY_ID} (preferably a key ARN; {@code AWS_KMS_KEY_ARN} is used as a
 * fallback) and AWS credentials in the test container. On MySQL the validation routines need the
 * privilege to create functions (for example {@code log_bin_trust_function_creators = 1} on
 * Aurora/RDS MySQL); a test is skipped where that privilege is missing.
 */
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
@Order(20)
public class KmsTriggerIntegrationTest {

  private static final String METADATA_SCHEMA = EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue;
  private static final String ALGORITHM = "AES-256-GCM";
  // A test-specific table name, so a DROP TABLE never removes an unrelated table.
  private static final String TABLE = "kms_trig_users";
  private static final String COLUMN = "ssn";
  private static final String KEY_NAME = "test-key-trig-ssn";
  private static final String PLAINTEXT = "123-45-6789";
  // At least 61 bytes, so it passes every length check and only the trigger's HMAC check can reject it
  // (the PostgreSQL domain rejects anything under 45 bytes before a trigger runs).
  private static final String LONG_PLAINTEXT =
      "plain-text-social-security-number-that-is-long-enough-to-pass-length-checks-123";
  private static final SecureRandom RANDOM = new SecureRandom();

  private static String kmsKeyId;
  private static String region;
  private static boolean isPg;
  // Typed as Object on purpose: the KMS SDK is absent from the classpath of non-encryption runs. A
  // field typed KmsClient makes JUnit fail while discovering this class (NoClassDefFoundError),
  // before the class-level conditions can skip it.
  private static Object kmsClient;
  private static EncryptionConfig encryptionConfig;

  private final List<MetadataManager> metadataManagers = new ArrayList<>();
  private ColumnKey columnKey;

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
    kmsClient = KmsTestUtility.kmsClient(region);
    encryptionConfig = EncryptionConfig.builder()
        .kmsRegion(region)
        .encryptionMetadataSchema(METADATA_SCHEMA)
        .build();
  }

  @AfterAll
  static void tearDownClass() {
    if (kmsClient != null) {
      ((KmsClient) kmsClient).close();
      kmsClient = null;
    }
  }

  @AfterEach
  void cleanUp() throws SQLException {
    for (final MetadataManager metadataManager : metadataManagers) {
      metadataManager.shutdown();
    }
    metadataManagers.clear();
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      KmsTestUtility.teardown(conn, SchemaName.of(METADATA_SCHEMA), TABLE);
    }
  }

  // -- common cases, both engines ---------------------------------------------------------------

  @TestTemplate
  void acceptsValueInEncryptedFormat() throws Exception {
    configureColumnWithTriggers(null);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      insertEncrypted(conn, encryptedPayload(PLAINTEXT));
      assertEquals(1, userCount(conn));
    }
  }

  @TestTemplate
  void rejectsPlaintextWrite() throws Exception {
    configureColumnWithTriggers(null);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      final SQLException error = assertThrows(SQLException.class,
          () -> insertEncrypted(conn, LONG_PLAINTEXT.getBytes(StandardCharsets.UTF_8)));
      assertHmacFailure(error);
      assertEquals(0, userCount(conn));
    }
  }

  @TestTemplate
  void rejectsRandomBinaryValue() throws Exception {
    configureColumnWithTriggers(null);
    final byte[] random = new byte[80];
    RANDOM.nextBytes(random);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      final SQLException error =
          assertThrows(SQLException.class, () -> insertEncrypted(conn, random));
      assertHmacFailure(error);
      assertEquals(0, userCount(conn));
    }
  }

  @TestTemplate
  void acceptsValueWithOneBytePlaintext() throws Exception {
    // A boolean serializes to a single plaintext byte. Its payload is 62 bytes (32 HMAC + 1 type +
    // 12 IV + 1 ciphertext + 16 GCM tag), just above the 61-byte floor, so the trigger accepts it.
    configureColumnWithTriggers(null);
    final byte[] payload = encryptedPayload(Boolean.TRUE);
    assertEquals(62, payload.length);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      insertEncrypted(conn, payload);
      assertEquals(1, userCount(conn));
    }
  }

  @TestTemplate
  void acceptsValuesInSeveralTransactionsOnOneConnection() throws Exception {
    configureColumnWithTriggers(null);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      for (int i = 0; i < 3; i++) {
        insertEncrypted(conn, encryptedPayload(PLAINTEXT + "-" + i));
      }
      assertEquals(3, userCount(conn));
    }
  }

  @TestTemplate
  void acceptsUnchangedUpdateAcrossRotation() throws Exception {
    configureColumnWithTriggers(null);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      insertEncrypted(conn, encryptedPayload(PLAINTEXT));
      rotateDataKey();
      // The encrypted value is left untouched, so the trigger does not re-check it against the new
      // key; a plain column update succeeds.
      updateName(conn, "Jane");
      assertEquals(Collections.singletonList("Jane"), names(conn));
    }
  }

  @TestTemplate
  void rejectsNewValueSignedWithPreviousKey() throws Exception {
    configureColumnWithTriggers(null);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      insertEncrypted(conn, encryptedPayload(PLAINTEXT));
      rotateDataKey();
      final byte[] freshUnderPreviousKey = encryptedPayload("987-65-4321");
      final SQLException error =
          assertThrows(SQLException.class, () -> insertEncrypted(conn, freshUnderPreviousKey));
      assertHmacFailure(error);
      assertEquals(1, userCount(conn));
      // Control: a value signed with the current key is accepted, so the rejection above is the key
      // check and not a blanket failure.
      insertEncrypted(conn, encryptedPayloadUnderCurrentKey(conn, "987-65-4321"));
      assertEquals(2, userCount(conn));
    }
  }

  @TestTemplate
  void rejectsChangingValueToOneSignedWithPreviousKey() throws Exception {
    configureColumnWithTriggers(null);
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      final byte[] previous = encryptedPayload(PLAINTEXT);
      insertEncrypted(conn, previous);
      rotateDataKey();
      final byte[] otherUnderPreviousKey = encryptedPayload("987-65-4321");
      final SQLException error =
          assertThrows(SQLException.class, () -> updateSsn(conn, otherUnderPreviousKey));
      assertHmacFailure(error);
      assertArrayEquals(previous, storedSsn(conn));
      // Control: changing the value to one signed with the current key is accepted.
      final byte[] underCurrentKey = encryptedPayloadUnderCurrentKey(conn, "987-65-4321");
      updateSsn(conn, underCurrentKey);
      assertArrayEquals(underCurrentKey, storedSsn(conn));
    }
  }

  // -- PostgreSQL only --------------------------------------------------------------------------

  @TestTemplate
  void postgresErrorMessageCarriesNoHmac() throws Exception {
    assumeTrue(isPg, "PostgreSQL only");
    configureColumnWithTriggers(null);
    final byte[] valid = encryptedPayload(PLAINTEXT);
    final String storedHmacHex = toHex(Arrays.copyOfRange(valid, 0, 32));
    // Same length and structure as a real payload, but the HMAC tag is zeroed, so the value reaches
    // the trigger's HMAC comparison and fails it.
    final byte[] tampered = valid.clone();
    Arrays.fill(tampered, 0, 32, (byte) 0);

    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      final SQLException error =
          assertThrows(SQLException.class, () -> insertEncrypted(conn, tampered));
      final String message = String.valueOf(error.getMessage());
      final String lower = message.toLowerCase(Locale.ROOT);
      assertTrue(message.contains("HMAC verification failed"), message);
      // The message names the column but never an HMAC or any other value a rejected writer could
      // retry with.
      assertFalse(lower.contains(storedHmacHex), message);
      assertFalse(message.contains(Base64.getEncoder().encodeToString(Arrays.copyOfRange(valid, 0, 32))), message);
      assertFalse(message.contains("Stored"), message);
      assertFalse(message.contains("Calculated"), message);
    }
  }

  @TestTemplate
  void postgresSessionDefinedHmacDoesNotBypassTrigger() throws Exception {
    assumeTrue(isPg, "PostgreSQL only");
    configureColumnWithTriggers(null);
    // Bytes 1-32 equal bytes 33-64, so a hijacked hmac() that returns the first 32 bytes of its
    // input would make this invalid value pass the comparison.
    final byte[] collision = collidingPayload();

    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      try {
        // pg_temp is never searched for functions, so the shadow lives in an ordinary schema that
        // the session puts first in its search_path.
        createShadowSchema(conn);
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(
              "CREATE FUNCTION " + SHADOW_SCHEMA + ".hmac(bytea, bytea, text) RETURNS bytea "
                  + "AS $$ SELECT substring($1 from 1 for 32) $$ LANGUAGE sql IMMUTABLE");
          stmt.execute("SET search_path = " + SHADOW_SCHEMA + ", public, pg_catalog");
        }
        // The trigger pins its search_path and calls public.hmac, so the shadow is never used.
        final SQLException error =
            assertThrows(SQLException.class, () -> insertEncrypted(conn, collision));
        assertHmacFailure(error);
        assertEquals(0, userCount(conn));
      } finally {
        dropShadowSchema(conn);
      }
    }
  }

  @TestTemplate
  void postgresSessionDefinedOperatorDoesNotBypassTrigger() throws Exception {
    assumeTrue(isPg, "PostgreSQL only");
    configureColumnWithTriggers(null);
    final byte[] random = new byte[80];
    RANDOM.nextBytes(random);

    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      try {
        createShadowSchema(conn);
        try (Statement stmt = conn.createStatement()) {
          // A <> on bytea that is never true, so a trigger resolving <> through the session's
          // search_path would treat every value as matching its HMAC.
          stmt.execute(
              "CREATE FUNCTION " + SHADOW_SCHEMA + ".never_differs(bytea, bytea) RETURNS boolean "
                  + "AS $$ SELECT false $$ LANGUAGE sql IMMUTABLE");
          stmt.execute(
              "CREATE OPERATOR " + SHADOW_SCHEMA + ".<> (LEFTARG = bytea, RIGHTARG = bytea, "
                  + "FUNCTION = " + SHADOW_SCHEMA + ".never_differs)");
          stmt.execute("SET search_path = " + SHADOW_SCHEMA + ", public, pg_catalog");
        }
        final SQLException error =
            assertThrows(SQLException.class, () -> insertEncrypted(conn, random));
        assertHmacFailure(error);
        assertEquals(0, userCount(conn));
      } finally {
        dropShadowSchema(conn);
      }
    }
  }

  // -- MySQL only -------------------------------------------------------------------------------

  @TestTemplate
  void mysqlAcceptsKeyWithHighBytes() throws Exception {
    assumeTrue(!isPg, "MySQL only");
    // Every byte of this random key is >= 128. The HMAC routine must reproduce these bytes exactly
    // (UNHEX(LPAD(HEX(...))) rather than CHAR(), which would re-encode them through the connection
    // charset and corrupt the key), otherwise a correct payload would fail verification.
    final byte[] highByteKey = new byte[32];
    RANDOM.nextBytes(highByteKey);
    for (int i = 0; i < highByteKey.length; i++) {
      highByteKey[i] |= (byte) 0x80;
    }
    configureColumnWithTriggers(highByteKey);

    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      insertEncrypted(conn, encryptedPayload(PLAINTEXT));
      assertEquals(1, userCount(conn));
    }
  }

  @TestTemplate
  void mysqlValidationRoutinesReadNoSessionVariables() throws Exception {
    assumeTrue(!isPg, "MySQL only");
    configureColumnWithTriggers(null);

    // An earlier version cached the column's HMAC key in a session user variable, which a session
    // could preset to slip a value past the check. The routines now read the key from key_storage for
    // every row, so none of them may reference a user variable (@name); system variables (@@name)
    // are not session-settable state of this kind and are allowed.
    final List<String> routines = Arrays.asList(
        "hmac_sha256", "verify_encrypted_data_hmac",
        "validate_encrypted_data_hmac_before_insert", "validate_encrypted_data_hmac_before_update");
    final Pattern userVariable = Pattern.compile("(?<!@)@(?!@)");
    final List<String> found = new ArrayList<>();
    try (Connection conn = KmsTestUtility.openPlainConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM information_schema.ROUTINES "
                 + "WHERE ROUTINE_SCHEMA = DATABASE()")) {
      while (rs.next()) {
        final String name = rs.getString(1);
        if (!routines.contains(name)) {
          continue;
        }
        found.add(name);
        final String definition = String.valueOf(rs.getString(2));
        assertFalse(userVariable.matcher(definition).find(),
            name + " references a session user variable");
      }
    }
    // Guard against the check passing because the routines were not found.
    Collections.sort(found);
    final List<String> expected = new ArrayList<>(routines);
    Collections.sort(expected);
    assertEquals(expected, found);
  }

  // -- setup and helpers ------------------------------------------------------------------------

  /**
   * Rebuilds a clean metadata schema, application table, column key and HMAC triggers for one test.
   * When {@code hmacKey} is given it is stored for the column instead of a random key.
   */
  private void configureColumnWithTriggers(final byte[] hmacKey) throws Exception {
    final SchemaName metadataSchema = SchemaName.of(METADATA_SCHEMA);
    final boolean installed;
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      KmsTestUtility.createMetadataTables(conn, metadataSchema, isPg);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + TABLE);
      }
      columnKey = hmacKey == null
          ? KmsTestUtility.configureColumn(
              conn, metadataSchema, isPg, region, kmsKeyId, KEY_NAME, TABLE, COLUMN)
          : KmsTestUtility.configureColumnWithHmacKey(
              conn, metadataSchema, isPg, region, kmsKeyId, KEY_NAME, TABLE, COLUMN, hmacKey);
      KmsTestUtility.createAppTable(conn, isPg, TABLE, Collections.singletonList(COLUMN), null);
      installed = KmsTestUtility.installTriggers(conn, metadataSchema, isPg, TABLE, COLUMN);
    }
    // On MySQL the validation routines need the create-routine privilege; a run without it is
    // skipped. On PostgreSQL installTriggers always succeeds or throws.
    assumeTrue(installed, "The HMAC validation triggers could not be installed");
  }

  private static final String SHADOW_SCHEMA = "kms_trig_shadow";

  private static void createShadowSchema(final Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP SCHEMA IF EXISTS " + SHADOW_SCHEMA + " CASCADE");
      stmt.execute("CREATE SCHEMA " + SHADOW_SCHEMA);
    } catch (final SQLException e) {
      assumeTrue(!"42501".equals(e.getSQLState()), "The test user cannot create a schema");
      throw e;
    }
  }

  private static void dropShadowSchema(final Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET search_path = public, pg_catalog");
      stmt.execute("DROP SCHEMA IF EXISTS " + SHADOW_SCHEMA + " CASCADE");
    }
  }

  /** A 80-byte value whose first 32 bytes equal the next 32, and so match a hijacked hmac(). */
  private static byte[] collidingPayload() {
    final byte[] tag = new byte[32];
    RANDOM.nextBytes(tag);
    final byte[] payload = new byte[80];
    RANDOM.nextBytes(payload);
    System.arraycopy(tag, 0, payload, 0, 32);
    System.arraycopy(tag, 0, payload, 32, 32);
    return payload;
  }

  /** Asserts the rejection came from the trigger's HMAC check rather than any other failure. */
  private static void assertHmacFailure(final SQLException error) {
    for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains("HMAC verification failed")) {
        return;
      }
    }
    throw new AssertionError("Expected an HMAC verification failure, got: " + error, error);
  }

  /** Signs a value with the column's current HMAC key, read back after a rotation. */
  private byte[] encryptedPayloadUnderCurrentKey(
      final Connection conn, final Object value) throws Exception {
    final byte[] currentHmacKey;
    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT ks.hmac_key FROM " + METADATA_SCHEMA + ".encryption_metadata em JOIN "
            + METADATA_SCHEMA + ".key_storage ks ON em.key_id = ks.id "
            + "WHERE em.table_name = ? AND em.column_name = ?")) {
      stmt.setString(1, TABLE);
      stmt.setString(2, COLUMN);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        currentHmacKey = rs.getBytes(1);
      }
    }
    // The trigger checks only the HMAC, so the data key used to encrypt is irrelevant to it.
    return new EncryptionService().encrypt(value, columnKey.dataKey(), currentHmacKey, ALGORITHM);
  }

  private byte[] encryptedPayload(final Object value) throws EncryptionException {
    return new EncryptionService().encrypt(value, columnKey.dataKey(), columnKey.hmacKey(), ALGORITHM);
  }

  private void rotateDataKey() throws SQLException {
    try (Connection conn = KmsTestUtility.openPlainConnection()) {
      newUtility(conn).rotateDataKey(TABLE, COLUMN, null);
    }
  }

  private KeyManagementUtility newUtility(final Connection conn) {
    final KmsClient client = (KmsClient) kmsClient;
    final KeyManager keyManager = new KeyManager(client, conn, isPg, encryptionConfig);
    final MetadataManager metadataManager = new MetadataManager(conn, encryptionConfig);
    metadataManagers.add(metadataManager);
    return new KeyManagementUtility(keyManager, metadataManager, conn, client, encryptionConfig);
  }

  private static void insertEncrypted(final Connection conn, final byte[] value) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "INSERT INTO " + TABLE + " (name, " + COLUMN + ") VALUES (?, ?)")) {
      stmt.setString(1, "Jo");
      stmt.setBytes(2, value);
      stmt.executeUpdate();
    }
  }

  private static void updateName(final Connection conn, final String name) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("UPDATE " + TABLE + " SET name = ?")) {
      stmt.setString(1, name);
      stmt.executeUpdate();
    }
  }

  private static void updateSsn(final Connection conn, final byte[] value) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("UPDATE " + TABLE + " SET " + COLUMN + " = ?")) {
      stmt.setBytes(1, value);
      stmt.executeUpdate();
    }
  }

  private static byte[] storedSsn(final Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT " + COLUMN + " FROM " + TABLE)) {
      assertTrue(rs.next());
      return rs.getBytes(1);
    }
  }

  private static List<String> names(final Connection conn) throws SQLException {
    final List<String> result = new ArrayList<>();
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT name FROM " + TABLE + " ORDER BY id")) {
      while (rs.next()) {
        result.add(rs.getString(1));
      }
    }
    return result;
  }

  private static int userCount(final Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE)) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }

  private static String toHex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
