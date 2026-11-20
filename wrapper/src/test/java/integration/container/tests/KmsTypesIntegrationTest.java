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
import integration.util.KmsTestUtility;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;

/**
 * Type coverage for the KMS encryption plugin. Each supported Java type is written to an encrypted
 * column and read back with {@code getObject}. The value must come back equal and with the same
 * type, and the bytes stored in the column, read over a plain connection, must be ciphertext.
 *
 * <p>Requires {@code KMS_KEY_ID} (a key ID or ARN; {@code AWS_KMS_KEY_ARN} is used as a fallback)
 * and AWS credentials in the test container.
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
public class KmsTypesIntegrationTest {

  private static final String METADATA_SCHEMA = EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.defaultValue;
  // A test-specific table name, so that DROP TABLE never removes an unrelated table.
  private static final String TABLE = "kms_types_values";
  private static final String KEY_NAME = "test-key-types-secret";
  // Smallest possible ciphertext: HMAC (32) + type (1) + IV (12) bytes, before the GCM payload.
  private static final int MIN_ENCRYPTED_LENGTH = 45;

  private static Connection connection;
  private static String kmsKeyId;
  private static String region;
  private static boolean isPg;
  private static boolean schemaReady;

  @BeforeAll
  static void setUp() {
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
   * which {@link TestDriverProvider} sets before {@code @BeforeEach} methods run.
   */
  @BeforeEach
  void setUpSchemaOnce() throws Exception {
    if (schemaReady) {
      return;
    }
    final SchemaName metadataSchema = SchemaName.of(METADATA_SCHEMA);
    try (Connection directConnection = DriverManager.getConnection(
        ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties())) {
      KmsTestUtility.createMetadataTables(directConnection, metadataSchema, isPg);
      try (Statement stmt = directConnection.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + TABLE);
      }
      KmsTestUtility.configureColumn(
          directConnection, metadataSchema, isPg, region, kmsKeyId, KEY_NAME, TABLE, "secret");
      KmsTestUtility.createAppTable(
          directConnection, isPg, TABLE,
          Collections.singletonList("secret"), Collections.emptyList());
    }
    connection = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), pluginProps());
    schemaReady = true;
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
    connection = null;

    if (schemaReady) {
      try (Connection directConnection = DriverManager.getConnection(
          ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties())) {
        KmsTestUtility.teardown(directConnection, SchemaName.of(METADATA_SCHEMA), TABLE);
      }
    }
    schemaReady = false;
  }

  private static Properties pluginProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_REGION.name, region);
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, kmsKeyId);
    return props;
  }

  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps, int index) throws SQLException;
  }

  /** Writes one value through the wrapper under {@code label} and reads it back with getObject. */
  private static @org.checkerframework.checker.nullness.qual.Nullable Object roundTrip(
      final String label, final Binder binder) throws SQLException {
    try (PreparedStatement insert = connection.prepareStatement(
        "INSERT INTO " + TABLE + " (name, secret) VALUES (?, ?)")) {
      insert.setString(1, label);
      binder.bind(insert, 2);
      insert.executeUpdate();
    }
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT secret FROM " + TABLE + " WHERE name = ?")) {
      select.setString(1, label);
      try (ResultSet rs = select.executeQuery()) {
        assertTrue(rs.next(), "row not found for " + label);
        return rs.getObject("secret");
      }
    }
  }

  /** The stored form of the value, read over a plain connection, with no decryption. */
  private static @org.checkerframework.checker.nullness.qual.Nullable String rawStored(final String label)
      throws SQLException {
    try (Connection plain = DriverManager.getConnection(
        ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
        PreparedStatement select = plain.prepareStatement(
            "SELECT secret FROM " + TABLE + " WHERE name = ?")) {
      select.setString(1, label);
      try (ResultSet rs = select.executeQuery()) {
        assertTrue(rs.next(), "row not found for " + label);
        return rs.getString("secret");
      }
    }
  }

  /** Asserts the stored value is ciphertext: present, long enough, and not the plaintext. */
  private static void assertStoredAsCiphertext(final String label, final String plaintextForm)
      throws SQLException {
    final String raw = rawStored(label);
    assertNotNull(raw, "stored value is NULL for " + label);
    assertTrue(raw.length() >= MIN_ENCRYPTED_LENGTH, "stored value is too short to be ciphertext: " + raw);
    assertNotEquals(plaintextForm, raw);
    // A very short plaintext can appear in random ciphertext by chance, so it is not searched for.
    if (plaintextForm.length() >= 4) {
      assertFalse(raw.contains(plaintextForm), "stored value contains the plaintext");
    }
  }

  @FunctionalInterface
  private interface Getter {
    @org.checkerframework.checker.nullness.qual.Nullable Object get(ResultSet rs) throws SQLException;
  }

  /**
   * Writes {@code expected} once, then checks it three ways: {@code getObject} returns an equal value
   * of the same class, the getter that matches the written type returns an equal value (skipped when
   * the type has no JDBC getter), and {@code getString} returns the value's text form (skipped for
   * byte[]). It also checks that the stored bytes are ciphertext.
   */
  private static void assertRoundTrip(
      final String label, final Object expected, final Binder binder, final String plaintextForm,
      final @org.checkerframework.checker.nullness.qual.Nullable Getter matchingGetter) throws SQLException {
    roundTrip(label, binder);
    assertEquals(expected.getClass(), readWith(label, rs -> rs.getObject("secret")).getClass());
    assertEquals(expected, readWith(label, rs -> rs.getObject("secret")));
    if (matchingGetter != null) {
      assertEquals(expected, readWith(label, matchingGetter));
    }
    if (!(expected instanceof byte[])) {
      assertEquals(expected.toString(), readWith(label, rs -> rs.getString("secret")));
    }
    assertStoredAsCiphertext(label, plaintextForm);
  }

  private static Object readWith(final String label, final Getter getter) throws SQLException {
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT secret FROM " + TABLE + " WHERE name = ?")) {
      select.setString(1, label);
      try (ResultSet rs = select.executeQuery()) {
        assertTrue(rs.next(), "row not found for " + label);
        final Object value = getter.get(rs);
        assertNotNull(value);
        return value;
      }
    }
  }

  @TestTemplate
  void testString() throws Exception {
    final String value = "hello world";
    assertRoundTrip("string", value, (ps, i) -> ps.setString(i, value), value, rs -> rs.getString("secret"));
  }

  @TestTemplate
  void testInt() throws Exception {
    assertRoundTrip("int", 424242, (ps, i) -> ps.setInt(i, 424242), "424242", rs -> rs.getInt("secret"));
  }

  @TestTemplate
  void testLong() throws Exception {
    final long value = 9_000_000_000L;
    assertRoundTrip("long", value, (ps, i) -> ps.setLong(i, value), "9000000000", rs -> rs.getLong("secret"));
  }

  @TestTemplate
  void testDouble() throws Exception {
    final double value = 3.14159;
    assertRoundTrip("double", value, (ps, i) -> ps.setDouble(i, value), "3.14159", rs -> rs.getDouble("secret"));
  }

  @TestTemplate
  void testFloat() throws Exception {
    final float value = 2.5f;
    assertRoundTrip("float", value, (ps, i) -> ps.setFloat(i, value), "2.5", rs -> rs.getFloat("secret"));
  }

  @TestTemplate
  void testShort() throws Exception {
    final short value = 1234;
    assertRoundTrip("short", value, (ps, i) -> ps.setShort(i, value), "1234", rs -> rs.getShort("secret"));
  }

  @TestTemplate
  void testByte() throws Exception {
    final byte value = -7;
    assertRoundTrip("byte", value, (ps, i) -> ps.setByte(i, value), "-7", rs -> rs.getByte("secret"));
  }

  /** BigDecimal.equals compares scale too, so this checks the scale survives, not just the value. */
  @TestTemplate
  void testBigDecimalKeepsScale() throws Exception {
    final BigDecimal value = new BigDecimal("12345.6789");
    assertRoundTrip("bigdecimal", value, (ps, i) -> ps.setBigDecimal(i, value), "12345.6789",
        rs -> rs.getBigDecimal("secret"));

    final BigDecimal trailingZeros = new BigDecimal("1.500");
    assertRoundTrip("bigdecimal_zeros", trailingZeros, (ps, i) -> ps.setBigDecimal(i, trailingZeros), "1.500",
        rs -> rs.getBigDecimal("secret"));
    assertEquals(3, ((BigDecimal) readWith("bigdecimal_zeros", rs -> rs.getObject("secret"))).scale());
  }

  @TestTemplate
  void testBooleanTrue() throws Exception {
    assertRoundTrip("bool_true", Boolean.TRUE, (ps, i) -> ps.setBoolean(i, true), "true",
        rs -> rs.getBoolean("secret"));
  }

  @TestTemplate
  void testBooleanFalse() throws Exception {
    assertRoundTrip("bool_false", Boolean.FALSE, (ps, i) -> ps.setBoolean(i, false), "false",
        rs -> rs.getBoolean("secret"));
  }

  @TestTemplate
  void testSqlDate() throws Exception {
    final Date value = Date.valueOf("2024-03-14");
    assertRoundTrip("sql_date", value, (ps, i) -> ps.setDate(i, value), "2024-03-14", rs -> rs.getDate("secret"));
  }

  @TestTemplate
  void testSqlTime() throws Exception {
    final Time value = Time.valueOf("09:26:53");
    assertRoundTrip("sql_time", value, (ps, i) -> ps.setTime(i, value), "09:26:53", rs -> rs.getTime("secret"));
  }

  /** A Timestamp is stored as epoch milliseconds, so it is compared at millisecond precision. */
  @TestTemplate
  void testTimestamp() throws Exception {
    final Timestamp value = Timestamp.valueOf("2024-03-14 09:26:53.123");
    assertRoundTrip("timestamp", value, (ps, i) -> ps.setTimestamp(i, value), "2024-03-14",
        rs -> rs.getTimestamp("secret"));
  }

  @TestTemplate
  void testLocalDate() throws Exception {
    final LocalDate value = LocalDate.of(2024, 3, 14);
    assertRoundTrip("local_date", value, (ps, i) -> ps.setObject(i, value), "2024-03-14", null);
  }

  @TestTemplate
  void testLocalTime() throws Exception {
    final LocalTime value = LocalTime.of(9, 26, 53);
    assertRoundTrip("local_time", value, (ps, i) -> ps.setObject(i, value), "09:26:53", null);
  }

  @TestTemplate
  void testLocalDateTime() throws Exception {
    final LocalDateTime value = LocalDateTime.of(2024, 3, 14, 9, 26, 53);
    assertRoundTrip("local_date_time", value, (ps, i) -> ps.setObject(i, value), "2024-03-14T09:26:53", null);
  }

  /**
   * getBytes on a column written with setString returns the string's UTF-8 bytes, including text that
   * is also valid base64 (which an earlier version decoded into different bytes) and non-ASCII text.
   */
  @TestTemplate
  void testGetBytesOnStringColumn() throws Exception {
    final String[] values = {"111-11-1111", "QUJD", "héllo ✓"};
    for (int n = 0; n < values.length; n++) {
      final String value = values[n];
      final String label = "string_bytes_" + n;
      roundTrip(label, (ps, i) -> ps.setString(i, value));
      assertArrayEquals(value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
          (byte[]) readWith(label, rs -> rs.getBytes("secret")));
    }
  }

  /** The numeric getters convert between numeric types, as in a plain JDBC read. */
  @TestTemplate
  void testNumericGettersConvertBetweenTypes() throws Exception {
    roundTrip("short_wide", (ps, i) -> ps.setShort(i, (short) 300));
    assertEquals(300, readWith("short_wide", rs -> rs.getInt("secret")));
    assertEquals(300L, readWith("short_wide", rs -> rs.getLong("secret")));

    roundTrip("byte_wide", (ps, i) -> ps.setByte(i, (byte) 5));
    assertEquals((short) 5, readWith("byte_wide", rs -> rs.getShort("secret")));
  }

  /** A getter from a different family than the written type fails rather than guessing. */
  @TestTemplate
  void testGetterOfDifferentFamilyFails() throws Exception {
    final LocalDateTime value = LocalDateTime.of(2024, 3, 14, 9, 26, 53);
    roundTrip("family_mismatch", (ps, i) -> ps.setObject(i, value));
    assertThrows(SQLException.class, () -> readWith("family_mismatch", rs -> rs.getTimestamp("secret")));
  }

  /** getObject and getBytes both return the bytes that were written. */
  @TestTemplate
  void testByteArray() throws Exception {
    final byte[] value = {0x00, 0x01, 0x02, (byte) 0xFF, 0x7F, (byte) 0x80};
    roundTrip("bytes", (ps, i) -> ps.setBytes(i, value));
    final Object viaObject = readWith("bytes", rs -> rs.getObject("secret"));
    assertEquals(byte[].class, viaObject.getClass());
    assertArrayEquals(value, (byte[]) viaObject);
    assertArrayEquals(value, (byte[]) readWith("bytes", rs -> rs.getBytes("secret")));
    final String raw = rawStored("bytes");
    assertNotNull(raw);
    assertTrue(raw.length() >= MIN_ENCRYPTED_LENGTH, "stored value is too short to be ciphertext");
  }

  /** A NULL is not encrypted: it is stored as NULL and read back as null. */
  @TestTemplate
  void testNull() throws Exception {
    assertNull(roundTrip("null_value", (ps, i) -> ps.setNull(i, Types.NULL)));
    assertNull(rawStored("null_value"));
  }
}
