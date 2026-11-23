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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.nio.charset.StandardCharsets;
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
import java.util.List;
import java.util.Locale;
import java.util.Properties;
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
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;

/**
 * Integration tests for the ways an application reads a row holding encrypted columns: by column
 * index and by label, row by row through a {@code PreparedStatement} and a plain {@code Statement},
 * through a scrollable {@code ResultSet}, with NULL encrypted values, and through column aliases.
 *
 * <p>Each test reads a table with a plain {@code name} column and three encrypted columns, each with
 * its own data key: {@code ssn} and {@code email} hold {@code String} values and {@code token} holds
 * {@code byte[]} values. The rows are written through the plugin, so the stored values are real
 * ciphertext. This class uses only these two types and checks the read paths rather than per-type
 * round trips.
 *
 * <p>Requires {@code KMS_KEY_ID} (preferably a key ARN; {@code AWS_KMS_KEY_ARN} is used as a
 * fallback) and AWS credentials in the test container. Each test uses its own table and key rows and
 * removes them afterward. KMS keys are never created or deleted.
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
public class KmsReadShapesIntegrationTest {

  private static final SchemaName METADATA_SCHEMA = SchemaName.of("encrypt");
  private static final String SSN = "ssn";
  private static final String EMAIL = "email";
  private static final String TOKEN = "token";
  private static final SecureRandom RANDOM = new SecureRandom();

  /** Rows written by {@link #createSeededTable}, in {@code ORDER BY name} order. */
  private static final List<Row> ROWS = Arrays.asList(
      new Row("Alice", "111-11-1111", "alice@example.com",
          new byte[] {0x00, 0x01, 0x7f, (byte) 0x80, (byte) 0xff}),
      new Row("Bob", "222-22-2222", "bob@example.com",
          new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}),
      // Carol has an ssn but NULL email and token, so a row mixes NULL and non-NULL encrypted values.
      new Row("Carol", "333-33-3333", null, null));

  private static String kmsKeyId;
  private static String region;
  private static boolean isPg;
  private static boolean metadataSchemaReady;

  private final List<String> tables = new ArrayList<>();
  private final List<Connection> connections = new ArrayList<>();

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
   * Creates the metadata schema once per class. This cannot run in {@code @BeforeAll}: the connection
   * URL depends on the current test driver, which {@link TestDriverProvider} sets for each test
   * invocation, before {@code @BeforeEach} methods run.
   */
  @BeforeEach
  void setUpMetadataSchemaOnce() throws Exception {
    if (metadataSchemaReady) {
      return;
    }
    try (Connection conn = openPlainConnection()) {
      KmsTestUtility.createMetadataTables(conn, METADATA_SCHEMA, isPg);
    }
    metadataSchemaReady = true;
  }

  @AfterAll
  static void tearDown() {
    metadataSchemaReady = false;
  }

  @AfterEach
  void cleanUp() throws SQLException {
    for (Connection conn : connections) {
      try {
        conn.close();
      } catch (SQLException e) {
        // Best effort: the test outcome is already decided.
      }
    }
    connections.clear();
    try (Connection conn = openPlainConnection()) {
      for (String table : tables) {
        KmsTestUtility.teardown(conn, METADATA_SCHEMA, table);
      }
    }
    tables.clear();
  }

  @TestTemplate
  void test01ReadsByIndexAndByLabel() throws Exception {
    final String table = createSeededTable("kms_reads_index_label");
    final Connection conn = openPluginConnection();
    final Row alice = ROWS.get(0);

    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT name, " + SSN + ", " + EMAIL + ", " + TOKEN + " FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, alice.name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());

        assertEquals(alice.ssn, rs.getString(2));
        assertEquals(alice.ssn, rs.getString(SSN));
        assertEquals(alice.email, rs.getString(3));
        assertEquals(alice.email, rs.getString(EMAIL));
        assertArrayEquals(alice.token, rs.getBytes(4));
        assertArrayEquals(alice.token, rs.getBytes(TOKEN));

        assertEquals(alice.ssn, rs.getObject(2));
        assertEquals(alice.ssn, rs.getObject(SSN));
        assertArrayEquals(alice.token, (byte[]) rs.getObject(4));
        assertArrayEquals(alice.token, (byte[]) rs.getObject(TOKEN));

        assertFalse(rs.next());
      }
    }
  }

  @TestTemplate
  void test02ReadsEveryRowAndColumnThroughPreparedStatement() throws Exception {
    final String table = createSeededTable("kms_reads_prepared");
    final Connection conn = openPluginConnection();

    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT name, " + SSN + ", " + EMAIL + ", " + TOKEN + " FROM " + table + " ORDER BY name")) {
      try (ResultSet rs = stmt.executeQuery()) {
        assertRows(rs);
      }
    }
  }

  @TestTemplate
  void test03ReadsEveryRowAndColumnThroughStatement() throws Exception {
    final String table = createSeededTable("kms_reads_statement");
    final Connection conn = openPluginConnection();

    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT name, " + SSN + ", " + EMAIL + ", " + TOKEN + " FROM " + table + " ORDER BY name")) {
        assertRows(rs);
      }
      // SELECT * returns id, name, ssn, email, token. The query above already reads the columns by
      // label, so read them here by index, at different positions than in the explicit column list.
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " ORDER BY name")) {
        for (Row expected : ROWS) {
          assertTrue(rs.next(), "Row " + expected.name + " not found");
          assertEquals(expected.name, rs.getString(2));
          assertEquals(expected.ssn, rs.getString(3), "ssn of " + expected.name);
          assertEquals(expected.email, rs.getString(4), "email of " + expected.name);
          assertArrayEquals(expected.token, rs.getBytes(5), "token of " + expected.name);
        }
        assertFalse(rs.next(), "Unexpected extra row");
      }
    }
  }

  @TestTemplate
  void test04ReadsNullEncryptedValues() throws Exception {
    final String table = createSeededTable("kms_reads_null");
    final Connection conn = openPluginConnection();
    final Row carol = ROWS.get(2);

    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT name, " + SSN + ", " + EMAIL + ", " + TOKEN + " FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, carol.name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());

        assertNull(rs.getString(EMAIL));
        assertTrue(rs.wasNull());
        // A decrypted non-NULL value resets wasNull.
        assertEquals(carol.ssn, rs.getString(SSN));
        assertFalse(rs.wasNull());

        assertNull(rs.getBytes(TOKEN));
        assertTrue(rs.wasNull());
        assertEquals(carol.ssn, rs.getObject(2));
        assertFalse(rs.wasNull());

        assertNull(rs.getObject(3));
        assertTrue(rs.wasNull());
        assertNull(rs.getObject(4));
        assertTrue(rs.wasNull());
      }
    }
  }

  @TestTemplate
  void test05ReadsThroughScrollableResultSet() throws Exception {
    final String table = createSeededTable("kms_reads_scroll");
    final Connection conn = openPluginConnection();

    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT name, " + SSN + ", " + EMAIL + ", " + TOKEN + " FROM " + table + " ORDER BY name",
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.last());
        assertRow(ROWS.get(2), rs);
        assertTrue(rs.first());
        assertRow(ROWS.get(0), rs);
        assertTrue(rs.absolute(2));
        assertRow(ROWS.get(1), rs);
        assertTrue(rs.relative(1));
        assertRow(ROWS.get(2), rs);
        assertTrue(rs.previous());
        assertRow(ROWS.get(1), rs);
        assertTrue(rs.absolute(-3));
        assertRow(ROWS.get(0), rs);
      }
    }
  }

  @TestTemplate
  void test06ReadsAliasedEncryptedColumns() throws Exception {
    final String table = createSeededTable("kms_reads_alias");
    final Connection conn = openPluginConnection();
    final Row alice = ROWS.get(0);

    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT name AS n, " + SSN + " AS s, " + EMAIL + " AS e, " + TOKEN + " AS t FROM " + table
            + " WHERE name = ?")) {
      stmt.setString(1, alice.name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());

        assertEquals(alice.name, rs.getString("n"));
        assertEquals(alice.ssn, rs.getString("s"));
        assertEquals(alice.ssn, rs.getString(2));
        assertEquals(alice.email, rs.getString("e"));
        assertEquals(alice.email, rs.getString(3));
        assertArrayEquals(alice.token, rs.getBytes("t"));
        assertArrayEquals(alice.token, rs.getBytes(4));

        assertEquals(alice.ssn, rs.getObject("s"));
        assertEquals(alice.ssn, rs.getObject(2));
        assertArrayEquals(alice.token, (byte[]) rs.getObject("t"));
        assertArrayEquals(alice.token, (byte[]) rs.getObject(4));

        assertFalse(rs.next());
      }
    }

    // A table alias as well as a column alias: the metadata still reports the real table.
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT u." + SSN + " AS s FROM " + table + " u ORDER BY u.name")) {
      for (Row expected : ROWS) {
        assertTrue(rs.next(), "Row " + expected.name + " not found");
        assertEquals(expected.ssn, rs.getString("s"), "ssn of " + expected.name);
        assertEquals(expected.ssn, rs.getString(1), "ssn of " + expected.name);
      }
      assertFalse(rs.next(), "Unexpected extra row");
    }
  }

  /**
   * A plain column or an expression labelled with an encrypted column's name is not encrypted, so it
   * is returned as-is instead of being decrypted.
   */
  @TestTemplate
  void test07ReadsColumnsLabelledWithEncryptedColumnNames() throws Exception {
    final String table = createSeededTable("kms_reads_relabel");
    final Connection conn = openPluginConnection();
    final Row alice = ROWS.get(0);
    final String upperName = alice.name.toUpperCase(Locale.ROOT);

    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT name AS " + SSN + ", upper(name) AS " + EMAIL + " FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, alice.name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());

        assertEquals(alice.name, rs.getString(SSN));
        assertEquals(alice.name, rs.getString(1));
        assertEquals(upperName, rs.getString(EMAIL));
        assertEquals(upperName, rs.getString(2));

        assertFalse(rs.next());
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** One seeded row. A {@code null} email or token is stored as NULL. */
  private static final class Row {
    private final String name;
    private final String ssn;
    private final String email;
    private final byte[] token;

    private Row(final String name, final String ssn, final String email, final byte[] token) {
      this.name = name;
      this.ssn = ssn;
      this.email = email;
      this.token = token;
    }
  }

  private static Connection openPlainConnection() throws SQLException {
    return DriverManager.getConnection(ConnectionStringHelper.getUrl(), ConnectionStringHelper.getDefaultProperties());
  }

  private static Properties pluginProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    // kmsEncryption needs the SQL parse results that the sqlParser plugin puts in the call context.
    props.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_REGION.name, region);
    props.setProperty(EncryptionConfig.ENCRYPTION_METADATA_SCHEMA.name, METADATA_SCHEMA.getValue());
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, kmsKeyId);
    return props;
  }

  private Connection openPluginConnection() throws SQLException {
    final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), pluginProps());
    connections.add(conn);
    return conn;
  }

  /**
   * Creates a table with the encrypted {@code ssn}, {@code email} and {@code token} columns, each with
   * its own data key, installs the HMAC validation triggers (skipped on MySQL when the user lacks the
   * privilege) and writes {@link #ROWS} through the plugin. It then checks that every non-NULL
   * encrypted value was stored as ciphertext.
   *
   * @return the table name, which has a random suffix
   */
  private String createSeededTable(final String prefix) throws Exception {
    final String table = prefix + "_" + randomHex(6);
    tables.add(table);
    final List<String> encryptedColumns = Arrays.asList(SSN, EMAIL, TOKEN);
    try (Connection conn = openPlainConnection()) {
      KmsTestUtility.createAppTable(conn, isPg, table, encryptedColumns, null);
      for (String column : encryptedColumns) {
        KmsTestUtility.configureColumn(
            conn, METADATA_SCHEMA, isPg, region, kmsKeyId, "key-" + table + "-" + column, table, column);
      }
      KmsTestUtility.installTriggers(conn, METADATA_SCHEMA, isPg, table, SSN, EMAIL, TOKEN);
    }

    try (Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), pluginProps())) {
      for (Row row : ROWS) {
        insert(conn, table, row);
      }
    }

    // Guards against the reads passing vacuously on values that were stored as-is. Without this, a
    // column that the plugin skipped on both the write and the read would round-trip as plaintext,
    // which only the HMAC triggers would catch, and those may be missing on MySQL.
    for (Row row : ROWS) {
      assertStoredAsCiphertext(table, SSN, row.name, row.ssn.getBytes(StandardCharsets.UTF_8));
      if (row.email != null) {
        assertStoredAsCiphertext(table, EMAIL, row.name, row.email.getBytes(StandardCharsets.UTF_8));
      }
      if (row.token != null) {
        assertStoredAsCiphertext(table, TOKEN, row.name, row.token);
      }
    }
    return table;
  }

  /**
   * Inserts a row. A NULL email or token is bound with {@code setNull(..., Types.BINARY)}: a NULL bound
   * through {@code setString} is typed as varchar, which the PostgreSQL {@code encrypted_data} column
   * rejects.
   */
  private static void insert(final Connection conn, final String table, final Row row) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "INSERT INTO " + table + " (name, " + SSN + ", " + EMAIL + ", " + TOKEN + ") VALUES (?, ?, ?, ?)")) {
      stmt.setString(1, row.name);
      stmt.setString(2, row.ssn);
      if (row.email != null) {
        stmt.setString(3, row.email);
      } else {
        stmt.setNull(3, Types.BINARY);
      }
      if (row.token != null) {
        stmt.setBytes(4, row.token);
      } else {
        stmt.setNull(4, Types.BINARY);
      }
      assertEquals(1, stmt.executeUpdate());
    }
  }

  /** Asserts that {@code rs} holds exactly {@link #ROWS}, in order, reading each column by label. */
  private static void assertRows(final ResultSet rs) throws SQLException {
    for (Row expected : ROWS) {
      assertTrue(rs.next(), "Row " + expected.name + " not found");
      assertRow(expected, rs);
    }
    assertFalse(rs.next(), "Unexpected extra row");
  }

  private static void assertRow(final Row expected, final ResultSet rs) throws SQLException {
    assertEquals(expected.name, rs.getString("name"));
    assertEquals(expected.ssn, rs.getString(SSN), "ssn of " + expected.name);
    assertEquals(expected.email, rs.getString(EMAIL), "email of " + expected.name);
    assertArrayEquals(expected.token, rs.getBytes(TOKEN), "token of " + expected.name);
  }

  private static void assertStoredAsCiphertext(
      final String table, final String column, final String name, final byte[] plaintext) throws SQLException {
    final byte[] stored;
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement(
             "SELECT " + column + " FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Row " + name + " not found");
        stored = rs.getBytes(1);
      }
    }
    assertNotNull(stored, column + " of " + name + " was stored as NULL");
    assertFalse(contains(stored, plaintext),
        column + " of " + name + " was stored as plaintext");
  }

  private static boolean contains(final byte[] haystack, final byte[] needle) {
    outer:
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      for (int j = 0; j < needle.length; j++) {
        if (haystack[i + j] != needle[j]) {
          continue outer;
        }
      }
      return true;
    }
    return false;
  }

  private static String randomHex(final int length) {
    final StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(Character.forDigit(RANDOM.nextInt(16), 16));
    }
    return sb.toString();
  }
}
