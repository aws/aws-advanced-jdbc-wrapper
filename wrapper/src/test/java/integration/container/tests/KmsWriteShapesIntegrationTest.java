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
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.junit.jupiter.api.function.Executable;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;

/**
 * Integration tests for the shapes of write the KMS encryption plugin encrypts, and the ones it
 * refuses because it cannot encrypt them.
 *
 * <p>Bound values are encrypted for INSERT, UPDATE, batched prepared statements, multi-row VALUES,
 * upserts and rows with several encrypted columns; each test checks the stored bytes on a plain
 * connection, since a round trip through the plugin alone cannot tell a ciphertext from a value
 * stored as-is. A client constant (a literal, or an expression that reads no column) written to an
 * encrypted column is refused before it reaches the database, whether the SQL is prepared or passed
 * to a plain {@code Statement}. So are statements the plugin cannot read when they have parameters
 * it would have to encrypt, such as a PostgreSQL data-modifying CTE or a MERGE. Everything else
 * (DEFAULT, values read from a column, {@code INSERT ... SELECT}, unreadable statements without
 * parameters) is passed through to the database, whose HMAC validation accepts ciphertext and
 * rejects plaintext.
 *
 * <p>Requires {@code KMS_KEY_ID} (preferably a key ARN; {@code AWS_KMS_KEY_ARN} is used as a
 * fallback) and AWS credentials in the test container. Each test uses its own tables and key rows
 * and removes them afterward. KMS keys are never created or deleted.
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
public class KmsWriteShapesIntegrationTest {

  private static final SchemaName METADATA_SCHEMA = SchemaName.of("encrypt");
  private static final String SSN = "ssn";
  private static final String EMAIL = "email";
  private static final String UNENCRYPTABLE = "not bind parameters";
  /**
   * A plaintext longer than the PostgreSQL encrypted_data domain's minimum length, so that it is the
   * HMAC check that rejects it rather than the domain's length check.
   */
  private static final String LONG_PLAINTEXT = "plaintext-that-is-long-enough-to-pass-the-length-check";
  private static final String HMAC_FAILURE = "HMAC verification failed";
  private static final SecureRandom RANDOM = new SecureRandom();

  private static String kmsKeyId;
  private static String region;
  private static boolean isPg;
  private static boolean metadataSchemaReady;

  private final List<String> tables = new ArrayList<>();
  private final List<String> plainTables = new ArrayList<>();
  private final List<Connection> connections = new ArrayList<>();
  private boolean triggersInstalled;

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
      try (Statement stmt = conn.createStatement()) {
        for (String table : plainTables) {
          stmt.execute("DROP TABLE IF EXISTS " + table);
        }
      }
    }
    tables.clear();
    plainTables.clear();
  }

  // ---------------------------------------------------------------------------------------------
  // Bound values are encrypted
  // ---------------------------------------------------------------------------------------------

  @TestTemplate
  void test01EncryptsInsert() throws Exception {
    final String table = createEncryptedTable("kms_shapes_insert", SSN);
    final Connection conn = openPluginConnection();

    insert(conn, table, "Insert", "111-11-1111");

    assertEquals("111-11-1111", readColumn(conn, table, SSN, "Insert"));
    assertStoredAsCiphertext(table, SSN, "Insert", "111-11-1111");
  }

  @TestTemplate
  void test02EncryptsUpdate() throws Exception {
    final String table = createEncryptedTable("kms_shapes_update", SSN);
    final Connection conn = openPluginConnection();
    insert(conn, table, "Update", "111-11-1111");

    try (PreparedStatement stmt = conn.prepareStatement(
        "UPDATE " + table + " SET " + SSN + " = ? WHERE name = ?")) {
      stmt.setString(1, "222-22-2222");
      stmt.setString(2, "Update");
      assertEquals(1, stmt.executeUpdate());
    }

    assertEquals("222-22-2222", readColumn(conn, table, SSN, "Update"));
    assertStoredAsCiphertext(table, SSN, "Update", "222-22-2222");
  }

  @TestTemplate
  void test03EncryptsBatchedPreparedStatement() throws Exception {
    final String table = createEncryptedTable("kms_shapes_batch", SSN);
    final Connection conn = openPluginConnection();

    try (PreparedStatement stmt = conn.prepareStatement(
        "INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, ?)")) {
      stmt.setString(1, "BatchA");
      stmt.setString(2, "333-33-3333");
      stmt.addBatch();
      stmt.setString(1, "BatchB");
      stmt.setString(2, "333-33-4444");
      stmt.addBatch();
      assertEquals(2, stmt.executeBatch().length);
    }

    assertEquals("333-33-3333", readColumn(conn, table, SSN, "BatchA"));
    assertEquals("333-33-4444", readColumn(conn, table, SSN, "BatchB"));
    assertStoredAsCiphertext(table, SSN, "BatchA", "333-33-3333");
    assertStoredAsCiphertext(table, SSN, "BatchB", "333-33-4444");
  }

  @TestTemplate
  void test04EncryptsMultiRowValues() throws Exception {
    final String table = createEncryptedTable("kms_shapes_multirow", SSN);
    final Connection conn = openPluginConnection();

    try (PreparedStatement stmt = conn.prepareStatement(
        "INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, ?), (?, ?)")) {
      stmt.setString(1, "MultiA");
      stmt.setString(2, "444-44-4444");
      stmt.setString(3, "MultiB");
      stmt.setString(4, "555-55-5555");
      assertEquals(2, stmt.executeUpdate());
    }

    assertEquals("444-44-4444", readColumn(conn, table, SSN, "MultiA"));
    assertEquals("555-55-5555", readColumn(conn, table, SSN, "MultiB"));
    assertStoredAsCiphertext(table, SSN, "MultiA", "444-44-4444");
    assertStoredAsCiphertext(table, SSN, "MultiB", "555-55-5555");
  }

  @TestTemplate
  void test05EncryptsUpsertAssignment() throws Exception {
    final String table = createEncryptedTable("kms_shapes_upsert", SSN);
    addUniqueNameIndex(table);
    final Connection conn = openPluginConnection();
    insert(conn, table, "Upsert", "111-11-1111");

    // The update branch binds the new value directly, so the plugin can encrypt it.
    final String sql = "INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, ?) "
        + (isPg ? "ON CONFLICT (name) DO UPDATE SET " + SSN + " = ?" : "ON DUPLICATE KEY UPDATE " + SSN + " = ?");
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, "Upsert");
      stmt.setString(2, "000-00-0000");
      stmt.setString(3, "666-66-6666");
      stmt.executeUpdate();
    }

    assertEquals("666-66-6666", readColumn(conn, table, SSN, "Upsert"));
    assertStoredAsCiphertext(table, SSN, "Upsert", "666-66-6666");
  }

  @TestTemplate
  void test06EncryptsSeveralEncryptedColumnsInOneRow() throws Exception {
    final String table = createEncryptedTable("kms_shapes_multicol", SSN, EMAIL);
    final Connection conn = openPluginConnection();

    try (PreparedStatement stmt = conn.prepareStatement(
        "INSERT INTO " + table + " (name, " + SSN + ", " + EMAIL + ") VALUES (?, ?, ?)")) {
      stmt.setString(1, "Alice");
      stmt.setString(2, "123-45-6789");
      stmt.setString(3, "alice@test.com");
      assertEquals(1, stmt.executeUpdate());
    }

    assertEquals("123-45-6789", readColumn(conn, table, SSN, "Alice"));
    assertEquals("alice@test.com", readColumn(conn, table, EMAIL, "Alice"));
    assertStoredAsCiphertext(table, SSN, "Alice", "123-45-6789");
    assertStoredAsCiphertext(table, EMAIL, "Alice", "alice@test.com");
  }

  // ---------------------------------------------------------------------------------------------
  // Values that are not bind parameters are refused
  // ---------------------------------------------------------------------------------------------

  @TestTemplate
  void test07FailsClosedOnLiteral() throws Exception {
    final String table = createEncryptedTable("kms_shapes_literal", SSN);
    final Connection conn = openPluginConnection();
    final String sql = "INSERT INTO " + table + " (name, " + SSN + ") VALUES ('Literal', '999-99-9999')";

    try (Statement stmt = conn.createStatement()) {
      assertRefused(() -> stmt.executeUpdate(sql), UNENCRYPTABLE);
    }

    assertEquals(0, countRows(table, "Literal"));
  }

  @TestTemplate
  void test08FailsClosedOnLiteralThroughEveryStatementMethod() throws Exception {
    final String table = createEncryptedTable("kms_shapes_methods", SSN);
    final Connection conn = openPluginConnection();
    final String sql = "INSERT INTO " + table + " (name, " + SSN + ") VALUES ('Methods', '999-99-9999')";

    try (Statement stmt = conn.createStatement()) {
      assertRefused(() -> stmt.execute(sql), UNENCRYPTABLE);
      assertRefused(() -> stmt.executeUpdate(sql), UNENCRYPTABLE);
      assertRefused(() -> stmt.execute(sql, Statement.RETURN_GENERATED_KEYS), UNENCRYPTABLE);
      assertRefused(() -> stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS), UNENCRYPTABLE);
      assertRefused(() -> stmt.addBatch(sql), UNENCRYPTABLE);
      // Nothing was added to the batch, so running it writes nothing.
      stmt.executeBatch();
    }

    assertEquals(0, countRows(table, "Methods"));
  }

  @TestTemplate
  void test09FailsClosedOnOtherClientConstants() throws Exception {
    final String table = createEncryptedTable("kms_shapes_constant", SSN);
    final Connection conn = openPluginConnection();
    insert(conn, table, "Seed", "111-11-1111");

    try (Statement stmt = conn.createStatement()) {
      // An expression that reads no column is as certain to be plaintext as a literal.
      assertRefused(() -> stmt.executeUpdate(
          "UPDATE " + table + " SET " + SSN + " = CONCAT('999', '-99-9999') WHERE name = 'Seed'"), UNENCRYPTABLE);
      // So is a constant selected into the column.
      assertRefused(() -> stmt.executeUpdate(
          "INSERT INTO " + table + " (name, " + SSN + ") SELECT 'Selected', '999-99-9999'"), UNENCRYPTABLE);
    }

    // A literal in a prepared statement is refused when it is prepared.
    assertRefused(() -> conn.prepareStatement(
        "INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, '999-99-9999')"), UNENCRYPTABLE);

    assertEquals("111-11-1111", readColumn(conn, table, SSN, "Seed"));
    assertEquals(0, countRows(table, "Selected"));
  }

  @TestTemplate
  void test10StatementWithoutEncryptedValuesRuns() throws Exception {
    final String table = createEncryptedTable("kms_shapes_plain", SSN);
    final Connection conn = openPluginConnection();

    try (Statement stmt = conn.createStatement()) {
      assertEquals(1, stmt.executeUpdate("INSERT INTO " + table + " (name) VALUES ('Plain')"));
      assertEquals(1, stmt.executeUpdate("UPDATE " + table + " SET name = 'Renamed' WHERE name = 'Plain'"));
      assertEquals(1, stmt.executeUpdate("UPDATE " + table + " SET " + SSN + " = NULL WHERE name = 'Renamed'"));
    }

    assertEquals(1, countRows(table, "Renamed"));
  }

  // ---------------------------------------------------------------------------------------------
  // Values that may be ciphertext are left to the database
  // ---------------------------------------------------------------------------------------------

  @TestTemplate
  void test16UpsertCopyingTheBoundValueIsEncrypted() throws Exception {
    final String table = createEncryptedTable("kms_shapes_excluded", SSN);
    addUniqueNameIndex(table);
    final Connection conn = openPluginConnection();
    insert(conn, table, "Upsert", "111-11-1111");

    // EXCLUDED.ssn / VALUES(ssn) copy the value bound in the VALUES row, which the plugin encrypts.
    final String sql = "INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, ?) "
        + (isPg
            ? "ON CONFLICT (name) DO UPDATE SET " + SSN + " = EXCLUDED." + SSN
            : "ON DUPLICATE KEY UPDATE " + SSN + " = VALUES(" + SSN + ")");
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, "Upsert");
      stmt.setString(2, "666-66-6666");
      stmt.executeUpdate();
    }

    assertEquals("666-66-6666", readColumn(conn, table, SSN, "Upsert"));
    assertStoredAsCiphertext(table, SSN, "Upsert", "666-66-6666");
  }

  @TestTemplate
  void test17DefaultAndColumnCopiesReachTheDatabase() throws Exception {
    final String table = createEncryptedTable("kms_shapes_default", SSN);
    final Connection conn = openPluginConnection();
    insert(conn, table, "Seed", "111-11-1111");

    try (Statement stmt = conn.createStatement()) {
      // The column's default is NULL, so DEFAULT stores nothing to encrypt.
      assertEquals(1, stmt.executeUpdate(
          "INSERT INTO " + table + " (name, " + SSN + ") VALUES ('Default', DEFAULT)"));
      // Assigning a column to itself rewrites the ciphertext it already holds.
      assertEquals(1, stmt.executeUpdate(
          "UPDATE " + table + " SET " + SSN + " = " + SSN + " WHERE name = 'Seed'"));
    }

    assertNull(readRaw(table, SSN, "Default"));
    assertEquals("111-11-1111", readColumn(conn, table, SSN, "Seed"));
  }

  @TestTemplate
  void test11InsertSelectCopiesCiphertextThrough() throws Exception {
    final String table = createEncryptedTable("kms_shapes_copy", SSN);
    final Connection conn = openPluginConnection();
    insert(conn, table, "Seed", "111-11-1111");

    try (Statement stmt = conn.createStatement()) {
      assertEquals(1, stmt.executeUpdate("INSERT INTO " + table + " (name, " + SSN + ") "
          + "SELECT 'Copied', " + SSN + " FROM " + table + " WHERE name = 'Seed'"));
    }

    assertEquals("111-11-1111", readColumn(conn, table, SSN, "Copied"));
    assertArrayEquals(readRaw(table, SSN, "Seed"), readRaw(table, SSN, "Copied"));
  }

  @TestTemplate
  void test12InsertSelectOfPlaintextIsRejectedByDatabase() throws Exception {
    final String table = createEncryptedTable("kms_shapes_staging", SSN);
    if (!isPg) {
      assumeTrue(triggersInstalled, "The MySQL HMAC validation triggers could not be installed");
    }
    final String staging = createStagingTable(table + "_src");
    // Longer than the PostgreSQL domain's minimum length, so it is the HMAC check that rejects it.
    final byte[] plaintext = "plaintext-that-is-long-enough-to-look-like-a-ciphertext-value"
        .getBytes(StandardCharsets.UTF_8);
    try (Connection plain = openPlainConnection();
         PreparedStatement stmt = plain.prepareStatement(
             "INSERT INTO " + staging + " (name, " + SSN + ") VALUES (?, ?)")) {
      stmt.setString(1, "Staged");
      stmt.setBytes(2, plaintext);
      stmt.executeUpdate();
    }

    final Connection conn = openPluginConnection();
    try (Statement stmt = conn.createStatement()) {
      final SQLException e = assertThrows(SQLException.class, () -> stmt.executeUpdate(
          "INSERT INTO " + table + " (name, " + SSN + ") SELECT name, " + SSN + " FROM " + staging));
      assertFalse(causeChainContains(e, "Refusing"),
          "Expected the database to reject the plaintext, not the plugin: " + e);
      assertTrue(causeChainContains(e, HMAC_FAILURE),
          "Expected the HMAC validation to reject the row: " + e);
    }

    assertEquals(0, countRows(table, "Staged"));
  }

  // ---------------------------------------------------------------------------------------------
  // Statements the plugin cannot read are refused when they have parameters, and otherwise left to
  // the database
  // ---------------------------------------------------------------------------------------------

  @TestTemplate
  void test13PgDataModifyingCte() throws Exception {
    assumeTrue(isPg, "Data-modifying CTEs are PostgreSQL only");
    final String table = createEncryptedTable("kms_shapes_cte", SSN);
    final Connection conn = openPluginConnection();

    assertRefused(() -> conn.prepareStatement(
        "WITH inserted AS (INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, ?) RETURNING id) "
            + "SELECT * FROM inserted"), table);
    try (Statement stmt = conn.createStatement()) {
      final SQLException e = assertRejectedByDatabase(() -> stmt.executeQuery(
          "WITH inserted AS (INSERT INTO " + table + " (name, " + SSN + ") VALUES ('Cte', '" + LONG_PLAINTEXT
              + "') RETURNING id) SELECT * FROM inserted"));
      assertTrue(causeChainContains(e, HMAC_FAILURE), "Expected the HMAC check to reject the row: " + e);
    }

    assertEquals(0, countRows(table, "Cte"));
  }

  @TestTemplate
  void test14PgRefusesMerge() throws Exception {
    assumeTrue(isPg, "MERGE is tested on PostgreSQL only");
    final String table = createEncryptedTable("kms_shapes_merge", SSN);
    final Connection conn = openPluginConnection();

    // Refused before the statement reaches the server, so the server version does not matter.
    assertRefused(() -> conn.prepareStatement(
        "MERGE INTO " + table + " AS t USING (VALUES (?::text)) AS s(join_name) "
            + "ON t.name = s.join_name "
            + "WHEN NOT MATCHED THEN INSERT (name, " + SSN + ") VALUES (?, ?)"), table);

    assertEquals(0, countRows(table, "Merge"));
  }

  @TestTemplate
  void test15MultiStatementWrite() throws Exception {
    final String table = createEncryptedTable("kms_shapes_multistmt", SSN);
    final Connection conn = openPluginConnection();

    assertRefused(() -> conn.prepareStatement(
        "SELECT 1; INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, ?)"), table);
    // Without parameters it reaches the server: PostgreSQL's HMAC check rejects the plaintext, and
    // MySQL rejects the multi-statement string unless allowMultiQueries is set.
    try (Statement stmt = conn.createStatement()) {
      final SQLException e = assertRejectedByDatabase(() -> stmt.execute(
          "SELECT 1; INSERT INTO " + table + " (name, " + SSN + ") VALUES ('Multi', '" + LONG_PLAINTEXT + "')"));
      if (isPg) {
        assertTrue(causeChainContains(e, HMAC_FAILURE), "Expected the HMAC check to reject the row: " + e);
      }
    }

    assertEquals(0, countRows(table, "Multi"));
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

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
   * Creates a table with the given encrypted columns, configures a data key for each and installs
   * the HMAC validation triggers.
   *
   * @return the table name, which has a random suffix
   */
  private String createEncryptedTable(String prefix, String... encryptedColumns) throws Exception {
    final String table = prefix + "_" + randomHex(6);
    tables.add(table);
    try (Connection conn = openPlainConnection()) {
      KmsTestUtility.createAppTable(conn, isPg, table, Arrays.asList(encryptedColumns), null);
      for (String column : encryptedColumns) {
        KmsTestUtility.configureColumn(
            conn, METADATA_SCHEMA, isPg, region, kmsKeyId, "key-" + table + "-" + column, table, column);
      }
      triggersInstalled = KmsTestUtility.installTriggers(conn, METADATA_SCHEMA, isPg, table, encryptedColumns);
    }
    return table;
  }

  /** A table without encryption metadata whose ssn column holds raw bytes. */
  private String createStagingTable(String table) throws SQLException {
    plainTables.add(table);
    try (Connection conn = openPlainConnection(); Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (name VARCHAR(100), " + SSN
          + (isPg ? " bytea)" : " VARBINARY(256))"));
    }
    return table;
  }

  private static void addUniqueNameIndex(String table) throws SQLException {
    try (Connection conn = openPlainConnection(); Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE UNIQUE INDEX " + table + "_name_uq ON " + table + " (name)");
    }
  }

  private static void insert(Connection conn, String table, String name, String ssn) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "INSERT INTO " + table + " (name, " + SSN + ") VALUES (?, ?)")) {
      stmt.setString(1, name);
      stmt.setString(2, ssn);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  private static String readColumn(Connection conn, String table, String column, String name)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT " + column + " FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Row " + name + " not found");
        return rs.getString(column);
      }
    }
  }

  /** The bytes a column holds, read on a plain connection so nothing is decrypted. */
  private static byte[] readRaw(String table, String column, String name) throws SQLException {
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement(
             "SELECT " + column + " FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Row " + name + " not found");
        return rs.getBytes(1);
      }
    }
  }

  private static void assertStoredAsCiphertext(String table, String column, String name, String plaintext)
      throws SQLException {
    final byte[] stored = readRaw(table, column, name);
    assertNotNull(stored, column + " of " + name + " was stored as NULL");
    assertFalse(contains(stored, plaintext.getBytes(StandardCharsets.UTF_8)),
        column + " of " + name + " was stored as plaintext");
  }

  private static boolean contains(byte[] haystack, byte[] needle) {
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

  private static int countRows(String table, String name) throws SQLException {
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement("SELECT count(*) FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getInt(1);
      }
    }
  }

  /** Asserts the plugin refused the call, with a message containing {@code expected}. */
  private static void assertRefused(Executable executable, String expected) {
    final SQLException e = assertThrows(SQLException.class, executable);
    assertTrue(causeChainContains(e, "Refusing"), "Expected the plugin to refuse the statement, got: " + e);
    assertTrue(causeChainContains(e, expected), "Expected '" + expected + "' in: " + e);
  }

  /** Asserts the call failed in the database rather than being refused by the plugin. */
  private static SQLException assertRejectedByDatabase(Executable executable) {
    final SQLException e = assertThrows(SQLException.class, executable);
    assertFalse(causeChainContains(e, "Refusing"), "Expected the database to reject the statement, got: " + e);
    return e;
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

  private static String randomHex(int length) {
    final StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(Character.forDigit(RANDOM.nextInt(16), 16));
    }
    return sb.toString();
  }
}
