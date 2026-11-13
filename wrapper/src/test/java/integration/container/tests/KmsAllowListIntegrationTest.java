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
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
import software.amazon.jdbc.plugin.encryption.key.KeyManagementException;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;

/**
 * Integration tests for {@code encryptionAllowedMasterKeyArns}. The master key a column's data key
 * is decrypted with is read from key_storage. These tests rewrite that row the way someone with
 * write access to the metadata schema could, and check that the plugin refuses to use a master key
 * outside the allow-list rather than calling KMS with it.
 *
 * <p>Requires {@code KMS_KEY_ID} (preferably a key ARN; {@code AWS_KMS_KEY_ARN} is used as a
 * fallback) and AWS credentials in the test container. Each test uses its own table and key rows
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
public class KmsAllowListIntegrationTest {

  private static final SchemaName METADATA_SCHEMA = SchemaName.of("encrypt");
  private static final String SSN = "ssn";
  private static final SecureRandom RANDOM = new SecureRandom();

  private static String kmsKeyId;
  private static String unlistedArn;
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
    unlistedArn = "arn:aws:kms:" + region + ":000000000000:key/00000000-0000-0000-0000-000000000000";

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
    if (tables.isEmpty()) {
      return;
    }
    try (Connection conn = openPlainConnection()) {
      for (String table : tables) {
        KmsTestUtility.teardown(conn, METADATA_SCHEMA, table);
      }
    }
    tables.clear();
  }

  @TestTemplate
  void test01PluginRefusesToStartWithoutAllowList() {
    final Properties props = pluginProps();
    props.remove(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name);

    final Exception e = assertThrows(Exception.class, () -> openPluginConnection(props));
    assertTrue(causeChainContains(e, EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name + " is required"),
        "Expected the missing allow-list to be reported, got: " + e);
  }

  @TestTemplate
  void test02RefusesToDecryptColumnWithUnlistedMasterKey() throws Exception {
    final String table = "kms_allow_read_" + randomHex(6);
    final int keyId = createEncryptedTable(table);
    insert(openPluginConnection(pluginProps()), table, "Alice", "111-11-1111");
    repointMasterKey(keyId, unlistedArn);

    // A fresh connection builds its own metadata and data key caches, so it reads key_storage as it
    // stands now.
    final Connection conn = openPluginConnection(pluginProps());
    assertUnauthorizedMasterKey(() -> readSsn(conn, table, "Alice"));
  }

  @TestTemplate
  void test03RefusesToEncryptIntoColumnWithUnlistedMasterKey() throws Exception {
    final String table = "kms_allow_write_" + randomHex(6);
    final int keyId = createEncryptedTable(table);
    insert(openPluginConnection(pluginProps()), table, "Alice", "111-11-1111");
    repointMasterKey(keyId, unlistedArn);

    final Connection conn = openPluginConnection(pluginProps());
    assertUnauthorizedMasterKey(() -> insert(conn, table, "Bob", "222-22-2222"));
    assertEquals(0, countRows(table, "Bob"));
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

  private Connection openPluginConnection(Properties props) throws SQLException {
    final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    connections.add(conn);
    return conn;
  }

  /**
   * Creates the table with an encrypted ssn column under the listed master key.
   *
   * @return the column's key_storage id
   */
  private int createEncryptedTable(String table) throws Exception {
    tables.add(table);
    try (Connection conn = openPlainConnection()) {
      KmsTestUtility.createAppTable(conn, isPg, table, Collections.singletonList(SSN), null);
      return KmsTestUtility.configureColumn(
          conn, METADATA_SCHEMA, isPg, region, kmsKeyId, "key-" + table + "-" + SSN, table, SSN).keyId();
    }
  }

  /** Rewrites the column's key_storage row the way someone with write access to it could. */
  private static void repointMasterKey(int keyId, String arn) throws SQLException {
    try (Connection conn = openPlainConnection();
         PreparedStatement stmt = conn.prepareStatement(
             "UPDATE " + METADATA_SCHEMA + ".key_storage SET master_key_arn = ? WHERE id = ?")) {
      stmt.setString(1, arn);
      stmt.setInt(2, keyId);
      assertEquals(1, stmt.executeUpdate());
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

  private static String readSsn(Connection conn, String table, String name) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("SELECT " + SSN + " FROM " + table + " WHERE name = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "Row " + name + " not found");
        return rs.getString(SSN);
      }
    }
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

  private static void assertUnauthorizedMasterKey(Executable executable) {
    final SQLException e = assertThrows(SQLException.class, executable);
    assertTrue(causeChainHasSqlState(e, KeyManagementException.UNAUTHORIZED_MASTER_KEY_STATE),
        "Expected an unauthorized master key error, got: " + e);
  }

  private static boolean causeChainHasSqlState(Throwable error, String sqlState) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof SQLException && sqlState.equals(((SQLException) t).getSQLState())) {
        return true;
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return false;
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
