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

package integration.util;

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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

/**
 * Shared setup for the KMS encryption integration tests.
 *
 * <p>It owns the parts that every KMS test class needs identically: resolving the KMS key and region
 * from the environment, building the {@link KmsClient}, creating the encryption metadata schema with
 * its {@code key_storage} and {@code encryption_metadata} tables (plus the PostgreSQL
 * {@code encrypted_data} type), and the per-table setup each test performs against those tables:
 *
 * <ul>
 *   <li>{@link #configureColumn} — generate a KMS data key and insert the {@code key_storage} and
 *       {@code encryption_metadata} rows for one column, returning the key id, HMAC key and data key.
 *   <li>{@link #createAppTable} — create an application table with encrypted columns
 *       ({@code encrypted_data} on PostgreSQL, {@code VARBINARY} on MySQL).
 *   <li>{@link #installTriggers} — install the HMAC validation triggers, skipping on MySQL privilege
 *       errors.
 *   <li>{@link #teardown} — drop the application table and delete its metadata and key rows.
 * </ul>
 *
 * <p>These let each KMS test class share the setup rather than copy it. A test that exercises the
 * production key-provisioning path (for example, the key rotation test) can still provision through
 * {@code KeyManagementUtility} instead of {@link #configureColumn}; the other tests insert the rows
 * directly through {@link #configureColumn} so that a bug in the utility cannot mask a bug in the
 * read/write path.
 *
 * <p>This class references the KMS SDK and the encryption plugin directly, which are on the test
 * classpath only for encryption runs. That is safe because the class is loaded lazily, the first
 * time a KMS test calls into it; a non-encryption run never references it, so the optional
 * dependencies are never resolved.
 */
public final class KmsTestUtility {

  /** Primary environment variable carrying the KMS key id or ARN. */
  public static final String KMS_KEY_ID_ENV = "KMS_KEY_ID";

  /** Fallback environment variable carrying the KMS key ARN. */
  public static final String KMS_KEY_ARN_ENV = "AWS_KMS_KEY_ARN";

  /**
   * MySQL error codes raised when the database user may not create routines or triggers — binary
   * logging without {@code SUPER} or {@code log_bin_trust_function_creators}, or a missing
   * privilege. The HMAC validation triggers are strongly recommended, but a test can still run
   * without them, so {@link #installTriggers} treats these as "not installed" rather than a failure.
   */
  public static final List<Integer> MYSQL_ROUTINE_PRIVILEGE_ERRORS =
      Collections.unmodifiableList(Arrays.asList(1419, 1227, 1142, 1370));

  private KmsTestUtility() {
  }

  /** The key id, HMAC key and encrypted data key produced by {@link #configureColumn}. */
  public static final class ColumnKey {
    private final int keyId;
    private final byte[] hmacKey;
    private final String encryptedDataKey;

    public ColumnKey(final int keyId, final byte[] hmacKey, final String encryptedDataKey) {
      this.keyId = keyId;
      this.hmacKey = hmacKey.clone();
      this.encryptedDataKey = encryptedDataKey;
    }

    /** The generated {@code key_storage.id} of the inserted key row. */
    public int keyId() {
      return keyId;
    }

    /** The random HMAC key stored with the key row. */
    public byte[] hmacKey() {
      return hmacKey.clone();
    }

    /** The KMS-encrypted data key, base64-encoded, stored with the key row. */
    public String encryptedDataKey() {
      return encryptedDataKey;
    }
  }

  /** Returns the first non-null, non-blank value, trimmed, or {@code null} if there is none. */
  public static String firstNonEmpty(final String... values) {
    for (final String value : values) {
      if (value != null && !value.trim().isEmpty()) {
        return value.trim();
      }
    }
    return null;
  }

  /**
   * Extracts the region from a KMS key ARN ({@code arn:aws:kms:<region>:<account>:key/<id>}).
   *
   * @return the region, or {@code null} if {@code keyId} is not an ARN
   */
  public static String regionFromArn(final String keyId) {
    if (keyId == null || !keyId.startsWith("arn:")) {
      return null;
    }
    final String[] parts = keyId.split(":");
    return parts.length > 3 ? parts[3] : null;
  }

  /** Resolves the KMS key id or ARN from {@link #KMS_KEY_ID_ENV}, falling back to {@link #KMS_KEY_ARN_ENV}. */
  public static String resolveKmsKeyId() {
    return firstNonEmpty(System.getenv(KMS_KEY_ID_ENV), System.getenv(KMS_KEY_ARN_ENV));
  }

  /**
   * Resolves the KMS region: the region embedded in the key ARN if present, otherwise
   * {@code fallbackRegion} (typically the current test environment's region).
   *
   * @return the resolved region, or {@code null} if neither source supplies one
   */
  public static String resolveRegion(final String kmsKeyId, final String fallbackRegion) {
    return firstNonEmpty(regionFromArn(kmsKeyId), fallbackRegion);
  }

  /**
   * Reports whether a MySQL error is a routine/trigger privilege error
   * (see {@link #MYSQL_ROUTINE_PRIVILEGE_ERRORS}).
   */
  public static boolean isRoutinePrivilegeError(final SQLException e) {
    return MYSQL_ROUTINE_PRIVILEGE_ERRORS.contains(e.getErrorCode());
  }

  /** Builds a {@link KmsClient} for the test region. The caller owns the returned client's lifecycle. */
  public static KmsClient kmsClient(final String region) {
    return KmsClient.builder()
        .region(Region.of(region))
        .build();
  }

  /**
   * Creates the encryption metadata schema and its {@code key_storage} and {@code encryption_metadata}
   * tables from scratch, dropping any existing schema first. On PostgreSQL it also installs the
   * {@code pgcrypto} extension and the {@code encrypted_data} custom type into the schema.
   *
   * <p>This does not create the key rows, the application tables, or the HMAC triggers — each test
   * populates those for its own tables.
   *
   * @param conn           a direct (non-wrapper) connection to the database
   * @param metadataSchema the schema to (re)create
   * @param isPg           {@code true} for PostgreSQL, {@code false} for MySQL
   */
  public static void createMetadataTables(
      final Connection conn, final SchemaName metadataSchema, final boolean isPg) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      // PostgreSQL supports CASCADE, MySQL doesn't.
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
  }

  /**
   * Generates a KMS data key for one column and inserts its {@code key_storage} and
   * {@code encryption_metadata} rows directly, without going through the production
   * {@code KeyManagementUtility}. Inserting the rows by hand keeps the utility out of the read/write
   * tests, so a bug in it cannot mask a bug in the path under test.
   *
   * @param conn           a direct (non-wrapper) connection to the database
   * @param metadataSchema the metadata schema holding {@code key_storage} and {@code encryption_metadata}
   * @param isPg           {@code true} for PostgreSQL, {@code false} for MySQL
   * @param region         the KMS region
   * @param kmsKeyId       the KMS master key id or ARN
   * @param keyName        the {@code key_storage.name} to store the key under
   * @param table          the application table name recorded in {@code encryption_metadata}
   * @param column         the encrypted column name recorded in {@code encryption_metadata}
   * @return the generated key id, the HMAC key and the encrypted data key
   */
  public static ColumnKey configureColumn(
      final Connection conn, final SchemaName metadataSchema, final boolean isPg, final String region,
      final String kmsKeyId, final String keyName, final String table, final String column) throws Exception {
    // Generate a KMS data key. KMS accepts a key id, key ARN or alias.
    final String encryptedDataKeyBase64;
    try (KmsClient kmsClient = kmsClient(region)) {
      final GenerateDataKeyRequest dataKeyRequest =
          GenerateDataKeyRequest.builder().keyId(kmsKeyId).keySpec("AES_256").build();
      final GenerateDataKeyResponse dataKeyResponse = kmsClient.generateDataKey(dataKeyRequest);
      encryptedDataKeyBase64 =
          Base64.getEncoder().encodeToString(dataKeyResponse.ciphertextBlob().asByteArray());
    }

    final byte[] hmacKey = new byte[32];
    new SecureRandom().nextBytes(hmacKey);

    final String prefix = (metadataSchema != null) ? metadataSchema.getValue() + "." : "";
    final String insertKeySql =
        "INSERT INTO " + prefix
            + "key_storage (name, master_key_arn, encrypted_data_key, hmac_key, key_spec) "
            + "VALUES (?, ?, ?, ?, ?)" + (isPg ? " RETURNING id" : "");

    final int keyId;
    try (PreparedStatement keyStmt = conn.prepareStatement(insertKeySql,
        isPg ? Statement.NO_GENERATED_KEYS : Statement.RETURN_GENERATED_KEYS)) {
      keyStmt.setString(1, keyName);
      keyStmt.setString(2, kmsKeyId);
      keyStmt.setString(3, encryptedDataKeyBase64);
      keyStmt.setBytes(4, hmacKey);
      keyStmt.setString(5, "AES_256");

      if (isPg) {
        try (ResultSet keyRs = keyStmt.executeQuery()) {
          keyRs.next();
          keyId = keyRs.getInt(1);
        }
      } else {
        keyStmt.executeUpdate();
        try (ResultSet keyRs = keyStmt.getGeneratedKeys()) {
          keyRs.next();
          keyId = keyRs.getInt(1);
        }
      }
    }

    try (PreparedStatement metaStmt = conn.prepareStatement(
        "INSERT INTO " + prefix
            + "encryption_metadata (table_name, column_name, encryption_algorithm, key_id) "
            + "VALUES (?, ?, ?, ?)")) {
      metaStmt.setString(1, table);
      metaStmt.setString(2, column);
      metaStmt.setString(3, "AES-256-GCM");
      metaStmt.setInt(4, keyId);
      metaStmt.executeUpdate();
    }

    return new ColumnKey(keyId, hmacKey, encryptedDataKeyBase64);
  }

  /**
   * Creates an application table with an {@code id} primary key, a {@code name} column, the given
   * encrypted columns ({@code encrypted_data} on PostgreSQL, {@code VARBINARY(256)} on MySQL) and any
   * trailing plain {@code VARCHAR} columns, in that order.
   *
   * @param conn                 a direct (non-wrapper) connection to the database
   * @param isPg                 {@code true} for PostgreSQL, {@code false} for MySQL
   * @param table                the table name to create
   * @param encryptedColumns     the encrypted column names, in order
   * @param trailingPlainColumns trailing plain {@code VARCHAR(100)} column names (may be {@code null})
   */
  public static void createAppTable(
      final Connection conn, final boolean isPg, final String table,
      final List<String> encryptedColumns, final List<String> trailingPlainColumns) throws SQLException {
    final StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(table).append(" (")
        .append(isPg ? "id SERIAL PRIMARY KEY, " : "id INT AUTO_INCREMENT PRIMARY KEY, ")
        .append("name VARCHAR(100)");
    for (final String column : encryptedColumns) {
      ddl.append(", ").append(column).append(isPg ? " encrypted_data" : " VARBINARY(256)");
    }
    if (trailingPlainColumns != null) {
      for (final String column : trailingPlainColumns) {
        ddl.append(", ").append(column).append(" VARCHAR(100)");
      }
    }
    ddl.append(")");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(ddl.toString());
    }
  }

  /**
   * Installs the HMAC validation triggers for the given columns of a table. On PostgreSQL the
   * {@code validate_encrypted_data_hmac} function (created with the metadata schema) backs one trigger
   * per column. On MySQL the shared validation routine and one trigger per column are installed; if
   * the database user lacks the privilege to create them (see {@link #MYSQL_ROUTINE_PRIVILEGE_ERRORS})
   * this returns {@code false} rather than failing, because a run without the triggers is still useful.
   *
   * @param conn           a direct (non-wrapper) connection to the database
   * @param metadataSchema the metadata schema (used on MySQL to install the validation routine)
   * @param isPg           {@code true} for PostgreSQL, {@code false} for MySQL
   * @param table          the application table name
   * @param columns        the encrypted column names to install triggers for
   * @return {@code true} if the triggers were installed, {@code false} if skipped on a MySQL privilege error
   */
  public static boolean installTriggers(
      final Connection conn, final SchemaName metadataSchema, final boolean isPg,
      final String table, final String... columns) throws Exception {
    try {
      if (isPg) {
        try (Statement stmt = conn.createStatement()) {
          for (final String column : columns) {
            stmt.execute(
                "CREATE TRIGGER " + table + "_" + column + "_hmac "
                    + "BEFORE INSERT OR UPDATE ON " + table + " "
                    + "FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('" + column + "')");
          }
        }
      } else {
        EncryptedDataTypeInstaller.installMySqlHmacValidation(conn, metadataSchema);
        for (final String column : columns) {
          EncryptedDataTypeInstaller.createMySqlHmacTriggers(conn, table, column);
        }
      }
      return true;
    } catch (final SQLException e) {
      if (isPg || !isRoutinePrivilegeError(e)) {
        throw e;
      }
      return false;
    }
  }

  /**
   * Drops the application table and deletes its {@code encryption_metadata} rows and the
   * {@code key_storage} rows they reference. The KMS master keys are never touched.
   *
   * @param conn           a direct (non-wrapper) connection to the database
   * @param metadataSchema the metadata schema holding {@code key_storage} and {@code encryption_metadata}
   * @param table          the application table name to tear down
   */
  public static void teardown(
      final Connection conn, final SchemaName metadataSchema, final String table) throws SQLException {
    final String schema = metadataSchema.getValue();

    // Read the key ids first, before the metadata rows that reference them are deleted.
    final List<Integer> keyIds = new ArrayList<>();
    try (PreparedStatement stmt = conn.prepareStatement(
        "SELECT key_id FROM " + schema + ".encryption_metadata WHERE table_name = ?")) {
      stmt.setString(1, table);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          keyIds.add(rs.getInt(1));
        }
      }
    }

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + table);
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "DELETE FROM " + schema + ".encryption_metadata WHERE table_name = ?")) {
      stmt.setString(1, table);
      stmt.executeUpdate();
    }

    if (!keyIds.isEmpty()) {
      try (PreparedStatement stmt = conn.prepareStatement(
          "DELETE FROM " + schema + ".key_storage WHERE id = ?")) {
        for (final Integer keyId : keyIds) {
          stmt.setInt(1, keyId);
          stmt.addBatch();
        }
        stmt.executeBatch();
      }
    }
  }
}
