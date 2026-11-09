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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

/**
 * Shared setup for the KMS encryption integration tests, modeled on the Ruby driver's
 * {@code spec/integration/container/utils/kms_encryption_helper.rb}.
 *
 * <p>It owns the parts that every KMS test class needs identically: resolving the KMS key and region
 * from the environment, building the {@link KmsClient}, and creating the encryption metadata schema
 * with its {@code key_storage} and {@code encryption_metadata} tables (plus the PostgreSQL
 * {@code encrypted_data} type). Key insertion is deliberately left to each test, because the classes
 * populate those tables differently — one by hand, one through the production
 * {@code KeyManagementUtility} — and sharing it would force one of them to change behaviour.
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
   * privilege. The HMAC validation triggers are optional, so a test treats these as "not installed"
   * rather than a failure.
   */
  public static final List<Integer> MYSQL_ROUTINE_PRIVILEGE_ERRORS =
      Collections.unmodifiableList(Arrays.asList(1419, 1227, 1142, 1370));

  private KmsTestUtility() {
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

  /**
   * Builds a {@link KmsClient} for the test region.
   *
   * <p>Credentials come from {@link DefaultCredentialsProvider} rather than the builder's implicit
   * default, so a client created once in {@code @BeforeAll} keeps working on runs that outlive their
   * initial session token. The caller owns the returned client's lifecycle.
   */
  public static KmsClient kmsClient(final String region) {
    return KmsClient.builder()
        .region(Region.of(region))
        .credentialsProvider(DefaultCredentialsProvider.create())
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
}
