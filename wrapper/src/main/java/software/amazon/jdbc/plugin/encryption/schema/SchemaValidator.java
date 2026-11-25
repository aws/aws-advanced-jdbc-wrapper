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

package software.amazon.jdbc.plugin.encryption.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;

/**
 * Validates that the required database schema for encryption metadata exists and has the correct
 * structure.
 *
 * <p>Tables and columns are looked up in the configured metadata schema through {@code
 * information_schema}, which PostgreSQL and MySQL both expose. (On MySQL a "schema" is a database.)
 * Constraints are read from {@code information_schema} on MySQL and from {@code pg_catalog} on
 * PostgreSQL, whose {@code information_schema} hides constraints on tables the user holds only
 * {@code SELECT} on. The lookup never depends on the session's default schema. The user running the
 * validation must be able to see the tables: {@code information_schema} only lists tables the user
 * has privileges on.
 */
public class SchemaValidator {

  private static final String ENCRYPTION_METADATA_TABLE = "encryption_metadata";
  private static final String KEY_STORAGE_TABLE = "key_storage";

  private static final Set<String> REQUIRED_ENCRYPTION_METADATA_COLUMNS =
      new HashSet<>(
          Arrays.asList(
              "table_name",
              "column_name",
              "encryption_algorithm",
              "key_id",
              "created_at",
              "updated_at"));

  private static final Set<String> REQUIRED_KEY_STORAGE_COLUMNS =
      new HashSet<>(
          Arrays.asList(
              "id",
              "key_id",
              "name",
              "master_key_arn",
              "encrypted_data_key",
              "hmac_key",
              "key_spec",
              "created_at",
              "last_used_at"));

  private static final String TABLE_EXISTS_SQL =
      "SELECT 1 FROM information_schema.tables"
          + " WHERE LOWER(table_schema) = ? AND LOWER(table_name) = ?";

  private static final String COLUMNS_SQL =
      "SELECT column_name FROM information_schema.columns"
          + " WHERE LOWER(table_schema) = ? AND LOWER(table_name) = ?";

  // MySQL: one row per column of every PRIMARY KEY and UNIQUE constraint on the table. MySQL lists a
  // table in information_schema for any user with a privilege on it, so a SELECT-only role sees it.
  private static final String MYSQL_KEY_CONSTRAINTS_SQL =
      "SELECT tc.constraint_name, kcu.column_name"
          + " FROM information_schema.table_constraints tc"
          + " JOIN information_schema.key_column_usage kcu"
          + "   ON kcu.constraint_schema = tc.constraint_schema"
          + "  AND kcu.constraint_name = tc.constraint_name"
          + "  AND kcu.table_schema = tc.table_schema"
          + "  AND kcu.table_name = tc.table_name"
          + " WHERE tc.constraint_type = ?"
          + "   AND LOWER(tc.table_schema) = ? AND LOWER(tc.table_name) = ?"
          + " ORDER BY tc.constraint_name, kcu.ordinal_position";

  // MySQL and MariaDB record the referenced table and column directly, which avoids the ambiguity of
  // joining on constraint names (a primary key is named PRIMARY in every table). The referenced
  // table must be in the same schema.
  private static final String MYSQL_FOREIGN_KEYS_SQL =
      "SELECT column_name, referenced_table_name, referenced_column_name"
          + " FROM information_schema.key_column_usage"
          + " WHERE LOWER(table_schema) = ? AND LOWER(table_name) = ?"
          + "   AND referenced_table_name IS NOT NULL"
          + "   AND referenced_table_schema = table_schema";

  // PostgreSQL: information_schema.table_constraints, referential_constraints and key_column_usage
  // only show tables the user owns or holds a privilege other than SELECT on, which would hide the
  // constraints from the SELECT-only runtime role. pg_catalog is not filtered by privileges.
  // contype is "p" for PRIMARY KEY and "u" for UNIQUE.
  private static final String PG_KEY_CONSTRAINTS_SQL =
      "SELECT c.conname, a.attname"
          + " FROM pg_catalog.pg_constraint c"
          + " JOIN pg_catalog.pg_class t ON t.oid = c.conrelid"
          + " JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace"
          + " CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)"
          + " JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum"
          + " WHERE c.contype = (CASE ? WHEN 'PRIMARY KEY' THEN 'p' ELSE 'u' END)::\"char\""
          + "   AND LOWER(n.nspname) = ? AND LOWER(t.relname) = ?"
          + " ORDER BY c.conname, k.ord";

  // PostgreSQL: one row per column of every foreign key on the table, with the column it references.
  // The referenced table must be in the same schema.
  private static final String PG_FOREIGN_KEYS_SQL =
      "SELECT a.attname, rt.relname, ra.attname"
          + " FROM pg_catalog.pg_constraint c"
          + " JOIN pg_catalog.pg_class t ON t.oid = c.conrelid"
          + " JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace"
          + " JOIN pg_catalog.pg_class rt ON rt.oid = c.confrelid"
          + " CROSS JOIN LATERAL unnest(c.conkey, c.confkey) AS k(attnum, refnum)"
          + " JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum"
          + " JOIN pg_catalog.pg_attribute ra ON ra.attrelid = rt.oid AND ra.attnum = k.refnum"
          + " WHERE c.contype = 'f'::\"char\""
          + "   AND rt.relnamespace = t.relnamespace"
          + "   AND LOWER(n.nspname) = ? AND LOWER(t.relname) = ?";

  private final SchemaName metadataSchema;

  public SchemaValidator(SchemaName metadataSchema) {
    this.metadataSchema = Objects.requireNonNull(metadataSchema, "Metadata schema cannot be null");
  }

  private String qualified(String table) {
    return metadataSchema + "." + table;
  }

  private String schemaKey() {
    return metadataSchema.getValue().toLowerCase(Locale.ROOT);
  }

  /**
   * Validates that all required tables and columns exist in the database.
   *
   * @param connection Database connection to validate against
   * @return ValidationResult containing validation status and any issues found
   * @throws SQLException if database access fails
   */
  public ValidationResult validateSchema(Connection connection) throws SQLException {
    List<String> issues = new ArrayList<>();

    boolean encryptionMetadataExists = tableExists(connection, ENCRYPTION_METADATA_TABLE);
    if (!encryptionMetadataExists) {
      issues.add("Table '" + qualified(ENCRYPTION_METADATA_TABLE) + "' does not exist");
    } else {
      issues.addAll(
          validateTableColumns(
              connection, ENCRYPTION_METADATA_TABLE, REQUIRED_ENCRYPTION_METADATA_COLUMNS));
      issues.addAll(validateEncryptionMetadataConstraints(connection));
    }

    boolean keyStorageExists = tableExists(connection, KEY_STORAGE_TABLE);
    if (!keyStorageExists) {
      issues.add("Table '" + qualified(KEY_STORAGE_TABLE) + "' does not exist");
    } else {
      issues.addAll(validateTableColumns(connection, KEY_STORAGE_TABLE, REQUIRED_KEY_STORAGE_COLUMNS));
      issues.addAll(validateKeyStorageConstraints(connection));
    }

    // The foreign key can only be checked once both tables are known to be sound.
    if (issues.isEmpty()) {
      issues.addAll(validateForeignKeyConstraints(connection));
    }

    return new ValidationResult(issues.isEmpty(), issues);
  }

  /** Checks if a table exists in the metadata schema. */
  private boolean tableExists(Connection connection, String table) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(TABLE_EXISTS_SQL)) {
      stmt.setString(1, schemaKey());
      stmt.setString(2, table.toLowerCase(Locale.ROOT));
      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** Validates that all required columns exist in a table. */
  private List<String> validateTableColumns(
      Connection connection, String table, Set<String> requiredColumns) throws SQLException {
    Set<String> existingColumns = new HashSet<>();
    try (PreparedStatement stmt = connection.prepareStatement(COLUMNS_SQL)) {
      stmt.setString(1, schemaKey());
      stmt.setString(2, table.toLowerCase(Locale.ROOT));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          // column_name is never null in information_schema; the guard satisfies the nullness checker.
          final @Nullable String columnName = rs.getString(1);
          if (columnName != null) {
            existingColumns.add(columnName.toLowerCase(Locale.ROOT));
          }
        }
      }
    }

    List<String> issues = new ArrayList<>();
    for (String requiredColumn : requiredColumns) {
      if (!existingColumns.contains(requiredColumn.toLowerCase(Locale.ROOT))) {
        issues.add(
            String.format(
                "Table '%s' is missing required column '%s'", qualified(table), requiredColumn));
      }
    }
    return issues;
  }

  /** Validates constraints specific to the encryption_metadata table. */
  private List<String> validateEncryptionMetadataConstraints(Connection connection)
      throws SQLException {
    List<String> issues = new ArrayList<>();
    // A primary key is a unique constraint, so either one satisfies this.
    List<String> columns = Arrays.asList("table_name", "column_name");
    if (!hasKeyConstraint(connection, ENCRYPTION_METADATA_TABLE, "PRIMARY KEY", columns)
        && !hasKeyConstraint(connection, ENCRYPTION_METADATA_TABLE, "UNIQUE", columns)) {
      issues.add(
          "Table '"
              + qualified(ENCRYPTION_METADATA_TABLE)
              + "' is missing unique constraint on (table_name, column_name)");
    }
    return issues;
  }

  /** Validates constraints specific to the key_storage table. */
  private List<String> validateKeyStorageConstraints(Connection connection) throws SQLException {
    List<String> issues = new ArrayList<>();
    if (!hasKeyConstraint(
        connection, KEY_STORAGE_TABLE, "PRIMARY KEY", java.util.Collections.singletonList("id"))) {
      issues.add("Table '" + qualified(KEY_STORAGE_TABLE) + "' is missing primary key on 'id'");
    }
    return issues;
  }

  /** Validates the foreign key from encryption_metadata.key_id to key_storage.id. */
  private List<String> validateForeignKeyConstraints(Connection connection) throws SQLException {
    List<String> issues = new ArrayList<>();
    if (!hasForeignKey(connection, ENCRYPTION_METADATA_TABLE, "key_id", KEY_STORAGE_TABLE, "id")) {
      issues.add(
          "Missing foreign key constraint from "
              + qualified(ENCRYPTION_METADATA_TABLE)
              + ".key_id to "
              + qualified(KEY_STORAGE_TABLE)
              + ".id");
    }
    return issues;
  }

  /**
   * Checks whether the table has a constraint of the given type covering exactly the given
   * columns.
   */
  private boolean hasKeyConstraint(
      Connection connection, String table, String constraintType, List<String> columnNames)
      throws SQLException {
    Map<String, Set<String>> columnsByConstraint = new LinkedHashMap<>();
    try (PreparedStatement stmt =
        connection.prepareStatement(
            isMySql(connection) ? MYSQL_KEY_CONSTRAINTS_SQL : PG_KEY_CONSTRAINTS_SQL)) {
      stmt.setString(1, constraintType);
      stmt.setString(2, schemaKey());
      stmt.setString(3, table.toLowerCase(Locale.ROOT));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          final @Nullable String constraintName = rs.getString(1);
          final @Nullable String columnName = rs.getString(2);
          if (constraintName != null && columnName != null) {
            columnsByConstraint
                .computeIfAbsent(constraintName, k -> new HashSet<>())
                .add(columnName.toLowerCase(Locale.ROOT));
          }
        }
      }
    }

    Set<String> wanted = new HashSet<>();
    for (String columnName : columnNames) {
      wanted.add(columnName.toLowerCase(Locale.ROOT));
    }
    return columnsByConstraint.containsValue(wanted);
  }

  private static boolean isMySql(Connection connection) throws SQLException {
    final String product = connection.getMetaData().getDatabaseProductName();
    return product != null && product.toLowerCase(Locale.ROOT).matches(".*(mysql|mariadb).*");
  }

  /** Checks if a foreign key exists between the specified tables and columns. */
  private boolean hasForeignKey(
      Connection connection, String fromTable, String fromColumn, String toTable, String toColumn)
      throws SQLException {
    try (PreparedStatement stmt =
        connection.prepareStatement(
            isMySql(connection) ? MYSQL_FOREIGN_KEYS_SQL : PG_FOREIGN_KEYS_SQL)) {
      stmt.setString(1, schemaKey());
      stmt.setString(2, fromTable.toLowerCase(Locale.ROOT));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          if (fromColumn.equalsIgnoreCase(rs.getString(1))
              && toTable.equalsIgnoreCase(rs.getString(2))
              && toColumn.equalsIgnoreCase(rs.getString(3))) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /** Result of schema validation containing status and any issues found. */
  public static class ValidationResult {
    private final boolean valid;
    private final List<String> issues;

    public ValidationResult(boolean valid, List<String> issues) {
      this.valid = valid;
      this.issues = new ArrayList<>(issues);
    }

    public boolean isValid() {
      return valid;
    }

    public List<String> getIssues() {
      return new ArrayList<>(issues);
    }

    @Override
    public String toString() {
      if (valid) {
        return "Schema validation passed";
      } else {
        return "Schema validation failed: " + String.join(", ", issues);
      }
    }
  }
}
