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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Validates that the required database schema for encryption metadata exists and has the correct
 * structure.
 */
public class SchemaValidator {

  private final String metadataSchema;

  public SchemaValidator(String metadataSchema) {
    this.metadataSchema = Objects.requireNonNull(metadataSchema, "Metadata schema cannot be null");
  }

  private String getEncryptionMetadataTable() {
    return metadataSchema + ".encryption_metadata";
  }

  private String getKeyStorageTable() {
    return metadataSchema + ".key_storage";
  }

  private static final Set<String> REQUIRED_ENCRYPTION_METADATA_COLUMNS =
      new HashSet<>(
          Arrays.asList(
              "id",
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
              "name",
              "master_key_arn",
              "encrypted_data_key",
              "key_spec",
              "created_at",
              "last_used_at"));

  /**
   * Validates that all required tables and columns exist in the database.
   *
   * @param connection Database connection to validate against
   * @return ValidationResult containing validation status and any issues found
   * @throws SQLException if database access fails
   */
  public ValidationResult validateSchema(Connection connection) throws SQLException {
    List<String> issues = new ArrayList<>();

    // Validate encryption_metadata table
    String encryptionMetadataTable = getEncryptionMetadataTable();
    if (!tableExists(connection, encryptionMetadataTable)) {
      issues.add("Table '" + encryptionMetadataTable + "' does not exist");
    } else {
      issues.addAll(
          validateTableColumns(
              connection, encryptionMetadataTable, REQUIRED_ENCRYPTION_METADATA_COLUMNS));
      issues.addAll(validateEncryptionMetadataConstraints(connection));
    }

    // Validate key_storage table
    String keyStorageTable = getKeyStorageTable();
    if (!tableExists(connection, keyStorageTable)) {
      issues.add("Table '" + keyStorageTable + "' does not exist");
    } else {
      issues.addAll(
          validateTableColumns(connection, keyStorageTable, REQUIRED_KEY_STORAGE_COLUMNS));
      issues.addAll(validateKeyStorageConstraints(connection));
    }

    // Validate foreign key relationship
    if (issues.isEmpty()) {
      issues.addAll(validateForeignKeyConstraints(connection));
    }

    return new ValidationResult(issues.isEmpty(), issues);
  }

  /** Checks if a table exists in the database. */
  private boolean tableExists(Connection connection, String tableName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // Get current schema
    String currentSchema = getCurrentSchema(connection);

    // Only check in the current schema to avoid cross-contamination
    try (ResultSet rs =
        metaData.getTables(null, currentSchema, tableName, new String[] {"TABLE"})) {
      return rs.next();
    }
  }

  /** Gets the current schema name from the connection. */
  private String getCurrentSchema(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT current_schema()")) {
      if (rs.next()) {
        return rs.getString(1);
      }
    }
    return null;
  }

  /** Validates that all required columns exist in a table. */
  private List<String> validateTableColumns(
      Connection connection, String tableName, Set<String> requiredColumns) throws SQLException {
    List<String> issues = new ArrayList<>();
    Set<String> existingColumns = new HashSet<>();

    DatabaseMetaData metaData = connection.getMetaData();
    String currentSchema = getCurrentSchema(connection);

    // Try with current schema first
    try (ResultSet rs = metaData.getColumns(null, currentSchema, tableName, null)) {
      while (rs.next()) {
        existingColumns.add(rs.getString("COLUMN_NAME").toLowerCase());
      }
    }

    // If no columns found, try without schema
    if (existingColumns.isEmpty()) {
      try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
        while (rs.next()) {
          existingColumns.add(rs.getString("COLUMN_NAME").toLowerCase());
        }
      }
    }

    for (String requiredColumn : requiredColumns) {
      if (!existingColumns.contains(requiredColumn.toLowerCase())) {
        issues.add(
            String.format("Table '%s' is missing required column '%s'", tableName, requiredColumn));
      }
    }

    return issues;
  }

  /** Validates constraints specific to encryption_metadata table. */
  private List<String> validateEncryptionMetadataConstraints(Connection connection)
      throws SQLException {
    List<String> issues = new ArrayList<>();

    // Check for unique constraint on table_name, column_name
    String encryptionMetadataTable = getEncryptionMetadataTable();
    if (!hasUniqueConstraint(
        connection, encryptionMetadataTable, Arrays.asList("table_name", "column_name"))) {
      issues.add(
          "Table '"
              + encryptionMetadataTable
              + "' is missing unique constraint on (table_name, column_name)");
    }

    return issues;
  }

  /** Validates constraints specific to key_storage table. */
  private List<String> validateKeyStorageConstraints(Connection connection) throws SQLException {
    List<String> issues = new ArrayList<>();

    // Check for primary key on id
    String keyStorageTable = getKeyStorageTable();
    if (!hasPrimaryKey(connection, keyStorageTable, "id")) {
      issues.add("Table '" + keyStorageTable + "' is missing primary key on 'id'");
    }

    return issues;
  }

  /** Validates foreign key constraints between tables. */
  private List<String> validateForeignKeyConstraints(Connection connection) throws SQLException {
    List<String> issues = new ArrayList<>();

    // Check for foreign key from encryption_metadata.key_id to key_storage.id
    String encryptionMetadataTable = getEncryptionMetadataTable();
    String keyStorageTable = getKeyStorageTable();
    if (!hasForeignKey(connection, encryptionMetadataTable, "key_id", keyStorageTable, "id")) {
      issues.add(
          "Missing foreign key constraint from "
              + encryptionMetadataTable
              + ".key_id to "
              + keyStorageTable
              + ".id");
    }

    return issues;
  }

  /** Checks if a unique constraint exists on the specified columns. */
  private boolean hasUniqueConstraint(
      Connection connection, String tableName, List<String> columnNames) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String currentSchema = getCurrentSchema(connection);
    try (ResultSet rs = metaData.getIndexInfo(null, currentSchema, tableName, true, false)) {
      Set<String> indexColumns = new HashSet<>();
      String currentIndexName = null;

      while (rs.next()) {
        String indexName = rs.getString("INDEX_NAME");
        String columnName = rs.getString("COLUMN_NAME");

        if (currentIndexName == null || !currentIndexName.equals(indexName)) {
          // Check previous index
          if (indexColumns.size() == columnNames.size()
              && indexColumns.containsAll(
                  columnNames.stream()
                      .map(String::toLowerCase)
                      .collect(java.util.stream.Collectors.toList()))) {
            return true;
          }
          // Start new index
          currentIndexName = indexName;
          indexColumns.clear();
        }

        if (columnName != null) {
          indexColumns.add(columnName.toLowerCase());
        }
      }

      // Check last index
      return indexColumns.size() == columnNames.size()
          && indexColumns.containsAll(
              columnNames.stream()
                  .map(String::toLowerCase)
                  .collect(java.util.stream.Collectors.toList()));
    }
  }

  /** Checks if a primary key exists on the specified column. */
  private boolean hasPrimaryKey(Connection connection, String tableName, String columnName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String currentSchema = getCurrentSchema(connection);
    try (ResultSet rs = metaData.getPrimaryKeys(null, currentSchema, tableName)) {
      while (rs.next()) {
        if (columnName.equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
          return true;
        }
      }
    }
    return false;
  }

  /** Checks if a foreign key exists between the specified tables and columns. */
  private boolean hasForeignKey(
      Connection connection, String fromTable, String fromColumn, String toTable, String toColumn)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String currentSchema = getCurrentSchema(connection);
    try (ResultSet rs = metaData.getImportedKeys(null, currentSchema, fromTable)) {
      while (rs.next()) {
        String fkColumnName = rs.getString("FKCOLUMN_NAME");
        String pkTableName = rs.getString("PKTABLE_NAME");
        String pkColumnName = rs.getString("PKCOLUMN_NAME");

        if (fromColumn.equalsIgnoreCase(fkColumnName)
            && toTable.equalsIgnoreCase(pkTableName)
            && toColumn.equalsIgnoreCase(pkColumnName)) {
          return true;
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
