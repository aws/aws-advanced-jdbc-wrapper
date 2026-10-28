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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.plugin.encryption.model.SchemaName;
import software.amazon.jdbc.util.Messages;

public class EncryptedDataTypeInstaller {

  private static final Logger LOGGER = Logger.getLogger(EncryptedDataTypeInstaller.class.getName());
  private static final String SQL_RESOURCE_PATH = "/sql/encrypted_data_type.sql";
  private static final String MYSQL_SQL_RESOURCE_PATH = "/sql/encrypted_data_type_mysql.sql";
  private static final String SCHEMA_PLACEHOLDER = "SCHEMA_NAME";
  private static final String MYSQL_DELIMITER = "$$";
  private static final int MYSQL_MAX_NAME_LENGTH = 64;

  public static void installEncryptedDataType(Connection connection, SchemaName metaDataSchema) throws SQLException {

    // PostgreSQL-specific installation
    LOGGER.info(Messages.get("EncryptedDataTypeInstaller.installing"));

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
      LOGGER.fine(Messages.get("EncryptedDataTypeInstaller.pgcryptoEnabled"));

      // Use DOMAIN-based implementation
      String sql = loadSqlScript(SQL_RESOURCE_PATH);
      sql = sql.replaceFirst(SCHEMA_PLACEHOLDER, metaDataSchema.getValue());
      stmt.execute(sql);

      LOGGER.info(Messages.get("EncryptedDataTypeInstaller.installed"));
    }
  }

  public static boolean isEncryptedDataTypeInstalled(Connection connection) throws SQLException {
    String checkSql = "SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'encrypted_data')";
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(checkSql)) {
      return rs.next() && rs.getBoolean(1);
    }
  }

  /**
   * Installs the MySQL HMAC validation routines from {@code /sql/encrypted_data_type_mysql.sql} into
   * the connection's current database. The metadata schema placeholder in the script is replaced
   * with {@code metaDataSchema}, because a procedure called from a trigger cannot use dynamic SQL.
   */
  public static void installMySqlHmacValidation(Connection connection, SchemaName metaDataSchema)
      throws SQLException {
    LOGGER.info(Messages.get("EncryptedDataTypeInstaller.installingMySql", new Object[]{metaDataSchema}));
    final String sql = loadSqlScript(MYSQL_SQL_RESOURCE_PATH)
        .replace(SCHEMA_PLACEHOLDER, metaDataSchema.getValue());
    final List<String> statements = splitMySqlScript(sql);
    try (Statement stmt = connection.createStatement()) {
      for (String statement : statements) {
        stmt.execute(statement);
      }
    }
    LOGGER.info(Messages.get("EncryptedDataTypeInstaller.installedMySql", new Object[]{statements.size()}));
  }

  /**
   * Creates the BEFORE INSERT and BEFORE UPDATE triggers that call {@code validate_encrypted_data_hmac}
   * for one column. The triggers pass the user variable {@code @hmac_key_<table>_<column>} as the
   * session cache of the HMAC key; this is the variable KeyManagementUtility sets before it
   * re-encrypts the column.
   */
  public static void createMySqlHmacTriggers(Connection connection, String table, String column)
      throws SQLException {
    final String insertSql = mysqlHmacTriggerSql(table, column, true);
    final String updateSql = mysqlHmacTriggerSql(table, column, false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TRIGGER IF EXISTS " + mysqlTriggerName(table, column, true));
      stmt.execute(insertSql);
      stmt.execute("DROP TRIGGER IF EXISTS " + mysqlTriggerName(table, column, false));
      stmt.execute(updateSql);
    }
  }

  static String mysqlHmacTriggerSql(String table, String column, boolean insert) {
    validateIdentifier(table);
    validateIdentifier(column);
    final String triggerName = mysqlTriggerName(table, column, insert);
    final String variableName = "hmac_key_" + table + "_" + column;
    validateMySqlNameLength(triggerName);
    validateMySqlNameLength(variableName);
    return "CREATE TRIGGER " + triggerName + " BEFORE " + (insert ? "INSERT" : "UPDATE")
        + " ON " + table + " FOR EACH ROW CALL validate_encrypted_data_hmac('"
        + table + "', '" + column + "', NEW." + column + ", @" + variableName + ")";
  }

  /**
   * Splits the MySQL script into statements for JDBC: drops the {@code DELIMITER} lines the mysql
   * client needs, splits on the {@code $$} delimiter and removes comment lines and empty chunks.
   */
  static List<String> splitMySqlScript(String script) {
    final StringBuilder body = new StringBuilder();
    for (String line : script.split("\\r?\\n")) {
      if (!line.trim().startsWith("DELIMITER")) {
        body.append(line).append('\n');
      }
    }
    final List<String> statements = new ArrayList<>();
    int start = 0;
    while (start <= body.length()) {
      int end = body.indexOf(MYSQL_DELIMITER, start);
      if (end < 0) {
        end = body.length();
      }
      final String chunk = stripCommentLines(body.substring(start, end));
      if (!chunk.isEmpty()) {
        statements.add(chunk);
      }
      start = end + MYSQL_DELIMITER.length();
    }
    return statements;
  }

  private static String stripCommentLines(String chunk) {
    final StringBuilder sb = new StringBuilder();
    for (String line : chunk.split("\n")) {
      if (!line.trim().startsWith("--")) {
        sb.append(line).append('\n');
      }
    }
    return sb.toString().trim();
  }

  private static String mysqlTriggerName(String table, String column, boolean insert) {
    return table + "_" + column + (insert ? "_hmac_ins" : "_hmac_upd");
  }

  private static void validateIdentifier(String name) {
    if (!SchemaName.isValidIdentifier(name)) {
      throw new IllegalArgumentException(
          Messages.get("EncryptedDataTypeInstaller.invalidIdentifier", new Object[]{name}));
    }
  }

  private static void validateMySqlNameLength(String name) {
    if (name.length() > MYSQL_MAX_NAME_LENGTH) {
      throw new IllegalArgumentException(
          Messages.get("EncryptedDataTypeInstaller.mysqlNameTooLong", new Object[]{name, MYSQL_MAX_NAME_LENGTH}));
    }
  }

  private static String loadSqlScript(String resourcePath) {
    try (InputStream is = EncryptedDataTypeInstaller.class.getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IllegalStateException(
            Messages.get("EncryptedDataTypeInstaller.sqlScriptNotFound", new Object[]{resourcePath}));
      }

      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (Exception e) {
      throw new IllegalStateException(
          Messages.get("EncryptedDataTypeInstaller.sqlScriptLoadFailed", new Object[]{resourcePath, e}));
    }
  }
}
