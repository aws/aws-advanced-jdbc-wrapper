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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class EncryptedDataTypeInstallerTest {

  @Test
  void triggerSqlPassesTheCacheVariableOfKeyManagementUtility() {
    assertEquals(
        "CREATE TRIGGER users_ssn_hmac_ins BEFORE INSERT ON users FOR EACH ROW "
            + "CALL validate_encrypted_data_hmac('users', 'ssn', NEW.ssn, @hmac_key_users_ssn)",
        EncryptedDataTypeInstaller.mysqlHmacTriggerSql("users", "ssn", true));
    final String updateSql = EncryptedDataTypeInstaller.mysqlHmacTriggerSql("users", "ssn", false);
    assertEquals(
        "CREATE TRIGGER users_ssn_hmac_upd BEFORE UPDATE ON users FOR EACH ROW "
            + "CALL validate_encrypted_data_hmac('users', 'ssn', NEW.ssn, @hmac_key_users_ssn)",
        updateSql);
    // Must match the variable KeyManagementUtility.primeTriggerHmacCache sets:
    // "hmac_key_" + table + "_" + column.
    assertTrue(updateSql.contains("@hmac_key_users_ssn"));
  }

  @Test
  void triggerSqlRejectsInvalidIdentifiers() {
    assertThrows(IllegalArgumentException.class,
        () -> EncryptedDataTypeInstaller.mysqlHmacTriggerSql("users; DROP TABLE x", "ssn", true));
    assertThrows(IllegalArgumentException.class,
        () -> EncryptedDataTypeInstaller.mysqlHmacTriggerSql("users", "ssn'", true));
  }

  @Test
  void triggerSqlRejectsNamesLongerThanMySqlAllows() {
    final StringBuilder table = new StringBuilder();
    for (int i = 0; i < 60; i++) {
      table.append('t');
    }
    assertThrows(IllegalArgumentException.class,
        () -> EncryptedDataTypeInstaller.mysqlHmacTriggerSql(table.toString(), "ssn", true));
  }

  @Test
  void mySqlScriptSplitsIntoStaticStatements() throws Exception {
    final List<String> statements = EncryptedDataTypeInstaller.splitMySqlScript(readScript());
    assertEquals(7, statements.size(), String.join("\n----\n", statements));
    assertTrue(statements.get(0).startsWith("DROP FUNCTION IF EXISTS hmac_sha256"));
    assertTrue(statements.get(1).startsWith("CREATE FUNCTION hmac_sha256"));
    assertTrue(statements.get(2).startsWith("DROP FUNCTION IF EXISTS verify_encrypted_data_hmac"));
    assertTrue(statements.get(3).startsWith("CREATE FUNCTION verify_encrypted_data_hmac"));
    assertTrue(statements.get(4).startsWith(
        "DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_insert"));
    assertTrue(statements.get(5).startsWith("DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac"));
    assertTrue(statements.get(6).startsWith("CREATE PROCEDURE validate_encrypted_data_hmac("));
    for (String statement : statements) {
      assertFalse(statement.contains("DELIMITER"), statement);
      assertFalse(statement.contains("$$"), statement);
      assertFalse(statement.contains("PREPARE"), statement);
      assertFalse(statement.contains("EXECUTE"), statement);
    }
    assertTrue(statements.get(6).contains("SCHEMA_NAME.encryption_metadata"));
  }

  private static String readScript() throws Exception {
    try (InputStream is = EncryptedDataTypeInstallerTest.class.getResourceAsStream(
        "/sql/encrypted_data_type_mysql.sql");
         BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining("\n"));
    }
  }
}
