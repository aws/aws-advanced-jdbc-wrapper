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

package software.amazon.jdbc.plugin.encryption.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** MySQL-specific parser tests. */
class MySqlParserTest {

  @Test
  void testMySqlBacktickIdentifiers() {
    String sql = "SELECT `user_id`, `email` FROM `users` WHERE `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlInsertWithBackticks() {
    String sql = "INSERT INTO `users` (`name`, `email`, `ssn`) VALUES (?, ?, ?)";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("INSERT", result.queryType);
    assertEquals(3, result.columns.size());
  }

  @Test
  void testMySqlUpdateWithBackticks() {
    String sql = "UPDATE `users` SET `name` = ?, `email` = ? WHERE `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("UPDATE", result.queryType);
    assertEquals(2, result.columns.size());
  }

  @Test
  void testMySqlDeleteWithBackticks() {
    String sql = "DELETE FROM `users` WHERE `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("DELETE", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlJoinWithBackticks() {
    String sql =
        "SELECT `u`.`name`, `o`.`total` FROM `users` `u` "
            + "JOIN `orders` `o` ON `u`.`id` = `o`.`user_id` WHERE `u`.`id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.size() >= 2);
  }

  @Test
  void testMySqlDoubleQuotes() {
    // MySQL can use double quotes in ANSI mode
    String sql = "SELECT \"name\" FROM \"users\" WHERE \"id\" = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertNotNull(result.tables);
  }

  @Test
  void testMySqlMixedQuoting() {
    String sql = "SELECT `name`, email FROM users WHERE `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
  }

  @Test
  void testMySqlReservedKeywordAsColumn() {
    // 'order' is a reserved keyword in MySQL, must be backticked
    String sql = "SELECT `order`, `date` FROM `orders` WHERE `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("orders") || result.tables.contains("`orders`"));
  }

  @Test
  void testMySqlInsertMultipleRows() {
    String sql = "INSERT INTO `users` (`name`, `email`) VALUES (?, ?), (?, ?)";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("INSERT", result.queryType);
    assertEquals(2, result.columns.size()); // Column count, not value count
  }

  @Test
  void testMySqlOnDuplicateKeyUpdate() {
    String sql =
        "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) "
            + "ON DUPLICATE KEY UPDATE `name` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlLimit() {
    String sql = "SELECT `name` FROM `users` WHERE `active` = ? LIMIT 10";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlLimitOffset() {
    String sql = "SELECT `name` FROM `users` WHERE `active` = ? LIMIT 10 OFFSET 20";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlCreateTable() {
    String sql =
        "CREATE TABLE `users` ("
            + "`id` INT AUTO_INCREMENT PRIMARY KEY, "
            + "`name` VARCHAR(100), "
            + "`email` VARCHAR(255)"
            + ")";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("CREATE", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlDropTable() {
    String sql = "DROP TABLE `users`";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("DROP", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlSubquery() {
    String sql =
        "SELECT `name` FROM `users` WHERE `id` IN "
            + "(SELECT `user_id` FROM `orders` WHERE `total` > ?)";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.size() >= 2);
  }

  @Test
  void testMySqlUnion() {
    String sql =
        "SELECT `name` FROM `users` WHERE `id` = ? "
            + "UNION "
            + "SELECT `name` FROM `archived_users` WHERE `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.size() >= 2);
  }

  @Test
  void testMySqlCaseInsensitiveKeywords() {
    String sql = "select `name` from `users` where `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testMySqlComplexWhere() {
    String sql =
        "SELECT `name` FROM `users` "
            + "WHERE `age` > ? AND (`status` = ? OR `role` = ?)";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.whereColumns.size() >= 3);
  }

  @Test
  void testMySqlGroupBy() {
    String sql =
        "SELECT `department`, COUNT(*) FROM `employees` "
            + "WHERE `active` = ? GROUP BY `department`";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("employees") || result.tables.contains("`employees`"));
  }

  @Test
  void testMySqlHaving() {
    String sql =
        "SELECT `department`, COUNT(*) as cnt FROM `employees` "
            + "GROUP BY `department` HAVING cnt > ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("employees") || result.tables.contains("`employees`"));
  }
}
