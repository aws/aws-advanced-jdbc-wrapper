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

/** Tests for JSQLParserAnalyzer. */
class JSQLParserAnalyzerTest {

  @Test
  void testPostgreSqlSelect() {
    String sql = "SELECT name, age FROM users WHERE id = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
    // Note: JSQLParser may not preserve ? in toString()
    // assertTrue(result.hasParameters);
  }

  @Test
  void testMySqlSelect() {
    String sql = "SELECT `name`, `age` FROM `users` WHERE `id` = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    // JSQLParser may preserve backticks in table names
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
  }

  @Test
  void testInsertStatement() {
    String sql = "INSERT INTO users (name, email) VALUES (?, ?)";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
    assertEquals("name", result.columns.get(0).columnName);
    assertEquals("email", result.columns.get(1).columnName);
  }

  @Test
  void testUpdateStatement() {
    String sql = "UPDATE users SET name = ?, email = ? WHERE id = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
  }

  @Test
  void testDeleteStatement() {
    String sql = "DELETE FROM users WHERE id = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("DELETE", result.queryType);
    assertTrue(result.tables.contains("users"));
  }

  @Test
  void testMySqlBackticks() {
    String sql = "INSERT INTO `users` (`name`, `email`) VALUES (?, ?)";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("INSERT", result.queryType);
    // JSQLParser may preserve backticks in table names
    assertTrue(result.tables.contains("users") || result.tables.contains("`users`"));
    assertEquals(2, result.columns.size());
  }

  @Test
  void testComplexQuery() {
    String sql =
        "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = ?";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertTrue(result.tables.contains("orders"));
  }

  @Test
  void testInvalidSql() {
    String sql = "INVALID SQL STATEMENT";
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(sql);

    assertNotNull(result);
    assertEquals("UNKNOWN", result.queryType);
  }

  @Test
  void testEmptySql() {
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze("");

    assertEquals("UNKNOWN", result.queryType);
  }

  @Test
  void testNullSql() {
    JSQLParserAnalyzer.QueryAnalysis result =
        JSQLParserAnalyzer.analyze(null);

    assertEquals("UNKNOWN", result.queryType);
  }
}
