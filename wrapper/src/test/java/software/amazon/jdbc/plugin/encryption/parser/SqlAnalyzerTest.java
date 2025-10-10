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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlAnalyzerTest {

  private SQLAnalyzer analyzer;

  @BeforeEach
  public void setUp() {
    analyzer = new SQLAnalyzer();
  }

  @Test
  public void testSelectWithColumns() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("SELECT name, age FROM users");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
  }

  @Test
  public void testSelectStar() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("SELECT * FROM products");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("products"));
    assertEquals(0, result.columns.size()); // * is not added to columns
  }

  @Test
  public void testSelectWithoutTable() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("SELECT 1, 'test'");
    assertEquals("SELECT", result.queryType);
  }

  @Test
  public void testInvalidSQL() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("INVALID SQL");
    assertEquals("UNKNOWN", result.queryType);
  }

  @Test
  public void testComplexSelect() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze(
        "SELECT u.name, u.email, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = true");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(3, result.columns.size());
  }

  @Test
  public void testCreateTable() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("CREATE TABLE test (id INT, name VARCHAR(50))");
    assertEquals("CREATE", result.queryType);
  }

  @Test
  public void testInsertWithoutPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')");
    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
  }

  @Test
  public void testInsertWithPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("INSERT INTO users (name, email, age) VALUES (?, ?, ?)");
    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(3, result.columns.size());
  }

  @Test
  public void testUpdateWithoutPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("UPDATE users SET name = 'Jane', email = 'jane@example.com' WHERE id = 1");
    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
  }

  @Test
  public void testUpdateWithPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("UPDATE users SET name = ?, email = ? WHERE id = ?");
    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
  }

  @Test
  public void testDelete() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("DELETE FROM users WHERE id = 1");
    assertEquals("DELETE", result.queryType);
    assertTrue(result.tables.contains("users"));
  }

  @Test
  public void testDrop() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("DROP TABLE users");
    assertEquals("DROP", result.queryType);
  }
}
