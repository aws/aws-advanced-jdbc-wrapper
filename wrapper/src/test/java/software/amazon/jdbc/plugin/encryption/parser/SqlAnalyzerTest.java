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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "age".equals(c.columnName)));
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
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze(
            "SELECT u.name, u.email, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = true");
    assertEquals("SELECT", result.queryType);
    assertEquals(2, result.tables.size());
    assertTrue(result.tables.contains("users"));
    assertTrue(result.tables.contains("posts"));
    assertEquals(3, result.columns.size());

    // Verify columns have correct table names (not aliases)
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "email".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "posts".equals(c.tableName) && "title".equals(c.columnName)));
  }

  @Test
  public void testCreateTable() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("CREATE TABLE test (id INT, name VARCHAR(50))");
    assertEquals("CREATE", result.queryType);
  }

  @Test
  public void testInsertWithoutPlaceholders() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')");
    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "email".equals(c.columnName)));
  }

  @Test
  public void testInsertWithPlaceholders() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("INSERT INTO users (name, email, age) VALUES (?, ?, ?)");
    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(3, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "email".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "age".equals(c.columnName)));
  }

  @Test
  public void testUpdateWithoutPlaceholders() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("UPDATE users SET name = 'Jane', email = 'jane@example.com' WHERE id = 1");
    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size()); // name, email (SET clause only)
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "email".equals(c.columnName)));
  }

  @Test
  public void testUpdateWithPlaceholders() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("UPDATE users SET name = ?, email = ? WHERE id = ?");
    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size()); // name, email (SET clause only)
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "email".equals(c.columnName)));
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

  @Test
  public void testMultiTableJoin() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze(
            "SELECT u.name, o.total, p.title FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id");
    assertEquals("SELECT", result.queryType);
    assertEquals(3, result.tables.size());
    assertTrue(result.tables.contains("users"));
    assertTrue(result.tables.contains("orders"));
    assertTrue(result.tables.contains("products"));
    assertEquals(3, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "orders".equals(c.tableName) && "total".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "products".equals(c.tableName) && "title".equals(c.columnName)));
  }

  @Test
  public void testCrossJoin() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("SELECT * FROM users CROSS JOIN products");
    assertEquals("SELECT", result.queryType);
    assertEquals(2, result.tables.size());
    assertTrue(result.tables.contains("users"));
    assertTrue(result.tables.contains("products"));
  }

  @Test
  public void testSelectWithCaseExpression() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze(
            "SELECT name, CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
    // Should extract 'name' column, CASE is treated as expression
    assertTrue(result.columns.stream().anyMatch(c -> "name".equals(c.columnName)));
  }

  @Test
  public void testSelectWithCast() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("SELECT CAST(price AS INTEGER) FROM products");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("products"));
  }

  @Test
  public void testUpdateMultipleColumns() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?");
    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(3, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "email".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "age".equals(c.columnName)));
  }

  @Test
  public void testInsertMultipleRows() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");
    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
  }

  @Test
  public void testSelectWithOrderBy() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("SELECT name, age FROM users ORDER BY age DESC NULLS LAST");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
  }

  @Test
  public void testSelectWithGroupBy() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("SELECT department, COUNT(*) FROM employees GROUP BY department");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("employees"));
  }

  @Test
  public void testSelectWithHaving() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("employees"));
  }

  @Test
  public void testSelectWithLimitOffset() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
  }

  @Test
  public void testDeleteWithWhere() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("DELETE FROM users WHERE age < 18");
    assertEquals("DELETE", result.queryType);
    assertTrue(result.tables.contains("users"));
  }

  @Test
  public void testSchemaQualifiedTable() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("SELECT * FROM public.users");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
  }

  @Test
  public void testLeftJoin() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze(
            "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id");
    assertEquals("SELECT", result.queryType);
    assertEquals(2, result.tables.size());
    assertTrue(result.tables.contains("users"));
    assertTrue(result.tables.contains("orders"));
  }

  @Test
  public void testRightJoin() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze(
            "SELECT u.name, o.total FROM users u RIGHT JOIN orders o ON u.id = o.user_id");
    assertEquals("SELECT", result.queryType);
    assertEquals(2, result.tables.size());
    assertTrue(result.tables.contains("users"));
    assertTrue(result.tables.contains("orders"));
  }

  @Test
  public void testInsertWithSchemaQualifiedTable() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("INSERT INTO myschema.users (name, ssn) VALUES (?, ?)");
    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "ssn".equals(c.columnName)));
    assertTrue(result.hasParameters);
  }

  @Test
  public void testUpdateWithSchemaQualifiedTable() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("UPDATE app_data.customers SET email = ? WHERE id = ?");
    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("customers"));
    assertEquals(1, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "customers".equals(c.tableName) && "email".equals(c.columnName)));
    assertEquals(1, result.whereColumns.size());
    assertTrue(
        result.whereColumns.stream()
            .anyMatch(c -> "customers".equals(c.tableName) && "id".equals(c.columnName)));
    assertTrue(result.hasParameters);
  }

  @Test
  public void testSelectWithSchemaQualifiedTableAndColumns() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("SELECT u.name, u.ssn FROM hr.users u WHERE u.id = ?");
    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "ssn".equals(c.columnName)));
    assertEquals(1, result.whereColumns.size());
    assertTrue(
        result.whereColumns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "id".equals(c.columnName)));
    assertTrue(result.hasParameters);
  }

  @Test
  public void testJoinWithMixedSchemaQualifiedTables() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze(
            "SELECT u.name, o.total FROM public.users u JOIN sales.orders o ON u.id = o.user_id");
    assertEquals("SELECT", result.queryType);
    assertEquals(2, result.tables.size());
    assertTrue(result.tables.contains("users"));
    assertTrue(result.tables.contains("orders"));
    assertEquals(2, result.columns.size());
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "users".equals(c.tableName) && "name".equals(c.columnName)));
    assertTrue(
        result.columns.stream()
            .anyMatch(c -> "orders".equals(c.tableName) && "total".equals(c.columnName)));
  }

  @Test
  public void testDeleteWithSchemaQualifiedTable() {
    SQLAnalyzer.QueryAnalysis result =
        analyzer.analyze("DELETE FROM archive.old_records WHERE created_at < ?");
    assertEquals("DELETE", result.queryType);
    assertTrue(result.tables.contains("old_records"));
    assertEquals(1, result.whereColumns.size());
    assertTrue(
        result.whereColumns.stream()
            .anyMatch(c -> "old_records".equals(c.tableName) && "created_at".equals(c.columnName)));
    assertTrue(result.hasParameters);
  }
}
