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
    assertEquals("users.name", result.columns.get(0).toString());
    assertEquals("users.age", result.columns.get(1).toString());
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
    assertEquals(2, result.columns.size());
    assertEquals("unknown.1", result.columns.get(0).toString());
    assertEquals("unknown.test", result.columns.get(1).toString());
  }

  @Test
  public void testInvalidSQL() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("SELECT FROM");

    assertEquals("UNKNOWN", result.queryType);
  }

  @Test
  public void testComplexSelect() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("SELECT id, name, email FROM customers WHERE active = true");

    assertEquals("SELECT", result.queryType);
    assertTrue(result.tables.contains("customers"));
    assertEquals(3, result.columns.size());
    assertEquals("customers.id", result.columns.get(0).toString());
    assertEquals("customers.name", result.columns.get(1).toString());
    assertEquals("customers.email", result.columns.get(2).toString());
  }

  @Test
  public void testMultipleQueries() {
    SQLAnalyzer.QueryAnalysis result1 = analyzer.analyze("SELECT first_name, last_name FROM employees");
    SQLAnalyzer.QueryAnalysis result2 = analyzer.analyze("SELECT product_name, price FROM inventory");

    assertEquals("SELECT", result1.queryType);
    assertEquals("SELECT", result2.queryType);
    assertTrue(result1.tables.contains("employees"));
    assertTrue(result2.tables.contains("inventory"));
  }

  @Test
  public void testInsertWithoutPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("INSERT INTO users (name, email) VALUES ('John', 'john@test.com')");

    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
    assertEquals("users.name", result.columns.get(0).toString());
    assertEquals("users.email", result.columns.get(1).toString());
  }

  @Test
  public void testInsertWithPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("INSERT INTO products (name, price, category) VALUES (?, ?, ?)");

    assertEquals("INSERT", result.queryType);
    assertTrue(result.tables.contains("products"));
    assertEquals(3, result.columns.size());
    assertEquals("products.name", result.columns.get(0).toString());
    assertEquals("products.price", result.columns.get(1).toString());
    assertEquals("products.category", result.columns.get(2).toString());
  }

  @Test
  public void testUpdateWithoutPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("UPDATE users SET name = 'Jane', email = 'jane@test.com' WHERE id = 1");

    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(2, result.columns.size());
    assertEquals("users.name", result.columns.get(0).toString());
    assertEquals("users.email", result.columns.get(1).toString());
  }

  @Test
  public void testUpdateWithPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("UPDATE inventory SET quantity = ?, price = ? WHERE product_id = ?");

    assertEquals("UPDATE", result.queryType);
    assertTrue(result.tables.contains("inventory"));
    assertEquals(2, result.columns.size());
    assertEquals("inventory.quantity", result.columns.get(0).toString());
    assertEquals("inventory.price", result.columns.get(1).toString());
  }

  @Test
  public void testDeleteWithoutPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("DELETE FROM orders WHERE status = 'cancelled'");

    assertEquals("DELETE", result.queryType);
    assertTrue(result.tables.contains("orders"));
    assertEquals(0, result.columns.size());
  }

  @Test
  public void testDeleteWithPlaceholders() {
    SQLAnalyzer.QueryAnalysis result = analyzer.analyze("DELETE FROM users WHERE created_date < ? AND active = ?");

    assertEquals("DELETE", result.queryType);
    assertTrue(result.tables.contains("users"));
    assertEquals(0, result.columns.size());
  }
}
