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

package software.amazon.jdbc.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SqlAnalysisServiceTest {

  @Test
  void testInsertStatements() {
    // Simple INSERT
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql("INSERT INTO customers (name, email) VALUES (?, ?)");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // INSERT with schema - extract just table name
    result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO public.users (id, username, password) VALUES (1, 'john', 'secret')");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("users"));

    // Multi-value INSERT
    result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO products (name, price) VALUES ('Product1', 10.99), ('Product2', 15.50)");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("products"));
  }

  @Test
  void testUpdateStatements() {
    // Simple UPDATE
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql("UPDATE customers SET email = ? WHERE id = ?");
    assertEquals(QueryType.UPDATE, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // UPDATE with JOIN - expect first table only
    result =
        SqlAnalysisService.analyzeSql(
            "UPDATE orders o SET status = 'shipped' FROM customers c WHERE o.customer_id = c.id");
    assertEquals(QueryType.UPDATE, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("orders"));

    // UPDATE with schema - extract just table name
    result =
        SqlAnalysisService.analyzeSql(
            "UPDATE public.inventory SET quantity = quantity - 1 WHERE product_id = ?");
    assertEquals(QueryType.UPDATE, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("inventory"));
  }

  @Test
  void testSelectStatements() {
    // Simple SELECT
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql("SELECT * FROM customers WHERE id = ?");
    assertEquals(QueryType.SELECT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // SELECT with JOIN - expect first table only
    result =
        SqlAnalysisService.analyzeSql(
            "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id");
    assertEquals(QueryType.SELECT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // SELECT with subquery - expect main table
    result =
        SqlAnalysisService.analyzeSql(
            "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)");
    assertEquals(QueryType.SELECT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("products"));
  }

  @Test
  void testInsertFromSelect() {
    // INSERT INTO ... SELECT FROM single table - expect target table
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO backup_customers SELECT * FROM customers WHERE active = true");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("backup_customers"));

    // INSERT INTO ... SELECT with specific columns - expect target table
    result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO customer_summary (name, total_orders) SELECT c.name, COUNT(o.id) "
            + "FROM customers c JOIN orders o ON c.id = o.customer_id "
            + "GROUP BY c.id, c.name");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customer_summary"));

    // INSERT INTO ... SELECT with WHERE clause - expect target table
    result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO archived_orders SELECT o.*, c.name FROM orders o "
            + "JOIN customers c ON o.customer_id = c.id WHERE o.created_date < '2023-01-01'");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("archived_orders"));

    // INSERT INTO ... SELECT with subquery - expect target table
    result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO high_value_customers SELECT * FROM customers "
            + "WHERE id IN (SELECT customer_id FROM orders WHERE total > 1000)");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("high_value_customers"));

    // INSERT INTO ... SELECT with UNION - expect target table
    result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO all_contacts SELECT name, email FROM customers UNION SELECT name, email FROM suppliers");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("all_contacts"));
  }

  @Test
  void testEdgeCases() {
    // Empty SQL
    SqlAnalysisService.SqlAnalysisResult result = SqlAnalysisService.analyzeSql("");
    assertEquals(QueryType.UNKNOWN, result.getQueryType());
    assertTrue(result.getAffectedTables().isEmpty());

    // Null SQL
    result = SqlAnalysisService.analyzeSql(null);
    assertEquals(QueryType.UNKNOWN, result.getQueryType());
    assertTrue(result.getAffectedTables().isEmpty());

    // Whitespace only
    result = SqlAnalysisService.analyzeSql("   \n\t  ");
    assertEquals(QueryType.UNKNOWN, result.getQueryType());
    assertTrue(result.getAffectedTables().isEmpty());
  }

  @Test
  void testOtherStatements() {
    // DELETE
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql("DELETE FROM customers WHERE id = ?");
    assertEquals(QueryType.DELETE, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // CREATE TABLE - parser works correctly
    result =
        SqlAnalysisService.analyzeSql(
            "CREATE TABLE new_table (id SERIAL PRIMARY KEY, name VARCHAR(100))");
    assertEquals(QueryType.CREATE, result.getQueryType());

    // DROP TABLE - jOOQ parser works correctly
    result = SqlAnalysisService.analyzeSql("DROP TABLE old_table");
    assertEquals(QueryType.DROP, result.getQueryType());
  }

  @Test
  void testBasicQueryAnalysis() {
    // INSERT statement
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO customers (name, ssn, credit_card, email) VALUES (?, ?, ?, ?)");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // UPDATE statement
    result = SqlAnalysisService.analyzeSql("UPDATE customers SET ssn = ?, email = ? WHERE id = ?");
    assertEquals(QueryType.UPDATE, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // SELECT statement
    result =
        SqlAnalysisService.analyzeSql("SELECT name, ssn, credit_card FROM customers WHERE id = ?");
    assertEquals(QueryType.SELECT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));
  }

  @Test
  void testUpdateParameterMapping() {
    // Simple UPDATE statement
    Map<Integer, String> mapping =
        SqlAnalysisService.getColumnParameterMapping(
            "UPDATE users SET ssn = ?, email = ? WHERE id = ?");
    assertEquals(2, mapping.size()); // ssn, email (SET clause only)
    assertEquals("ssn", mapping.get(1));
    assertEquals("email", mapping.get(2));

    // UPDATE with single column
    mapping =
        SqlAnalysisService.getColumnParameterMapping("UPDATE customers SET name = ? WHERE id = ?");
    assertEquals(1, mapping.size()); // name (SET clause only)
    assertEquals("name", mapping.get(1));

    // UPDATE with multiple columns
    mapping =
        SqlAnalysisService.getColumnParameterMapping(
            "UPDATE products SET name = ?, price = ?, description = ? WHERE category = ?");
    assertEquals(3, mapping.size()); // name, price, description (SET clause only)
    assertEquals("name", mapping.get(1));
    assertEquals("price", mapping.get(2));
    assertEquals("description", mapping.get(3));
  }

  @Test
  void testSelectParameterMapping() {
    // SELECT with WHERE clause parameter
    Map<Integer, String> mapping =
        SqlAnalysisService.getColumnParameterMapping("SELECT ssn FROM users WHERE name = ?");
    assertEquals(1, mapping.size());
    assertEquals("name", mapping.get(1));

    // SELECT with multiple WHERE parameters
    mapping =
        SqlAnalysisService.getColumnParameterMapping(
            "SELECT ssn, email FROM users WHERE name = ? AND age = ?");
    assertEquals(2, mapping.size());
    assertEquals("name", mapping.get(1));
    assertEquals("age", mapping.get(2));

    // SELECT with no parameters - should have no parameter mapping
    mapping =
        SqlAnalysisService.getColumnParameterMapping("SELECT ssn FROM users WHERE name = 'John'");
    assertEquals(0, mapping.size());
  }

  @Test
  void testMultiTableQueries() {
    // JOIN query - expect first table only
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql(
            "SELECT c.name, c.ssn, o.payment_info FROM customers c JOIN orders o ON c.id = o.customer_id");
    assertEquals(QueryType.SELECT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // INSERT FROM SELECT - expect target table
    result =
        SqlAnalysisService.analyzeSql(
            "INSERT INTO backup_customers SELECT name, ssn, credit_card FROM customers WHERE active = true");
    assertEquals(QueryType.INSERT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("backup_customers"));
  }

  @Test
  void testComplexQueryAnalysis() {
    // Test complex UPDATE query analysis
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql("UPDATE customers SET name = ?, ssn = ? WHERE id = 123");

    assertEquals(QueryType.UPDATE, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // Test parameter mapping for UPDATE (only SET clause parameters are mapped)
    Map<Integer, String> mapping =
        SqlAnalysisService.getColumnParameterMapping(
            "UPDATE customers SET name = ?, ssn = ? WHERE id = 123");
    assertEquals(2, mapping.size()); // name, ssn (SET clause only)
    assertEquals("name", mapping.get(1));
    assertEquals("ssn", mapping.get(2));

    // Test JOIN query analysis
    result =
        SqlAnalysisService.analyzeSql(
            "SELECT c.name, c.ssn FROM customers c JOIN orders o ON c.id = o.customer_id");
    assertEquals(QueryType.SELECT, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // Test DELETE query analysis
    result = SqlAnalysisService.analyzeSql("DELETE FROM customers WHERE id = ?");
    assertEquals(QueryType.DELETE, result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));
  }

  @Test
  void testCaseInsensitivity() {
    // Lowercase
    SqlAnalysisService.SqlAnalysisResult result =
        SqlAnalysisService.analyzeSql("insert into customers (name) values (?)");
    assertEquals(QueryType.INSERT, result.getQueryType());

    // Mixed case
    result = SqlAnalysisService.analyzeSql("Update Customers Set Name = ? Where Id = ?");
    assertEquals(QueryType.UPDATE, result.getQueryType());

    // Uppercase
    result = SqlAnalysisService.analyzeSql("SELECT * FROM CUSTOMERS");
    assertEquals(QueryType.SELECT, result.getQueryType());
  }
}
