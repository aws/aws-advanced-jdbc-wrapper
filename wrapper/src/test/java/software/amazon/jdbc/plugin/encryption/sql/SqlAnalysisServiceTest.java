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

package software.amazon.jdbc.plugin.encryption.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;

class SqlAnalysisServiceTest {

  @Mock private PluginService pluginService;

  @Mock private MetadataManager metadataManager;

  private SqlAnalysisService sqlAnalysisService;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    sqlAnalysisService = new SqlAnalysisService(pluginService, metadataManager);
  }

  @Test
  void testInsertStatements() {
    // Simple INSERT
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql("INSERT INTO customers (name, email) VALUES (?, ?)");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // INSERT with schema - extract just table name
    result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO public.users (id, username, password) VALUES (1, 'john', 'secret')");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("users"));

    // Multi-value INSERT
    result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO products (name, price) VALUES ('Product1', 10.99), ('Product2', 15.50)");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("products"));
  }

  @Test
  void testUpdateStatements() {
    // Simple UPDATE
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql("UPDATE customers SET email = ? WHERE id = ?");
    assertEquals("UPDATE", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // UPDATE with JOIN - expect first table only
    result =
        sqlAnalysisService.analyzeSql(
            "UPDATE orders o SET status = 'shipped' FROM customers c WHERE o.customer_id = c.id");
    assertEquals("UPDATE", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("orders"));

    // UPDATE with schema - extract just table name
    result =
        sqlAnalysisService.analyzeSql(
            "UPDATE public.inventory SET quantity = quantity - 1 WHERE product_id = ?");
    assertEquals("UPDATE", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("inventory"));
  }

  @Test
  void testSelectStatements() {
    // Simple SELECT
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql("SELECT * FROM customers WHERE id = ?");
    assertEquals("SELECT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // SELECT with JOIN - expect first table only
    result =
        sqlAnalysisService.analyzeSql(
            "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id");
    assertEquals("SELECT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // SELECT with subquery - expect main table
    result =
        sqlAnalysisService.analyzeSql(
            "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)");
    assertEquals("SELECT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("products"));
  }

  @Test
  void testInsertFromSelect() {
    // INSERT INTO ... SELECT FROM single table - expect target table
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO backup_customers SELECT * FROM customers WHERE active = true");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("backup_customers"));

    // INSERT INTO ... SELECT with specific columns - expect target table
    result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO customer_summary (name, total_orders) SELECT c.name, COUNT(o.id) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id, c.name");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customer_summary"));

    // INSERT INTO ... SELECT with WHERE clause - expect target table
    result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO archived_orders SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.created_date < '2023-01-01'");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("archived_orders"));

    // INSERT INTO ... SELECT with subquery - expect target table
    result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO high_value_customers SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE total > 1000)");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("high_value_customers"));

    // INSERT INTO ... SELECT with UNION - expect target table
    result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO all_contacts SELECT name, email FROM customers UNION SELECT name, email FROM suppliers");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("all_contacts"));
  }

  @Test
  void testEdgeCases() {
    // Empty SQL
    SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql("");
    assertEquals("UNKNOWN", result.getQueryType());
    assertTrue(result.getAffectedTables().isEmpty());

    // Null SQL
    result = sqlAnalysisService.analyzeSql(null);
    assertEquals("UNKNOWN", result.getQueryType());
    assertTrue(result.getAffectedTables().isEmpty());

    // Whitespace only
    result = sqlAnalysisService.analyzeSql("   \n\t  ");
    assertEquals("UNKNOWN", result.getQueryType());
    assertTrue(result.getAffectedTables().isEmpty());
  }

  @Test
  void testOtherStatements() {
    // DELETE
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql("DELETE FROM customers WHERE id = ?");
    assertEquals("DELETE", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // CREATE TABLE - parser works correctly
    result =
        sqlAnalysisService.analyzeSql(
            "CREATE TABLE new_table (id SERIAL PRIMARY KEY, name VARCHAR(100))");
    assertEquals("CREATE", result.getQueryType());

    // DROP TABLE - jOOQ parser works correctly
    result = sqlAnalysisService.analyzeSql("DROP TABLE old_table");
    assertEquals("DROP", result.getQueryType());
  }

  @Test
  void testBasicQueryAnalysis() {
    // INSERT statement
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO customers (name, ssn, credit_card, email) VALUES (?, ?, ?, ?)");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // UPDATE statement
    result = sqlAnalysisService.analyzeSql("UPDATE customers SET ssn = ?, email = ? WHERE id = ?");
    assertEquals("UPDATE", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // SELECT statement
    result =
        sqlAnalysisService.analyzeSql("SELECT name, ssn, credit_card FROM customers WHERE id = ?");
    assertEquals("SELECT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));
  }

  @Test
  void testUpdateParameterMapping() {
    // Simple UPDATE statement
    Map<Integer, String> mapping =
        sqlAnalysisService.getColumnParameterMapping(
            "UPDATE users SET ssn = ?, email = ? WHERE id = ?");
    assertEquals(2, mapping.size()); // ssn, email (SET clause only)
    assertEquals("ssn", mapping.get(1));
    assertEquals("email", mapping.get(2));

    // UPDATE with single column
    mapping =
        sqlAnalysisService.getColumnParameterMapping("UPDATE customers SET name = ? WHERE id = ?");
    assertEquals(1, mapping.size()); // name (SET clause only)
    assertEquals("name", mapping.get(1));

    // UPDATE with multiple columns
    mapping =
        sqlAnalysisService.getColumnParameterMapping(
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
        sqlAnalysisService.getColumnParameterMapping("SELECT ssn FROM users WHERE name = ?");
    assertEquals(1, mapping.size());
    assertEquals("name", mapping.get(1));

    // SELECT with multiple WHERE parameters
    mapping =
        sqlAnalysisService.getColumnParameterMapping(
            "SELECT ssn, email FROM users WHERE name = ? AND age = ?");
    assertEquals(2, mapping.size());
    assertEquals("name", mapping.get(1));
    assertEquals("age", mapping.get(2));

    // SELECT with no parameters - should have no parameter mapping
    mapping =
        sqlAnalysisService.getColumnParameterMapping("SELECT ssn FROM users WHERE name = 'John'");
    assertEquals(0, mapping.size());
  }

  @Test
  void testMultiTableQueries() {
    // JOIN query - expect first table only
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql(
            "SELECT c.name, c.ssn, o.payment_info FROM customers c JOIN orders o ON c.id = o.customer_id");
    assertEquals("SELECT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // INSERT FROM SELECT - expect target table
    result =
        sqlAnalysisService.analyzeSql(
            "INSERT INTO backup_customers SELECT name, ssn, credit_card FROM customers WHERE active = true");
    assertEquals("INSERT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("backup_customers"));
  }

  @Test
  void testComplexQueryAnalysis() {
    // Test complex UPDATE query analysis
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql("UPDATE customers SET name = ?, ssn = ? WHERE id = 123");

    assertEquals("UPDATE", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // Test parameter mapping for UPDATE (only SET clause parameters are mapped)
    Map<Integer, String> mapping =
        sqlAnalysisService.getColumnParameterMapping(
            "UPDATE customers SET name = ?, ssn = ? WHERE id = 123");
    assertEquals(2, mapping.size()); // name, ssn (SET clause only)
    assertEquals("name", mapping.get(1));
    assertEquals("ssn", mapping.get(2));

    // Test JOIN query analysis
    result =
        sqlAnalysisService.analyzeSql(
            "SELECT c.name, c.ssn FROM customers c JOIN orders o ON c.id = o.customer_id");
    assertEquals("SELECT", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));

    // Test DELETE query analysis
    result = sqlAnalysisService.analyzeSql("DELETE FROM customers WHERE id = ?");
    assertEquals("DELETE", result.getQueryType());
    assertTrue(result.getAffectedTables().contains("customers"));
  }

  @Test
  void testCaseInsensitivity() {
    // Lowercase
    SqlAnalysisService.SqlAnalysisResult result =
        sqlAnalysisService.analyzeSql("insert into customers (name) values (?)");
    assertEquals("INSERT", result.getQueryType());

    // Mixed case
    result = sqlAnalysisService.analyzeSql("Update Customers Set Name = ? Where Id = ?");
    assertEquals("UPDATE", result.getQueryType());

    // Uppercase
    result = sqlAnalysisService.analyzeSql("SELECT * FROM CUSTOMERS");
    assertEquals("SELECT", result.getQueryType());
  }
}
