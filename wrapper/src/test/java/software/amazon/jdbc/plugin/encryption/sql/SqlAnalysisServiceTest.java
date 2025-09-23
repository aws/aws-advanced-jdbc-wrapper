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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class SqlAnalysisServiceTest {

    @Mock
    private PluginService pluginService;
    
    @Mock
    private MetadataManager metadataManager;

    private SqlAnalysisService sqlAnalysisService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        sqlAnalysisService = new SqlAnalysisService(pluginService, metadataManager);
    }

    @Test
    void testInsertStatements() {
        // Simple INSERT
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "INSERT INTO customers (name, email) VALUES (?, ?)");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));

        // INSERT with schema
        result = sqlAnalysisService.analyzeSql(
            "INSERT INTO public.users (id, username, password) VALUES (1, 'john', 'secret')");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("users") || result.getAffectedTables().contains("public.users"));

        // Multi-value INSERT
        result = sqlAnalysisService.analyzeSql(
            "INSERT INTO products (name, price) VALUES ('Product1', 10.99), ('Product2', 15.50)");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("products"));
    }

    @Test
    void testUpdateStatements() {
        // Simple UPDATE
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "UPDATE customers SET email = ? WHERE id = ?");
        assertEquals("UPDATE", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));

        // UPDATE with JOIN
        result = sqlAnalysisService.analyzeSql(
            "UPDATE orders o SET status = 'shipped' FROM customers c WHERE o.customer_id = c.id");
        assertEquals("UPDATE", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("orders"));

        // UPDATE with schema
        result = sqlAnalysisService.analyzeSql(
            "UPDATE public.inventory SET quantity = quantity - 1 WHERE product_id = ?");
        assertEquals("UPDATE", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("inventory") || result.getAffectedTables().contains("public.inventory"));
    }

    @Test
    void testSelectStatements() {
        // Simple SELECT
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "SELECT * FROM customers WHERE id = ?");
        assertEquals("SELECT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));

        // SELECT with JOIN
        result = sqlAnalysisService.analyzeSql(
            "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id");
        assertEquals("SELECT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));
        assertTrue(result.getAffectedTables().contains("orders"));

        // SELECT with subquery
        result = sqlAnalysisService.analyzeSql(
            "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)");
        assertEquals("SELECT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("products"));
    }

    @Test
    void testInsertFromSelect() {
        // INSERT INTO ... SELECT FROM single table
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "INSERT INTO backup_customers SELECT * FROM customers WHERE active = true");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("backup_customers"));
        assertTrue(result.getAffectedTables().contains("customers"));

        // INSERT INTO ... SELECT with specific columns
        result = sqlAnalysisService.analyzeSql(
            "INSERT INTO customer_summary (name, total_orders) SELECT c.name, COUNT(o.id) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id, c.name");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customer_summary"));
        assertTrue(result.getAffectedTables().contains("customers"));
        assertTrue(result.getAffectedTables().contains("orders"));

        // INSERT INTO ... SELECT with WHERE clause
        result = sqlAnalysisService.analyzeSql(
            "INSERT INTO archived_orders SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.created_date < '2023-01-01'");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("archived_orders"));
        assertTrue(result.getAffectedTables().contains("orders"));
        assertTrue(result.getAffectedTables().contains("customers"));

        // INSERT INTO ... SELECT with subquery
        result = sqlAnalysisService.analyzeSql(
            "INSERT INTO high_value_customers SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE total > 1000)");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("high_value_customers"));
        assertTrue(result.getAffectedTables().contains("customers"));
        assertTrue(result.getAffectedTables().contains("orders"));

        // INSERT INTO ... SELECT with UNION
        result = sqlAnalysisService.analyzeSql(
            "INSERT INTO all_contacts SELECT name, email FROM customers UNION SELECT name, email FROM suppliers");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("all_contacts"));
        assertTrue(result.getAffectedTables().contains("customers"));
        assertTrue(result.getAffectedTables().contains("suppliers"));
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
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "DELETE FROM customers WHERE id = ?");
        assertEquals("DELETE", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));

        // CREATE TABLE
        result = sqlAnalysisService.analyzeSql(
            "CREATE TABLE new_table (id SERIAL PRIMARY KEY, name VARCHAR(100))");
        assertEquals("CREATE", result.getQueryType());

        // DROP TABLE
        result = sqlAnalysisService.analyzeSql(
            "DROP TABLE old_table");
        assertEquals("DROP", result.getQueryType());
    }

    @Test
    void testBasicQueryAnalysis() {
        // INSERT statement
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "INSERT INTO customers (name, ssn, credit_card, email) VALUES (?, ?, ?, ?)");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));

        // UPDATE statement
        result = sqlAnalysisService.analyzeSql(
            "UPDATE customers SET ssn = ?, email = ? WHERE id = ?");
        assertEquals("UPDATE", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));

        // SELECT statement
        result = sqlAnalysisService.analyzeSql(
            "SELECT name, ssn, credit_card FROM customers WHERE id = ?");
        assertEquals("SELECT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));
    }

    @Test
    void testMultiTableQueries() {
        // JOIN query
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "SELECT c.name, c.ssn, o.payment_info FROM customers c JOIN orders o ON c.id = o.customer_id");
        assertEquals("SELECT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("customers"));
        assertTrue(result.getAffectedTables().contains("orders"));

        // INSERT FROM SELECT
        result = sqlAnalysisService.analyzeSql(
            "INSERT INTO backup_customers SELECT name, ssn, credit_card FROM customers WHERE active = true");
        assertEquals("INSERT", result.getQueryType());
        assertTrue(result.getAffectedTables().contains("backup_customers"));
        assertTrue(result.getAffectedTables().contains("customers"));
    }

    @Test
    void testEncryptedColumnPlaceholder() {
        // Note: Current implementation doesn't populate encrypted columns
        // This test verifies the structure is in place for future implementation
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "SELECT ssn, credit_card FROM customers");
        
        assertNotNull(result.getEncryptedColumns());
        assertEquals(0, result.getEncryptedColumnCount()); // Currently returns empty map
        assertFalse(result.hasEncryptedColumns()); // Will be true when implementation is complete
    }

    @Test
    void testCaseInsensitivity() {
        // Lowercase
        SqlAnalysisService.SqlAnalysisResult result = sqlAnalysisService.analyzeSql(
            "insert into customers (name) values (?)");
        assertEquals("INSERT", result.getQueryType());

        // Mixed case
        result = sqlAnalysisService.analyzeSql(
            "Update Customers Set Name = ? Where Id = ?");
        assertEquals("UPDATE", result.getQueryType());

        // Uppercase
        result = sqlAnalysisService.analyzeSql(
            "SELECT * FROM CUSTOMERS");
        assertEquals("SELECT", result.getQueryType());
    }
}
