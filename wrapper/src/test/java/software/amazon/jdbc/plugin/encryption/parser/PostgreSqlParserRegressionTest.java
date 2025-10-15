package software.amazon.jdbc.plugin.encryption.parser;

import software.amazon.jdbc.plugin.encryption.parser.ast.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests based on PostgreSQL's src/test/regress/sql test files
 */
class PostgreSqlParserRegressionTest {

    private PostgreSqlParser parser;

    @BeforeEach
    void setUp() {
        parser = new PostgreSqlParser();
    }

    // SELECT regression tests
    @Test
    void testSelectWithOrderBy() {
        String sql = "SELECT * FROM onek WHERE onek.unique1 < 10 ORDER BY onek.unique1";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertNotNull(select.getOrderBy());
        assertEquals(1, select.getOrderBy().size());
    }

    @Test
    void testSelectWithQualifiedColumns() {
        String sql = "SELECT onek.unique1, onek.stringu1 FROM onek WHERE onek.unique1 < 20";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertEquals(2, select.getSelectList().size());
    }

    @Test
    void testSelectWithComparison() {
        String sql = "SELECT onek.unique1 FROM onek WHERE onek.unique1 > 980";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertNotNull(select.getWhereClause());
        assertTrue(select.getWhereClause() instanceof BinaryExpression);
    }

    // INSERT regression tests
    @Test
    void testInsertWithMultipleValues() {
        String sql = "INSERT INTO inserttest VALUES (10, 20), (30, 40)";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof InsertStatement);
        InsertStatement insert = (InsertStatement) stmt;
        assertEquals(2, insert.getValues().size());
    }

    @Test
    void testInsertWithColumnList() {
        String sql = "INSERT INTO inserttest (col1, col2) VALUES (3, 5)";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof InsertStatement);
        InsertStatement insert = (InsertStatement) stmt;
        assertNotNull(insert.getColumns());
        assertEquals(2, insert.getColumns().size());
    }

    @Test
    void testInsertWithStringLiterals() {
        String sql = "INSERT INTO inserttest VALUES (1, 'test string')";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof InsertStatement);
        InsertStatement insert = (InsertStatement) stmt;
        assertEquals(1, insert.getValues().size());
        assertEquals(2, insert.getValues().get(0).size());
    }

    // UPDATE regression tests
    @Test
    void testUpdateWithMultipleAssignments() {
        String sql = "UPDATE update_test SET a = 10, b = 20 WHERE c = 'foo'";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof UpdateStatement);
        UpdateStatement update = (UpdateStatement) stmt;
        assertEquals(2, update.getAssignments().size());
        assertNotNull(update.getWhereClause());
    }

    @Test
    void testUpdateWithNumericValues() {
        String sql = "UPDATE test_table SET price = 19.99, quantity = 5";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof UpdateStatement);
        UpdateStatement update = (UpdateStatement) stmt;
        assertEquals(2, update.getAssignments().size());
    }

    // CREATE TABLE regression tests
    @Test
    void testCreateTableWithMultipleColumns() {
        String sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, price DECIMAL)";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof CreateTableStatement);
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertEquals(3, create.getColumns().size());
    }

    @Test
    void testCreateTableWithConstraints() {
        String sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR NOT NULL)";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof CreateTableStatement);
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertEquals(2, create.getColumns().size());
        assertTrue(create.getColumns().get(0).isPrimaryKey());
        assertTrue(create.getColumns().get(1).isNotNull());
    }

    // DELETE regression tests
    @Test
    void testDeleteWithComplexWhere() {
        String sql = "DELETE FROM products WHERE price > 100 AND category = 'electronics'";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof DeleteStatement);
        DeleteStatement delete = (DeleteStatement) stmt;
        assertNotNull(delete.getWhereClause());
        assertTrue(delete.getWhereClause() instanceof BinaryExpression);
    }

    @Test
    void testDeleteWithNumericComparison() {
        String sql = "DELETE FROM inventory WHERE quantity < 5";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof DeleteStatement);
        DeleteStatement delete = (DeleteStatement) stmt;
        assertNotNull(delete.getWhereClause());
    }

    // Expression complexity tests
    @Test
    void testComplexBooleanExpression() {
        String sql = "SELECT * FROM products WHERE (price > 50 AND category = 'books') OR (price < 20 AND category = 'music')";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertNotNull(select.getWhereClause());
        assertTrue(select.getWhereClause() instanceof BinaryExpression);
    }

    @Test
    void testArithmeticExpression() {
        String sql = "SELECT price * quantity FROM orders WHERE total > price + tax";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertEquals(1, select.getSelectList().size());
        assertNotNull(select.getWhereClause());
    }

    // String and numeric literal tests
    @Test
    void testStringLiteralsWithQuotes() {
        String sql = "INSERT INTO messages VALUES ('Hello World', 'Test message')";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof InsertStatement);
        InsertStatement insert = (InsertStatement) stmt;
        assertEquals(2, insert.getValues().get(0).size());
    }

    @Test
    void testNumericLiterals() {
        String sql = "INSERT INTO measurements VALUES (42, 3.14159, 2.5e10)";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof InsertStatement);
        InsertStatement insert = (InsertStatement) stmt;
        assertEquals(3, insert.getValues().get(0).size());
    }

    // Edge cases from PostgreSQL tests
    @Test
    void testSelectWithParentheses() {
        String sql = "SELECT (price + tax) * quantity FROM orders";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertEquals(1, select.getSelectList().size());
    }

    @Test
    void testMultipleTableReferences() {
        String sql = "SELECT users.name, orders.total FROM users, orders WHERE users.id = orders.user_id";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertEquals(2, select.getFromClause().size());
        assertEquals(2, select.getSelectList().size());
    }

    @Test
    void testComplexUpdateExpression() {
        String sql = "UPDATE accounts SET balance = balance + 100 WHERE account_id = 12345";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof UpdateStatement);
        UpdateStatement update = (UpdateStatement) stmt;
        assertEquals(1, update.getAssignments().size());
        assertNotNull(update.getWhereClause());
    }

    @Test
    void testSelectWithSubquery() {
        String sql = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)";
        Statement stmt = parser.parse(sql);
        
        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement selectStmt = (SelectStatement) stmt;
        
        assertEquals(1, selectStmt.getFromList().size());
        assertEquals("products", selectStmt.getFromList().get(0).getTableName().getName());
        assertNotNull(selectStmt.getWhereClause());
    }
}
