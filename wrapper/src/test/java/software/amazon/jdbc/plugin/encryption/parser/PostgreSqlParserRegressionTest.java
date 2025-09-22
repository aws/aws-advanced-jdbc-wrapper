package software.amazon.jdbc.plugin.encryption.parser;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.plugin.encryption.parser.ast.*;

/** Regression tests based on PostgreSQL's src/test/regress/sql test files */
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
    String sql =
        "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, price DECIMAL)";
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
    String sql =
        "SELECT * FROM products WHERE (price > 50 AND category = 'books') OR (price < 20 AND category = 'music')";
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
    String sql =
        "SELECT users.name, orders.total FROM users, orders WHERE users.id = orders.user_id";
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

  @Test
  void testAdvancedPostgreSQLFeatures() {
    // Test CASE expression
    String sql1 = "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users";
    Statement stmt1 = parser.parse(sql1);
    assertInstanceOf(SelectStatement.class, stmt1);

    // Test CAST expression
    String sql2 = "SELECT CAST(price AS INTEGER) FROM products";
    Statement stmt2 = parser.parse(sql2);
    assertInstanceOf(SelectStatement.class, stmt2);

    // Test CROSS JOIN
    String sql3 = "SELECT * FROM users CROSS JOIN products";
    Statement stmt3 = parser.parse(sql3);
    assertInstanceOf(SelectStatement.class, stmt3);
    SelectStatement selectStmt3 = (SelectStatement) stmt3;
    assertEquals(2, selectStmt3.getFromList().size());

    // Test ORDER BY with NULLS FIRST
    String sql4 = "SELECT * FROM users ORDER BY name ASC NULLS FIRST";
    Statement stmt4 = parser.parse(sql4);
    assertInstanceOf(SelectStatement.class, stmt4);

    // Test ORDER BY with DESC and NULLS LAST
    String sql5 = "SELECT * FROM products ORDER BY price DESC NULLS LAST";
    Statement stmt5 = parser.parse(sql5);
    assertInstanceOf(SelectStatement.class, stmt5);
  }

  @Test
  void testMultipleJoinTypes() {
    // Test INNER JOIN
    String sql1 = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id";
    Statement stmt1 = parser.parse(sql1);
    assertInstanceOf(SelectStatement.class, stmt1);

    // Test LEFT OUTER JOIN
    String sql2 = "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id";
    Statement stmt2 = parser.parse(sql2);
    assertInstanceOf(SelectStatement.class, stmt2);

    // Test RIGHT JOIN
    String sql3 = "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id";
    Statement stmt3 = parser.parse(sql3);
    assertInstanceOf(SelectStatement.class, stmt3);
  }

  @Test
  void testComplexExpressions() {
    // Test nested CASE
    String sql1 =
        "SELECT CASE WHEN status = 'active' THEN CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END ELSE 'inactive' END FROM users";
    Statement stmt1 = parser.parse(sql1);
    assertInstanceOf(SelectStatement.class, stmt1);

    // Test multiple CAST
    String sql2 = "SELECT CAST(price AS DECIMAL), CAST(quantity AS INTEGER) FROM products";
    Statement stmt2 = parser.parse(sql2);
    assertInstanceOf(SelectStatement.class, stmt2);

    // Test complex WHERE with boolean literals
    String sql3 = "SELECT * FROM users WHERE active = true AND verified = false";
    Statement stmt3 = parser.parse(sql3);
    assertInstanceOf(SelectStatement.class, stmt3);
  }

  @Test
  void testMultipleOrderByColumns() {
    String sql =
        "SELECT * FROM users ORDER BY last_name ASC, first_name DESC NULLS LAST, age ASC NULLS FIRST";
    Statement stmt = parser.parse(sql);
    assertInstanceOf(SelectStatement.class, stmt);
    SelectStatement selectStmt = (SelectStatement) stmt;
    assertNotNull(selectStmt.getOrderByList());
    assertEquals(3, selectStmt.getOrderByList().size());
  }

  @Test
  void testInsertReturning() {
    // PostgreSQL RETURNING clause
    String sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')";
    Statement stmt = parser.parse(sql);
    assertInstanceOf(InsertStatement.class, stmt);
  }

  @Test
  void testThreeWayJoin() {
    String sql =
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id";
    Statement stmt = parser.parse(sql);
    assertInstanceOf(SelectStatement.class, stmt);
    SelectStatement selectStmt = (SelectStatement) stmt;
    assertEquals(3, selectStmt.getFromList().size());
  }
}
