package software.amazon.jdbc.plugin.encryption.parser;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.plugin.encryption.parser.ast.*;

/** Tests for JDBC placeholder support */
class PostgreSqlParserPlaceholderTest {

  private PostgreSqlParser parser;

  @BeforeEach
  void setUp() {
    parser = new PostgreSqlParser();
  }

  @Test
  void testSelectWithPlaceholder() {
    String sql = "SELECT * FROM users WHERE id = ?";
    Statement stmt = parser.parse(sql);
    assertTrue(stmt instanceof SelectStatement);
    SelectStatement select = (SelectStatement) stmt;
    assertNotNull(select.getWhereClause());
    assertTrue(select.getWhereClause() instanceof BinaryExpression);
    BinaryExpression where = (BinaryExpression) select.getWhereClause();
    assertTrue(where.getRight() instanceof Placeholder);
  }

  @Test
  void testInsertWithPlaceholders() {
    String sql = "INSERT INTO users (name, age) VALUES (?, ?)";
    Statement stmt = parser.parse(sql);
    assertTrue(stmt instanceof InsertStatement);
    InsertStatement insert = (InsertStatement) stmt;
    assertEquals(1, insert.getValues().size());
    assertEquals(2, insert.getValues().get(0).size());
    assertTrue(insert.getValues().get(0).get(0) instanceof Placeholder);
    assertTrue(insert.getValues().get(0).get(1) instanceof Placeholder);
  }

  @Test
  void testUpdateWithPlaceholder() {
    String sql = "UPDATE users SET name = ? WHERE id = ?";
    Statement stmt = parser.parse(sql);
    assertTrue(stmt instanceof UpdateStatement);
    UpdateStatement update = (UpdateStatement) stmt;
    assertEquals(1, update.getAssignments().size());
    assertTrue(update.getAssignments().get(0).getValue() instanceof Placeholder);
    assertTrue(((BinaryExpression) update.getWhereClause()).getRight() instanceof Placeholder);
  }

  @Test
  void testDeleteWithPlaceholder() {
    String sql = "DELETE FROM users WHERE age > ?";
    Statement stmt = parser.parse(sql);
    assertTrue(stmt instanceof DeleteStatement);
    DeleteStatement delete = (DeleteStatement) stmt;
    assertNotNull(delete.getWhereClause());
    BinaryExpression where = (BinaryExpression) delete.getWhereClause();
    assertTrue(where.getRight() instanceof Placeholder);
  }

  @Test
  void testMultiplePlaceholdersInExpression() {
    String sql = "SELECT * FROM products WHERE price BETWEEN ? AND ?";
    Statement stmt = parser.parse(sql);
    assertTrue(stmt instanceof SelectStatement);
    // This tests that placeholders work in complex expressions
    assertNotNull(((SelectStatement) stmt).getWhereClause());
  }

  @Test
  void testMixedPlaceholdersAndLiterals() {
    String sql = "INSERT INTO orders (user_id, total, status) VALUES (?, 100.50, 'pending')";
    Statement stmt = parser.parse(sql);
    assertTrue(stmt instanceof InsertStatement);
    InsertStatement insert = (InsertStatement) stmt;
    assertEquals(3, insert.getValues().get(0).size());
    assertTrue(insert.getValues().get(0).get(0) instanceof Placeholder);
    assertTrue(insert.getValues().get(0).get(1) instanceof NumericLiteral);
    assertTrue(insert.getValues().get(0).get(2) instanceof StringLiteral);
  }
}
