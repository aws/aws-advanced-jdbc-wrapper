package software.amazon.jdbc.plugin.encryption.parser;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.plugin.encryption.parser.ast.*;

/** Test cases for PostgreSQL SQL Parser */
public class PostgreSqlParserTest {

  private PostgreSqlParser parser;

  @BeforeEach
  void setUp() {
    parser = new PostgreSqlParser();
  }

  @Test
  void testSimpleSelectStatement() {
    String sql = "SELECT id, name FROM users";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(SelectStatement.class, stmt);
    SelectStatement selectStmt = (SelectStatement) stmt;

    assertEquals(2, selectStmt.getSelectList().size());
    assertEquals("id", ((Identifier) selectStmt.getSelectList().get(0).getExpression()).getName());
    assertEquals(
        "name", ((Identifier) selectStmt.getSelectList().get(1).getExpression()).getName());

    assertEquals(1, selectStmt.getFromList().size());
    assertEquals("users", selectStmt.getFromList().get(0).getTableName().getName());
  }

  @Test
  void testSelectWithWhereClause() {
    String sql = "SELECT * FROM users WHERE age > 18";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(SelectStatement.class, stmt);
    SelectStatement selectStmt = (SelectStatement) stmt;

    assertNotNull(selectStmt.getWhereClause());
    assertInstanceOf(BinaryExpression.class, selectStmt.getWhereClause());

    BinaryExpression whereExpr = (BinaryExpression) selectStmt.getWhereClause();
    assertEquals(BinaryExpression.Operator.GREATER_THAN, whereExpr.getOperator());
  }

  @Test
  void testSelectWithOrderBy() {
    String sql = "SELECT name, age FROM users ORDER BY name ASC, age DESC";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(SelectStatement.class, stmt);
    SelectStatement selectStmt = (SelectStatement) stmt;

    assertNotNull(selectStmt.getOrderByList());
    assertEquals(2, selectStmt.getOrderByList().size());

    OrderByItem firstOrder = selectStmt.getOrderByList().get(0);
    assertEquals("name", ((Identifier) firstOrder.getExpression()).getName());
    assertEquals(OrderByItem.Direction.ASC, firstOrder.getDirection());

    OrderByItem secondOrder = selectStmt.getOrderByList().get(1);
    assertEquals("age", ((Identifier) secondOrder.getExpression()).getName());
    assertEquals(OrderByItem.Direction.DESC, secondOrder.getDirection());
  }

  @Test
  void testInsertStatement() {
    String sql = "INSERT INTO users (name, age) VALUES ('John', 25)";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(InsertStatement.class, stmt);
    InsertStatement insertStmt = (InsertStatement) stmt;

    assertEquals("users", insertStmt.getTable().getTableName().getName());
    assertEquals(2, insertStmt.getColumns().size());
    assertEquals("name", insertStmt.getColumns().get(0).getName());
    assertEquals("age", insertStmt.getColumns().get(1).getName());

    assertEquals(1, insertStmt.getValues().size());
    assertEquals(2, insertStmt.getValues().get(0).size());

    assertInstanceOf(StringLiteral.class, insertStmt.getValues().get(0).get(0));
    assertEquals("John", ((StringLiteral) insertStmt.getValues().get(0).get(0)).getValue());

    assertInstanceOf(NumericLiteral.class, insertStmt.getValues().get(0).get(1));
    assertEquals("25", ((NumericLiteral) insertStmt.getValues().get(0).get(1)).getValue());
  }

  @Test
  void testUpdateStatement() {
    String sql = "UPDATE users SET age = 26 WHERE name = 'John'";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(UpdateStatement.class, stmt);
    UpdateStatement updateStmt = (UpdateStatement) stmt;

    assertEquals("users", updateStmt.getTable().getTableName().getName());
    assertEquals(1, updateStmt.getAssignments().size());

    Assignment assignment = updateStmt.getAssignments().get(0);
    assertEquals("age", assignment.getColumn().getName());
    assertInstanceOf(NumericLiteral.class, assignment.getValue());
    assertEquals("26", ((NumericLiteral) assignment.getValue()).getValue());

    assertNotNull(updateStmt.getWhereClause());
    assertInstanceOf(BinaryExpression.class, updateStmt.getWhereClause());
  }

  @Test
  void testDeleteStatement() {
    String sql = "DELETE FROM users WHERE age < 18";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(DeleteStatement.class, stmt);
    DeleteStatement deleteStmt = (DeleteStatement) stmt;

    assertEquals("users", deleteStmt.getTable().getTableName().getName());
    assertNotNull(deleteStmt.getWhereClause());

    BinaryExpression whereExpr = (BinaryExpression) deleteStmt.getWhereClause();
    assertEquals(BinaryExpression.Operator.LESS_THAN, whereExpr.getOperator());
  }

  @Test
  void testCreateTableStatement() {
    String sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(CreateTableStatement.class, stmt);
    CreateTableStatement createStmt = (CreateTableStatement) stmt;

    assertEquals("users", createStmt.getTableName().getName());
    assertEquals(2, createStmt.getColumns().size());

    ColumnDefinition idCol = createStmt.getColumns().get(0);
    assertEquals("id", idCol.getColumnName().getName());
    assertEquals("INTEGER", idCol.getDataType());
    assertTrue(idCol.isPrimaryKey());

    ColumnDefinition nameCol = createStmt.getColumns().get(1);
    assertEquals("name", nameCol.getColumnName().getName());
    assertEquals("VARCHAR", nameCol.getDataType());
    assertTrue(nameCol.isNotNull());
  }

  @Test
  void testComplexExpression() {
    String sql = "SELECT * FROM users WHERE age > 18 AND name LIKE 'J%' OR status = 'active'";
    Statement stmt = parser.parse(sql);

    assertInstanceOf(SelectStatement.class, stmt);
    SelectStatement selectStmt = (SelectStatement) stmt;

    assertNotNull(selectStmt.getWhereClause());
    assertInstanceOf(BinaryExpression.class, selectStmt.getWhereClause());

    // The expression should be parsed with correct operator precedence
    BinaryExpression whereExpr = (BinaryExpression) selectStmt.getWhereClause();
    assertEquals(BinaryExpression.Operator.OR, whereExpr.getOperator());
  }

  @Test
  void testLexerTokenization() {
    SqlLexer lexer = new SqlLexer("SELECT id, 'test', 123, 45.67 FROM users");
    java.util.List<Token> tokens = lexer.tokenize();

    assertEquals(Token.Type.SELECT, tokens.get(0).getType());
    assertEquals(Token.Type.IDENT, tokens.get(1).getType());
    assertEquals("id", tokens.get(1).getValue());
    assertEquals(Token.Type.COMMA, tokens.get(2).getType());
    assertEquals(Token.Type.SCONST, tokens.get(3).getType());
    assertEquals("test", tokens.get(3).getValue());
    assertEquals(Token.Type.COMMA, tokens.get(4).getType());
    assertEquals(Token.Type.ICONST, tokens.get(5).getType());
    assertEquals("123", tokens.get(5).getValue());
    assertEquals(Token.Type.COMMA, tokens.get(6).getType());
    assertEquals(Token.Type.FCONST, tokens.get(7).getType());
    assertEquals("45.67", tokens.get(7).getValue());
    assertEquals(Token.Type.FROM, tokens.get(8).getType());
    assertEquals(Token.Type.IDENT, tokens.get(9).getType());
    assertEquals("users", tokens.get(9).getValue());
    assertEquals(Token.Type.EOF, tokens.get(10).getType());
  }

  @Test
  void testParseError() {
    String invalidSql = "SELECT FROM"; // Missing column list

    assertThrows(
        SqlParser.ParseException.class,
        () -> {
          parser.parse(invalidSql);
        });
  }

  @Test
  void testFormatting() {
    String sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name";
    String formatted = parser.parseAndFormat(sql);

    assertTrue(formatted.contains("SELECT"));
    assertTrue(formatted.contains("FROM"));
    assertTrue(formatted.contains("WHERE"));
    assertTrue(formatted.contains("ORDER BY"));
  }
}
