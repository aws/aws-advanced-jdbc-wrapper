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

import java.util.ArrayList;
import java.util.List;
import software.amazon.jdbc.plugin.encryption.parser.ast.Assignment;
import software.amazon.jdbc.plugin.encryption.parser.ast.BinaryExpression;
import software.amazon.jdbc.plugin.encryption.parser.ast.BooleanLiteral;
import software.amazon.jdbc.plugin.encryption.parser.ast.ColumnDefinition;
import software.amazon.jdbc.plugin.encryption.parser.ast.CreateTableStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.DeleteStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.Expression;
import software.amazon.jdbc.plugin.encryption.parser.ast.Identifier;
import software.amazon.jdbc.plugin.encryption.parser.ast.InsertStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.NumericLiteral;
import software.amazon.jdbc.plugin.encryption.parser.ast.OrderByItem;
import software.amazon.jdbc.plugin.encryption.parser.ast.Placeholder;
import software.amazon.jdbc.plugin.encryption.parser.ast.SelectItem;
import software.amazon.jdbc.plugin.encryption.parser.ast.SelectStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.Statement;
import software.amazon.jdbc.plugin.encryption.parser.ast.StringLiteral;
import software.amazon.jdbc.plugin.encryption.parser.ast.SubqueryExpression;
import software.amazon.jdbc.plugin.encryption.parser.ast.TableReference;
import software.amazon.jdbc.plugin.encryption.parser.ast.UpdateStatement;

/**
 * SQL Parser based on PostgreSQL's gram.y Implements a recursive descent parser for basic SQL
 * statements
 */
public class SqlParser {
  private final List<Token> tokens;
  private int position;

  public SqlParser(List<Token> tokens) {
    this.tokens = tokens;
    this.position = 0;
  }

  public Statement parse() {
    return parseStatement();
  }

  private Statement parseStatement() {
    Token token = peek();
    if (token.getType() == Token.Type.EOF) {
      return null;
    }

    switch (token.getType()) {
      case SELECT:
        return parseSelectStatement();
      case INSERT:
        return parseInsertStatement();
      case UPDATE:
        return parseUpdateStatement();
      case DELETE:
        return parseDeleteStatement();
      case CREATE:
        return parseCreateStatement();
      default:
        throw new ParseException("Unexpected token: " + token);
    }
  }

  private SelectStatement parseSelectStatement() {
    consume(Token.Type.SELECT);

    // Parse SELECT list
    List<SelectItem> selectList = parseSelectList();

    // Parse FROM clause
    List<TableReference> fromClause = null;
    if (peek().getType() == Token.Type.FROM) {
      consume(Token.Type.FROM);
      fromClause = parseFromClause();
    }

    // Parse WHERE clause
    Expression whereClause = null;
    if (peek().getType() == Token.Type.WHERE) {
      consume(Token.Type.WHERE);
      whereClause = parseExpression();
    }

    // Parse GROUP BY clause
    List<Expression> groupByClause = null;
    if (peek().getType() == Token.Type.GROUP) {
      consume(Token.Type.GROUP);
      consume(Token.Type.BY);
      groupByClause = parseExpressionList();
    }

    // Parse HAVING clause
    Expression havingClause = null;
    if (peek().getType() == Token.Type.HAVING) {
      consume(Token.Type.HAVING);
      havingClause = parseExpression();
    }

    // Parse ORDER BY clause
    List<OrderByItem> orderByClause = null;
    if (peek().getType() == Token.Type.ORDER) {
      consume(Token.Type.ORDER);
      consume(Token.Type.BY);
      orderByClause = parseOrderByList();
    }

    // Parse LIMIT clause
    Expression limitClause = null;
    if (peek().getType() == Token.Type.LIMIT) {
      consume(Token.Type.LIMIT);
      limitClause = parseExpression();
    }

    Integer limitValue = null;
    if (limitClause instanceof NumericLiteral) {
      limitValue = Integer.parseInt(((NumericLiteral) limitClause).getValue());
    }

    return new SelectStatement(
        selectList,
        fromClause,
        whereClause,
        groupByClause,
        havingClause,
        orderByClause,
        limitValue);
  }

  private InsertStatement parseInsertStatement() {
    consume(Token.Type.INSERT);
    consume(Token.Type.INTO);

    TableReference table = parseTableReference();

    // Parse column list (optional)
    List<Identifier> columns = null;
    if (peek().getType() == Token.Type.LPAREN) {
      consume(Token.Type.LPAREN);
      columns = parseIdentifierList();
      consume(Token.Type.RPAREN);
    }

    // Parse VALUES clause or SELECT statement
    List<List<Expression>> values = null;
    if (peek().getType() == Token.Type.VALUES) {
      consume(Token.Type.VALUES);
      values = parseValuesList();
    } else if (peek().getType() == Token.Type.SELECT) {
      // For INSERT ... SELECT, we'll just parse it as a simple INSERT
      // and let the analyzer handle the SELECT part separately
      values = new java.util.ArrayList<>();
    }

    return new InsertStatement(table, columns, values);
  }

  private UpdateStatement parseUpdateStatement() {
    consume(Token.Type.UPDATE);

    TableReference table = parseTableReference();

    consume(Token.Type.SET);
    List<Assignment> assignments = parseAssignmentList();

    Expression whereClause = null;
    if (peek().getType() == Token.Type.WHERE) {
      consume(Token.Type.WHERE);
      whereClause = parseExpression();
    }

    return new UpdateStatement(table, assignments, whereClause);
  }

  private DeleteStatement parseDeleteStatement() {
    consume(Token.Type.DELETE);
    consume(Token.Type.FROM);

    TableReference table = parseTableReference();

    Expression whereClause = null;
    if (peek().getType() == Token.Type.WHERE) {
      consume(Token.Type.WHERE);
      whereClause = parseExpression();
    }

    return new DeleteStatement(table, whereClause);
  }

  private Statement parseCreateStatement() {
    consume(Token.Type.CREATE);

    if (peek().getType() == Token.Type.TABLE) {
      return parseCreateTableStatement();
    }

    throw new ParseException("Unsupported CREATE statement");
  }

  private CreateTableStatement parseCreateTableStatement() {
    consume(Token.Type.TABLE);

    Identifier tableName = parseIdentifier();

    consume(Token.Type.LPAREN);
    List<ColumnDefinition> columns = parseColumnDefinitionList();
    consume(Token.Type.RPAREN);

    return new CreateTableStatement(tableName, columns);
  }

  private List<SelectItem> parseSelectList() {
    List<SelectItem> items = new ArrayList<>();

    do {
      Expression expr = parseExpression();
      String alias = null;

      if (peek().getType() == Token.Type.AS) {
        consume(Token.Type.AS);
        alias = consume(Token.Type.IDENT).getValue();
      } else if (peek().getType() == Token.Type.IDENT) {
        alias = consume(Token.Type.IDENT).getValue();
      }

      items.add(new SelectItem(expr, alias));

      if (peek().getType() == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
      } else {
        break;
      }
    } while (true);

    return items;
  }

  private List<TableReference> parseFromClause() {
    List<TableReference> tables = new ArrayList<>();

    // Parse first table
    tables.add(parseTableReference());

    // Parse JOINs or comma-separated tables
    while (true) {
      Token.Type nextType = peek().getType();
      if (nextType == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
        tables.add(parseTableReference());
      } else if (nextType == Token.Type.JOIN
          || nextType == Token.Type.INNER
          || nextType == Token.Type.LEFT
          || nextType == Token.Type.RIGHT
          || nextType == Token.Type.CROSS) {
        // Handle JOIN - consume JOIN keywords and add the joined table
        if (nextType == Token.Type.INNER
            || nextType == Token.Type.LEFT
            || nextType == Token.Type.RIGHT
            || nextType == Token.Type.CROSS) {
          consume(nextType); // consume INNER/LEFT/RIGHT/CROSS
          // Optional OUTER keyword after LEFT/RIGHT/FULL
          if (peek().getType() == Token.Type.OUTER) {
            consume(Token.Type.OUTER);
          }
        }
        if (peek().getType() == Token.Type.JOIN) {
          consume(Token.Type.JOIN);
        }
        tables.add(parseTableReference());

        // Skip ON clause for now (not needed for CROSS JOIN)
        if (peek().getType() == Token.Type.ON) {
          consume(Token.Type.ON);
          parseExpression(); // consume but ignore the join condition
        }
      } else {
        break;
      }
    }

    return tables;
  }

  private TableReference parseTableReference() {
    Identifier tableName = parseIdentifier();
    String alias = null;

    if (peek().getType() == Token.Type.AS) {
      consume(Token.Type.AS);
      alias = consume(Token.Type.IDENT).getValue();
    } else if (peek().getType() == Token.Type.IDENT) {
      alias = consume(Token.Type.IDENT).getValue();
    }

    return new TableReference(tableName, alias);
  }

  private Expression parseExpression() {
    return parseOrExpression();
  }

  private Expression parseOrExpression() {
    Expression left = parseAndExpression();

    while (peek().getType() == Token.Type.OR) {
      consume(Token.Type.OR);
      Expression right = parseAndExpression();
      left = new BinaryExpression(left, BinaryExpression.Operator.OR, right);
    }

    return left;
  }

  private Expression parseAndExpression() {
    Expression left = parseEqualityExpression();

    while (peek().getType() == Token.Type.AND) {
      consume(Token.Type.AND);
      Expression right = parseEqualityExpression();
      left = new BinaryExpression(left, BinaryExpression.Operator.AND, right);
    }

    return left;
  }

  private Expression parseEqualityExpression() {
    Expression left = parseRelationalExpression();

    while (true) {
      Token.Type type = peek().getType();
      BinaryExpression.Operator op = null;

      switch (type) {
        case EQUALS:
          op = BinaryExpression.Operator.EQUALS;
          break;
        case NOT_EQUALS:
          op = BinaryExpression.Operator.NOT_EQUALS;
          break;
        default:
          return left;
      }

      consume(type);
      Expression right = parseRelationalExpression();
      left = new BinaryExpression(left, op, right);
    }
  }

  private Expression parseRelationalExpression() {
    Expression left = parseAdditiveExpression();

    while (true) {
      Token.Type type = peek().getType();
      BinaryExpression.Operator op = null;

      switch (type) {
        case LESS_THAN:
          op = BinaryExpression.Operator.LESS_THAN;
          break;
        case GREATER_THAN:
          op = BinaryExpression.Operator.GREATER_THAN;
          break;
        case LESS_EQUALS:
          op = BinaryExpression.Operator.LESS_EQUALS;
          break;
        case GREATER_EQUALS:
          op = BinaryExpression.Operator.GREATER_EQUALS;
          break;
        case LIKE:
          op = BinaryExpression.Operator.LIKE;
          break;
        case IN:
          op = BinaryExpression.Operator.IN;
          break;
        default:
          return left;
      }

      consume(type);
      Expression right = parseAdditiveExpression();
      left = new BinaryExpression(left, op, right);
    }
  }

  private Expression parseAdditiveExpression() {
    Expression left = parseMultiplicativeExpression();

    while (true) {
      Token.Type type = peek().getType();
      BinaryExpression.Operator op = null;

      switch (type) {
        case PLUS:
          op = BinaryExpression.Operator.PLUS;
          break;
        case MINUS:
          op = BinaryExpression.Operator.MINUS;
          break;
        default:
          return left;
      }

      consume(type);
      Expression right = parseMultiplicativeExpression();
      left = new BinaryExpression(left, op, right);
    }
  }

  private Expression parseMultiplicativeExpression() {
    Expression left = parsePrimaryExpression();

    while (true) {
      Token.Type type = peek().getType();
      BinaryExpression.Operator op = null;

      switch (type) {
        case MULTIPLY:
          op = BinaryExpression.Operator.MULTIPLY;
          break;
        case DIVIDE:
          op = BinaryExpression.Operator.DIVIDE;
          break;
        case MODULO:
          op = BinaryExpression.Operator.MODULO;
          break;
        default:
          return left;
      }

      consume(type);
      Expression right = parsePrimaryExpression();
      left = new BinaryExpression(left, op, right);
    }
  }

  private Expression parsePrimaryExpression() {
    Token token = peek();

    switch (token.getType()) {
      case MULTIPLY:
        consume(Token.Type.MULTIPLY);
        return new Identifier("*");
      case IDENT:
        Token identToken = peek();
        // Check if this is a function call
        if (tokens.size() > position + 1
            && tokens.get(position + 1).getType() == Token.Type.LPAREN) {
          consume(Token.Type.IDENT);
          consume(Token.Type.LPAREN);
          // Skip function arguments for now
          int parenCount = 1;
          while (parenCount > 0 && peek().getType() != Token.Type.EOF) {
            Token t = consume();
            if (t.getType() == Token.Type.LPAREN) {
              parenCount++;
            } else if (t.getType() == Token.Type.RPAREN) {
              parenCount--;
            }
          }
          return new Identifier(identToken.getValue() + "()");
        } else {
          return parseIdentifier();
        }
      case SCONST:
        consume(Token.Type.SCONST);
        return new StringLiteral(token.getValue());
      case ICONST:
        consume(Token.Type.ICONST);
        return new NumericLiteral(token.getValue(), true);
      case FCONST:
        consume(Token.Type.FCONST);
        return new NumericLiteral(token.getValue(), false);
      case PLACEHOLDER:
        consume(Token.Type.PLACEHOLDER);
        return new Placeholder();
      case TRUE:
        consume(Token.Type.TRUE);
        return new BooleanLiteral(true);
      case FALSE:
        consume(Token.Type.FALSE);
        return new BooleanLiteral(false);
      case CASE:
        return parseCaseExpression();
      case CAST:
        return parseCastExpression();
      case LPAREN:
        consume(Token.Type.LPAREN);
        // Check if this is a subquery
        if (peek().getType() == Token.Type.SELECT) {
          SelectStatement subquery = parseSelectStatement();
          consume(Token.Type.RPAREN);
          return new SubqueryExpression(subquery);
        } else {
          Expression expr = parseExpression();
          consume(Token.Type.RPAREN);
          return expr;
        }
      default:
        throw new ParseException("Unexpected token in expression: " + token);
    }
  }

  private Identifier parseIdentifier() {
    Token token = consume(Token.Type.IDENT);
    String name = token.getValue();

    // Check for qualified name (table.column)
    if (peek().getType() == Token.Type.DOT) {
      consume(Token.Type.DOT);
      Token columnToken = consume(Token.Type.IDENT);
      name = name + "." + columnToken.getValue();
    }

    return new Identifier(name);
  }

  private List<Expression> parseExpressionList() {
    List<Expression> expressions = new ArrayList<>();

    do {
      expressions.add(parseExpression());

      if (peek().getType() == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
      } else {
        break;
      }
    } while (true);

    return expressions;
  }

  private List<Identifier> parseIdentifierList() {
    List<Identifier> identifiers = new ArrayList<>();

    do {
      identifiers.add(parseIdentifier());

      if (peek().getType() == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
      } else {
        break;
      }
    } while (true);

    return identifiers;
  }

  private List<OrderByItem> parseOrderByList() {
    List<OrderByItem> items = new ArrayList<>();

    do {
      Expression expr = parseExpression();
      OrderByItem.Direction direction = OrderByItem.Direction.ASC;

      // Handle ASC/DESC
      Token token = peek();
      if (token.getType() == Token.Type.ASC) {
        consume(Token.Type.ASC);
        direction = OrderByItem.Direction.ASC;
      } else if (token.getType() == Token.Type.DESC) {
        consume(Token.Type.DESC);
        direction = OrderByItem.Direction.DESC;
      } else if (token.getType() == Token.Type.IDENT) {
        String dir = token.getValue().toUpperCase();
        if ("ASC".equals(dir)) {
          consume(Token.Type.IDENT);
          direction = OrderByItem.Direction.ASC;
        } else if ("DESC".equals(dir)) {
          consume(Token.Type.IDENT);
          direction = OrderByItem.Direction.DESC;
        }
      }

      // Handle NULLS FIRST/LAST
      if (peek().getType() == Token.Type.NULLS) {
        consume(Token.Type.NULLS);
        if (peek().getType() == Token.Type.FIRST) {
          consume(Token.Type.FIRST);
        } else if (peek().getType() == Token.Type.LAST) {
          consume(Token.Type.LAST);
        }
      }

      items.add(new OrderByItem(expr, direction));

      if (peek().getType() == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
      } else {
        break;
      }
    } while (true);

    return items;
  }

  private List<List<Expression>> parseValuesList() {
    List<List<Expression>> valuesList = new ArrayList<>();

    do {
      consume(Token.Type.LPAREN);
      List<Expression> values = parseExpressionList();
      consume(Token.Type.RPAREN);
      valuesList.add(values);

      if (peek().getType() == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
      } else {
        break;
      }
    } while (true);

    return valuesList;
  }

  private List<Assignment> parseAssignmentList() {
    List<Assignment> assignments = new ArrayList<>();

    do {
      Identifier column = parseIdentifier();
      consume(Token.Type.EQUALS);
      Expression value = parseExpression();
      assignments.add(new Assignment(column, value));

      if (peek().getType() == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
      } else {
        break;
      }
    } while (true);

    return assignments;
  }

  private List<ColumnDefinition> parseColumnDefinitionList() {
    List<ColumnDefinition> columns = new ArrayList<>();

    do {
      Identifier name = parseIdentifier();
      String dataType = consume(Token.Type.IDENT).getValue();
      boolean notNull = false;
      boolean primaryKey = false;

      // Parse constraints (simplified)
      while (peek().getType() == Token.Type.NOT || peek().getType() == Token.Type.PRIMARY) {
        if (peek().getType() == Token.Type.NOT) {
          consume(Token.Type.NOT);
          consume(Token.Type.NULL);
          notNull = true;
        } else if (peek().getType() == Token.Type.PRIMARY) {
          consume(Token.Type.PRIMARY);
          consume(Token.Type.KEY);
          primaryKey = true;
        }
      }

      columns.add(new ColumnDefinition(name, dataType, notNull, primaryKey));

      if (peek().getType() == Token.Type.COMMA) {
        consume(Token.Type.COMMA);
      } else {
        break;
      }
    } while (true);

    return columns;
  }

  private Token peek() {
    if (position >= tokens.size()) {
      return new Token(Token.Type.EOF, "", 0, 0);
    }
    return tokens.get(position);
  }

  private Token consume(Token.Type expectedType) {
    Token token = peek();
    if (token.getType() != expectedType) {
      throw new ParseException("Expected " + expectedType + " but got " + token.getType());
    }
    position++;
    return token;
  }

  private Expression parseCaseExpression() {
    consume(Token.Type.CASE);

    // Skip WHEN/THEN/ELSE/END for now - just consume tokens until END
    int depth = 1;
    while (depth > 0 && peek().getType() != Token.Type.EOF) {
      if (peek().getType() == Token.Type.CASE) {
        depth++;
      } else if (peek().getType() == Token.Type.END) {
        depth--;
        if (depth == 0) {
          consume(Token.Type.END);
          break;
        }
      }
      consume();
    }

    return new Identifier("CASE");
  }

  private Expression parseCastExpression() {
    consume(Token.Type.CAST);
    consume(Token.Type.LPAREN);

    // Parse the expression being cast
    parseExpression();

    // Skip AS and type
    if (peek().getType() == Token.Type.AS) {
      consume(Token.Type.AS);
      consume(Token.Type.IDENT); // type name
    }

    consume(Token.Type.RPAREN);

    return new Identifier("CAST");
  }

  private Token consume() {
    if (position >= tokens.size()) {
      throw new ParseException("Unexpected end of input");
    }
    return tokens.get(position++);
  }

  public static class ParseException extends RuntimeException {
    public ParseException(String message) {
      super(message);
    }
  }
}
