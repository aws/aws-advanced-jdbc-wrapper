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

import java.util.List;
import software.amazon.jdbc.plugin.encryption.parser.ast.Assignment;
import software.amazon.jdbc.plugin.encryption.parser.ast.BinaryExpression;
import software.amazon.jdbc.plugin.encryption.parser.ast.ColumnDefinition;
import software.amazon.jdbc.plugin.encryption.parser.ast.CreateTableStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.DeleteStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.Expression;
import software.amazon.jdbc.plugin.encryption.parser.ast.Identifier;
import software.amazon.jdbc.plugin.encryption.parser.ast.InsertStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.NumericLiteral;
import software.amazon.jdbc.plugin.encryption.parser.ast.OrderByItem;
import software.amazon.jdbc.plugin.encryption.parser.ast.SelectItem;
import software.amazon.jdbc.plugin.encryption.parser.ast.SelectStatement;
import software.amazon.jdbc.plugin.encryption.parser.ast.Statement;
import software.amazon.jdbc.plugin.encryption.parser.ast.StringLiteral;
import software.amazon.jdbc.plugin.encryption.parser.ast.TableReference;
import software.amazon.jdbc.plugin.encryption.parser.ast.UpdateStatement;

/** Main PostgreSQL SQL Parser Combines lexer and parser to parse SQL statements. */
public class PostgreSqlParser {

  /** Parse a SQL string and return the AST. */
  public Statement parse(String sql) {
    // Tokenize the input
    SqlLexer lexer = new SqlLexer(sql);
    List<Token> tokens = lexer.tokenize();

    // Parse the tokens
    SqlParser parser = new SqlParser(tokens);
    return parser.parse();
  }

  /** Parse and pretty print the AST. */
  public String parseAndFormat(String sql) {
    Statement stmt = parse(sql);
    return formatStatement(stmt);
  }

  private String formatStatement(Statement stmt) {
    if (stmt instanceof SelectStatement) {
      return formatSelectStatement((SelectStatement) stmt);
    } else if (stmt instanceof InsertStatement) {
      return formatInsertStatement((InsertStatement) stmt);
    } else if (stmt instanceof UpdateStatement) {
      return formatUpdateStatement((UpdateStatement) stmt);
    } else if (stmt instanceof DeleteStatement) {
      return formatDeleteStatement((DeleteStatement) stmt);
    } else if (stmt instanceof CreateTableStatement) {
      return formatCreateTableStatement((CreateTableStatement) stmt);
    }
    return stmt.toString();
  }

  private String formatSelectStatement(SelectStatement stmt) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");

    // Format select list
    for (int i = 0; i < stmt.getSelectList().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      SelectItem item = stmt.getSelectList().get(i);
      sb.append(formatExpression(item.getExpression()));
      if (item.getAlias() != null) {
        sb.append(" AS ").append(item.getAlias());
      }
    }

    // Format FROM clause
    if (stmt.getFromList() != null && !stmt.getFromList().isEmpty()) {
      sb.append("\nFROM ");
      for (int i = 0; i < stmt.getFromList().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        TableReference table = stmt.getFromList().get(i);
        sb.append(table.getTableName().getName());
        if (table.getAlias() != null) {
          sb.append(" AS ").append(table.getAlias());
        }
      }
    }

    // Format WHERE clause
    if (stmt.getWhereClause() != null) {
      sb.append("\nWHERE ").append(formatExpression(stmt.getWhereClause()));
    }

    // Format GROUP BY clause
    if (stmt.getGroupByList() != null && !stmt.getGroupByList().isEmpty()) {
      sb.append("\nGROUP BY ");
      for (int i = 0; i < stmt.getGroupByList().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(formatExpression(stmt.getGroupByList().get(i)));
      }
    }

    // Format HAVING clause
    if (stmt.getHavingClause() != null) {
      sb.append("\nHAVING ").append(formatExpression(stmt.getHavingClause()));
    }

    // Format ORDER BY clause
    if (stmt.getOrderByList() != null && !stmt.getOrderByList().isEmpty()) {
      sb.append("\nORDER BY ");
      for (int i = 0; i < stmt.getOrderByList().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        OrderByItem item = stmt.getOrderByList().get(i);
        sb.append(formatExpression(item.getExpression()));
        sb.append(" ").append(item.getDirection());
      }
    }

    // Format LIMIT clause
    if (stmt.getLimit() != null) {
      sb.append("\nLIMIT ").append(stmt.getLimit());
    }

    return sb.toString();
  }

  private String formatInsertStatement(InsertStatement stmt) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(stmt.getTable().getTableName().getName());

    if (stmt.getColumns() != null && !stmt.getColumns().isEmpty()) {
      sb.append(" (");
      for (int i = 0; i < stmt.getColumns().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(stmt.getColumns().get(i).getName());
      }
      sb.append(")");
    }

    sb.append("\nVALUES ");
    for (int i = 0; i < stmt.getValues().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("(");
      List<Expression> values = stmt.getValues().get(i);
      for (int j = 0; j < values.size(); j++) {
        if (j > 0) {
          sb.append(", ");
        }
        sb.append(formatExpression(values.get(j)));
      }
      sb.append(")");
    }

    return sb.toString();
  }

  private String formatUpdateStatement(UpdateStatement stmt) {
    StringBuilder sb = new StringBuilder();
    sb.append("UPDATE ").append(stmt.getTable().getTableName().getName());
    sb.append("\nSET ");

    for (int i = 0; i < stmt.getAssignments().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      Assignment assignment = stmt.getAssignments().get(i);
      sb.append(assignment.getColumn().getName());
      sb.append(" = ");
      sb.append(formatExpression(assignment.getValue()));
    }

    if (stmt.getWhereClause() != null) {
      sb.append("\nWHERE ").append(formatExpression(stmt.getWhereClause()));
    }

    return sb.toString();
  }

  private String formatDeleteStatement(DeleteStatement stmt) {
    StringBuilder sb = new StringBuilder();
    sb.append("DELETE FROM ").append(stmt.getTable().getTableName().getName());

    if (stmt.getWhereClause() != null) {
      sb.append("\nWHERE ").append(formatExpression(stmt.getWhereClause()));
    }

    return sb.toString();
  }

  private String formatCreateTableStatement(CreateTableStatement stmt) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(stmt.getTableName().getName()).append(" (\n");

    for (int i = 0; i < stmt.getColumns().size(); i++) {
      if (i > 0) {
        sb.append(",\n");
      }
      ColumnDefinition col = stmt.getColumns().get(i);
      sb.append("  ").append(col.getColumnName().getName());
      sb.append(" ").append(col.getDataType());

      if (col.isNotNull()) {
        sb.append(" NOT NULL");
      }
      if (col.isPrimaryKey()) {
        sb.append(" PRIMARY KEY");
      }
    }

    sb.append("\n)");
    return sb.toString();
  }

  private String formatExpression(Expression expr) {
    if (expr instanceof Identifier) {
      return ((Identifier) expr).getName();
    } else if (expr instanceof StringLiteral) {
      return "'" + ((StringLiteral) expr).getValue() + "'";
    } else if (expr instanceof NumericLiteral) {
      return ((NumericLiteral) expr).getValue();
    } else if (expr instanceof BinaryExpression) {
      BinaryExpression binExpr = (BinaryExpression) expr;
      return formatExpression(binExpr.getLeft())
          + " "
          + formatOperator(binExpr.getOperator())
          + " "
          + formatExpression(binExpr.getRight());
    }
    return expr.toString();
  }

  private String formatOperator(BinaryExpression.Operator op) {
    switch (op) {
      case EQUALS:
        return "=";
      case NOT_EQUALS:
        return "<>";
      case LESS_THAN:
        return "<";
      case GREATER_THAN:
        return ">";
      case LESS_EQUALS:
        return "<=";
      case GREATER_EQUALS:
        return ">=";
      case PLUS:
        return "+";
      case MINUS:
        return "-";
      case MULTIPLY:
        return "*";
      case DIVIDE:
        return "/";
      case MODULO:
        return "%";
      case AND:
        return "AND";
      case OR:
        return "OR";
      case LIKE:
        return "LIKE";
      case IN:
        return "IN";
      case BETWEEN:
        return "BETWEEN";
      default:
        return op.toString();
    }
  }

  /** Main method for testing. */
  public static void main(String[] args) {
    PostgreSqlParser parser = new PostgreSqlParser();

    // Test SELECT statement
    String selectSql = "SELECT id, name, age FROM users WHERE age > 18 ORDER BY name";
    System.out.println("Original SQL: " + selectSql);
    System.out.println("Parsed AST:");
    System.out.println(parser.parseAndFormat(selectSql));
    System.out.println();

    // Test INSERT statement
    String insertSql = "INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30)";
    System.out.println("Original SQL: " + insertSql);
    System.out.println("Parsed AST:");
    System.out.println(parser.parseAndFormat(insertSql));
    System.out.println();

    // Test UPDATE statement
    String updateSql = "UPDATE users SET age = 26 WHERE name = 'John'";
    System.out.println("Original SQL: " + updateSql);
    System.out.println("Parsed AST:");
    System.out.println(parser.parseAndFormat(updateSql));
    System.out.println();

    // Test DELETE statement
    String deleteSql = "DELETE FROM users WHERE age < 18";
    System.out.println("Original SQL: " + deleteSql);
    System.out.println("Parsed AST:");
    System.out.println(parser.parseAndFormat(deleteSql));
    System.out.println();

    // Test CREATE TABLE statement
    String createSql =
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, age INTEGER)";
    System.out.println("Original SQL: " + createSql);
    System.out.println("Parsed AST:");
    System.out.println(parser.parseAndFormat(createSql));
  }
}
