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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * SQL analyzer using JSQLParser library for multi-dialect support.
 *
 * <p>Supports PostgreSQL, MySQL, and MariaDB dialects.
 */
public final class JSQLParserAnalyzer {

  private static final Logger LOGGER = Logger.getLogger(JSQLParserAnalyzer.class.getName());

  private JSQLParserAnalyzer() {
    // Utility class
  }

  /** Column information with table and column name. */
  public static class ColumnInfo {
    public String tableName;
    public String columnName;

    public ColumnInfo(String tableName, String columnName) {
      this.tableName = tableName;
      this.columnName = columnName;
    }

    @Override
    public String toString() {
      return tableName + "." + columnName;
    }
  }

  /** Query analysis result. */
  public static class QueryAnalysis {
    public String queryType;
    public List<ColumnInfo> columns = new ArrayList<>();
    public List<ColumnInfo> whereColumns = new ArrayList<>();
    public Set<String> tables = new HashSet<>();
    public boolean hasParameters = false;

    @Override
    public String toString() {
      return String.format(
          "QueryAnalysis{queryType='%s', tables=%s, columns=%s, whereColumns=%s, hasParameters=%s}",
          queryType, tables, columns, whereColumns, hasParameters);
    }
  }

  /**
   * Analyze SQL statement.
   *
   * @param sql SQL statement to analyze
   * @return Query analysis result
   */
  public static QueryAnalysis analyze(String sql) {
    QueryAnalysis analysis = new QueryAnalysis();

    if (sql == null || sql.trim().isEmpty()) {
      analysis.queryType = "UNKNOWN";
      return analysis;
    }

    try {
      Statement statement = CCJSqlParserUtil.parse(sql);
      analysis.hasParameters = containsParameters(statement);

      if (statement instanceof Select) {
        analysis.queryType = "SELECT";
        extractFromSelect((Select) statement, analysis);
      } else if (statement instanceof Insert) {
        analysis.queryType = "INSERT";
        extractFromInsert((Insert) statement, analysis);
      } else if (statement instanceof Update) {
        analysis.queryType = "UPDATE";
        extractFromUpdate((Update) statement, analysis);
      } else if (statement instanceof Delete) {
        analysis.queryType = "DELETE";
        extractFromDelete((Delete) statement, analysis);
      } else if (statement instanceof net.sf.jsqlparser.statement.create.table.CreateTable) {
        analysis.queryType = "CREATE";
        extractFromCreateTable(
            (net.sf.jsqlparser.statement.create.table.CreateTable) statement, analysis);
      } else if (statement instanceof net.sf.jsqlparser.statement.drop.Drop) {
        analysis.queryType = "DROP";
        extractFromDrop((net.sf.jsqlparser.statement.drop.Drop) statement, analysis);
      } else {
        analysis.queryType = "UNKNOWN";
      }

    } catch (JSQLParserException e) {
      LOGGER.fine(() -> String.format("Failed to parse SQL: %s", e.getMessage()));
      // Fallback to simple string parsing
      analysis.queryType = detectQueryType(sql);
    }

    return analysis;
  }

  private static boolean containsParameters(Statement statement) {
    // JSQLParser represents ? as JdbcParameter
    // Check by converting to string and looking for ?
    // or use visitor pattern for more accurate detection
    return statement.toString().contains("?");
  }

  private static void extractFromSelect(Select select, QueryAnalysis analysis) {
    // Extract table names
    TablesNamesFinder tablesFinder = new TablesNamesFinder();
    List<String> tableList = tablesFinder.getTableList(select);
    analysis.tables.addAll(tableList);

    // Extract columns from SELECT clause
    if (select.getSelectBody() instanceof PlainSelect) {
      PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

      // Extract WHERE clause columns only if there are parameters
      if (plainSelect.getWhere() != null) {
        String whereClause = plainSelect.getWhere().toString();
        // Only extract columns if WHERE clause contains parameters
        if (whereClause.contains("?")) {
          extractColumnsFromExpression(plainSelect.getWhere(), analysis.whereColumns);
        }
      }
    }
  }

  private static void extractFromInsert(Insert insert, QueryAnalysis analysis) {
    // Extract table name
    if (insert.getTable() != null) {
      analysis.tables.add(insert.getTable().getName());
    }

    // Extract column names
    if (insert.getColumns() != null) {
      String tableName = insert.getTable().getName();
      for (Column column : insert.getColumns()) {
        analysis.columns.add(new ColumnInfo(tableName, column.getColumnName()));
      }
    }
  }

  private static void extractFromUpdate(Update update, QueryAnalysis analysis) {
    // Extract table name
    if (update.getTable() != null) {
      analysis.tables.add(update.getTable().getName());
    }

    // Extract columns being updated
    String tableName = update.getTable().getName();
    for (UpdateSet updateSet : update.getUpdateSets()) {
      for (Column column : updateSet.getColumns()) {
        analysis.columns.add(new ColumnInfo(tableName, column.getColumnName()));
      }
    }

    // Extract WHERE clause columns
    if (update.getWhere() != null) {
      extractColumnsFromExpression(update.getWhere(), analysis.whereColumns);
    }
  }

  private static void extractFromDelete(Delete delete, QueryAnalysis analysis) {
    // Extract table name
    if (delete.getTable() != null) {
      analysis.tables.add(delete.getTable().getName());
    }

    // Extract WHERE clause columns
    if (delete.getWhere() != null) {
      extractColumnsFromExpression(delete.getWhere(), analysis.whereColumns);
    }
  }

  private static void extractFromCreateTable(
      net.sf.jsqlparser.statement.create.table.CreateTable createTable, QueryAnalysis analysis) {
    // Extract table name
    if (createTable.getTable() != null) {
      analysis.tables.add(createTable.getTable().getName());
    }
  }

  private static void extractFromDrop(
      net.sf.jsqlparser.statement.drop.Drop drop, QueryAnalysis analysis) {
    // Extract table name
    if (drop.getName() != null) {
      analysis.tables.add(drop.getName().getName());
    }
  }

  private static void extractColumnsFromExpression(Expression expression, List<ColumnInfo> columns) {
    if (expression == null) {
      return;
    }

    // Extract columns from expression
    if (expression instanceof Column) {
      Column column = (Column) expression;
      String tableName = column.getTable() != null ? column.getTable().getName() : null;
      columns.add(new ColumnInfo(tableName, column.getColumnName()));
    }

    // Handle comparison operators
    if (expression instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo) {
      net.sf.jsqlparser.expression.operators.relational.EqualsTo equals =
          (net.sf.jsqlparser.expression.operators.relational.EqualsTo) expression;
      extractColumnsFromExpression(equals.getLeftExpression(), columns);
      extractColumnsFromExpression(equals.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThan) {
      net.sf.jsqlparser.expression.operators.relational.GreaterThan gt =
          (net.sf.jsqlparser.expression.operators.relational.GreaterThan) expression;
      extractColumnsFromExpression(gt.getLeftExpression(), columns);
      extractColumnsFromExpression(gt.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals) {
      net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals gte =
          (net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals) expression;
      extractColumnsFromExpression(gte.getLeftExpression(), columns);
      extractColumnsFromExpression(gte.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.MinorThan) {
      net.sf.jsqlparser.expression.operators.relational.MinorThan lt =
          (net.sf.jsqlparser.expression.operators.relational.MinorThan) expression;
      extractColumnsFromExpression(lt.getLeftExpression(), columns);
      extractColumnsFromExpression(lt.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.MinorThanEquals) {
      net.sf.jsqlparser.expression.operators.relational.MinorThanEquals lte =
          (net.sf.jsqlparser.expression.operators.relational.MinorThanEquals) expression;
      extractColumnsFromExpression(lte.getLeftExpression(), columns);
      extractColumnsFromExpression(lte.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.operators.relational.NotEqualsTo) {
      net.sf.jsqlparser.expression.operators.relational.NotEqualsTo ne =
          (net.sf.jsqlparser.expression.operators.relational.NotEqualsTo) expression;
      extractColumnsFromExpression(ne.getLeftExpression(), columns);
      extractColumnsFromExpression(ne.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
      net.sf.jsqlparser.expression.operators.conditional.AndExpression and =
          (net.sf.jsqlparser.expression.operators.conditional.AndExpression) expression;
      extractColumnsFromExpression(and.getLeftExpression(), columns);
      extractColumnsFromExpression(and.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.operators.conditional.OrExpression) {
      net.sf.jsqlparser.expression.operators.conditional.OrExpression or =
          (net.sf.jsqlparser.expression.operators.conditional.OrExpression) expression;
      extractColumnsFromExpression(or.getLeftExpression(), columns);
      extractColumnsFromExpression(or.getRightExpression(), columns);
    } else if (expression instanceof net.sf.jsqlparser.expression.Parenthesis) {
      net.sf.jsqlparser.expression.Parenthesis paren =
          (net.sf.jsqlparser.expression.Parenthesis) expression;
      extractColumnsFromExpression(paren.getExpression(), columns);
    }

    // Recursively process sub-expressions
    // Note: JSQLParser provides visitor pattern for more complex traversal
  }

  private static String detectQueryType(String sql) {
    String trimmed = sql.trim().toUpperCase();
    if (trimmed.startsWith("SELECT")) {
      return "SELECT";
    } else if (trimmed.startsWith("INSERT")) {
      return "INSERT";
    } else if (trimmed.startsWith("UPDATE")) {
      return "UPDATE";
    } else if (trimmed.startsWith("DELETE")) {
      return "DELETE";
    } else if (trimmed.startsWith("CREATE")) {
      return "CREATE";
    } else if (trimmed.startsWith("DROP")) {
      return "DROP";
    }
    return "UNKNOWN";
  }
}
