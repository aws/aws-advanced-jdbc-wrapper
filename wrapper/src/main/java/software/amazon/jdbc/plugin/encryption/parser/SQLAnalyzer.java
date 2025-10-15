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

import software.amazon.jdbc.plugin.encryption.parser.ast.*;

import java.util.*;

public class SQLAnalyzer {

    private final PostgreSqlParser parser = new PostgreSqlParser();

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

    public static class QueryAnalysis {
        public String queryType;
        public List<ColumnInfo> columns = new ArrayList<>();
        public List<ColumnInfo> whereColumns = new ArrayList<>(); // Separate WHERE clause columns
        public Set<String> tables = new HashSet<>();

        @Override
        public String toString() {
            return String.format("QueryAnalysis{queryType='%s', tables=%s, columns=%s, whereColumns=%s}",
                queryType, tables, columns, whereColumns);
        }
    }

    private boolean containsParameters(Expression expression) {
        if (expression == null) return false;

        if (expression instanceof Placeholder) {
            return true;
        } else if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) expression;
            return containsParameters(binaryExpr.getLeft()) || containsParameters(binaryExpr.getRight());
        }
        return false;
    }

    public QueryAnalysis analyze(String sql) {
        QueryAnalysis analysis = new QueryAnalysis();

        try {
            Statement statement = parser.parse(sql);

            if (statement instanceof SelectStatement) {
                analysis.queryType = "SELECT";
                extractFromSelect((SelectStatement) statement, analysis);
            } else if (statement instanceof InsertStatement) {
                analysis.queryType = "INSERT";
                extractFromInsert((InsertStatement) statement, analysis);
            } else if (statement instanceof UpdateStatement) {
                analysis.queryType = "UPDATE";
                extractFromUpdate((UpdateStatement) statement, analysis);
            } else if (statement instanceof DeleteStatement) {
                analysis.queryType = "DELETE";
                extractFromDelete((DeleteStatement) statement, analysis);
            } else if (statement instanceof CreateTableStatement) {
                analysis.queryType = "CREATE";
                extractFromCreateTable((CreateTableStatement) statement, analysis);
            } else {
                analysis.queryType = "UNKNOWN";
            }

        } catch (SqlParser.ParseException e) {
            // Fallback to string parsing if parser fails
            String trimmedSql = sql.trim().toUpperCase();
            if (trimmedSql.startsWith("SELECT")) {
                analysis.queryType = "SELECT";
            } else if (trimmedSql.startsWith("INSERT")) {
                analysis.queryType = "INSERT";
            } else if (trimmedSql.startsWith("UPDATE")) {
                analysis.queryType = "UPDATE";
            } else if (trimmedSql.startsWith("DELETE")) {
                analysis.queryType = "DELETE";
            } else if (trimmedSql.startsWith("CREATE")) {
                analysis.queryType = "CREATE";
            } else if (trimmedSql.startsWith("DROP")) {
                analysis.queryType = "DROP";
            } else {
                analysis.queryType = "UNKNOWN";
            }
        }

        return analysis;
    }

    private String extractTableName(String fullName) {
        if (fullName.contains(".")) {
            return fullName.substring(fullName.lastIndexOf(".") + 1);
        }
        return fullName;
    }

    private void extractFromSelect(SelectStatement select, QueryAnalysis analysis) {
        // Extract tables and build alias map
        Map<String, String> aliasToTable = new HashMap<>();
        if (select.getFromList() != null) {
            for (TableReference table : select.getFromList()) {
                String tableName = extractTableName(table.getTableName().getName());
                analysis.tables.add(tableName);

                // Map alias to table name
                if (table.getAlias() != null) {
                    aliasToTable.put(table.getAlias(), tableName);
                }
            }
        }

        // Extract columns from SELECT clause (skip * and literals)
        for (SelectItem selectItem : select.getSelectList()) {
            if (selectItem.getExpression() instanceof Identifier) {
                Identifier column = (Identifier) selectItem.getExpression();
                // Skip * wildcard
                if (!"*".equals(column.getName())) {
                    String fullName = column.getName();
                    String tableName;
                    String columnName;

                    // Parse qualified column name (e.g., "u.name" or "name")
                    if (fullName.contains(".")) {
                        String[] parts = fullName.split("\\.", 2);
                        String tableOrAlias = parts[0];
                        columnName = parts[1];
                        // Resolve alias to actual table name
                        tableName = aliasToTable.getOrDefault(tableOrAlias, tableOrAlias);
                    } else {
                        tableName = analysis.tables.isEmpty() ? "unknown" : analysis.tables.iterator().next();
                        columnName = fullName;
                    }

                    analysis.columns.add(new ColumnInfo(tableName, columnName));
                }
            }
        }

        // Extract columns from WHERE clause only if WHERE contains parameters
        if (select.getWhereClause() != null && containsParameters(select.getWhereClause())) {
            extractWhereColumnsFromExpression(select.getWhereClause(), analysis);
        }
    }

    private void extractColumnsFromExpression(Expression expression, QueryAnalysis analysis) {
        if (expression instanceof Identifier) {
            Identifier column = (Identifier) expression;
            String tableName = analysis.tables.isEmpty() ? "unknown" : analysis.tables.iterator().next();
            analysis.columns.add(new ColumnInfo(tableName, column.getName()));
        } else if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) expression;
            extractColumnsFromExpression(binaryExpr.getLeft(), analysis);
            extractColumnsFromExpression(binaryExpr.getRight(), analysis);
        } else if (expression instanceof SubqueryExpression) {
            SubqueryExpression subquery = (SubqueryExpression) expression;
            // Extract tables from the subquery
            extractFromSelect(subquery.getSelectStatement(), analysis);
        }
    }

    private void extractWhereColumnsFromExpression(Expression expression, QueryAnalysis analysis) {
        if (expression instanceof Identifier) {
            Identifier column = (Identifier) expression;
            String tableName = analysis.tables.isEmpty() ? "unknown" : analysis.tables.iterator().next();
            analysis.whereColumns.add(new ColumnInfo(tableName, column.getName()));
        } else if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) expression;
            extractWhereColumnsFromExpression(binaryExpr.getLeft(), analysis);
            extractWhereColumnsFromExpression(binaryExpr.getRight(), analysis);
        } else if (expression instanceof SubqueryExpression) {
            SubqueryExpression subquery = (SubqueryExpression) expression;
            // Extract tables from the subquery
            extractFromSelect(subquery.getSelectStatement(), analysis);
        }
    }

    private void extractFromInsert(InsertStatement insert, QueryAnalysis analysis) {
        // Extract table (handle schema.table format)
        String tableName = extractTableName(insert.getTable().getTableName().getName());
        analysis.tables.add(tableName);

        // Extract columns (only if they exist)
        if (insert.getColumns() != null) {
            for (Identifier column : insert.getColumns()) {
                analysis.columns.add(new ColumnInfo(tableName, column.getName()));
            }
        }
    }

    private void extractFromUpdate(UpdateStatement update, QueryAnalysis analysis) {
        // Extract table
        String tableName = extractTableName(update.getTable().getTableName().getName());
        analysis.tables.add(tableName);

        // Extract columns from assignments
        for (Assignment assignment : update.getAssignments()) {
            analysis.columns.add(new ColumnInfo(tableName, assignment.getColumn().getName()));
        }

        // Extract columns from WHERE clause only if WHERE contains parameters
        if (update.getWhereClause() != null && containsParameters(update.getWhereClause())) {
            extractWhereColumnsFromExpression(update.getWhereClause(), analysis);
        }
    }

    private void extractFromDelete(DeleteStatement delete, QueryAnalysis analysis) {
        // Extract table
        String tableName = extractTableName(delete.getTable().getTableName().getName());
        analysis.tables.add(tableName);

        // Extract columns from WHERE clause only if WHERE contains parameters
        if (delete.getWhereClause() != null && containsParameters(delete.getWhereClause())) {
            extractWhereColumnsFromExpression(delete.getWhereClause(), analysis);
        }
    }

    private void extractFromCreateTable(CreateTableStatement create, QueryAnalysis analysis) {
        // Extract table
        String tableName = extractTableName(create.getTableName().getName());
        analysis.tables.add(tableName);

        // Extract columns
        for (ColumnDefinition column : create.getColumns()) {
            analysis.columns.add(new ColumnInfo(tableName, column.getColumnName().getName()));
        }
    }
}
