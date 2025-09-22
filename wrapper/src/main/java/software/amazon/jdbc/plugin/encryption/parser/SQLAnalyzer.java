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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.*;

public class SQLAnalyzer {

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
        public Set<String> tables = new HashSet<>();

        @Override
        public String toString() {
            return String.format("QueryAnalysis{queryType='%s', tables=%s, columns=%s}",
                queryType, tables, columns);
        }
    }

    public QueryAnalysis analyze(String sql) {
        QueryAnalysis analysis = new QueryAnalysis();

        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            
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
            } else {
                String className = statement.getClass().getSimpleName();
                if (className.contains("Create")) {
                    analysis.queryType = "CREATE";
                } else if (className.contains("Drop")) {
                    analysis.queryType = "DROP";
                } else {
                    analysis.queryType = "UNKNOWN";
                }
            }

        } catch (JSQLParserException e) {
            // Fallback to string parsing if JSqlParser fails
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

    private void extractFromSelect(Select select, QueryAnalysis analysis) {
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        
        // Extract table
        if (plainSelect.getFromItem() instanceof Table) {
            Table table = (Table) plainSelect.getFromItem();
            analysis.tables.add(table.getName());
        }
        
        // Extract columns
        for (SelectItem selectItem : plainSelect.getSelectItems()) {
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem item = (SelectExpressionItem) selectItem;
                if (item.getExpression() instanceof Column) {
                    Column column = (Column) item.getExpression();
                    String tableName = analysis.tables.isEmpty() ? "unknown" : analysis.tables.iterator().next();
                    analysis.columns.add(new ColumnInfo(tableName, column.getColumnName()));
                }
            }
        }
    }

    private void extractFromInsert(Insert insert, QueryAnalysis analysis) {
        // Extract table
        analysis.tables.add(insert.getTable().getName());
        
        // Extract columns
        if (insert.getColumns() != null) {
            for (Column column : insert.getColumns()) {
                String tableName = insert.getTable().getName();
                analysis.columns.add(new ColumnInfo(tableName, column.getColumnName()));
            }
        }
    }

    private void extractFromUpdate(Update update, QueryAnalysis analysis) {
        // Extract table
        analysis.tables.add(update.getTable().getName());
        
        // Extract columns from UPDATE SET expressions
        if (update.getUpdateSets() != null) {
            update.getUpdateSets().forEach(updateSet -> {
                updateSet.getColumns().forEach(column -> {
                    String tableName = update.getTable().getName();
                    analysis.columns.add(new ColumnInfo(tableName, column.getColumnName()));
                });
            });
        }
    }

    private void extractFromDelete(Delete delete, QueryAnalysis analysis) {
        // Extract table
        analysis.tables.add(delete.getTable().getName());
    }
}
