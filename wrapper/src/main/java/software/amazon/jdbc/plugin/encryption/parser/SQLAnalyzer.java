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

    private PostgreSQLParser parser = new PostgreSQLParser();

    public QueryAnalysis analyze(String sql) {
        QueryAnalysis analysis = new QueryAnalysis();

        try {
            List<PostgreSQLParser.ParseNode> parseTree = parser.rawParser(sql, PostgreSQLParser.RawParseMode.DEFAULT);

            if (!parseTree.isEmpty()) {
                // Get query type from first node
                analysis.queryType = parseTree.get(0).type;
                
                // Handle both single-node and multi-node parse trees
                if (parseTree.size() == 1) {
                    // Simple query - use original logic with children
                    analyzeNode(parseTree.get(0), analysis);
                } else {
                    // Complex query - use flat parse tree logic
                    if ("SELECT".equals(analysis.queryType)) {
                        extractSelectInfo(parseTree, analysis);
                    } else if ("INSERT".equals(analysis.queryType)) {
                        extractInsertInfo(parseTree, analysis);
                    } else if ("UPDATE".equals(analysis.queryType)) {
                        extractUpdateInfo(parseTree, analysis);
                    } else if ("DELETE".equals(analysis.queryType)) {
                        extractDeleteInfo(parseTree, analysis);
                    }
                }
            }

        } catch (Exception e) {
            analysis.queryType = "UNKNOWN";
        }

        return analysis;
    }

    private void analyzeNode(PostgreSQLParser.ParseNode node, QueryAnalysis analysis) {
        if (node == null) return;

        if ("TABLE_REF".equals(node.type)) {
            analysis.tables.add(node.value);
        }

        for (PostgreSQLParser.ParseNode child : node.children) {
            analyzeNode(child, analysis);
        }
    }

    private void extractSelectInfo(List<PostgreSQLParser.ParseNode> parseTree, QueryAnalysis analysis) {
        // Look for FROM keyword followed by table names
        for (int i = 0; i < parseTree.size() - 1; i++) {
            if ("UNKNOWN".equals(parseTree.get(i).type) && "FROM".equals(parseTree.get(i).value)) {
                // Next non-keyword token should be a table name
                for (int j = i + 1; j < parseTree.size(); j++) {
                    String value = parseTree.get(j).value;
                    if ("UNKNOWN".equals(parseTree.get(j).type) && 
                        !isKeyword(value) && !value.matches("[.,=()]")) {
                        analysis.tables.add(value);
                        break; // Get first table after FROM
                    }
                }
                break;
            }
        }
        
        // Extract column references (simplified)
        for (int i = 1; i < parseTree.size(); i++) {
            if ("UNKNOWN".equals(parseTree.get(i).type) && ".".equals(parseTree.get(i).value) &&
                i > 0 && i < parseTree.size() - 1) {
                String table = parseTree.get(i - 1).value;
                String column = parseTree.get(i + 1).value;
                analysis.columns.add(new ColumnInfo(table, column));
            }
        }
    }

    private void extractInsertInfo(List<PostgreSQLParser.ParseNode> parseTree, QueryAnalysis analysis) {
        // Look for pattern: INSERT, INTO, table_name
        for (int i = 0; i < parseTree.size() - 2; i++) {
            if ("INSERT".equals(parseTree.get(i).type) &&
                "UNKNOWN".equals(parseTree.get(i + 1).type) && "INTO".equals(parseTree.get(i + 1).value) &&
                "UNKNOWN".equals(parseTree.get(i + 2).type)) {
                analysis.tables.add(parseTree.get(i + 2).value);
                break;
            }
        }
        
        // Extract column names from INSERT
        boolean inColumnList = false;
        for (int i = 0; i < parseTree.size(); i++) {
            String value = parseTree.get(i).value;
            if ("(".equals(value)) {
                inColumnList = true;
            } else if (")".equals(value)) {
                inColumnList = false;
            } else if (inColumnList && "UNKNOWN".equals(parseTree.get(i).type) && 
                      !",".equals(value) && !analysis.tables.isEmpty()) {
                analysis.columns.add(new ColumnInfo(analysis.tables.iterator().next(), value));
            }
        }
    }

    private void extractUpdateInfo(List<PostgreSQLParser.ParseNode> parseTree, QueryAnalysis analysis) {
        // Look for UPDATE table_name
        for (int i = 0; i < parseTree.size() - 1; i++) {
            if ("UPDATE".equals(parseTree.get(i).type) &&
                "UNKNOWN".equals(parseTree.get(i + 1).type)) {
                analysis.tables.add(parseTree.get(i + 1).value);
                break;
            }
        }
        
        // Extract SET columns
        boolean inSetClause = false;
        for (int i = 0; i < parseTree.size(); i++) {
            String value = parseTree.get(i).value;
            if ("SET".equals(value)) {
                inSetClause = true;
            } else if ("WHERE".equals(value)) {
                inSetClause = false;
            } else if (inSetClause && "UNKNOWN".equals(parseTree.get(i).type) && 
                      !isKeyword(value) && !analysis.tables.isEmpty()) {
                analysis.columns.add(new ColumnInfo(analysis.tables.iterator().next(), value));
            }
        }
    }

    private void extractDeleteInfo(List<PostgreSQLParser.ParseNode> parseTree, QueryAnalysis analysis) {
        // Look for DELETE FROM table_name
        for (int i = 0; i < parseTree.size() - 2; i++) {
            if ("DELETE".equals(parseTree.get(i).type) &&
                "UNKNOWN".equals(parseTree.get(i + 1).type) && "FROM".equals(parseTree.get(i + 1).value) &&
                "UNKNOWN".equals(parseTree.get(i + 2).type)) {
                analysis.tables.add(parseTree.get(i + 2).value);
                break;
            }
        }
    }

    private boolean isKeyword(String value) {
        return value != null && value.matches("(?i)(SELECT|FROM|WHERE|JOIN|ON|SET|VALUES|INTO|UPDATE|DELETE|INSERT|AND|OR|ORDER|BY|GROUP|HAVING|LIMIT)");
    }
}
