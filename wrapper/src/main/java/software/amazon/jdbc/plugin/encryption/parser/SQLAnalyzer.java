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

            for (PostgreSQLParser.ParseNode node : parseTree) {
                analysis.queryType = node.type;
                analyzeNode(node, analysis);

                // For INSERT, extract table from the flat structure
                if ("INSERT".equals(node.type)) {
                    extractInsertTable(parseTree, analysis);
                }
            }

        } catch (Exception e) {
            analysis.queryType = "UNKNOWN";
        }

        return analysis;
    }

    private void extractInsertTable(List<PostgreSQLParser.ParseNode> parseTree, QueryAnalysis analysis) {
        // Look for pattern: INSERT, INTO, table_name
        for (int i = 0; i < parseTree.size() - 2; i++) {
            if ("INSERT".equals(parseTree.get(i).type) &&
                "UNKNOWN".equals(parseTree.get(i + 1).type) && "INTO".equals(parseTree.get(i + 1).value) &&
                "UNKNOWN".equals(parseTree.get(i + 2).type)) {
                analysis.tables.add(parseTree.get(i + 2).value);
                break;
            }
        }
    }

    private void analyzeNode(PostgreSQLParser.ParseNode node, QueryAnalysis analysis) {
        if (node == null) return;

        if ("TABLE_REF".equals(node.type)) {
            analysis.tables.add(node.value);
        }

        if ("SELECT".equals(node.type)) {
            extractSelectInfo(node, analysis);
        } else if ("INSERT".equals(node.type)) {
            extractInsertInfo(node, analysis);
        } else if ("UPDATE".equals(node.type)) {
            extractUpdateInfo(node, analysis);
        } else if ("DELETE".equals(node.type)) {
            extractDeleteInfo(node, analysis);
        }

        for (PostgreSQLParser.ParseNode child : node.children) {
            analyzeNode(child, analysis);
        }
    }

    private void extractSelectInfo(PostgreSQLParser.ParseNode selectNode, QueryAnalysis analysis) {
        String tableName = null;
        List<String> columnNames = new ArrayList<>();

        // First pass: find table name
        for (PostgreSQLParser.ParseNode child : selectNode.children) {
            if ("FROM".equals(child.type)) {
                tableName = findTableName(child);
                break;
            }
        }

        // Second pass: find columns
        for (PostgreSQLParser.ParseNode child : selectNode.children) {
            if ("SELECT_LIST".equals(child.type)) {
                extractColumns(child, columnNames);
                break;
            }
        }

        // Combine table and columns
        String finalTableName = tableName != null ? tableName : "unknown";
        for (String columnName : columnNames) {
            analysis.columns.add(new ColumnInfo(finalTableName, columnName));
        }
    }

    private String findTableName(PostgreSQLParser.ParseNode fromNode) {
        for (PostgreSQLParser.ParseNode child : fromNode.children) {
            if ("TABLE_REF".equals(child.type)) {
                return child.value;
            }
        }
        return null;
    }

    private void extractColumns(PostgreSQLParser.ParseNode selectListNode, List<String> columnNames) {
        for (PostgreSQLParser.ParseNode child : selectListNode.children) {
            if ("EXPRESSION".equals(child.type) && !"*".equals(child.value)) {
                columnNames.add(child.value);
            }
        }
    }

    private void extractInsertInfo(PostgreSQLParser.ParseNode insertNode, QueryAnalysis analysis) {
        String tableName = null;

        // Find table name and columns from children
        for (PostgreSQLParser.ParseNode child : insertNode.children) {
            if ("TABLE_REF".equals(child.type)) {
                tableName = child.value;
            } else if ("COLUMN_LIST".equals(child.type)) {
                // Extract columns from COLUMN_LIST
                for (PostgreSQLParser.ParseNode colNode : child.children) {
                    if ("COLUMN".equals(colNode.type)) {
                        analysis.columns.add(new ColumnInfo(tableName != null ? tableName : "unknown", colNode.value));
                    }
                }
            }
        }
    }

    private void extractUpdateInfo(PostgreSQLParser.ParseNode updateNode, QueryAnalysis analysis) {
        String tableName = null;

        // Find table name and columns from children
        for (PostgreSQLParser.ParseNode child : updateNode.children) {
            if ("TABLE_REF".equals(child.type)) {
                tableName = child.value;
            } else if ("SET_CLAUSE".equals(child.type)) {
                // Extract columns from SET_CLAUSE
                for (PostgreSQLParser.ParseNode colNode : child.children) {
                    if ("COLUMN".equals(colNode.type)) {
                        analysis.columns.add(new ColumnInfo(tableName != null ? tableName : "unknown", colNode.value));
                    }
                }
            }
        }
    }

    private void extractDeleteInfo(PostgreSQLParser.ParseNode deleteNode, QueryAnalysis analysis) {
        String tableName = null;

        // Find table name from children
        for (PostgreSQLParser.ParseNode child : deleteNode.children) {
            if ("TABLE_REF".equals(child.type)) {
                tableName = child.value;
                break;
            }
        }

        // DELETE doesn't typically have columns to extract (it deletes entire rows)
        // We could potentially extract WHERE clause columns, but that's complex
    }
}
