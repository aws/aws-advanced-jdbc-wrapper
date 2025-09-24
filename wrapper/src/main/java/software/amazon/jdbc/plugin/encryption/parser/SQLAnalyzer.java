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

import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.ParserException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private final DSLContext dsl = DSL.using(SQLDialect.POSTGRES);

    public QueryAnalysis analyze(String sql) {
        QueryAnalysis analysis = new QueryAnalysis();

        try {
            // Parse with jOOQ to validate SQL syntax
            Query query = dsl.parser().parseQuery(sql);
            
            // Extract query type from the SQL string
            String trimmedSql = sql.trim().toUpperCase();
            if (trimmedSql.startsWith("SELECT")) {
                analysis.queryType = "SELECT";
                extractTablesFromSelect(sql, analysis);
            } else if (trimmedSql.startsWith("INSERT")) {
                analysis.queryType = "INSERT";
                extractTablesFromInsert(sql, analysis);
            } else if (trimmedSql.startsWith("UPDATE")) {
                analysis.queryType = "UPDATE";
                extractTablesFromUpdate(sql, analysis);
            } else if (trimmedSql.startsWith("DELETE")) {
                analysis.queryType = "DELETE";
                extractTablesFromDelete(sql, analysis);
            } else if (trimmedSql.startsWith("CREATE")) {
                analysis.queryType = "CREATE";
            } else if (trimmedSql.startsWith("DROP")) {
                analysis.queryType = "DROP";
            } else {
                analysis.queryType = "UNKNOWN";
            }

        } catch (ParserException e) {
            analysis.queryType = "UNKNOWN";
        } catch (Exception e) {
            analysis.queryType = "UNKNOWN";
        }

        return analysis;
    }

    private void extractTablesFromSelect(String sql, QueryAnalysis analysis) {
        // Pattern to match FROM clause
        Pattern fromPattern = Pattern.compile("\\bFROM\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = fromPattern.matcher(sql);
        if (matcher.find()) {
            analysis.tables.add(matcher.group(1));
        }
    }

    private void extractTablesFromInsert(String sql, QueryAnalysis analysis) {
        // Pattern to match INSERT INTO table
        Pattern insertPattern = Pattern.compile("\\bINSERT\\s+INTO\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = insertPattern.matcher(sql);
        if (matcher.find()) {
            analysis.tables.add(matcher.group(1));
        }
    }

    private void extractTablesFromUpdate(String sql, QueryAnalysis analysis) {
        // Pattern to match UPDATE table
        Pattern updatePattern = Pattern.compile("\\bUPDATE\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = updatePattern.matcher(sql);
        if (matcher.find()) {
            analysis.tables.add(matcher.group(1));
        }
    }

    private void extractTablesFromDelete(String sql, QueryAnalysis analysis) {
        // Pattern to match DELETE FROM table
        Pattern deletePattern = Pattern.compile("\\bDELETE\\s+FROM\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = deletePattern.matcher(sql);
        if (matcher.find()) {
            analysis.tables.add(matcher.group(1));
        }
    }
}
