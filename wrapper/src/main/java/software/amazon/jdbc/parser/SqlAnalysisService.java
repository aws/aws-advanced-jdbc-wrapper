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

package software.amazon.jdbc.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.util.Messages;

/**
 * Stateless utility for analyzing SQL statements using JSQLParser.
 * Extracts query type, table names, and parameter-to-column mappings.
 */
public final class SqlAnalysisService {

  private static final Logger LOGGER = Logger.getLogger(SqlAnalysisService.class.getName());

  private SqlAnalysisService() {
  }

  /**
   * Analyzes a SQL statement.
   *
   * @param sql The SQL statement to analyze
   * @return Analysis result containing affected tables and query type
   */
  public static SqlAnalysisResult analyzeSql(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return new SqlAnalysisResult(Collections.emptySet(), QueryType.UNKNOWN);
    }

    try {
      // Use JSQLParser for analysis
      JSQLParserAnalyzer.QueryAnalysis queryAnalysis = JSQLParserAnalyzer.analyze(sql);

      if (queryAnalysis != null) {
        Set<String> tables = extractTablesFromJSQLAnalysis(queryAnalysis);
        QueryType queryType = extractQueryTypeFromJSQLAnalysis(queryAnalysis);
        return analyzeFromTables(tables, queryType);
      }
    } catch (Exception e) {
      LOGGER.severe(() -> Messages.get("SqlAnalysisService.errorAnalyzing", new Object[]{e.getMessage()}));
      throw new RuntimeException(Messages.get("SqlAnalysisService.analysisFailed"), e);
    }

    return new SqlAnalysisResult(Collections.emptySet(), QueryType.UNKNOWN);
  }

  /** Extracts table names from JSQLParserAnalyzer QueryAnalysis result. */
  private static Set<String> extractTablesFromJSQLAnalysis(
      JSQLParserAnalyzer.QueryAnalysis queryAnalysis) {
    Set<String> tables = new HashSet<>();
    if (queryAnalysis != null) {
      // Remove backticks from MySQL table names
      for (String table : queryAnalysis.tables) {
        tables.add(table.replace("`", ""));
      }
    }
    return tables;
  }

  /** Extracts query type from JSQLParserAnalyzer QueryAnalysis result. */
  private static QueryType extractQueryTypeFromJSQLAnalysis(
      JSQLParserAnalyzer.QueryAnalysis queryAnalysis) {
    if (queryAnalysis != null && queryAnalysis.queryType != null) {
      return queryAnalysis.queryType;
    }
    return QueryType.UNKNOWN;
  }

  /** Analyzes SQL using the extracted table names from parser. */
  private static SqlAnalysisResult analyzeFromTables(Set<String> tables, QueryType queryType) {
    LOGGER.finest(() -> Messages.get("SqlAnalysisService.parserFoundTables", new Object[]{tables.size()}));
    return new SqlAnalysisResult(tables, queryType);
  }

  /** Result of SQL analysis containing affected tables and query type. */
  public static class SqlAnalysisResult {
    private final Set<String> affectedTables;
    private final QueryType queryType;

    public SqlAnalysisResult(Set<String> affectedTables, QueryType queryType) {
      this.affectedTables = Collections.unmodifiableSet(new HashSet<>(affectedTables));
      this.queryType = queryType;
    }

    public Set<String> getAffectedTables() {
      return affectedTables;
    }

    public QueryType getQueryType() {
      return queryType;
    }

    @Override
    public String toString() {
      return String.format(
          "SqlAnalysisResult{tables=%s, queryType=%s}",
          affectedTables, queryType);
    }
  }

  /**
   * Gets column-to-parameter mapping for prepared statement parameters.
   *
   * @param sql the SQL statement to analyze
   * @return a map of parameter index to column name
   */
  public static Map<Integer, String> getColumnParameterMapping(String sql) {
    Map<Integer, String> mapping = new HashMap<>();

    try {
      // Use JSQLParser for analysis
      JSQLParserAnalyzer.QueryAnalysis queryAnalysis = JSQLParserAnalyzer.analyze(sql);

      if (queryAnalysis != null) {
        // For SELECT statements, map parameters to WHERE clause columns
        if (queryAnalysis.queryType == QueryType.SELECT) {
          for (int i = 0; i < queryAnalysis.whereColumns.size(); i++) {
            JSQLParserAnalyzer.ColumnInfo column = queryAnalysis.whereColumns.get(i);
            mapping.put(i + 1, column.columnName.replace("`", ""));
          }
        } else if (!queryAnalysis.columns.isEmpty()) {
          // For INSERT/UPDATE, map parameters to main columns in order
          int parameterIndex = 1;
          for (JSQLParserAnalyzer.ColumnInfo column : queryAnalysis.columns) {
            mapping.put(parameterIndex++, column.columnName.replace("`", ""));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warning(
          () -> Messages.get("SqlAnalysisService.failedColumnMapping", new Object[]{sql}));
    }

    return mapping;
  }
}
