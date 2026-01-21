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

package software.amazon.jdbc.plugin.encryption.sql;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.parser.DialectDetector;
import software.amazon.jdbc.plugin.encryption.parser.JSQLParserAnalyzer;

/**
 * Service that analyzes SQL statements to identify columns that need encryption/decryption. Uses
 * jOOQ parser via SQLAnalyzer class.
 */
public class SqlAnalysisService {

  private static final Logger LOGGER = Logger.getLogger(SqlAnalysisService.class.getName());

  private final MetadataManager metadataManager;

  public SqlAnalysisService(PluginService pluginService, MetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  /**
   * Analyzes a SQL statement to determine which columns need encryption/decryption.
   *
   * @param sql The SQL statement to analyze
   * @return Analysis result containing affected columns and their encryption configs
   */
  public static SqlAnalysisResult analyzeSql(String sql) {
    return analyzeSql(sql, null);
  }

  /**
   * Analyzes a SQL statement with dialect detection.
   *
   * @param sql The SQL statement to analyze
   * @param jdbcUrl JDBC URL for dialect detection (optional)
   * @return Analysis result containing affected columns and their encryption configs
   */
  public static SqlAnalysisResult analyzeSql(String sql, String jdbcUrl) {
    if (sql == null || sql.trim().isEmpty()) {
      return new SqlAnalysisResult(Collections.emptySet(), Collections.emptyMap(), "UNKNOWN");
    }

    try {
      // Detect dialect from JDBC URL
      JSQLParserAnalyzer.Dialect dialect = DialectDetector.detectFromUrl(jdbcUrl);
      
      // Use JSQLParser for analysis
      JSQLParserAnalyzer.QueryAnalysis queryAnalysis = 
          JSQLParserAnalyzer.analyze(sql, dialect);
      
      if (queryAnalysis != null) {
        Set<String> tables = extractTablesFromJSQLAnalysis(queryAnalysis);
        String queryType = extractQueryTypeFromJSQLAnalysis(queryAnalysis);
        return analyzeFromTables(tables, queryType);
      }
    } catch (Exception e) {
      LOGGER.severe(() -> String.format("Error analyzing SQL: %s", e.getMessage()));
      throw new RuntimeException("SQL analysis failed", e);
    }

    return new SqlAnalysisResult(Collections.emptySet(), Collections.emptyMap(), "UNKNOWN");
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
  private static String extractQueryTypeFromJSQLAnalysis(
      JSQLParserAnalyzer.QueryAnalysis queryAnalysis) {
    if (queryAnalysis != null) {
      return queryAnalysis.queryType != null ? queryAnalysis.queryType : "UNKNOWN";
    }
    return "UNKNOWN";
  }

  /** Analyzes SQL using the extracted table names from parser. */
  private static SqlAnalysisResult analyzeFromTables(Set<String> tables, String queryType) {
    Map<String, ColumnEncryptionConfig> encryptedColumns = new HashMap<>();

    LOGGER.finest(() -> String.format("Parser analysis found %s tables", tables.size()));

    return new SqlAnalysisResult(tables, encryptedColumns, queryType);
  }

  /** Result of SQL analysis containing affected tables and encrypted columns. */
  public static class SqlAnalysisResult {
    private final Set<String> affectedTables;
    private final Map<String, ColumnEncryptionConfig> encryptedColumns;
    private final String queryType;

    public SqlAnalysisResult(
        Set<String> affectedTables,
        Map<String, ColumnEncryptionConfig> encryptedColumns,
        String queryType) {
      this.affectedTables = Collections.unmodifiableSet(new HashSet<>(affectedTables));
      this.encryptedColumns = Collections.unmodifiableMap(new HashMap<>(encryptedColumns));
      this.queryType = queryType;
    }

    public Set<String> getAffectedTables() {
      return affectedTables;
    }

    public Map<String, ColumnEncryptionConfig> getEncryptedColumns() {
      return encryptedColumns;
    }

    public String getQueryType() {
      return queryType;
    }

    public boolean hasEncryptedColumns() {
      return !encryptedColumns.isEmpty();
    }

    public int getTableCount() {
      return affectedTables.size();
    }

    public int getEncryptedColumnCount() {
      return encryptedColumns.size();
    }

    @Override
    public String toString() {
      return String.format(
          "SqlAnalysisResult{tables=%d, encryptedColumns=%d}",
          getTableCount(), getEncryptedColumnCount());
    }
  }

  /** Gets column-to-parameter mapping for prepared statement parameters. */
  public Map<Integer, String> getColumnParameterMapping(String sql) {
    return getColumnParameterMapping(sql, null);
  }

  /** Gets column-to-parameter mapping with dialect detection. */
  public Map<Integer, String> getColumnParameterMapping(String sql, String jdbcUrl) {
    Map<Integer, String> mapping = new HashMap<>();

    try {
      // Detect dialect from JDBC URL
      JSQLParserAnalyzer.Dialect dialect = DialectDetector.detectFromUrl(jdbcUrl);
      
      // Use JSQLParser for analysis
      JSQLParserAnalyzer.QueryAnalysis queryAnalysis = 
          JSQLParserAnalyzer.analyze(sql, dialect);
      
      if (queryAnalysis != null) {
        // For SELECT statements, map parameters to WHERE clause columns
        if ("SELECT".equals(queryAnalysis.queryType)) {
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
          () -> String.format("Failed to get column parameter mapping for SQL: %s", sql));
    }

    return mapping;
  }

  /** Count the number of parameter placeholders (?) in SQL. */
  private int countParameters(String sql) {
    int count = 0;
    for (int i = 0; i < sql.length(); i++) {
      if (sql.charAt(i) == '?') {
        count++;
      }
    }
    return count;
  }
}
