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

import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.parser.SQLAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * Service that analyzes SQL statements to identify columns that need encryption/decryption.
 * Uses jOOQ parser via SQLAnalyzer class.
 */
public class SqlAnalysisService {

    private static final Logger logger = LoggerFactory.getLogger(SqlAnalysisService.class);

    private final MetadataManager metadataManager;
    private final SQLAnalyzer analyzer;

    public SqlAnalysisService(PluginService pluginService, MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        this.analyzer = new SQLAnalyzer();
    }

    /**
     * Analyzes a SQL statement to determine which columns need encryption/decryption.
     *
     * @param sql The SQL statement to analyze
     * @return Analysis result containing affected columns and their encryption configs
     */
    public SqlAnalysisResult analyzeSql(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return new SqlAnalysisResult(Collections.emptySet(), Collections.emptyMap(), "UNKNOWN");
        }

        try {
            SQLAnalyzer.QueryAnalysis queryAnalysis = analyzer.analyze(sql);
            if (queryAnalysis != null) {
                Set<String> tables = extractTablesFromAnalysis(queryAnalysis);
                String queryType = extractQueryTypeFromAnalysis(queryAnalysis);
                return analyzeFromTables(tables, queryType);
            }
        } catch (Exception e) {
            logger.error("Error analyzing SQL: {}", e.getMessage(), e);
            throw new RuntimeException("SQL analysis failed", e);
        }

        return new SqlAnalysisResult(Collections.emptySet(), Collections.emptyMap(), "UNKNOWN");
    }

    /**
     * Extracts table names from SQLAnalyzer QueryAnalysis result.
     */
    private Set<String> extractTablesFromAnalysis(SQLAnalyzer.QueryAnalysis queryAnalysis) {
        Set<String> tables = new HashSet<>();
        if (queryAnalysis != null) {
            tables.addAll(queryAnalysis.tables);
        }
        return tables;
    }

    /**
     * Extracts query type from SQLAnalyzer QueryAnalysis result.
     */
    private String extractQueryTypeFromAnalysis(SQLAnalyzer.QueryAnalysis queryAnalysis) {
        if (queryAnalysis != null) {
            return queryAnalysis.queryType != null ? queryAnalysis.queryType : "UNKNOWN";
        }
        return "UNKNOWN";
    }

    /**
     * Analyzes SQL using the extracted table names from parser.
     */
    private SqlAnalysisResult analyzeFromTables(Set<String> tables, String queryType) {
        Map<String, ColumnEncryptionConfig> encryptedColumns = new HashMap<>();

        logger.debug("Parser analysis found {} tables", tables.size());

        return new SqlAnalysisResult(tables, encryptedColumns, queryType);
    }

    /**
     * Checks if a specific column is encrypted.
     * 
     * @param tableName Table name
     * @param columnName Column name
     * @return True if column is encrypted, false otherwise
     */
    public boolean isColumnEncrypted(String tableName, String columnName) {
        try {
            return metadataManager.isColumnEncrypted(tableName, columnName);
        } catch (SQLException e) {
            logger.warn("Error checking if column is encrypted: {}.{}", tableName, columnName, e);
            return false;
        }
    }

    /**
     * Gets the encryption configuration for a specific column.
     *
     * @param tableName Table name
     * @param columnName Column name
     * @return Column encryption configuration, or null if not found
     */
    public ColumnEncryptionConfig getColumnConfig(String tableName, String columnName) {
        try {
            return metadataManager.getColumnConfig(tableName, columnName);
        } catch (SQLException e) {
            logger.warn("Error getting column config: {}.{}", tableName, columnName, e);
            return null;
        }
    }

    /**
     * Result of SQL analysis containing affected tables and encrypted columns.
     */
    public static class SqlAnalysisResult {
        private final Set<String> affectedTables;
        private final Map<String, ColumnEncryptionConfig> encryptedColumns;
        private final String queryType;

        public SqlAnalysisResult(Set<String> affectedTables, Map<String, ColumnEncryptionConfig> encryptedColumns, String queryType) {
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
            return String.format("SqlAnalysisResult{tables=%d, encryptedColumns=%d}",
                               getTableCount(), getEncryptedColumnCount());
        }
    }
}
