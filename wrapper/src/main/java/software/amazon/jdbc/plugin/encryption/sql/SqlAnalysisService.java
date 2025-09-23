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


package software.amazon.jdbc.sql;

import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.metadata.MetadataManager;
import software.amazon.jdbc.model.ColumnEncryptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service that analyzes SQL statements to identify columns that need encryption/decryption.
 * Uses PostgreSQLParser and SQLAnalyzer classes directly.
 */
public class SqlAnalysisService {
    
    private static final Logger logger = LoggerFactory.getLogger(SqlAnalysisService.class);
    
    private final MetadataManager metadataManager;
    private final Object parser;
    private final Object analyzer;
    
    // Pattern to extract table names from simple queries as fallback
    private static final Pattern TABLE_PATTERN = Pattern.compile(
        "(?i)(?:FROM|INTO|UPDATE|JOIN)\\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\\s+[a-zA-Z_][a-zA-Z0-9_]*)?", 
        Pattern.CASE_INSENSITIVE
    );
    
    public SqlAnalysisService(PluginService pluginService, MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        
        // Initialize parser and analyzer using reflection to avoid compile-time dependencies
        Object tempParser = null;
        Object tempAnalyzer = null;
        try {
            Class<?> parserClass = Class.forName("PostgreSQLParser");
            Class<?> analyzerClass = Class.forName("SQLAnalyzer");
            tempParser = parserClass.getDeclaredConstructor().newInstance();
            tempAnalyzer = analyzerClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            logger.warn("Could not initialize parser classes: {}", e.getMessage());
        }
        this.parser = tempParser;
        this.analyzer = tempAnalyzer;
    }
    
    /**
     * Analyzes a SQL statement to determine which columns need encryption/decryption.
     * Uses PostgreSQLParser and SQLAnalyzer for accurate parsing.
     * 
     * @param sql The SQL statement to analyze
     * @return Analysis result containing affected columns and their encryption configs
     */
    public SqlAnalysisResult analyzeSql(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return new SqlAnalysisResult(Collections.emptySet(), Collections.emptyMap());
        }
        
        try {
            if (analyzer != null) {
                // Use SQLAnalyzer to analyze the SQL
                Object queryAnalysis = analyzer.getClass().getMethod("analyze", String.class).invoke(analyzer, sql);
                
                if (queryAnalysis != null) {
                    Set<String> tables = extractTablesFromAnalysis(queryAnalysis);
                    return analyzeFromTables(tables);
                }
            }
            
            // Fallback to regex-based analysis
            logger.debug("Parser not available, using regex analysis for SQL: {}", sanitizeSql(sql));
            return analyzeWithRegex(sql);
            
        } catch (Exception e) {
            logger.warn("Error analyzing SQL with parser, using fallback: {}", e.getMessage());
            return analyzeWithRegex(sql);
        }
    }
    
    /**
     * Extracts table names from SQLAnalyzer QueryAnalysis result.
     */
    private Set<String> extractTablesFromAnalysis(Object queryAnalysis) {
        Set<String> tables = new HashSet<>();
        try {
            // Access the public tables field directly
            Object tablesField = queryAnalysis.getClass().getField("tables").get(queryAnalysis);
            if (tablesField instanceof Set) {
                Set<?> tableSet = (Set<?>) tablesField;
                for (Object table : tableSet) {
                    if (table != null) {
                        tables.add(table.toString().toLowerCase());
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Error extracting tables from analysis: {}", e.getMessage());
        }
        return tables;
    }
    
    /**
     * Analyzes SQL using the extracted table names from parser.
     */
    private SqlAnalysisResult analyzeFromTables(Set<String> tables) {
        Map<String, ColumnEncryptionConfig> encryptedColumns = new HashMap<>();
        
        // For each table, get encrypted columns
        for (String tableName : tables) {
            Map<String, ColumnEncryptionConfig> tableColumns = getEncryptedColumnsForTable(tableName);
            encryptedColumns.putAll(tableColumns);
        }
        
        logger.debug("Parser analysis found {} tables, {} encrypted columns", 
                    tables.size(), encryptedColumns.size());
        
        return new SqlAnalysisResult(tables, encryptedColumns);
    }
    
    /**
     * Fallback analysis using regex patterns when parser fails.
     */
    private SqlAnalysisResult analyzeWithRegex(String sql) {
        Set<String> affectedTables = new HashSet<>();
        Map<String, ColumnEncryptionConfig> encryptedColumns = new HashMap<>();
        
        // Extract table names using regex
        Matcher matcher = TABLE_PATTERN.matcher(sql);
        while (matcher.find()) {
            String tableName = matcher.group(1);
            if (tableName != null) {
                affectedTables.add(tableName.toLowerCase());
            }
        }
        
        // For each table, get encrypted columns
        for (String tableName : affectedTables) {
            Map<String, ColumnEncryptionConfig> tableColumns = getEncryptedColumnsForTable(tableName);
            encryptedColumns.putAll(tableColumns);
        }
        
        logger.debug("Regex fallback analysis found {} tables, {} encrypted columns", 
                    affectedTables.size(), encryptedColumns.size());
        
        return new SqlAnalysisResult(affectedTables, encryptedColumns);
    }
    
    /**
     * Gets all encrypted columns for a specific table.
     */
    private Map<String, ColumnEncryptionConfig> getEncryptedColumnsForTable(String tableName) {
        Map<String, ColumnEncryptionConfig> columns = new HashMap<>();
        
        try {
            // This would need to be implemented to query all columns for a table
            // For now, we'll check if specific columns are encrypted
            // In a real implementation, you'd query the metadata to get all columns for the table
            
            logger.debug("Checking encrypted columns for table: {}", tableName);
            
        } catch (Exception e) {
            logger.warn("Error getting encrypted columns for table {}: {}", tableName, e.getMessage());
        }
        
        return columns;
    }
    
    /**
     * Checks if a specific column is encrypted.
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
     * Sanitizes SQL for logging by removing sensitive data.
     */
    private String sanitizeSql(String sql) {
        if (sql == null) return "null";
        
        // Remove potential sensitive values in WHERE clauses
        return sql.replaceAll("'[^']*'", "'***'")
                  .replaceAll("= [^\\s]+", "= ***");
    }
    
    /**
     * Result of SQL analysis containing affected tables and encrypted columns.
     */
    public static class SqlAnalysisResult {
        private final Set<String> affectedTables;
        private final Map<String, ColumnEncryptionConfig> encryptedColumns;
        
        public SqlAnalysisResult(Set<String> affectedTables, Map<String, ColumnEncryptionConfig> encryptedColumns) {
            this.affectedTables = Collections.unmodifiableSet(new HashSet<>(affectedTables));
            this.encryptedColumns = Collections.unmodifiableMap(new HashMap<>(encryptedColumns));
        }
        
        public Set<String> getAffectedTables() {
            return affectedTables;
        }
        
        public Map<String, ColumnEncryptionConfig> getEncryptedColumns() {
            return encryptedColumns;
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
