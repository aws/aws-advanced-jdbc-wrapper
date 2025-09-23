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


package software.amazon.jdbc.plugin.encryption.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Audit logger for KMS operations and encryption activities.
 * Provides structured logging without exposing sensitive data.
 */
public class AuditLogger {
    
    private static final Logger auditLogger = LoggerFactory.getLogger("software.amazon.jdbc.audit");
    private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
    
    // Thread-local context for audit information
    private static final ThreadLocal<Map<String, String>> auditContext = 
        ThreadLocal.withInitial(ConcurrentHashMap::new);
    
    private final boolean auditEnabled;
    
    public AuditLogger(boolean auditEnabled) {
        this.auditEnabled = auditEnabled;
    }
    
    /**
     * Sets audit context information for the current thread.
     * 
     * @param key Context key
     * @param value Context value
     */
    public static void setContext(String key, String value) {
        auditContext.get().put(key, value);
        MDC.put(key, value);
    }
    
    /**
     * Clears audit context for the current thread.
     */
    public static void clearContext() {
        auditContext.get().clear();
        MDC.clear();
    }
    
    /**
     * Logs KMS key creation operation.
     * 
     * @param masterKeyArn Master key ARN
     * @param description Key description
     * @param success Whether operation succeeded
     * @param errorMessage Error message if failed
     */
    public void logKeyCreation(String masterKeyArn, String description, boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "CREATE_MASTER_KEY");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            
            if (success) {
                auditLogger.info("KMS master key created successfully - ARN: {}, Description: {}", 
                    sanitizeArn(masterKeyArn), sanitizeDescription(description));
            } else {
                auditLogger.warn("KMS master key creation failed - Description: {}, Error: {}", 
                    sanitizeDescription(description), sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs data key generation operation.
     */
    public void logDataKeyGeneration(String masterKeyArn, String keyId, boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "GENERATE_DATA_KEY");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            
            if (success) {
                auditLogger.info("Data key generated successfully - Master Key: {}, Key ID: {}", 
                    sanitizeArn(masterKeyArn), sanitizeKeyId(keyId));
            } else {
                auditLogger.warn("Data key generation failed - Master Key: {}, Error: {}", 
                    sanitizeArn(masterKeyArn), sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs data key decryption operation.
     */
    public void logDataKeyDecryption(String masterKeyArn, String keyId, boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "DECRYPT_DATA_KEY");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            
            if (success) {
                auditLogger.info("Data key decrypted successfully - Master Key: {}, Key ID: {}", 
                    sanitizeArn(masterKeyArn), sanitizeKeyId(keyId));
            } else {
                auditLogger.warn("Data key decryption failed - Master Key: {}, Key ID: {}, Error: {}", 
                    sanitizeArn(masterKeyArn), sanitizeKeyId(keyId), sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs encryption operation.
     */
    public void logEncryption(String tableName, String columnName, String keyId, boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "ENCRYPT_DATA");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            
            if (success) {
                auditLogger.info("Data encrypted successfully - Table: {}, Column: {}, Key ID: {}", 
                    sanitizeTableName(tableName), sanitizeColumnName(columnName), sanitizeKeyId(keyId));
            } else {
                auditLogger.warn("Data encryption failed - Table: {}, Column: {}, Key ID: {}, Error: {}", 
                    sanitizeTableName(tableName), sanitizeColumnName(columnName), 
                    sanitizeKeyId(keyId), sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs decryption operation.
     */
    public void logDecryption(String tableName, String columnName, String keyId, boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "DECRYPT_DATA");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            
            if (success) {
                auditLogger.info("Data decrypted successfully - Table: {}, Column: {}, Key ID: {}", 
                    sanitizeTableName(tableName), sanitizeColumnName(columnName), sanitizeKeyId(keyId));
            } else {
                auditLogger.warn("Data decryption failed - Table: {}, Column: {}, Key ID: {}, Error: {}", 
                    sanitizeTableName(tableName), sanitizeColumnName(columnName), 
                    sanitizeKeyId(keyId), sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs metadata operations.
     */
    public void logMetadataOperation(String operation, String tableName, String columnName, 
                                   boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "METADATA_" + operation.toUpperCase());
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            
            if (success) {
                auditLogger.info("Metadata operation completed - Operation: {}, Table: {}, Column: {}", 
                    operation, sanitizeTableName(tableName), sanitizeColumnName(columnName));
            } else {
                auditLogger.warn("Metadata operation failed - Operation: {}, Table: {}, Column: {}, Error: {}", 
                    operation, sanitizeTableName(tableName), sanitizeColumnName(columnName), 
                    sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs configuration changes.
     */
    public void logConfigurationChange(String configType, String details, boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "CONFIG_CHANGE");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            
            if (success) {
                auditLogger.info("Configuration changed successfully - Type: {}, Details: {}", 
                    configType, sanitizeConfigDetails(details));
            } else {
                auditLogger.warn("Configuration change failed - Type: {}, Details: {}, Error: {}", 
                    configType, sanitizeConfigDetails(details), sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs connection parameter extraction operations.
     */
    public void logConnectionParameterExtraction(String strategy, String connectionType, 
                                               boolean success, String errorMessage) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "CONNECTION_PARAMETER_EXTRACTION");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            setContext("strategy", strategy);
            setContext("connectionType", connectionType);
            
            if (success) {
                auditLogger.info("Connection parameter extraction successful - Strategy: {}, Type: {}", 
                    strategy, connectionType);
            } else {
                auditLogger.warn("Connection parameter extraction failed - Strategy: {}, Type: {}, Error: {}", 
                    strategy, connectionType, sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs independent connection creation operations.
     */
    public void logIndependentConnectionCreation(String jdbcUrl, boolean success, String errorMessage, 
                                               boolean usedFallback) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "INDEPENDENT_CONNECTION_CREATION");
            setContext("timestamp", Instant.now().toString());
            setContext("success", String.valueOf(success));
            setContext("usedFallback", String.valueOf(usedFallback));
            
            String sanitizedUrl = sanitizeJdbcUrl(jdbcUrl);
            
            if (success) {
                if (usedFallback) {
                    auditLogger.warn("Independent connection created using fallback - URL: {}", 
                        sanitizedUrl);
                } else {
                    auditLogger.info("Independent connection created successfully - URL: {}", 
                        sanitizedUrl);
                }
            } else {
                auditLogger.error("Independent connection creation failed - URL: {}, Error: {}", 
                    sanitizedUrl, sanitizeErrorMessage(errorMessage));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs connection sharing fallback activation.
     */
    public void logConnectionSharingFallback(String reason, String originalFailure, boolean isActive) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "CONNECTION_SHARING_FALLBACK");
            setContext("timestamp", Instant.now().toString());
            setContext("isActive", String.valueOf(isActive));
            
            if (isActive) {
                auditLogger.error("CONNECTION SHARING FALLBACK ACTIVATED - Reason: {}, Original Failure: {}", 
                    sanitizeErrorMessage(reason), sanitizeErrorMessage(originalFailure));
                auditLogger.error("WARNING: MetadataManager will share connections with client application!");
                auditLogger.error("This may cause connection closure issues when MetadataManager operations complete.");
            } else {
                auditLogger.info("Connection sharing fallback deactivated - Reason: {}", 
                    sanitizeErrorMessage(reason));
            }
        } finally {
            clearContext();
        }
    }
    
    /**
     * Logs connection health monitoring events.
     */
    public void logConnectionHealthCheck(String dataSourceType, boolean isHealthy, 
                                       long successCount, long failureCount, double successRate) {
        if (!auditEnabled) return;
        
        try {
            setContext("operation", "CONNECTION_HEALTH_CHECK");
            setContext("timestamp", Instant.now().toString());
            setContext("dataSourceType", dataSourceType);
            setContext("isHealthy", String.valueOf(isHealthy));
            setContext("successCount", String.valueOf(successCount));
            setContext("failureCount", String.valueOf(failureCount));
            setContext("successRate", String.format("%.2f", successRate * 100));
            
            if (isHealthy) {
                auditLogger.info("Connection health check passed - Type: {}, Success Rate: {:.2f}%, " +
                               "Successful: {}, Failed: {}", 
                               dataSourceType, successRate * 100, successCount, failureCount);
            } else {
                auditLogger.warn("Connection health check failed - Type: {}, Success Rate: {:.2f}%, " +
                               "Successful: {}, Failed: {}", 
                               dataSourceType, successRate * 100, successCount, failureCount);
            }
        } finally {
            clearContext();
        }
    }
    
    // Sanitization methods to prevent sensitive data exposure
    
    private String sanitizeArn(String arn) {
        if (arn == null) return "null";
        // Keep only the key ID part of the ARN for audit purposes
        int lastSlash = arn.lastIndexOf('/');
        if (lastSlash != -1 && lastSlash < arn.length() - 1) {
            return "arn:aws:kms:***:***:key/" + arn.substring(lastSlash + 1);
        }
        return "arn:aws:kms:***:***:key/***";
    }
    
    private String sanitizeKeyId(String keyId) {
        if (keyId == null) return "null";
        // Show only first and last 4 characters of key ID
        if (keyId.length() > 8) {
            return keyId.substring(0, 4) + "***" + keyId.substring(keyId.length() - 4);
        }
        return "***";
    }
    
    private String sanitizeTableName(String tableName) {
        if (tableName == null) return "null";
        // Table names are generally not sensitive, but limit length
        return tableName.length() > 50 ? tableName.substring(0, 47) + "..." : tableName;
    }
    
    private String sanitizeColumnName(String columnName) {
        if (columnName == null) return "null";
        // Column names are generally not sensitive, but limit length
        return columnName.length() > 50 ? columnName.substring(0, 47) + "..." : columnName;
    }
    
    private String sanitizeDescription(String description) {
        if (description == null) return "null";
        // Limit description length and remove potential sensitive patterns
        String sanitized = description.replaceAll("(?i)(password|secret|key|token)=[^\\s]+", "$1=***");
        return sanitized.length() > 100 ? sanitized.substring(0, 97) + "..." : sanitized;
    }
    
    private String sanitizeErrorMessage(String errorMessage) {
        if (errorMessage == null) return "null";
        // Remove potential sensitive information from error messages
        String sanitized = errorMessage
            .replaceAll("(?i)(password|secret|key|token)=[^\\s]+", "$1=***")
            .replaceAll("arn:aws:kms:[^:]+:[^:]+:key/[a-f0-9-]+", "arn:aws:kms:***:***:key/***");
        return sanitized.length() > 200 ? sanitized.substring(0, 197) + "..." : sanitized;
    }
    
    private String sanitizeConfigDetails(String details) {
        if (details == null) return "null";
        // Remove sensitive configuration values
        String sanitized = details
            .replaceAll("(?i)(password|secret|key|token|credential)=[^\\s,}]+", "$1=***")
            .replaceAll("arn:aws:kms:[^:]+:[^:]+:key/[a-f0-9-]+", "arn:aws:kms:***:***:key/***");
        return sanitized.length() > 150 ? sanitized.substring(0, 147) + "..." : sanitized;
    }
    
    private String sanitizeJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) return "null";
        
        // Remove password parameters from URL
        String sanitized = jdbcUrl.replaceAll("(?i)[?&]password=[^&]*", "?password=***")
                                 .replaceAll("(?i)[?&]pwd=[^&]*", "?pwd=***")
                                 .replaceAll("(?i)://[^:]+:[^@]+@", "://***:***@");
        
        return sanitized;
    }
}