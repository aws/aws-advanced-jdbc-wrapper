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

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Audit LOGGER for KMS operations and encryption activities. Provides structured logging without
 * exposing sensitive data.
 */
public class AuditLogger {

  private static final Logger auditLogger = Logger.getLogger(AuditLogger.class.getName());

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
  }

  /** Clears audit context for the current thread. */
  public static void clearContext() {
    auditContext.get().clear();
  }

  /**
   * Logs KMS key creation operation.
   *
   * @param masterKeyArn Master key ARN
   * @param description Key description
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logKeyCreation(
      String masterKeyArn, String description, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "CREATE_MASTER_KEY");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "KMS master key created successfully - ARN: %s, Description: %s",
                    sanitizeArn(masterKeyArn), sanitizeDescription(description)));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "KMS master key creation failed - Description: %s, Error: %s",
                    sanitizeDescription(description), sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs data key generation operation.
   *
   * @param masterKeyArn Master key ARN
   * @param keyId Key ID
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logDataKeyGeneration(
      String masterKeyArn, String keyId, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "GENERATE_DATA_KEY");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "Data key generated successfully - Master Key: %s, Key ID: %s",
                    sanitizeArn(masterKeyArn), sanitizeKeyId(keyId)));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "()->String.format(Data key generation failed - Master Key: %s, Error: %s",
                    sanitizeArn(masterKeyArn), sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs data key decryption operation.
   *
   * @param masterKeyArn Master key ARN
   * @param keyId Key ID
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logDataKeyDecryption(
      String masterKeyArn, String keyId, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "DECRYPT_DATA_KEY");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "Data key decrypted successfully - Master Key: %s, Key ID: %s",
                    sanitizeArn(masterKeyArn), sanitizeKeyId(keyId)));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "Data key decryption failed - Master Key: %s, Key ID: %s, Error: %s",
                    sanitizeArn(masterKeyArn),
                    sanitizeKeyId(keyId),
                    sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs encryption operation.
   *
   * @param tableName Table name
   * @param columnName Column name
   * @param keyId Key ID
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logEncryption(
      String tableName, String columnName, String keyId, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "ENCRYPT_DATA");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "Data encrypted successfully - Table: %s, Column: %s, Key ID: %s",
                    sanitizeTableName(tableName),
                    sanitizeColumnName(columnName),
                    sanitizeKeyId(keyId)));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "Data encryption failed - Table: %s, Column: %s, Key ID: %s, Error: %s",
                    sanitizeTableName(tableName),
                    sanitizeColumnName(columnName),
                    sanitizeKeyId(keyId),
                    sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs decryption operation.
   *
   * @param tableName Table name
   * @param columnName Column name
   * @param keyId Key ID
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logDecryption(
      String tableName, String columnName, String keyId, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "DECRYPT_DATA");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "Data decrypted successfully - Table: %s, Column: %s, Key ID: %s",
                    sanitizeTableName(tableName),
                    sanitizeColumnName(columnName),
                    sanitizeKeyId(keyId)));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "Data decryption failed - Table: %s, Column: %s, Key ID: %s, Error: %s",
                    sanitizeTableName(tableName),
                    sanitizeColumnName(columnName),
                    sanitizeKeyId(keyId),
                    sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs metadata operations.
   *
   * @param operation Operation type
   * @param tableName Table name
   * @param columnName Column name
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logMetadataOperation(
      String operation, String tableName, String columnName, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "METADATA_" + operation.toUpperCase());
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "Metadata operation completed - Operation: %s, Table: %s, Column: %s",
                    operation, sanitizeTableName(tableName), sanitizeColumnName(columnName)));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "Metadata operation failed - Operation: %s, Table: %s, Column: %s, Error: %s",
                    operation,
                    sanitizeTableName(tableName),
                    sanitizeColumnName(columnName),
                    sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs configuration changes.
   *
   * @param configType Configuration type
   * @param details Configuration details
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logConfigurationChange(
      String configType, String details, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "CONFIG_CHANGE");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "Configuration changed successfully - Type: %s, Details: %s",
                    configType, sanitizeConfigDetails(details)));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "Configuration change failed - Type: %s, Details: %s, Error: %s",
                    configType,
                    sanitizeConfigDetails(details),
                    sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs connection parameter extraction operations.
   *
   * @param strategy Extraction strategy
   * @param connectionType Connection type
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   */
  public void logConnectionParameterExtraction(
      String strategy, String connectionType, boolean success, String errorMessage) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "CONNECTION_PARAMETER_EXTRACTION");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));
      setContext("strategy", strategy);
      setContext("connectionType", connectionType);

      if (success) {
        auditLogger.info(
            () ->
                String.format(
                    "Connection parameter extraction successful - Strategy: %s, Type: %s",
                    strategy, connectionType));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "Connection parameter extraction failed - Strategy: %s, Type: %s, Error: %s",
                    strategy, connectionType, sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs independent connection creation operations.
   *
   * @param jdbcUrl JDBC URL
   * @param success Whether operation succeeded
   * @param errorMessage Error message if failed
   * @param usedFallback Whether fallback was used
   */
  public void logIndependentConnectionCreation(
      String jdbcUrl, boolean success, String errorMessage, boolean usedFallback) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "INDEPENDENT_CONNECTION_CREATION");
      setContext("timestamp", Instant.now().toString());
      setContext("success", String.valueOf(success));
      setContext("usedFallback", String.valueOf(usedFallback));

      String sanitizedUrl = sanitizeJdbcUrl(jdbcUrl);

      if (success) {
        if (usedFallback) {
          auditLogger.warning(
              () ->
                  String.format(
                      "Independent connection created using fallback - URL: %s", sanitizedUrl));
        } else {
          auditLogger.info(
              () ->
                  String.format(
                      "Independent connection created successfully - URL: %s", sanitizedUrl));
        }
      } else {
        auditLogger.fine(
            () ->
                String.format(
                    "Independent connection creation failed - URL: %s, Error: %s",
                    sanitizedUrl, sanitizeErrorMessage(errorMessage)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs connection sharing fallback activation.
   *
   * @param reason Reason for fallback
   * @param originalFailure Original failure message
   * @param isActive Whether fallback is active
   */
  public void logConnectionSharingFallback(
      String reason, String originalFailure, boolean isActive) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "CONNECTION_SHARING_FALLBACK");
      setContext("timestamp", Instant.now().toString());
      setContext("isActive", String.valueOf(isActive));

      if (isActive) {
        auditLogger.fine(
            () ->
                String.format(
                    "CONNECTION SHARING FALLBACK ACTIVATED - Reason: %s, Original Failure: %s",
                    sanitizeErrorMessage(reason), sanitizeErrorMessage(originalFailure)));
        auditLogger.fine(
            () -> "WARNING: MetadataManager will share connections with client application!");
        auditLogger.fine(
            () ->
                "This may cause connection closure issues when MetadataManager operations complete.");
      } else {
        auditLogger.info(
            () ->
                String.format(
                    "Connection sharing fallback deactivated - Reason: %s",
                    sanitizeErrorMessage(reason)));
      }
    } finally {
      clearContext();
    }
  }

  /**
   * Logs connection health monitoring events.
   *
   * @param dataSourceType Data source type
   * @param isHealthy Whether connection is healthy
   * @param successCount Number of successful connections
   * @param failureCount Number of failed connections
   * @param successRate Success rate as decimal
   */
  public void logConnectionHealthCheck(
      String dataSourceType,
      boolean isHealthy,
      long successCount,
      long failureCount,
      double successRate) {
    if (!auditEnabled) {
      return;
    }

    try {
      setContext("operation", "CONNECTION_HEALTH_CHECK");
      setContext("timestamp", Instant.now().toString());
      setContext("dataSourceType", dataSourceType);
      setContext("isHealthy", String.valueOf(isHealthy));
      setContext("successCount", String.valueOf(successCount));
      setContext("failureCount", String.valueOf(failureCount));
      setContext("successRate", String.format("%.2f", successRate * 100));

      if (isHealthy) {
        auditLogger.info(
            () ->
                String.format(
                    "Connection health check passed - Type: %s, Success Rate: {:.2f}%, "
                        + "Successful: %s, Failed: %s",
                    dataSourceType, successRate * 100, successCount, failureCount));
      } else {
        auditLogger.warning(
            () ->
                String.format(
                    "Connection health check failed - Type: %s, Success Rate: {:.2f}%, "
                        + "Successful: %s, Failed: %s",
                    dataSourceType, successRate * 100, successCount, failureCount));
      }
    } finally {
      clearContext();
    }
  }

  // Sanitization methods to prevent sensitive data exposure

  private String sanitizeArn(String arn) {
    if (arn == null) {
      return "null";
    }
    // Keep only the key ID part of the ARN for audit purposes
    int lastSlash = arn.lastIndexOf('/');
    if (lastSlash != -1 && lastSlash < arn.length() - 1) {
      return "arn:aws:kms:***:***:key/" + arn.substring(lastSlash + 1);
    }
    return "arn:aws:kms:***:***:key/***";
  }

  private String sanitizeKeyId(String keyId) {
    if (keyId == null) {
      return "null";
    }
    // Show only first and last 4 characters of key ID
    if (keyId.length() > 8) {
      return keyId.substring(0, 4) + "***" + keyId.substring(keyId.length() - 4);
    }
    return "***";
  }

  private String sanitizeTableName(String tableName) {
    if (tableName == null) {
      return "null";
    }
    // Table names are generally not sensitive, but limit length
    return tableName.length() > 50 ? tableName.substring(0, 47) + "..." : tableName;
  }

  private String sanitizeColumnName(String columnName) {
    if (columnName == null) {
      return "null";
    }
    // Column names are generally not sensitive, but limit length
    return columnName.length() > 50 ? columnName.substring(0, 47) + "..." : columnName;
  }

  private String sanitizeDescription(String description) {
    if (description == null) {
      return "null";
    }
    // Limit description length and remove potential sensitive patterns
    String sanitized = description.replaceAll("(?i)(password|secret|key|token)=[^\\s]+", "$1=***");
    return sanitized.length() > 100 ? sanitized.substring(0, 97) + "..." : sanitized;
  }

  private String sanitizeErrorMessage(String errorMessage) {
    if (errorMessage == null) {
      return "null";
    }
    // Remove potential sensitive information from error messages
    String sanitized =
        errorMessage
            .replaceAll("(?i)(password|secret|key|token)=[^\\s]+", "$1=***")
            .replaceAll("arn:aws:kms:[^:]+:[^:]+:key/[a-f0-9-]+", "arn:aws:kms:***:***:key/***");
    return sanitized.length() > 200 ? sanitized.substring(0, 197) + "..." : sanitized;
  }

  private String sanitizeConfigDetails(String details) {
    if (details == null) {
      return "null";
    }
    // Remove sensitive configuration values
    String sanitized =
        details
            .replaceAll("(?i)(password|secret|key|token|credential)=[^\\s,}]+", "$1=***")
            .replaceAll("arn:aws:kms:[^:]+:[^:]+:key/[a-f0-9-]+", "arn:aws:kms:***:***:key/***");
    return sanitized.length() > 150 ? sanitized.substring(0, 147) + "..." : sanitized;
  }

  private String sanitizeJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null) {
      return "null";
    }

    // Remove password parameters from URL
    String sanitized =
        jdbcUrl
            .replaceAll("(?i)[?&]password=[^&]*", "?password=***")
            .replaceAll("(?i)[?&]pwd=[^&]*", "?pwd=***")
            .replaceAll("(?i)://[^:]+:[^@]+@", "://***:***@");

    return sanitized;
  }
}
