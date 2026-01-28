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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for building detailed error messages with context information. Helps provide clear
 * error messages that include table/column information without exposing sensitive data.
 */
public class ErrorContext {

  private final Map<String, Object> context = new HashMap<>();

  private ErrorContext() {}

  /**
   * Creates a new error context builder.
   *
   * @return New ErrorContext instance
   */
  public static ErrorContext builder() {
    return new ErrorContext();
  }

  /**
   * Adds table name to the error context.
   *
   * @param tableName Table name
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext table(String tableName) {
    context.put("table", tableName);
    return this;
  }

  /**
   * Adds column name to the error context.
   *
   * @param columnName Column name
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext column(String columnName) {
    context.put("column", columnName);
    return this;
  }

  /**
   * Adds operation type to the error context.
   *
   * @param operation Operation type
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext operation(String operation) {
    context.put("operation", operation);
    return this;
  }

  /**
   * Adds key ID to the error context.
   *
   * @param keyId Key ID
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext keyId(String keyId) {
    context.put("keyId", sanitizeKeyId(keyId));
    return this;
  }

  /**
   * Adds master key ARN to the error context.
   *
   * @param masterKeyArn Master key ARN
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext masterKeyArn(String masterKeyArn) {
    context.put("masterKeyArn", sanitizeArn(masterKeyArn));
    return this;
  }

  /**
   * Adds algorithm to the error context.
   *
   * @param algorithm Algorithm name
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext algorithm(String algorithm) {
    context.put("algorithm", algorithm);
    return this;
  }

  /**
   * Adds parameter index to the error context.
   *
   * @param parameterIndex Parameter index
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext parameterIndex(int parameterIndex) {
    context.put("parameterIndex", parameterIndex);
    return this;
  }

  /**
   * Adds column index to the error context.
   *
   * @param columnIndex Column index
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext columnIndex(int columnIndex) {
    context.put("columnIndex", columnIndex);
    return this;
  }

  /**
   * Adds SQL statement to the error context (sanitized).
   *
   * @param sql SQL statement
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext sql(String sql) {
    context.put("sql", sanitizeSql(sql));
    return this;
  }

  /**
   * Adds data type to the error context.
   *
   * @param dataType Data type
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext dataType(String dataType) {
    context.put("dataType", dataType);
    return this;
  }

  /**
   * Adds retry attempt information to the error context.
   *
   * @param attempt Current attempt number
   * @param maxAttempts Maximum number of attempts
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext retryAttempt(int attempt, int maxAttempts) {
    context.put("retryAttempt", attempt);
    context.put("maxRetryAttempts", maxAttempts);
    return this;
  }

  /**
   * Adds cache information to the error context.
   *
   * @param cacheType Type of cache
   * @param cacheHit Whether cache was hit
   * @return This ErrorContext instance for chaining
   */
  public ErrorContext cacheInfo(String cacheType, boolean cacheHit) {
    context.put("cacheType", cacheType);
    context.put("cacheHit", cacheHit);
    return this;
  }

  /**
   * Builds an error message with the provided base message and context.
   *
   * @param baseMessage Base error message
   * @return Formatted error message with context
   */
  public String buildMessage(String baseMessage) {
    if (context.isEmpty()) {
      return baseMessage;
    }

    StringBuilder sb = new StringBuilder(baseMessage);
    sb.append(" [Context: ");

    boolean first = true;
    for (Map.Entry<String, Object> entry : context.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
      first = false;
    }

    sb.append("]");
    return sb.toString();
  }

  /**
   * Builds an error message for encryption operations.
   *
   * @param baseMessage Base error message
   * @return Formatted encryption error message
   */
  public String buildEncryptionErrorMessage(String baseMessage) {
    StringBuilder sb = new StringBuilder("Encryption failed");

    if (baseMessage != null && !baseMessage.trim().isEmpty()) {
      sb.append(": ").append(baseMessage);
    }

    addContextualInfo(sb);
    return sb.toString();
  }

  /**
   * Builds an error message for decryption operations.
   *
   * @param baseMessage Base error message
   * @return Formatted decryption error message
   */
  public String buildDecryptionErrorMessage(String baseMessage) {
    StringBuilder sb = new StringBuilder("Decryption failed");

    if (baseMessage != null && !baseMessage.trim().isEmpty()) {
      sb.append(": ").append(baseMessage);
    }

    addContextualInfo(sb);
    return sb.toString();
  }

  /**
   * Builds an error message for key management operations.
   *
   * @param baseMessage Base error message
   * @return Formatted key management error message
   */
  public String buildKeyManagementErrorMessage(String baseMessage) {
    StringBuilder sb = new StringBuilder("Key management operation failed");

    if (baseMessage != null && !baseMessage.trim().isEmpty()) {
      sb.append(": ").append(baseMessage);
    }

    addContextualInfo(sb);
    return sb.toString();
  }

  /**
   * Builds an error message for metadata operations.
   *
   * @param baseMessage Base error message
   * @return Formatted metadata error message
   */
  public String buildMetadataErrorMessage(String baseMessage) {
    StringBuilder sb = new StringBuilder("Metadata operation failed");

    if (baseMessage != null && !baseMessage.trim().isEmpty()) {
      sb.append(": ").append(baseMessage);
    }

    addContextualInfo(sb);
    return sb.toString();
  }

  /**
   * Gets the context map for external use.
   *
   * @return Copy of the context map
   */
  public Map<String, Object> getContext() {
    return new HashMap<>(context);
  }

  /** Adds contextual information to the error message. */
  private void addContextualInfo(StringBuilder sb) {
    // Add table.column information if available
    String table = (String) context.get("table");
    String column = (String) context.get("column");

    if (table != null && column != null) {
      sb.append(" for column ").append(table).append(".").append(column);
    } else if (table != null) {
      sb.append(" for table ").append(table);
    } else if (column != null) {
      sb.append(" for column ").append(column);
    }

    // Add operation information if available
    String operation = (String) context.get("operation");
    if (operation != null) {
      sb.append(" during ").append(operation);
    }

    // Add parameter/column index information if available
    Integer paramIndex = (Integer) context.get("parameterIndex");
    Integer colIndex = (Integer) context.get("columnIndex");

    if (paramIndex != null) {
      sb.append(" (parameter index: ").append(paramIndex).append(")");
    } else if (colIndex != null) {
      sb.append(" (column index: ").append(colIndex).append(")");
    }

    // Add retry information if available
    Integer retryAttempt = (Integer) context.get("retryAttempt");
    Integer maxRetries = (Integer) context.get("maxRetryAttempts");

    if (retryAttempt != null && maxRetries != null) {
      sb.append(" (retry ").append(retryAttempt).append("/").append(maxRetries).append(")");
    }

    // Add additional context in brackets
    Map<String, Object> additionalContext = new HashMap<>();
    for (Map.Entry<String, Object> entry : context.entrySet()) {
      String key = entry.getKey();
      if (!key.equals("table")
          && !key.equals("column")
          && !key.equals("operation")
          && !key.equals("parameterIndex")
          && !key.equals("columnIndex")
          && !key.equals("retryAttempt")
          && !key.equals("maxRetryAttempts")) {
        additionalContext.put(key, entry.getValue());
      }
    }

    if (!additionalContext.isEmpty()) {
      sb.append(" [");
      boolean first = true;
      for (Map.Entry<String, Object> entry : additionalContext.entrySet()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(entry.getKey()).append("=").append(entry.getValue());
        first = false;
      }
      sb.append("]");
    }
  }

  // Sanitization methods

  private String sanitizeKeyId(String keyId) {
    if (keyId == null) {
      return null;
    }
    // Show only first and last 4 characters of key ID
    if (keyId.length() > 8) {
      return keyId.substring(0, 4) + "***" + keyId.substring(keyId.length() - 4);
    }
    return "***";
  }

  private String sanitizeArn(String arn) {
    if (arn == null) {
      return null;
    }
    // Keep only the key ID part of the ARN
    int lastSlash = arn.lastIndexOf('/');
    if (lastSlash != -1 && lastSlash < arn.length() - 1) {
      return "arn:aws:kms:***:***:key/" + arn.substring(lastSlash + 1);
    }
    return "arn:aws:kms:***:***:key/***";
  }

  private String sanitizeSql(String sql) {
    if (sql == null) {
      return null;
    }
    // Remove potential sensitive data from SQL and limit length
    String sanitized =
        sql.replaceAll("'[^']*'", "'***'") // Replace string literals
            .replaceAll("\\b\\d+\\b", "***"); // Replace numeric literals

    return sanitized.length() > 100 ? sanitized.substring(0, 97) + "..." : sanitized;
  }
}
