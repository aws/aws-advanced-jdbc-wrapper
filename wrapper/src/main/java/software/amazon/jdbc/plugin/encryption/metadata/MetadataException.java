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

package software.amazon.jdbc.plugin.encryption.metadata;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when metadata operations fail, such as loading encryption configuration from
 * database or cache operations. Provides enhanced error context information for better
 * troubleshooting.
 */
public class MetadataException extends SQLException {

  private static final long serialVersionUID = 1L;

  // SQL State codes for different metadata error types
  public static final String METADATA_LOAD_FAILED_STATE = "META01";
  public static final String METADATA_CACHE_FAILED_STATE = "META02";
  public static final String METADATA_REFRESH_FAILED_STATE = "META03";
  public static final String METADATA_LOOKUP_FAILED_STATE = "META04";
  public static final String METADATA_VALIDATION_FAILED_STATE = "META05";

  private final Map<String, Object> errorContext = new HashMap<>();

  /**
   * Constructs a MetadataException with the specified detail message.
   *
   * @param message the detail message
   */
  public MetadataException(String message) {
    super(message, METADATA_LOOKUP_FAILED_STATE);
  }

  /**
   * Constructs a MetadataException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of this exception
   */
  public MetadataException(String message, Throwable cause) {
    super(message, METADATA_LOOKUP_FAILED_STATE, cause);
  }

  /**
   * Constructs a MetadataException with the specified cause.
   *
   * @param cause the cause of this exception
   */
  public MetadataException(Throwable cause) {
    super(cause.getMessage(), METADATA_LOOKUP_FAILED_STATE, cause);
  }

  /**
   * Constructs a MetadataException with the specified detail message, cause, SQL state, and vendor
   * code.
   *
   * @param message the detail message
   * @param sqlState the SQL state
   * @param vendorCode the vendor-specific error code
   * @param cause the cause of this exception
   */
  public MetadataException(String message, String sqlState, int vendorCode, Throwable cause) {
    super(message, sqlState, vendorCode, cause);
  }

  /**
   * Constructs a MetadataException with the specified detail message, SQL state and cause.
   *
   * @param message the detail message
   * @param sqlState the SQL state
   * @param cause the cause of this exception
   */
  public MetadataException(String message, String sqlState, Throwable cause) {
    super(message, sqlState, cause);
  }

  /**
   * Adds context information to the exception.
   *
   * @param key the context key
   * @param value the context value
   * @return this exception for method chaining
   */
  public MetadataException withContext(String key, Object value) {
    errorContext.put(key, value);
    return this;
  }

  /**
   * Adds table name to the error context.
   *
   * @param tableName the table name
   * @return this exception for method chaining
   */
  public MetadataException withTable(String tableName) {
    return withContext("table", tableName);
  }

  /**
   * Adds column name to the error context.
   *
   * @param columnName the column name
   * @return this exception for method chaining
   */
  public MetadataException withColumn(String columnName) {
    return withContext("column", columnName);
  }

  /**
   * Adds operation type to the error context.
   *
   * @param operation the operation being performed
   * @return this exception for method chaining
   */
  public MetadataException withOperation(String operation) {
    return withContext("operation", operation);
  }

  /**
   * Adds cache information to the error context.
   *
   * @param cacheSize the current cache size
   * @param cacheHit whether this was a cache hit or miss
   * @return this exception for method chaining
   */
  public MetadataException withCacheInfo(int cacheSize, boolean cacheHit) {
    return withContext("cacheSize", cacheSize).withContext("cacheHit", cacheHit);
  }

  /**
   * Adds SQL query information to the error context (sanitized).
   *
   * @param sql the SQL query
   * @return this exception for method chaining
   */
  public MetadataException withSql(String sql) {
    return withContext("sql", sanitizeSql(sql));
  }

  /**
   * Gets the error context map.
   *
   * @return a copy of the error context
   */
  public Map<String, Object> getErrorContext() {
    return new HashMap<>(errorContext);
  }

  /**
   * Gets a formatted error message including context information.
   *
   * @return formatted error message with context
   */
  public String getDetailedMessage() {
    if (errorContext.isEmpty()) {
      return getMessage();
    }

    StringBuilder sb = new StringBuilder(getMessage());
    sb.append(" [Context: ");

    boolean first = true;
    for (Map.Entry<String, Object> entry : errorContext.entrySet()) {
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
   * Creates a MetadataException for metadata loading failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return New MetadataException instance
   */
  public static MetadataException loadFailed(String message, Throwable cause) {
    return new MetadataException(message, METADATA_LOAD_FAILED_STATE, cause);
  }

  /**
   * Creates a MetadataException for cache operation failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return New MetadataException instance
   */
  public static MetadataException cacheFailed(String message, Throwable cause) {
    return new MetadataException(message, METADATA_CACHE_FAILED_STATE, cause);
  }

  /**
   * Creates a MetadataException for metadata refresh failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return New MetadataException instance
   */
  public static MetadataException refreshFailed(String message, Throwable cause) {
    return new MetadataException(message, METADATA_REFRESH_FAILED_STATE, cause);
  }

  /**
   * Creates a MetadataException for metadata lookup failures.
   *
   * @param tableName Table name
   * @param columnName Column name
   * @param cause Root cause
   * @return New MetadataException instance
   */
  public static MetadataException lookupFailed(
      String tableName, String columnName, Throwable cause) {
    return new MetadataException("Failed to lookup metadata", METADATA_LOOKUP_FAILED_STATE, cause)
        .withTable(tableName)
        .withColumn(columnName);
  }

  /**
   * Creates a MetadataException for metadata validation failures.
   *
   * @param message Error message
   * @return New MetadataException instance
   */
  public static MetadataException validationFailed(String message) {
    return new MetadataException(message, METADATA_VALIDATION_FAILED_STATE, null);
  }

  // Sanitization methods

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
