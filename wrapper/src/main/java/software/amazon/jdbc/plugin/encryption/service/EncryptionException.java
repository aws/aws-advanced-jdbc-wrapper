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

package software.amazon.jdbc.plugin.encryption.service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when encryption or decryption operations fail. Extends SQLException to integrate
 * with JDBC error handling. Provides enhanced error context information for better troubleshooting.
 */
public class EncryptionException extends SQLException {

  private static final long serialVersionUID = 1L;

  // SQL State codes for different encryption error types
  public static final String ENCRYPTION_FAILED_STATE = "ENC01";
  public static final String DECRYPTION_FAILED_STATE = "ENC02";
  public static final String INVALID_ALGORITHM_STATE = "ENC03";
  public static final String INVALID_KEY_STATE = "ENC04";
  public static final String TYPE_CONVERSION_FAILED_STATE = "ENC05";

  private final Map<String, Object> errorContext = new HashMap<>();

  /**
   * Constructs an EncryptionException with the specified detail message.
   *
   * @param message the detail message
   */
  public EncryptionException(String message) {
    super(message, ENCRYPTION_FAILED_STATE);
  }

  /**
   * Constructs an EncryptionException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of this exception
   */
  public EncryptionException(String message, Throwable cause) {
    super(message, ENCRYPTION_FAILED_STATE, cause);
  }

  /**
   * Constructs an EncryptionException with the specified detail message, SQL state and cause.
   *
   * @param message the detail message
   * @param sqlState the SQL state
   * @param cause the cause of this exception
   */
  public EncryptionException(String message, String sqlState, Throwable cause) {
    super(message, sqlState, cause);
  }

  /**
   * Constructs an EncryptionException with the specified cause.
   *
   * @param cause the cause of this exception
   */
  public EncryptionException(Throwable cause) {
    super(cause.getMessage(), ENCRYPTION_FAILED_STATE, cause);
  }

  /**
   * Adds context information to the exception.
   *
   * @param key the context key
   * @param value the context value
   * @return this exception for method chaining
   */
  public EncryptionException withContext(String key, Object value) {
    errorContext.put(key, value);
    return this;
  }

  /**
   * Adds table name to the error context.
   *
   * @param tableName the table name
   * @return this exception for method chaining
   */
  public EncryptionException withTable(String tableName) {
    return withContext("table", tableName);
  }

  /**
   * Adds column name to the error context.
   *
   * @param columnName the column name
   * @return this exception for method chaining
   */
  public EncryptionException withColumn(String columnName) {
    return withContext("column", columnName);
  }

  /**
   * Adds algorithm to the error context.
   *
   * @param algorithm the encryption algorithm
   * @return this exception for method chaining
   */
  public EncryptionException withAlgorithm(String algorithm) {
    return withContext("algorithm", algorithm);
  }

  /**
   * Adds data type to the error context.
   *
   * @param dataType the data type being processed
   * @return this exception for method chaining
   */
  public EncryptionException withDataType(String dataType) {
    return withContext("dataType", dataType);
  }

  /**
   * Adds operation type to the error context.
   *
   * @param operation the operation being performed
   * @return this exception for method chaining
   */
  public EncryptionException withOperation(String operation) {
    return withContext("operation", operation);
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
   * Creates an EncryptionException for encryption failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return New EncryptionException instance
   */
  public static EncryptionException encryptionFailed(String message, Throwable cause) {
    return new EncryptionException(message, ENCRYPTION_FAILED_STATE, cause);
  }

  /**
   * Creates an EncryptionException for decryption failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return New EncryptionException instance
   */
  public static EncryptionException decryptionFailed(String message, Throwable cause) {
    return new EncryptionException(message, DECRYPTION_FAILED_STATE, cause);
  }

  /**
   * Creates an EncryptionException for invalid algorithm errors.
   *
   * @param algorithm Invalid algorithm name
   * @return New EncryptionException instance
   */
  public static EncryptionException invalidAlgorithm(String algorithm) {
    return new EncryptionException(
            "Unsupported encryption algorithm: " + algorithm, INVALID_ALGORITHM_STATE, null)
        .withAlgorithm(algorithm);
  }

  /**
   * Creates an EncryptionException for invalid key errors.
   *
   * @param message Error message
   * @return New EncryptionException instance
   */
  public static EncryptionException invalidKey(String message) {
    return new EncryptionException(message, INVALID_KEY_STATE, null);
  }

  /**
   * Creates an EncryptionException for type conversion errors.
   *
   * @param fromType Source type
   * @param toType Target type
   * @param cause Root cause
   * @return New EncryptionException instance
   */
  public static EncryptionException typeConversionFailed(
      String fromType, String toType, Throwable cause) {
    return new EncryptionException(
            String.format("Cannot convert %s to %s", fromType, toType),
            TYPE_CONVERSION_FAILED_STATE,
            cause)
        .withContext("fromType", fromType)
        .withContext("toType", toType);
  }
}
