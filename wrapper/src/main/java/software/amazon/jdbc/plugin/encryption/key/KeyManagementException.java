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

package software.amazon.jdbc.plugin.encryption.key;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when key management operations fail. Extends SQLException to integrate with JDBC
 * error handling. Provides enhanced error context information for better troubleshooting.
 */
public class KeyManagementException extends SQLException {

  private static final long serialVersionUID = 1L;

  // SQL State codes for different key management error types
  public static final String KEY_CREATION_FAILED_STATE = "KEY01";
  public static final String KEY_RETRIEVAL_FAILED_STATE = "KEY02";
  public static final String KEY_DECRYPTION_FAILED_STATE = "KEY03";
  public static final String KEY_STORAGE_FAILED_STATE = "KEY04";
  public static final String KMS_CONNECTION_FAILED_STATE = "KEY05";
  public static final String INVALID_KEY_METADATA_STATE = "KEY06";

  private final Map<String, Object> errorContext = new HashMap<>();

  /**
   * Constructs a KeyManagementException with the specified detail message.
   *
   * @param message the detail message
   */
  public KeyManagementException(String message) {
    super(message, KEY_RETRIEVAL_FAILED_STATE);
  }

  /**
   * Constructs a KeyManagementException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of this exception
   */
  public KeyManagementException(String message, Throwable cause) {
    super(message, KEY_RETRIEVAL_FAILED_STATE, cause);
  }

  /**
   * Constructs a KeyManagementException with the specified cause.
   *
   * @param cause the cause of this exception
   */
  public KeyManagementException(Throwable cause) {
    super(cause.getMessage(), KEY_RETRIEVAL_FAILED_STATE, cause);
  }

  /**
   * Constructs a KeyManagementException with the specified detail message, SQL state, and vendor
   * code.
   *
   * @param message the detail message
   * @param sqlState the SQL state
   * @param vendorCode the vendor-specific error code
   */
  public KeyManagementException(String message, String sqlState, int vendorCode) {
    super(message, sqlState, vendorCode);
  }

  /**
   * Constructs a KeyManagementException with the specified detail message, SQL state, vendor code,
   * and cause.
   *
   * @param message the detail message
   * @param sqlState the SQL state
   * @param vendorCode the vendor-specific error code
   * @param cause the cause of this exception
   */
  public KeyManagementException(String message, String sqlState, int vendorCode, Throwable cause) {
    super(message, sqlState, vendorCode, cause);
  }

  /**
   * Constructs a KeyManagementException with the specified detail message, SQL state and cause.
   *
   * @param message the detail message
   * @param sqlState the SQL state
   * @param cause the cause of this exception
   */
  public KeyManagementException(String message, String sqlState, Throwable cause) {
    super(message, sqlState, cause);
  }

  /**
   * Adds context information to the exception.
   *
   * @param key the context key
   * @param value the context value
   * @return this exception for method chaining
   */
  public KeyManagementException withContext(String key, Object value) {
    errorContext.put(key, value);
    return this;
  }

  /**
   * Adds key ID to the error context (sanitized).
   *
   * @param keyId the key ID
   * @return this exception for method chaining
   */
  public KeyManagementException withKeyId(String keyId) {
    return withContext("keyId", sanitizeKeyId(keyId));
  }

  /**
   * Adds master key ARN to the error context (sanitized).
   *
   * @param masterKeyArn the master key ARN
   * @return this exception for method chaining
   */
  public KeyManagementException withMasterKeyArn(String masterKeyArn) {
    return withContext("masterKeyArn", sanitizeArn(masterKeyArn));
  }

  /**
   * Adds operation type to the error context.
   *
   * @param operation the operation being performed
   * @return this exception for method chaining
   */
  public KeyManagementException withOperation(String operation) {
    return withContext("operation", operation);
  }

  /**
   * Adds retry attempt information to the error context.
   *
   * @param attempt the current attempt number
   * @param maxAttempts the maximum number of attempts
   * @return this exception for method chaining
   */
  public KeyManagementException withRetryInfo(int attempt, int maxAttempts) {
    return withContext("retryAttempt", attempt).withContext("maxRetryAttempts", maxAttempts);
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
   * Creates a KeyManagementException for key creation failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return KeyManagementException instance
   */
  public static KeyManagementException keyCreationFailed(String message, Throwable cause) {
    return new KeyManagementException(message, KEY_CREATION_FAILED_STATE, cause);
  }

  /**
   * Creates a KeyManagementException for key decryption failures.
   *
   * @param keyId Key ID
   * @param masterKeyArn Master key ARN
   * @param cause Root cause
   * @return KeyManagementException instance
   */
  public static KeyManagementException keyDecryptionFailed(
      String keyId, String masterKeyArn, Throwable cause) {
    return new KeyManagementException(
            "Failed to decrypt data key", KEY_DECRYPTION_FAILED_STATE, cause)
        .withKeyId(keyId)
        .withMasterKeyArn(masterKeyArn);
  }

  /**
   * Creates a KeyManagementException for key storage failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return KeyManagementException instance
   */
  public static KeyManagementException keyStorageFailed(String message, Throwable cause) {
    return new KeyManagementException(message, KEY_STORAGE_FAILED_STATE, cause);
  }

  /**
   * Creates a KeyManagementException for KMS connection failures.
   *
   * @param message Error message
   * @param cause Root cause
   * @return KeyManagementException instance
   */
  public static KeyManagementException kmsConnectionFailed(String message, Throwable cause) {
    return new KeyManagementException(message, KMS_CONNECTION_FAILED_STATE, cause);
  }

  /**
   * Creates a KeyManagementException for invalid key metadata.
   *
   * @param message Error message
   * @return New KeyManagementException instance
   */
  public static KeyManagementException invalidKeyMetadata(String message) {
    return new KeyManagementException(message, INVALID_KEY_METADATA_STATE, null);
  }

  // Sanitization methods to prevent sensitive data exposure

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
}
