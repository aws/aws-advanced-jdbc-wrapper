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

package software.amazon.jdbc.plugin.encryption.exception;

import java.sql.SQLException;
import software.amazon.jdbc.plugin.encryption.model.ConnectionParameters;

/**
 * Exception thrown when independent connection creation fails. This exception provides detailed
 * context about the connection creation failure, including the connection parameters that were
 * attempted.
 */
public class IndependentConnectionException extends SQLException {

  private final ConnectionParameters attemptedParameters;
  private final String connectionAttempt;
  private final String failureReason;

  /**
   * Creates a new IndependentConnectionException with a message and connection parameters.
   *
   * @param message the detailed error message
   * @param attemptedParameters the connection parameters that failed to create a connection
   */
  public IndependentConnectionException(String message, ConnectionParameters attemptedParameters) {
    super(formatMessage(message, attemptedParameters, null));
    this.attemptedParameters = attemptedParameters;
    this.connectionAttempt = null;
    this.failureReason = null;
  }

  /**
   * Creates a new IndependentConnectionException with a message, cause, and connection parameters.
   *
   * @param message the detailed error message
   * @param cause the underlying cause of the connection failure
   * @param attemptedParameters the connection parameters that failed to create a connection
   */
  public IndependentConnectionException(
      String message, Throwable cause, ConnectionParameters attemptedParameters) {
    super(formatMessage(message, attemptedParameters, cause), cause);
    this.attemptedParameters = attemptedParameters;
    this.connectionAttempt = null;
    this.failureReason = null;
  }

  /**
   * Creates a new IndependentConnectionException with detailed context.
   *
   * @param message the detailed error message
   * @param attemptedParameters the connection parameters that failed to create a connection
   * @param connectionAttempt description of what connection creation was attempted
   * @param failureReason specific reason for the connection failure
   */
  public IndependentConnectionException(
      String message,
      ConnectionParameters attemptedParameters,
      String connectionAttempt,
      String failureReason) {
    super(formatMessage(message, attemptedParameters, null, connectionAttempt, failureReason));
    this.attemptedParameters = attemptedParameters;
    this.connectionAttempt = connectionAttempt;
    this.failureReason = failureReason;
  }

  /**
   * Creates a new IndependentConnectionException with detailed context and cause.
   *
   * @param message the detailed error message
   * @param cause the underlying cause of the connection failure
   * @param attemptedParameters the connection parameters that failed to create a connection
   * @param connectionAttempt description of what connection creation was attempted
   * @param failureReason specific reason for the connection failure
   */
  public IndependentConnectionException(
      String message,
      Throwable cause,
      ConnectionParameters attemptedParameters,
      String connectionAttempt,
      String failureReason) {
    super(
        formatMessage(message, attemptedParameters, cause, connectionAttempt, failureReason),
        cause);
    this.attemptedParameters = attemptedParameters;
    this.connectionAttempt = connectionAttempt;
    this.failureReason = failureReason;
  }

  /**
   * Gets the connection parameters that failed to create a connection.
   *
   * @return the attempted connection parameters
   */
  public ConnectionParameters getAttemptedParameters() {
    return attemptedParameters;
  }

  /**
   * Gets the description of what connection creation was attempted.
   *
   * @return the connection attempt description, or null if not provided
   */
  public String getConnectionAttempt() {
    return connectionAttempt;
  }

  /**
   * Gets the specific reason for the connection failure.
   *
   * @return the failure reason, or null if not provided
   */
  public String getFailureReason() {
    return failureReason;
  }

  /** Formats the error message with connection parameters and cause information. */
  private static String formatMessage(
      String message, ConnectionParameters attemptedParameters, Throwable cause) {
    StringBuilder sb = new StringBuilder();
    sb.append("Independent connection creation failed");

    if (message != null && !message.isEmpty()) {
      sb.append(" - ").append(message);
    }

    if (attemptedParameters != null) {
      sb.append(" (attempted URL: ");
      String jdbcUrl = attemptedParameters.getJdbcUrl();
      if (jdbcUrl != null) {
        // Mask sensitive information in URL
        sb.append(maskSensitiveUrl(jdbcUrl));
      } else {
        sb.append("null");
      }
      sb.append(")");
    }

    if (cause != null) {
      sb.append(" (caused by: ").append(cause.getClass().getSimpleName());
      if (cause.getMessage() != null) {
        sb.append(": ").append(cause.getMessage());
      }
      sb.append(")");
    }

    return sb.toString();
  }

  /** Formats the error message with detailed context information. */
  private static String formatMessage(
      String message,
      ConnectionParameters attemptedParameters,
      Throwable cause,
      String connectionAttempt,
      String failureReason) {
    StringBuilder sb = new StringBuilder();
    sb.append("Independent connection creation failed");

    if (connectionAttempt != null && !connectionAttempt.isEmpty()) {
      sb.append(" while attempting: ").append(connectionAttempt);
    }

    if (message != null && !message.isEmpty()) {
      sb.append(" - ").append(message);
    }

    if (failureReason != null && !failureReason.isEmpty()) {
      sb.append(" (reason: ").append(failureReason).append(")");
    }

    if (attemptedParameters != null) {
      sb.append(" (attempted URL: ");
      String jdbcUrl = attemptedParameters.getJdbcUrl();
      if (jdbcUrl != null) {
        sb.append(maskSensitiveUrl(jdbcUrl));
      } else {
        sb.append("null");
      }
      sb.append(")");
    }

    if (cause != null) {
      sb.append(" (caused by: ").append(cause.getClass().getSimpleName());
      if (cause.getMessage() != null) {
        sb.append(": ").append(cause.getMessage());
      }
      sb.append(")");
    }

    return sb.toString();
  }

  /**
   * Masks sensitive information in JDBC URLs for logging purposes. Removes passwords and other
   * sensitive parameters while preserving useful debugging information.
   */
  private static String maskSensitiveUrl(String jdbcUrl) {
    if (jdbcUrl == null) {
      return null;
    }

    // Remove password parameters from URL
    String masked = jdbcUrl.replaceAll("([?&]password=)[^&]*", "$1***");
    masked = masked.replaceAll("([?&]pwd=)[^&]*", "$1***");

    // Remove user credentials from URL if present
    masked = masked.replaceAll("://[^:/@]+:[^@]*@", "://***:***@");

    return masked;
  }
}
