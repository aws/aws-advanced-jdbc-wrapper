/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import java.util.ResourceBundle;

public class Messages {

  private static final ResourceBundle MESSAGES = ResourceBundle.getBundle("messages");
  private static final Object[] emptyArgs = {};

  /**
   * Retrieve the localized error message associated with the provided key.
   *
   * @return The associated localized error message.
   */
  public static String get(String key) {
    return get(key, emptyArgs);
  }

  public static String get(String key, Object[] args) {
    final String message = MESSAGES.getString(key);
    return String.format(message, args);
  }
}
