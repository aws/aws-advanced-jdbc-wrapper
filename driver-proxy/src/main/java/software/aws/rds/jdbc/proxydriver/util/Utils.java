/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import java.util.List;

public class Utils {
  public static boolean isNullOrEmpty(List<?> list) {
    return list == null || list.isEmpty();
  }
}
