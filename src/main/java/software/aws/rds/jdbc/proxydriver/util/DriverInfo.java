/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

public final class DriverInfo {

  private DriverInfo() {
  }

  // Driver name
  public static final String DRIVER_NAME = "Amazon Web Services (AWS) JDBC Proxy Driver";
  public static final String DRIVER_VERSION = "/*$version$*/"; // TODO: substitute

  // Driver version
  public static final int MAJOR_VERSION = 0;
  public static final int MINOR_VERSION = 1;
  public static final int PATCH_VERSION = 0;

  // JDBC specification
  public static final String JDBC_VERSION = "4.2";
  public static final int JDBC_MAJOR_VERSION = JDBC_VERSION.charAt(0) - '0';
  public static final int JDBC_MINOR_VERSION = JDBC_VERSION.charAt(2) - '0';
}
