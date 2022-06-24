/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.util;

public class TestSettings {

  public static final String mysqlServerName = System.getProperty("mysqlServerName");
  public static final String mysqlUser = System.getProperty("mysqlUser");
  public static final String mysqlPassword = System.getProperty("mysqlPassword");
  public static final String mysqlDatabase = System.getProperty("mysqlDatabase");

  public static final String postgresqlServerName = System.getProperty("postgresqlServerName");
  public static final String postgresqlUser = System.getProperty("postgresqlUser");
  public static final String postgresqlPassword = System.getProperty("postgresqlPassword");
  public static final String postgresqlDatabase = System.getProperty("postgresqlDatabase");
}
