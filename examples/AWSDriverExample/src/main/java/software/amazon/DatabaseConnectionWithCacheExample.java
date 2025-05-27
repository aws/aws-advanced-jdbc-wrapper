package software.amazon;

import software.amazon.util.EnvLoader;
import java.sql.*;
import java.util.*;

public class DatabaseConnectionWithCacheExample {

  private static final EnvLoader env = new EnvLoader();

  private static final String DB_CONNECTION_STRING = env.get("DB_CONNECTION_STRING");
  private static final String CACHE_RW_SERVER_ADDR = env.get("CACHE_RW_SERVER_ADDR");
  private static final String CACHE_RO_SERVER_ADDR = env.get("CACHE_RO_SERVER_ADDR");
  private static final String USERNAME = env.get("DB_USERNAME");
  private static final String PASSWORD = env.get("DB_PASSWORD");
  private static final String USE_SSL = env.get("USE_SSL");

  public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();

    // Configuring connection properties for the underlying JDBC driver.
    properties.setProperty("user", USERNAME);
    properties.setProperty("password", PASSWORD);

    // Configuring connection properties for the JDBC Wrapper.
    properties.setProperty("wrapperPlugins", "dataRemoteCache");
    properties.setProperty("cacheEndpointAddrRw", CACHE_RW_SERVER_ADDR);
    properties.setProperty("cacheEndpointAddrRo", CACHE_RO_SERVER_ADDR);
    properties.setProperty("cacheUseSSL", USE_SSL); // "true" or "false"
    properties.setProperty("wrapperLogUnclosedConnections", "true");
    String queryStr = "select * from cinemas";
    String queryStr2 = "SELECT * from cinemas";

    for (int i = 0 ; i < 5; i++) {
      // Create a new database connection and issue queries to it
      try {
        Connection conn = DriverManager.getConnection(DB_CONNECTION_STRING, properties);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(queryStr);
        ResultSet rs2 = stmt.executeQuery(queryStr2);
        System.out.println("Executed the SQL query with result sets: " + rs.toString() + " and " + rs2.toString());
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
