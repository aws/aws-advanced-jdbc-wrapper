package software.amazon;

import java.sql.*;
import java.util.*;

public class PgConnectionWithCacheExample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://dev-dsk-quchen-2a-3a165932.us-west-2.amazon.com:5432/postgres";
  private static final String CACHE_RW_SERVER_ADDR = "dev-dsk-quchen-2a-3a165932.us-west-2.amazon.com:6379";
  private static final String CACHE_RO_SERVER_ADDR = "dev-dsk-quchen-2a-3a165932.us-west-2.amazon.com:6380";
  private static final String USERNAME = "postgres";
  private static final String PASSWORD = "admin";

  public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();

    // Configuring connection properties for the underlying JDBC driver.
    properties.setProperty("user", USERNAME);
    properties.setProperty("password", PASSWORD);

    // Configuring connection properties for the JDBC Wrapper.
    properties.setProperty("wrapperPlugins", "dataRemoteCache");
    properties.setProperty("cacheEndpointAddrRw", CACHE_RW_SERVER_ADDR);
    properties.setProperty("cacheEndpointAddrRo", CACHE_RO_SERVER_ADDR);
    properties.setProperty("wrapperLogUnclosedConnections", "true");
    String queryStr = "select * from cinemas";
    String queryStr2 = "SELECT * from cinemas";

    for (int i = 0 ; i < 5; i++) {
      // Create a new database connection and issue queries to it
      try {
        Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
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
