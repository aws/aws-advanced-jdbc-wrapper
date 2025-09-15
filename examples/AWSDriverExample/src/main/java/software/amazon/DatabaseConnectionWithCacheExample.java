package software.amazon;

import software.amazon.util.EnvLoader;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class DatabaseConnectionWithCacheExample {

  private static final EnvLoader env = new EnvLoader();

  private static final String DB_CONNECTION_STRING = env.get("DB_CONNECTION_STRING");
  private static final String CACHE_RW_SERVER_ADDR = env.get("CACHE_RW_SERVER_ADDR");
  private static final String CACHE_RO_SERVER_ADDR = env.get("CACHE_RO_SERVER_ADDR");
  // If the cache server is authenticated with IAM
  private static final String CACHE_NAME = env.get("CACHE_NAME");
  // Both IAM and traditional auth uses the same CACHE_USERNAME
  private static final String CACHE_USERNAME = env.get("CACHE_USERNAME"); // e.g., "iam-user-01" / "username"
  private static final String CACHE_IAM_REGION = env.get("CACHE_IAM_REGION"); // e.g., "us-west-2"
  // If the cache server is authenticated with traditional username password
  // private static final String CACHE_PASSWORD = env.get("CACHE_PASSWORD");
  private static final String USERNAME = env.get("DB_USERNAME");
  private static final String PASSWORD = env.get("DB_PASSWORD");
  private static final String USE_SSL = env.get("USE_SSL");
  private static final int THREAD_COUNT = 8; //Use 8 Threads
  private static final long TEST_DURATION_MS = 16000; //Test duration for 16 seconds

  public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();
    final Logger LOGGER = Logger.getLogger(DatabaseConnectionWithCacheExample.class.getName());

    // Configuring connection properties for the underlying JDBC driver.
    properties.setProperty("user", USERNAME);
    properties.setProperty("password", PASSWORD);

    // Configuring connection properties for the JDBC Wrapper.
    properties.setProperty("wrapperPlugins", "dataRemoteCache");
    properties.setProperty("cacheEndpointAddrRw", CACHE_RW_SERVER_ADDR);
    properties.setProperty("cacheEndpointAddrRo", CACHE_RO_SERVER_ADDR);
    // If the cache server is authenticated with IAM
    properties.setProperty("cacheName", CACHE_NAME);
    properties.setProperty("cacheUsername", CACHE_USERNAME);
    properties.setProperty("cacheIamRegion", CACHE_IAM_REGION);
    // If the cache server is authenticated with traditional username password
    // properties.setProperty("cachePassword", PASSWORD);
    properties.setProperty("cacheUseSSL", USE_SSL); // "true" or "false"
    properties.setProperty("wrapperLogUnclosedConnections", "true");
    String queryStr = "/*+ CACHE_PARAM(ttl=300s) */ select * from cinemas";

    // Create threads for concurrent connection testing
    Thread[] threads = new Thread[THREAD_COUNT];
    for (int t = 0; t < THREAD_COUNT; t++) {
      // Each thread uses a single connection for multiple queries
      threads[t] = new Thread(() -> {
        try {
          try (Connection conn = DriverManager.getConnection(DB_CONNECTION_STRING, properties)) {
            long endTime = System.currentTimeMillis() + TEST_DURATION_MS;
            try (Statement stmt = conn.createStatement()) {
              while (System.currentTimeMillis() < endTime) {
                ResultSet rs = stmt.executeQuery(queryStr);
                System.out.println("Executed the SQL query with result sets: " + rs.toString());
              }
            }
          }
        } catch (Exception e) {
          LOGGER.warning("Error: " + e.getMessage());
        }
      });
      threads[t].start();
    }
    // Wait for all threads to complete
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOGGER.warning("Thread interrupted: " + e.getMessage());
      }
    }
  }
}
