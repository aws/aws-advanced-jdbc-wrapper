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
  private static final String CACHE_USE_SSL = env.get("CACHE_USE_SSL");
  // If the cache server is authenticated with traditional username password
  // private static final String CACHE_PASSWORD = env.get("CACHE_PASSWORD");
  private static final String USERNAME = env.get("DB_USERNAME");
  private static final String PASSWORD = env.get("DB_PASSWORD");
  private static final int THREAD_COUNT = 8; //Use 8 Threads
  private static final long TEST_DURATION_MS = 16000; //Test duration for 16 seconds
  private static final String CACHE_CONNECTION_TIMEOUT = env.get("CACHE_CONNECTION_TIMEOUT"); //Set connection timeout in milliseconds
  private static final String CACHE_CONNECTION_POOL_SIZE = env.get("CACHE_CONNECTION_POOL_SIZE"); //Set connection pool size
  // Failure handling configurations
  private static final String FAIL_WHEN_CACHE_DOWN = env.get("FAIL_WHEN_CACHE_DOWN");
  private static final String CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT = env.get("CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT");
  private static final String CACHE_HEALTH_CHECK_IN_HEALTHY_STATE = env.get("CACHE_HEALTH_CHECK_IN_HEALTHY_STATE");

  // If multi endpoint is configured
  private static final String CACHE_RW_SERVER_ADDR2 = env.get("CACHE_RW_SERVER_ADDR2");
  private static final String CACHE_RO_SERVER_ADDR2 = env.get("CACHE_RO_SERVER_ADDR2");
  private static final String CACHE_NAME2 = env.get("CACHE_NAME2");
  // Both IAM and traditional auth uses the same CACHE_USERNAME
  private static final String CACHE_USERNAME2 = env.get("CACHE_USERNAME2"); // e.g., "iam-user-01" / "username"
  private static final String CACHE_IAM_REGION2 = env.get("CACHE_IAM_REGION2");

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
    // properties.setProperty("cachePassword", CACHE_PASSWORD);
    properties.setProperty("cacheUseSSL", CACHE_USE_SSL); // "true" or "false"
    properties.setProperty("wrapperLogUnclosedConnections", "true");
    properties.setProperty("cacheConnectionTimeout", CACHE_CONNECTION_TIMEOUT);
    properties.setProperty("cacheConnectionPoolSize", CACHE_CONNECTION_POOL_SIZE);
    properties.setProperty("failWhenCacheDown", FAIL_WHEN_CACHE_DOWN);
    properties.setProperty("cacheInFlightWriteSizeLimitBytes", CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT);
    properties.setProperty("cacheHealthCheckInHealthyState", CACHE_HEALTH_CHECK_IN_HEALTHY_STATE);

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

    // multi cache endpoint example.
    runMultiEndPointExample();
  }

  /*
  * Multi cache Endpoint Example
  * This example demonstrates how to use multiple cache endpoints.
  * It creates two threads, each using a different cache endpoint.
  * The cache endpoints are configured in the properties object.
  * */
  public static void runMultiEndPointExample() throws SQLException {
    final Logger LOGGER = Logger.getLogger(DatabaseConnectionWithCacheExample.class.getName());
    String queryStr = "/*+ CACHE_PARAM(ttl=300s) */ select * from cinemas";

    Properties properties1 = new Properties();
    properties1.setProperty("user", USERNAME);
    properties1.setProperty("password", PASSWORD);
    properties1.setProperty("wrapperPlugins", "dataRemoteCache");
    properties1.setProperty("cacheEndpointAddrRw", CACHE_RW_SERVER_ADDR);
    properties1.setProperty("cacheEndpointAddrRo", CACHE_RO_SERVER_ADDR);
    properties1.setProperty("cacheUseSSL", CACHE_USE_SSL);
    properties1.setProperty("wrapperLogUnclosedConnections", "true");
    properties1.setProperty("cacheConnectionTimeout", CACHE_CONNECTION_TIMEOUT);
    properties1.setProperty("cacheConnectionPoolSize", CACHE_CONNECTION_POOL_SIZE);
    // If the cache server is authenticated with IAM
    properties1.setProperty("cacheName", CACHE_NAME);
    properties1.setProperty("cacheUsername", CACHE_USERNAME);
    properties1.setProperty("cacheIamRegion", CACHE_IAM_REGION);
    // If the cache server is authenticated with traditional username password
    // properties.setProperty("cachePassword", CACHE_PASSWORD);
    properties1.setProperty("failWhenCacheDown", FAIL_WHEN_CACHE_DOWN);
    properties1.setProperty("cacheInFlightWriteSizeLimitBytes", CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT);
    properties1.setProperty("cacheHealthCheckInHealthyState", CACHE_HEALTH_CHECK_IN_HEALTHY_STATE);


    Properties properties2 = new Properties();
    properties2.setProperty("user", USERNAME);
    properties2.setProperty("password", PASSWORD);
    properties2.setProperty("wrapperPlugins", "dataRemoteCache");
    properties2.setProperty("cacheEndpointAddrRw", CACHE_RW_SERVER_ADDR2);
    properties2.setProperty("cacheEndpointAddrRo", CACHE_RO_SERVER_ADDR2);
    properties2.setProperty("cacheUseSSL", CACHE_USE_SSL);
    properties2.setProperty("wrapperLogUnclosedConnections", "true");
    properties2.setProperty("cacheConnectionTimeout", CACHE_CONNECTION_TIMEOUT);
    properties2.setProperty("cacheConnectionPoolSize", CACHE_CONNECTION_POOL_SIZE);
    // If the cache server is authenticated with IAM
    properties2.setProperty("cacheName", CACHE_NAME2);
    properties2.setProperty("cacheUsername", CACHE_USERNAME2);
    properties2.setProperty("cacheIamRegion", CACHE_IAM_REGION2);
    // If the cache server is authenticated with traditional username password
    // properties.setProperty("cachePassword", CACHE_PASSWORD2);
    properties2.setProperty("failWhenCacheDown", FAIL_WHEN_CACHE_DOWN);
    properties2.setProperty("cacheInFlightWriteSizeLimitBytes", CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT);
    properties2.setProperty("cacheHealthCheckInHealthyState", CACHE_HEALTH_CHECK_IN_HEALTHY_STATE);

    // Create threads with different cache endpoints
    Thread[] threads = new Thread[THREAD_COUNT];

    for (int t = 0; t < THREAD_COUNT; t++) {
      final Properties threadProps = (t%2 == 0) ? properties1 : properties2;
      final int threadNum = t + 1;

      threads[t] = new Thread(() -> {
        try (Connection conn = DriverManager.getConnection(DB_CONNECTION_STRING, threadProps)) {
          long endTime = System.currentTimeMillis() + TEST_DURATION_MS;
          try (Statement stmt = conn.createStatement()) {
            while (System.currentTimeMillis() < endTime) {
              ResultSet rs = stmt.executeQuery(queryStr);
              System.out.println("Thread " + threadNum + " executed SQL query with result sets: " + rs.toString());
            }
          }
        } catch (Exception e) {
          LOGGER.warning("Thread " + threadNum + " error: " + e.getMessage());
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
