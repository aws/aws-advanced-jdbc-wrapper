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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration tests that validate the wrapper correctly handles MySQL's loadbalance://
 * protocol against a real Aurora MySQL cluster.
 *
 * <p>Requires environment variables:
 *   MYSQL_CLUSTER_ENDPOINT - Aurora MySQL cluster endpoint
 *   MYSQL_INSTANCES        - Comma-separated list of all instance endpoints (host:port)
 *   MYSQL_INSTANCE_1       - First instance endpoint (legacy, used as fallback)
 *   MYSQL_INSTANCE_2       - Second instance endpoint (legacy, used as fallback)
 *   MYSQL_PORT             - MySQL port (default 3306)
 *   MYSQL_USER             - Database username
 *   MYSQL_PASSWORD         - Database password
 *   MYSQL_DATABASE         - Database name (default "test")
 */
@Tag("loadbalance-integration")
@EnabledIfEnvironmentVariable(named = "MYSQL_CLUSTER_ENDPOINT", matches = ".+")
class LoadBalanceIntegrationTest {

  private static String clusterEndpoint;
  private static String allInstancesHostPortList; // "host1:port,host2:port,..."
  private static int port;
  private static String user;
  private static String password;
  private static String database;
  private static int instanceCount;

  @BeforeAll
  static void setUp() throws SQLException {
    DriverManager.registerDriver(new Driver());

    clusterEndpoint = System.getenv("MYSQL_CLUSTER_ENDPOINT");
    port = Integer.parseInt(System.getenv().getOrDefault("MYSQL_PORT", "3306"));
    user = System.getenv("MYSQL_USER");
    password = System.getenv("MYSQL_PASSWORD");
    database = System.getenv().getOrDefault("MYSQL_DATABASE", "test");

    // Build the full host:port list from MYSQL_INSTANCES env var,
    // or fall back to MYSQL_INSTANCE_1 / MYSQL_INSTANCE_2.
    String instancesCsv = System.getenv("MYSQL_INSTANCES");
    if (instancesCsv != null && !instancesCsv.isEmpty()) {
      allInstancesHostPortList = instancesCsv;
    } else {
      String i1 = System.getenv("MYSQL_INSTANCE_1");
      String i2 = System.getenv("MYSQL_INSTANCE_2");
      allInstancesHostPortList = i1 + ":" + port + "," + i2 + ":" + port;
    }
    instanceCount = allInstancesHostPortList.split(",").length;
    System.out.println("Loadbalance test using " + instanceCount + " instances: " + allInstancesHostPortList);
  }

  private String loadBalanceUrl() {
    return "jdbc:aws-wrapper:mysql:loadbalance://" + allInstancesHostPortList + "/" + database;
  }

  /**
   * Basic connectivity test: connect through the wrapper using a standard single-host URL.
   */
  @Test
  void testBasicWrapperConnection() throws SQLException {
    String url = String.format(
        "jdbc:aws-wrapper:mysql://%s:%d/%s", clusterEndpoint, port, database);

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);

    try (Connection conn = DriverManager.getConnection(url, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1 AS val")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("val"));
    }
  }

  /**
   * Core test: connect through the wrapper using loadbalance:// protocol with
   * ALL instance endpoints. The wrapper should preserve the multi-host URL
   * and the MySQL driver should create a LoadBalancedConnection.
   */
  @Test
  void testLoadBalanceProtocolConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("wrapperPlugins", "");
    props.setProperty("wrapperDialect", "mysql");
    props.setProperty("loadBalanceAutoCommitStatementThreshold", "0");
    props.setProperty("connectTimeout", "10000");

    try (Connection conn = DriverManager.getConnection(loadBalanceUrl(), props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1 AS val")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("val"));
      assertNotNull(conn);
    }
  }

  /**
   * Validates that the loadbalance:// connection can execute many queries
   * without the wrapper incorrectly detecting connection changes.
   */
  @Test
  void testLoadBalanceMultipleQueriesStable() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("wrapperPlugins", "");
    props.setProperty("wrapperDialect", "mysql");
    props.setProperty("connectTimeout", "10000");

    try (Connection conn = DriverManager.getConnection(loadBalanceUrl(), props)) {
      for (int i = 0; i < 50; i++) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT @@hostname AS h, " + i + " AS i")) {
          assertTrue(rs.next());
          assertNotNull(rs.getString("h"));
        }
      }
      assertFalse(conn.isClosed());
    }
  }

  /**
   * Validates that the loadbalance:// protocol distributes queries across
   * multiple hosts. With loadBalanceAutoCommitStatementThreshold=1,
   * the MySQL driver should rebalance after every statement.
   * With 2+ instances and enough connections, we expect to see at least 2 distinct hosts.
   */
  @Test
  void testLoadBalanceDistributesAcrossAllHosts() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("wrapperPlugins", "");
    props.setProperty("wrapperDialect", "mysql");
    props.setProperty("loadBalanceAutoCommitStatementThreshold", "1");
    props.setProperty("connectTimeout", "10000");

    Set<String> hostnames = new HashSet<>();
    // Open multiple separate connections to observe distribution.
    // Each new connection picks a random host from the loadbalance pool.
    // With 2 instances, 20 connections gives very high probability of hitting both.
    for (int i = 0; i < 20; i++) {
      try (Connection conn = DriverManager.getConnection(loadBalanceUrl(), props);
           Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT @@hostname AS h")) {
        if (rs.next()) {
          hostnames.add(rs.getString("h"));
        }
      }
    }

    System.out.println("Distinct hostnames observed (" + hostnames.size() + "/" + instanceCount + "): " + hostnames);
    assertFalse(hostnames.isEmpty(), "Should have received at least one hostname");
    if (instanceCount >= 2) {
      assertTrue(hostnames.size() >= 2,
          "Expected queries to be distributed across at least 2 hosts, but only saw: " + hostnames);
    }
  }

  /**
   * Stress test: open multiple connections concurrently, each executing queries
   * through the loadbalance URL with all instances.
   */
  @Test
  void testLoadBalanceConcurrentConnections() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("wrapperPlugins", "");
    props.setProperty("wrapperDialect", "mysql");
    props.setProperty("loadBalanceAutoCommitStatementThreshold", "1");
    props.setProperty("connectTimeout", "10000");

    int numConnections = 3;
    Set<String> allHostnames = ConcurrentHashMap.newKeySet();
    List<Future<?>> futures = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(numConnections);

    try {
      for (int c = 0; c < numConnections; c++) {
        futures.add(executor.submit(() -> {
          try {
            // Each thread opens 5 separate connections to observe distribution
            for (int i = 0; i < 5; i++) {
              try (Connection conn = DriverManager.getConnection(loadBalanceUrl(), props);
                   Statement stmt = conn.createStatement();
                   ResultSet rs = stmt.executeQuery("SELECT @@hostname AS h")) {
                if (rs.next()) {
                  allHostnames.add(rs.getString("h"));
                }
              }
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }));
      }

      for (Future<?> f : futures) {
        f.get(300, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    System.out.println("Concurrent test - distinct hostnames (" + allHostnames.size()
        + "/" + instanceCount + "): " + allHostnames);
    if (instanceCount >= 2) {
      assertTrue(allHostnames.size() >= 2,
          "Concurrent connections should hit at least 2 distinct hosts, saw: " + allHostnames);
    }
  }

  // ---- IAM Authentication + Loadbalance tests ----

  /**
   * Validates that the loadbalance:// protocol works with IAM database authentication.
   * This is the primary use case: IAM auth generates a temporary token as the password,
   * and the loadbalance URL distributes connections across multiple Aurora instances.
   *
   * <p>Requires additional environment variables:
   *   MYSQL_IAM_USER     - IAM-enabled database user (e.g. "iam_user")
   *   MYSQL_IAM_REGION   - AWS region (e.g. "us-east-1")
   *
   * <p>The IAM user must be created with:
   *   CREATE USER 'iam_user' IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';
   *   GRANT SELECT ON test.* TO 'iam_user';
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "MYSQL_IAM_USER", matches = ".+")
  void testLoadBalanceWithIamAuth() throws SQLException {
    String iamUser = System.getenv("MYSQL_IAM_USER");
    String iamRegion = System.getenv().getOrDefault("MYSQL_IAM_REGION", "us-east-1");

    Properties props = new Properties();
    props.setProperty("user", iamUser);
    props.setProperty("wrapperPlugins", "iam");
    props.setProperty("iamRegion", iamRegion);
    // Use the cluster endpoint for IAM token generation. The IAM token is host-specific,
    // but with loadbalance:// the MySQL driver may connect to any instance. Using the
    // cluster endpoint ensures the token is valid regardless of which instance is chosen,
    // because Aurora resolves the cluster endpoint to any instance.
    props.setProperty("iamHost", clusterEndpoint);
    props.setProperty("wrapperDialect", "mysql");
    props.setProperty("enableSsl", "true");
    props.setProperty("useSSL", "true");
    props.setProperty("connectTimeout", "10000");

    try (Connection conn = DriverManager.getConnection(loadBalanceUrl(), props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1 AS val, @@hostname AS h")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("val"));
      String hostname = rs.getString("h");
      assertNotNull(hostname);
      System.out.println("IAM auth loadbalance connected to host: " + hostname);
    }
  }

  /**
   * Validates that IAM auth + loadbalance can execute multiple queries without
   * the wrapper incorrectly detecting connection changes.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "MYSQL_IAM_USER", matches = ".+")
  void testLoadBalanceWithIamAuthMultipleQueries() throws SQLException {
    String iamUser = System.getenv("MYSQL_IAM_USER");
    String iamRegion = System.getenv().getOrDefault("MYSQL_IAM_REGION", "us-east-1");

    Properties props = new Properties();
    props.setProperty("user", iamUser);
    props.setProperty("wrapperPlugins", "iam");
    props.setProperty("iamRegion", iamRegion);
    props.setProperty("iamHost", clusterEndpoint);
    props.setProperty("wrapperDialect", "mysql");
    props.setProperty("enableSsl", "true");
    props.setProperty("useSSL", "true");
    props.setProperty("connectTimeout", "10000");

    try (Connection conn = DriverManager.getConnection(loadBalanceUrl(), props)) {
      for (int i = 0; i < 10; i++) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT @@hostname AS h, " + i + " AS i")) {
          assertTrue(rs.next());
          assertNotNull(rs.getString("h"));
        }
      }
      assertFalse(conn.isClosed());
    }
  }

  /**
   * Validates that IAM auth + loadbalance distributes connections across hosts.
   * Opens multiple separate connections, each authenticated via IAM, and verifies
   * that at least 2 different hosts are reached.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "MYSQL_IAM_USER", matches = ".+")
  void testLoadBalanceWithIamAuthDistribution() throws SQLException {
    String iamUser = System.getenv("MYSQL_IAM_USER");
    String iamRegion = System.getenv().getOrDefault("MYSQL_IAM_REGION", "us-east-1");

    Properties props = new Properties();
    props.setProperty("user", iamUser);
    props.setProperty("wrapperPlugins", "iam");
    props.setProperty("iamRegion", iamRegion);
    props.setProperty("iamHost", clusterEndpoint);
    props.setProperty("wrapperDialect", "mysql");
    props.setProperty("enableSsl", "true");
    props.setProperty("useSSL", "true");
    props.setProperty("connectTimeout", "10000");

    Set<String> hostnames = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      try (Connection conn = DriverManager.getConnection(loadBalanceUrl(), props);
           Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT @@hostname AS h")) {
        if (rs.next()) {
          hostnames.add(rs.getString("h"));
        }
      }
    }

    System.out.println("IAM auth distribution - distinct hostnames (" + hostnames.size()
        + "/" + instanceCount + "): " + hostnames);
    assertFalse(hostnames.isEmpty(), "Should have connected to at least one host via IAM");
    assertTrue(hostnames.size() >= 2,
        "IAM auth loadbalance should distribute across at least 2 hosts, saw: " + hostnames);
  }
}
