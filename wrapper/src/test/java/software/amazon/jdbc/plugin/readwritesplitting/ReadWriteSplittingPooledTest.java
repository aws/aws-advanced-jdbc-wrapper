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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

@Disabled("Replace connection details to run")
public class ReadWriteSplittingPooledTest {
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://"
          + "test-db.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/readWriteSplittingExample";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private final Properties props = new Properties();

  @BeforeEach
  public void setup() throws SQLException {
    PropertyDefinition.USER.set(props, USERNAME);
    PropertyDefinition.PASSWORD.set(props, PASSWORD);
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover,efm");

    DriverManager.registerDriver(new software.amazon.jdbc.Driver());
    DriverManager.registerDriver(new org.postgresql.Driver());

    ConnectionProviderManager.setConnectionProvider(
        new HikariPooledConnectionProvider(ReadWriteSplittingPooledTest::getHikariConfig));
  }

  @AfterEach
  public void cleanup() {
    ConnectionProviderManager.releaseResources();
  }

  @Test
  public void testSingleConnection() throws SQLException {
    try (Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
      Statement stmt = conn.createStatement();

      // Test write statements
      stmt.execute("DROP TABLE IF EXISTS poolTest");
      stmt.execute("CREATE TABLE poolTest (id int, employee varchar(255))");
      stmt.execute("DELETE FROM poolTest WHERE id=1");
      stmt.execute("INSERT INTO poolTest VALUES (1, 'George')");

      String writerId = queryInstanceId(conn);

      conn.setReadOnly(true);
      // Should indicate different instance than previous query
      String readerId = queryInstanceId(conn);
      assertNotEquals(writerId, readerId);

      conn.setReadOnly(false);
      // Should indicate original writer
      String currentId = queryInstanceId(conn);
      assertEquals(writerId, currentId);
    }
  }

  @Test
  public void testTwoConnections() throws SQLException {
    try (Connection conn1 = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props);
         Connection conn2 = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
      String conn1WriterId = queryInstanceId(conn1);
      String conn2WriterId = queryInstanceId(conn2);
      assertEquals(conn1WriterId, conn2WriterId);

      conn1.setReadOnly(true);
      conn2.setReadOnly(true);

      String conn1ReaderId = queryInstanceId(conn1);
      String conn2ReaderId = queryInstanceId(conn2);
      assertNotEquals(conn1WriterId, conn1ReaderId);
      assertNotEquals(conn2WriterId, conn2ReaderId);

      conn1.setReadOnly(false);
      conn2.setReadOnly(false);

      String conn1CurrentId = queryInstanceId(conn1);
      String conn2CurrentId = queryInstanceId(conn2);
      assertEquals(conn1WriterId, conn1CurrentId);
      assertEquals(conn2WriterId, conn2CurrentId);

      conn1.setReadOnly(true);
      conn2.setReadOnly(true);

      conn1CurrentId = queryInstanceId(conn1);
      conn2CurrentId = queryInstanceId(conn2);
      assertEquals(conn1ReaderId, conn1CurrentId);
      assertEquals(conn2ReaderId, conn2CurrentId);
    }
  }

  @Test
  public void testConcurrentConnections() throws ExecutionException, InterruptedException {
    int numThreads = 20;
    List<Future> futures = new ArrayList<>();

    final ExecutorService executorService = Executors.newCachedThreadPool(
        r -> {
          final Thread testThread = new Thread(r);
          testThread.setDaemon(true);
          return testThread;
        });

    for (int i = 0; i < numThreads; i++) {
      futures.add(executorService.submit(new TestThread(i)));
    }

    executorService.shutdown();
    for (int i = 0; i < numThreads; i++) {
      futures.get(i).get();
    }
  }

  class TestThread implements Runnable {

    private final int id;

    TestThread(int id) {
      this.id = id;
    }

    @Override
    public void run() {
      try (Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
        String writerId = queryInstanceId(conn);
        System.out.println("Thread-" + id + " connected to writer " + writerId);

        conn.setReadOnly(true);
        // Should indicate different instance than previous query
        String readerId = queryInstanceId(conn);
        System.out.println("Thread-" + id + " connected to reader " + readerId);
        assertNotEquals(writerId, readerId);

        conn.setReadOnly(false);
        // Should indicate original writer
        String currentId = queryInstanceId(conn);
        System.out.println("Thread-" + id + " connected to writer " + currentId);
        assertEquals(writerId, currentId);
      } catch (SQLException e) {
        fail("Encountered exception in thread-" + id + ": " + e.getMessage());
      }
    }
  }

  private static String queryInstanceId(Connection conn) throws SQLException {
    ResultSet rs;
    Statement stmt;
    stmt = conn.createStatement();
    rs = stmt.executeQuery("SELECT aurora_db_instance_identifier()");
    rs.next();
    return rs.getString(1) + " **TAG";
  }

  private static HikariConfig getHikariConfig(HostSpec hostSpec, Properties props) {
    HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(10);
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(1000);
    return config;
  }
}
