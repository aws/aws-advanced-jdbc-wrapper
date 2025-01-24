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

package software.amazon;

import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;

public class ReadWriteSplittingSample {
  // Adjust this value to run the sample with different driver configurations:
  // 1: no r/w splitting
  // 2: r/w splitting using r/w plugin
  // 3: r/w plugin with internal connection pools
  static final int APPROACH_ID = 1;
  static final int NUM_THREADS = 250;
  static final int NUM_WRITES = 2500;
  static final int NUM_READS = 5000;
  static final int MAX_SIMULTANEOUS = 40;
  static final int THREAD_DELAY_MS = 2000;
  static final int EXECUTOR_TIMEOUT_MINS = 90;
  static final boolean DELETE_TABLES_ON_STARTUP = true;
  static final String WRITER_CLUSTER =
      "jdbc:aws-wrapper:postgresql://test-db.cluster-XYZ.us-east-2.rds.amazonaws.com/readWriteSplittingSample";
  static final String USER = "username";
  static final String PASSWORD = "password";
  static final Semaphore sem = new Semaphore(MAX_SIMULTANEOUS);
  static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingSample.class.getName());
  static final Properties noRwProps;
  static final Properties rwProps;
  static final Properties poolProps;

  static {
    noRwProps = new Properties();
    noRwProps.setProperty(PropertyDefinition.USER.name, USER);
    noRwProps.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);

    rwProps = new Properties();
    rwProps.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting");
    rwProps.setProperty(PropertyDefinition.USER.name, USER);
    rwProps.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);

    poolProps = new Properties();
    poolProps.setProperty(ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.name, "leastConnections");
    poolProps.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting");
    poolProps.setProperty(PropertyDefinition.USER.name, USER);
    poolProps.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);
  }

  public static void main(String[] args) throws SQLException {
    LOGGER.info(String.format(
        "Approach ID: %d, Total threads: %d, Writes per thread: %d, Reads per thread: %d, Max simultaneous threads: %d, Thread delay: %d",
        APPROACH_ID, NUM_THREADS, NUM_WRITES, NUM_READS, MAX_SIMULTANEOUS, THREAD_DELAY_MS));

    Properties props;
    if (APPROACH_ID == 1) {
      props = noRwProps;
    } else if (APPROACH_ID == 2) {
      props = rwProps;
    } else if (APPROACH_ID == 3) {
      props = poolProps;
    } else {
      throw new RuntimeException(
          String.format(
              "The approach ID should be set to a value between 1 and 3 (inclusive). Detected value: %d", APPROACH_ID));
    }

    if (DELETE_TABLES_ON_STARTUP) {
      deleteTables();
    }

    long start = System.nanoTime();
    if (APPROACH_ID == 3) {
      LOGGER.info("Enabling internal connection pools...");
      final HikariPooledConnectionProvider provider =
          new HikariPooledConnectionProvider(ReadWriteSplittingSample::getHikariConfig);
      Driver.setCustomConnectionProvider(provider);
    }

    final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    try {
      for (int i = 0; i < NUM_THREADS; i++) {
        if (APPROACH_ID == 1) {
          executorService.submit(new NoRWSplittingThread(i));
        } else {
          executorService.submit(new RWSplittingThread(i, props));  // RWThread should be used for approach 2 and approach 3.
        }

        if (i < MAX_SIMULTANEOUS) {
          // Space out initial threads to distribute workload across time.
          TimeUnit.MILLISECONDS.sleep(THREAD_DELAY_MS);
        }
      }

      executorService.shutdown();
      LOGGER.info("Waiting for threads to complete...");
      boolean successfullyTerminated = executorService.awaitTermination(EXECUTOR_TIMEOUT_MINS, TimeUnit.MINUTES);
      if (!successfullyTerminated) {
        LOGGER.warning(String.format(
            "The executor service timed out after waiting %d minutes for termination. " +
                "Consider increasing the EXECUTOR_TIMEOUT_MINS value.", EXECUTOR_TIMEOUT_MINS));
      }

      if (APPROACH_ID == 3) {
        LOGGER.info("Closing internal connection pools...");
        ConnectionProviderManager.releaseResources();
      }

      long duration = System.nanoTime() - start;
      LOGGER.info(String.format("Test completed in %dms", TimeUnit.NANOSECONDS.toMillis(duration)));
    } catch (InterruptedException e) {
      LOGGER.severe("The main thread was interrupted.");
      throw new RuntimeException(e);
    } finally {
      deleteTables();
    }
  }

  private static void deleteTables() throws SQLException {
    try (Connection conn = DriverManager.getConnection(WRITER_CLUSTER, noRwProps);
         Statement stmt = conn.createStatement()) {
      for (int i = 0; i < NUM_THREADS; i++) {
        String dropTableSql = String.format("drop table if exists rw_sample_%s", i);
        stmt.addBatch(dropTableSql);
      }
      stmt.executeBatch();
    }
  }

  private static HikariConfig getHikariConfig(HostSpec hostSpec, Properties props) {
    final HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(10);
    return config;
  }

  private static void executeWrites(Connection conn, int tableNum) throws SQLException {
    long start = System.nanoTime();

    String createSql = String.format("create table rw_sample_%s(some_num int not null)", tableNum);
    String insertSql = String.format("insert into rw_sample_%s values (1)", tableNum);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(createSql);

      for (int i = 0; i < NUM_WRITES; i++) {
        stmt.addBatch(insertSql);
      }
      stmt.executeBatch();
    }

    long duration = System.nanoTime() - start;
    long durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
    LOGGER.finest(String.format("Thread %d write duration: %dms", tableNum, durationMs));
  }

  private static void executeReads(Connection conn, int tableNum) throws SQLException {
    long start = System.nanoTime();
    String selectSQL = String.format("select * from rw_sample_%s", tableNum);

    try (Statement stmt = conn.createStatement()) {
      for (int i = 0; i < NUM_READS; i++) {
        stmt.execute(selectSQL);
      }
    }

    long duration = System.nanoTime() - start;
    long durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
    LOGGER.finest(String.format("Thread %d read duration: %dms", tableNum, durationMs));
  }

  static class NoRWSplittingThread implements Callable<Void> {
    private final int id;

    NoRWSplittingThread(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws SQLException, InterruptedException {
      sem.acquire();
      try {
        long start = System.nanoTime();
        try (Connection conn = DriverManager.getConnection(WRITER_CLUSTER, noRwProps)) {
          long duration = System.nanoTime() - start;
          long durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
          LOGGER.finest(String.format("Thread %d connect duration: %dms", this.id, durationMs));

          executeWrites(conn, this.id);
          executeReads(conn, this.id);
        }
      } catch (SQLException e) {
        LOGGER.severe(String.format("Thread %d encountered SQLException: %s", this.id, e.getMessage()));
        throw e;
      } finally {
        sem.release();
      }

      return null;
    }
  }

  static class RWSplittingThread implements Callable<Void> {
    private final int id;
    private final Properties props;

    RWSplittingThread(int id, Properties props) {
      this.id = id;
      this.props = props;
    }

    @Override
    public Void call() throws SQLException, InterruptedException {
      sem.acquire();
      try {
        long start = System.nanoTime();
        try (Connection conn = DriverManager.getConnection(WRITER_CLUSTER, this.props)) {
          long duration = System.nanoTime() - start;
          long durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
          LOGGER.finest(String.format("Thread %d connect duration: %dms", this.id, durationMs));

          executeWrites(conn, this.id);

          start = System.nanoTime();
          conn.setReadOnly(true);
          duration = System.nanoTime() - start;
          durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
          LOGGER.finest(String.format("Thread %d switch to reader duration: %dms", this.id, durationMs));

          executeReads(conn, this.id);
        }
      } catch (SQLException e) {
        LOGGER.severe(String.format("Thread %d encountered SQLException: %s", this.id, e.getMessage()));
        throw e;
      } finally {
        sem.release();
      }

      return null;
    }
  }
}
