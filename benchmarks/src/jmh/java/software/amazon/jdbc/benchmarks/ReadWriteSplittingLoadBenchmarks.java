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

package software.amazon.jdbc.benchmarks;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.postgresql.PGProperty;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReadWriteSplittingLoadBenchmarks {

  // User configures connection properties here
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://test-db.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/readWriteSplittingExample";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  private static final int NUM_ITERATIONS = 10;
  private static final int NUM_THREADS = 10;
  private static final int NUM_EXECUTE_QUERIES = 10;
  private static final String QUERY_1 = "SELECT pg_sleep(10)";

  @Setup(Level.Iteration)
  public static void setUp() throws SQLException {
    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  protected static Connection connectToInstance(final String url, final Properties props)
      throws SQLException {
    return DriverManager.getConnection(url, props);
  }

  protected static Properties initNoPluginPropsWithTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), USERNAME);
    props.setProperty(PGProperty.PASSWORD.getName(), PASSWORD);
    props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), Boolean.FALSE.toString());
    return props;
  }

  protected static Properties initReadWritePluginProps() {
    final Properties props = initNoPluginPropsWithTimeouts();
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraHostList,readWriteSplitting");
    return props;
  }

  protected static Properties initReadWritePluginLoadBalancingProps() {
    final Properties props = initNoPluginPropsWithTimeouts();
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraHostList,readWriteSplitting");
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    return props;
  }

  private Thread getThread_PGReadWriteSplitting(final Properties props) {
    return new Thread(() -> {
      try (Connection conn = connectToInstance(POSTGRESQL_CONNECTION_STRING, props);
           Statement stmt1 = conn.createStatement()) {
        stmt1.executeQuery(QUERY_1);

        // switch to reader if read-write splitting plugin is enabled
        conn.setReadOnly(true);

        // execute multiple queries to trigger reader load balancing, if enabled
        for (int i = 0; i < NUM_EXECUTE_QUERIES; i++) {
          try (Statement statement2 = conn.createStatement()) {
            statement2.executeQuery(QUERY_1);
          }
        }

      } catch (Exception e) {
        fail("Encountered an error while executing benchmark load test: " + e.getMessage());
      }
    });
  }

  private void runBenchmarkTest(Properties props) throws InterruptedException {
    List<Thread> connectionThreadsList = new ArrayList<>(NUM_THREADS);

    for (int j = 0; j < NUM_THREADS; j++) {
      connectionThreadsList.add(getThread_PGReadWriteSplitting(props));
    }

    // start all connection threads
    for (Thread connectionThread : connectionThreadsList) {
      connectionThread.start();
    }

    // stop all connection threads after finishing
    for (Thread connectionThread : connectionThreadsList) {
      connectionThread.join();
    }
  }

  @Benchmark
  public void noPluginEnabledBenchmarkTest() throws InterruptedException {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      runBenchmarkTest(initNoPluginPropsWithTimeouts());
    }
  }

  @Benchmark
  public void readWriteSplittingPluginEnabledBenchmarkTest() throws InterruptedException {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      runBenchmarkTest(initReadWritePluginProps());
    }
  }

  @Benchmark
  public void readWriteSplittingPluginLoadBalancingEnabledBenchmarkTest() throws InterruptedException {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      runBenchmarkTest(initReadWritePluginLoadBalancingProps());
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
