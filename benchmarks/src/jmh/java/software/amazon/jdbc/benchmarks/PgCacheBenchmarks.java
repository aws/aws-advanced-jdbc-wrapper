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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Performance benchmark program against PG.
 *
 * This test program runs JMH benchmark tests the performance of the remote cache plugin against a
 * a remote PG database and a remote cache server for both indexed queries and non-indexed queries.
 *
 * The database table schema is as follows:
 *
 * postgres=# CREATE TABLE test (id SERIAL PRIMARY KEY, int_col INTEGER, varchar_col varchar(50) NOT NULL, text_col TEXT,
 *    num_col DOUBLE PRECISION, date_col date, time_col TIME WITHOUT TIME ZONE, time_tz TIME WITH TIME ZONE,
 *    ts_col TIMESTAMP WITHOUT TIME ZONE, ts_tz TIMESTAMP WITH TIME ZONE, description TEXT);
 * CREATE TABLE
 * postgres=# select * from test;
 *  id | int_col | varchar_col | text_col | num_col | date_col | time_col | time_tz | ts_col | ts_tz | description
 * ----+---------+-------------+----------+---------+----------+----------+---------+--------+-------+--------------
 * (0 rows)
 *
 * <p>Unlike the other benchmarks in this module, this one needs live infrastructure: a reachable
 * PostgreSQL instance holding the pre-populated {@code test} table plus a reachable cache server.
 * The endpoints are therefore not hardcoded - supply them as system properties, for example:
 *
 * <pre>
 * java -Dbenchmarks.pg.url=jdbc:aws-wrapper:postgresql://my-db:5432/postgres \
 *      -Dbenchmarks.cache.rw=my-cache:6379 \
 *      -Dbenchmarks.cache.ro=my-cache:6380 \
 *      -jar build/libs/benchmarks-&lt;version&gt;-jmh.jar PgCacheBenchmarks
 * </pre>
 *
 * <p>With no properties set, setup fails fast with a clear message instead of timing out against a
 * placeholder host name. This class is excluded from the default benchmark run for that reason.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 60, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PgCacheBenchmarks {
  private static final String DB_URL_PROPERTY = "benchmarks.pg.url";
  private static final String CACHE_RW_PROPERTY = "benchmarks.cache.rw";
  private static final String CACHE_RO_PROPERTY = "benchmarks.cache.ro";

  private Connection connection;
  private int counter;
  long startTime;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(PgCacheBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setup() throws SQLException {
    final String dbUrl = requiredProperty(DB_URL_PROPERTY);
    final String cacheRw = requiredProperty(CACHE_RW_PROPERTY);
    final String cacheRo = requiredProperty(CACHE_RO_PROPERTY);
    try {
      software.amazon.jdbc.Driver.register();
    } catch (IllegalStateException e) {
      System.out.println("exception during register() is " + e.getMessage());
    }
    Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "remoteQueryCache");
    properties.setProperty("cacheEndpointAddrRw", cacheRw);
    properties.setProperty("cacheEndpointAddrRo", cacheRo);
    properties.setProperty("wrapperLogUnclosedConnections", "true");
    counter = 0;
    connection = DriverManager.getConnection(dbUrl, properties);
    startTime = System.currentTimeMillis();
  }

  private static String requiredProperty(final String name) {
    final String value = System.getProperty(name);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalStateException(
          "PgCacheBenchmarks requires a live database and cache server. Set the system property '"
              + name + "'. See the class javadoc and benchmarks/README.md for details.");
    }
    return value;
  }

  @TearDown(Level.Trial)
  public void tearDown() throws SQLException {
    // setup() can fail before the connection is opened (e.g. missing endpoint properties); a plain
    // connection.close() would then mask the real cause with an NPE from the teardown.
    if (connection != null) {
      connection.close();
    }
  }

  // Code to warm up the data in the table
  public void warmUpDataSet() throws SQLException {
    String desc_1KB = "mP48pHrR5vreBo3N6ecmlDgvfEAz0kQEOUQ89U3Rh05BTG9LhB8R0HBFBp5RIqc8vVcrphu89kW1OE2c2xApwpczFMdDAuk2SxOl9OrLvfk9zGYrdfzedcepT8LVeE6NTtYDeP3yo6UFC6AiOeqRBY5NEaNcZ8fuoXVpqOrqAhz910v5XrFxeXUyPDFxuaKFLaHfEFq7BRasUc9nfhP8gblKAGfEEmgYBpUKio27Rfo0xnavfVJQkAA2kME2PT4qZRSqeDkLmn7VBAzT9ghHqe9D4kQLQKjIyIPKqYoS8kW3ShW44VqYENwPSRAXw7UqOJqlKJ4pnmx4sPZO2kI4NYOl1JZXNlbGaSzJR0cOloKiY0z2OmUNvmD0Wju1DC9TT4OY6a6DOfFvk265BfDVxT6ufN68YG9sZuVsl7jq8SZSJg3x2cqlJuAtdSTIoKmJT1a6cEXxVusmdO27kRRp1BfWR4gz4w9HawYf9nBQOq76ObctlNvj0fYUUG3I49s3iP33CL8qZjj9RnyNUus6ieiZgta6L3mZuMRYOgCLyJrAKUYEL9KND7qirCPzVgmJHWIOnVewu8mldYFhroL89yvV3bZx4MGeyPU4KvbCsRgdORCTN0XhuLYUdiehHXnDBfuZ5yyR0saWLh8gjkLV5GkxTeKpOhpoK1o1cMiCDPYqTa64g5JundlW707c9zxc3Xnf2pW7E74YJl5oBu5vWEyPqXtYOtZOjOIRxxDY8QpoW8mpbQXxgB8DjkZZMiUCe0qHZYxvktVZJmHoaYBwpYpXVTZCfq9WajmkIOdIad1VnH5HpaECLRs6loa259yH8qesak2feDiKjfb8p3uj3s7WZUvPJwAWX9PIW1p7x6OiszXQCntOFRC3bQFNz1c98wlCBJnBSxbbYhU057TDNnoaib1h9bH7LAcqD1caE5KwLMAc5HqugkkRzT5NszkdJcpF0SxakdrAQLOKS6sNwDUzBJA76F775vmaqe3XIYecPmGtfoAKMychfEI4vfNr";
    for (int i = 0; i < 400000; i++) {
      Statement stmt = connection.createStatement();
      String description = "description " + i;
      String text = "here is my text data " + i;
      String query = String.format(
          "insert into test values (%d, %d, '%s', '%s', %f, '2024-01-10', '10:00:00', '10:00:00-07', '2025-07-15 10:00:00', '2025-07-15 10:00:00-07', '%s');",
          i,
          i * 10,
          description,
          text,
          i * 100 + 0.1234,
          desc_1KB
      );
      int rs = stmt.executeUpdate(query);
      assert rs == 1;
    }
  }

  private void validateResultSet(ResultSet rs, Blackhole b) throws SQLException {
    while (rs.next()) {
      b.consume(rs.getInt(1));
      b.consume(rs.getInt(2));
      b.consume(rs.getString(3));
      b.consume(rs.getString(4));
      b.consume(rs.getDouble(5));
      b.consume(rs.getDate(6));
      b.consume(rs.getTime(7));
      b.consume(rs.getTime(8));
      b.consume(rs.getTimestamp(9));
      b.consume(rs.getTimestamp(10));
      b.consume(rs.wasNull());
    }
  }

  @Benchmark
  public void runBenchmarkPrimaryKeyLookupNoCaching(Blackhole b) throws SQLException {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM test where id = " + counter)) {
      validateResultSet(rs, b);
    }
    counter++;
  }

  @Benchmark
  public void runBenchmarkNonIndexedLookupNoCaching(Blackhole b) throws SQLException {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM test where int_col = " + counter*10)) {
      validateResultSet(rs, b);
    }
    counter++;
  }

  @Benchmark
  public void runBenchmarkPrimaryKeyLookupWithCaching(Blackhole b) throws SQLException {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("/*+ CACHE_PARAM(ttl=172800s) */ SELECT * FROM test where id = " + counter)) {
      validateResultSet(rs, b);
    }
    counter++;
  }

  @Benchmark
  public void runBenchmarkNonIndexedLookupWithCaching(Blackhole b) throws SQLException {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("/*+ CACHE_PARAM(ttl=172800s) */ SELECT * FROM test where int_col = " + counter*10)) {
      validateResultSet(rs, b);
    }
    counter++;
  }
}
