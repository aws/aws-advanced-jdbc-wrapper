package software.amazon.jdbc.benchmarks;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 60, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PgCacheBenchmarks {
  private static final String DB_CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://db-0.XYZ.us-east-2.rds.amazonaws.com:5432/postgres";
  private static final String CACHE_RW_SERVER_ADDR = "cache-0.XYZ.us-east-2.rds.amazonaws.com:6379";
  private static final String CACHE_RO_SERVER_ADDR = "cache-0.XYZ.us-east-2.rds.amazonaws.com:6380";

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
    try {
      software.amazon.jdbc.Driver.register();
    } catch (IllegalStateException e) {
      System.out.println("exception during register() is " + e.getMessage());
    }
    Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "dataRemoteCache");
    properties.setProperty("cacheEndpointAddrRw", CACHE_RW_SERVER_ADDR);
    properties.setProperty("cacheEndpointAddrRo", CACHE_RO_SERVER_ADDR);
    properties.setProperty("wrapperLogUnclosedConnections", "true");
    counter = 0;
    connection = DriverManager.getConnection(DB_CONNECTION_STRING, properties);
    startTime = System.currentTimeMillis();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws SQLException {
    connection.close();
  }

  // Code to warm up the data in the table
  public void warmUpDataSet() throws SQLException {
    String desc_1KB = "mP48pHrR5vreBo3N6ecmlDgvfEAz0kQEOUQ89U3Rh05BTG9LhB8R0HBFBp5RIqc8vVcrphu89kW1OE2c2xApwpczFMdDAuk2SxOl9OrLvfk9zGYrdfzedcepT8LVeE6NTtYDeP3yo6UFC6AiOeqRBY5NEaNcZ8fuoXVpqOrqAhz910v5XrFxeXUyPDFxuaKFLaHfEFq7BRasUc9nfhP8gblKAGfEEmgYBpUKio27Rfo0xnavfVJQkAA2kME2PT4qZRSqeDkLmn7VBAzT9ghHqe9D4kQLQKjIyIPKqYoS8kW3ShW44VqYENwPSRAXw7UqOJqlKJ4pnmx4sPZO2kI4NYOl1JZXNlbGaSzJR0cOloKiY0z2OmUNvmD0Wju1DC9TT4OY6a6DOfFvk265BfDVxT6ufN68YG9sZuVsl7jq8SZSJg3x2cqlJuAtdSTIoKmJT1a6cEXxVusmdO27kRRp1BfWR4gz4w9HawYf9nBQOq76ObctlNvj0fYUUG3I49s3iP33CL8qZjj9RnyNUus6ieiZgta6L3mZuMRYOgCLyJrAKUYEL9KND7qirCPzVgmJHWIOnVewu8mldYFhroL89yvV3bZx4MGeyPU4KvbCsRgdORCTN0XhuLYUdiehHXnDBfuZ5yyR0saWLh8gjkLV5GkxTeKpOhpoK1o1cMiCDPYqTa64g5JundlW707c9zxc3Xnf2pW7E74YJl5oBu5vWEyPqXtYOtZOjOIRxxDY8QpoW8mpbQXxgB8DjkZZMiUCe0qHZYxvktVZJmHoaYBwpYpXVTZCfq9WajmkIOdIad1VnH5HpaECLRs6loa259yH8qesak2feDiKjfb8p3uj3s7WZUvPJwAWX9PIW1p7x6OiszXQCntOFRC3bQFNz1c98wlCBJnBSxbbYhU057TDNnoaib1h9bH7LAcqD1caE5KwLMAc5HqugkkRzT5NszkdJcpF0SxakdrAQLOKS6sNwDUzBJA76F775vmaqe3XIYecPmGtfoAKMychfEI4vfNr";
    for (int i = 0; i < 400000; i++) {
      Statement stmt = connection.createStatement();
      String description = "description " + i;
      String text = "here is my text data " + i;
      String query = "insert into test values (" + i + ", " + i * 10 + ", '" + description + "', '" + text + "', " + i * 100 + 0.1234 + ", '2024-01-10', '10:00:00', '10:00:00-07', '2025-07-15 10:00:00', '2025-07-15 10:00:00-07'" + ", '" + desc_1KB + "');";
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
