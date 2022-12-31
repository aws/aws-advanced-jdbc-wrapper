package software.amazon.jdbc.benchmarks;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.junit.jupiter.api.BeforeAll;

import software.amazon.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGProperty;

import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.util.StringUtils;

public class ReadWriteSplittingLoadBenchmarks {

  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://atlas-postgres.cluster-czygpppufgy4.us-east-2.rds.amazonaws" +
          ".com:5432/postgres";
  private static final String AURORA_POSTGRES_USERNAME = "pgadmin";
  private static final String AURORA_POSTGRES_PASSWORD = "my_password_2020";


  protected static final String QUERY_1 = "select " +
      "l_returnflag, " +
      "l_linestatus, " +
      "sum(l_quantity) as sum_qty, " +
      "sum(l_extendedprice) as sum_base_price, " +
      "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, " +
      "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, " +
      "avg(l_quantity) as avg_qty, " +
      "avg(l_extendedprice) as avg_price, " +
      "avg(l_discount) as avg_disc, " +
      "count(*) as count_order " +
      "from " +
      "LINEITEM " +
      "where " +
      "l_shipdate <= date '1998-12-01' - interval '110' day " +
      "group by " +
      "l_returnflag, " +
      "l_linestatus " +
      "order by " +
      "l_returnflag, " +
      "l_linestatus;";

  public static void setUp() throws SQLException {
    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  @Benchmark
//   @Fork(value = 1, warmups = 1)
//   @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
//   @OutputTimeUnit(TimeUnit.MILLISECONDS)
//   @Warmup(iterations = 1)
  /**
   * the test we want to run
   */
  public void readWriteSplittingPluginEnabledBenchmarkTester() throws SQLException {
    setUp();
    final CountDownLatch startLatch = new CountDownLatch(5);
    final CountDownLatch finishLatch = new CountDownLatch(5);
    final Thread rwThread =
        getThread_PGReadWriteSplitting(startLatch, finishLatch);
    final Thread rwThread2 =
        getThread_PGReadWriteSplitting(startLatch, finishLatch);
    final Thread rwThread3 =
        getThread_PGReadWriteSplitting(startLatch, finishLatch);
    final Thread rwThread4 =
        getThread_PGReadWriteSplitting(startLatch, finishLatch);

    rwThread.start();
    rwThread2.start();
    rwThread3.start();
    rwThread4.start();

    debug("all threads started");

    rwThread.interrupt();
    rwThread2.interrupt();
    rwThread3.interrupt();
    rwThread4.interrupt();

    debug("all threads stopped");
  }

  void debug(String msg) {
    System.out.println(msg);
  }

  protected static Properties initNoPluginPropsWithTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), AURORA_POSTGRES_USERNAME);
    props.setProperty(PGProperty.PASSWORD.getName(), AURORA_POSTGRES_PASSWORD);
    props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), Boolean.FALSE.toString());
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), "5");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), "5");

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

  protected static Connection connectToInstance(String instanceUrl, Properties props)
      throws SQLException {
    return DriverManager.getConnection(instanceUrl, props);
  }

  private Thread getThread_PGReadWriteSplitting(
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      try {
         Properties props = initNoPluginPropsWithTimeouts();
       // Properties props = initReadWritePluginProps();
        // Properties props = initReadWritePluginLoadBalancingProps();

        Connection conn = connectToInstance(POSTGRESQL_CONNECTION_STRING, props);
        debug("PG connection is open.");

        Thread.sleep(5000);
        startLatch.countDown(); // notify that this thread is ready for work
        startLatch.await(5,
            TimeUnit.MINUTES); // wait for another threads to be ready to start the test

        // Execute long query
        final Statement statement = conn.createStatement();
        try (final ResultSet result = statement.executeQuery(QUERY_1)) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException throwable) { // Catching executing query
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
     //   fail("PG thread exception: " + exception);
      } finally {
        finishLatch.countDown();
        debug("pg thread completed");
      }
    });
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
