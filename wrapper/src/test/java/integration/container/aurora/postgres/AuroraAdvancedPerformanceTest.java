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

package integration.container.aurora.postgres;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.PGProperty;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.util.StringUtils;

public class AuroraAdvancedPerformanceTest extends AuroraPostgresBaseTest {

  private static final Logger LOGGER = Logger.getLogger(AuroraAdvancedPerformanceTest.class.getName());

  private static final int REPEAT_TIMES = StringUtils.isNullOrEmpty(System.getenv("REPEAT_TIMES"))
      ? 5
      : Integer.parseInt(System.getenv("REPEAT_TIMES"));
  private static final int TIMEOUT = 5;
  private static final int CONNECT_TIMEOUT = 5;
  private static final int FAILOVER_TIMEOUT_MS = 300000;
  private static final int EFM_FAILURE_DETECTION_TIME_MS = 30000;
  private static final int EFM_FAILURE_DETECTION_INTERVAL_MS = 5000;
  private static final int EFM_FAILURE_DETECTION_COUNT = 3;
  private static final String QUERY = "SELECT pg_sleep(600)"; // 600s -> 10min

  private static final ConcurrentLinkedQueue<PerfStat> perfDataList = new ConcurrentLinkedQueue<>();

  @AfterAll
  public static void cleanUp() throws IOException {
    doWritePerfDataToFile(
        "./build/reports/tests/AdvancedPerformanceResults.xlsx",
        perfDataList);
  }

  private static void doWritePerfDataToFile(
      String fileName,
      ConcurrentLinkedQueue<PerfStat> dataList)
      throws IOException {
    if (dataList.isEmpty()) {
      return;
    }

    List<PerfStat> sortedData = dataList.stream()
        .sorted((d1, d2) -> d1.paramFailoverDelayMillis == d2.paramFailoverDelayMillis
            ? d1.paramDriverName.compareTo(d2.paramDriverName)
            : 0)
        .collect(Collectors.toList());

    try (XSSFWorkbook workbook = new XSSFWorkbook()) {

      final XSSFSheet sheet = workbook.createSheet("PerformanceResults");

      for (int rows = 0; rows < dataList.size(); rows++) {
        PerfStat perfStat = sortedData.get(rows);
        Row row;

        if (rows == 0) {
          // Header
          row = sheet.createRow(0);
          perfStat.writeHeader(row);
        }

        row = sheet.createRow(rows + 1);
        perfStat.writeData(row);
      }

      // Write to file
      final File newExcelFile = new File(fileName);
      newExcelFile.createNewFile();
      try (FileOutputStream fileOut = new FileOutputStream(newExcelFile)) {
        workbook.write(fileOut);
      }
    }
  }

  @BeforeEach
  @Override
  public void setUpEach() throws IOException, InterruptedException, SQLException {
    super.setUpEach();
    ensureDnsHealthy();
  }

  @ParameterizedTest
  @MethodSource("generateParams")
  public void test_AdvancedPerformance(int failoverDelayTimeMillis, int runNumber)
      throws SQLException, InterruptedException, UnknownHostException {
    LOGGER.finest("Iteration " + runNumber + "/" + REPEAT_TIMES + " for " + failoverDelayTimeMillis + "ms delay");
    doMeasurePerformance(failoverDelayTimeMillis);
  }

  private void doMeasurePerformance(int sleepDelayMillis) throws InterruptedException {

    final AtomicLong downtime = new AtomicLong();
    final CountDownLatch startLatch = new CountDownLatch(5);
    final CountDownLatch finishLatch = new CountDownLatch(5);

    downtime.set(0);

    final Thread failoverThread = getThread_Failover(sleepDelayMillis, downtime, startLatch, finishLatch);
    final Thread pgThread = getThread_PG(sleepDelayMillis, downtime, startLatch, finishLatch);
    final Thread wrapperEfmThread = getThread_WrapperEfm(sleepDelayMillis, downtime, startLatch, finishLatch);
    final Thread wrapperEfmFailoverThread =
        getThread_WrapperEfmFailover(sleepDelayMillis, downtime, startLatch, finishLatch);
    final Thread dnsThread = getThread_DNS(sleepDelayMillis, downtime, startLatch, finishLatch);

    failoverThread.start();
    pgThread.start();
    wrapperEfmThread.start();
    wrapperEfmFailoverThread.start();
    dnsThread.start();

    LOGGER.finest("All threads started.");

    finishLatch.await(5, TimeUnit.MINUTES); // wait for all threads to complete

    LOGGER.finest("Test is over.");

    failoverThread.interrupt();
    pgThread.interrupt();
    wrapperEfmThread.interrupt();
    wrapperEfmFailoverThread.interrupt();
    dnsThread.interrupt();
  }

  private void ensureDnsHealthy() throws UnknownHostException, InterruptedException {
    LOGGER.finest("Writer is " + instanceIDs[0] + DB_CONN_STR_SUFFIX);
    final String writerIpAddress = InetAddress.getByName(instanceIDs[0] + DB_CONN_STR_SUFFIX).getHostAddress();
    LOGGER.finest("Writer resolves to " + writerIpAddress);
    LOGGER.finest("Cluster Endpoint is " + POSTGRES_CLUSTER_URL);
    String clusterIpAddress = InetAddress.getByName(POSTGRES_CLUSTER_URL).getHostAddress();
    LOGGER.finest("Cluster Endpoint resolves to " + clusterIpAddress);

    long startTimeNano = System.nanoTime();
    while (!clusterIpAddress.equals(writerIpAddress)
        && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startTimeNano) < 5) {
      Thread.sleep(1000);
      clusterIpAddress = InetAddress.getByName(POSTGRES_CLUSTER_URL).getHostAddress();
      LOGGER.finest("Cluster Endpoint resolves to " + clusterIpAddress);
    }

    if (!clusterIpAddress.equals(writerIpAddress)) {
      fail("DNS has stale data");
    }
  }

  private Thread getThread_Failover(
      final int sleepDelayMillis,
      final AtomicLong downtime,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      try {
        Thread.sleep(1000);
        startLatch.countDown(); // notify that this thread is ready for work
        startLatch.await(5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

        LOGGER.finest("Waiting " + sleepDelayMillis + "ms...");
        Thread.sleep(sleepDelayMillis);
        LOGGER.finest("Trigger failover...");

        // trigger failover
        failoverCluster();
        downtime.set(System.nanoTime());
        LOGGER.finest("Failover is started.");

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        fail("Failover thread exception: " + exception);
      } finally {
        finishLatch.countDown();
        LOGGER.finest("Failover thread is completed.");
      }
    });
  }

  private Thread getThread_PG(
      final int sleepDelayMillis,
      final AtomicLong downtime,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      long failureTime = 0;
      try {
        //DB_CONN_STR_PREFIX
        final Properties props = getDefaultPropsNoTimeouts();
        final Connection conn = openConnectionWithRetry(
            "jdbc:postgresql://",
            POSTGRES_CLUSTER_URL,
            AURORA_POSTGRES_PORT,
            props);
        LOGGER.finest("PG connection is open.");

        Thread.sleep(1000);
        startLatch.countDown(); // notify that this thread is ready for work
        startLatch.await(5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

        LOGGER.finest("PG Starting long query...");
        // Execute long query
        final Statement statement = conn.createStatement();
        try (final ResultSet result = statement.executeQuery(QUERY)) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException throwable) { // Catching executing query
          LOGGER.finest("PG thread exception: " + throwable);
          // Calculate and add detection time
          failureTime = (System.nanoTime() - downtime.get());
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        fail("PG thread exception: " + exception);
      } finally {
        PerfStat data = new PerfStat();
        data.paramFailoverDelayMillis = sleepDelayMillis;
        data.paramDriverName = "PG";
        data.failureDetectionTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTime);
        perfDataList.add(data);
        LOGGER.finest("PG Failure detection time is " + data.failureDetectionTimeMillis + "ms");

        finishLatch.countDown();
        LOGGER.finest("PG thread is completed.");
      }
    });
  }

  private Thread getThread_WrapperEfm(
      final int sleepDelayMillis,
      final AtomicLong downtime,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      long failureTime = 0;
      try {
        final Properties props = getDefaultPropsNoTimeouts();
        props.setProperty("monitoring-connectTimeout", Integer.toString(CONNECT_TIMEOUT));
        props.setProperty("monitoring-socketTimeout", Integer.toString(TIMEOUT));
        props.setProperty("connectTimeout", Integer.toString(CONNECT_TIMEOUT));
        props.setProperty("failureDetectionTime", Integer.toString(EFM_FAILURE_DETECTION_TIME_MS));
        props.setProperty("failureDetectionInterval", Integer.toString(EFM_FAILURE_DETECTION_INTERVAL_MS));
        props.setProperty("failureDetectionCount", Integer.toString(EFM_FAILURE_DETECTION_COUNT));
        props.setProperty("wrapperPlugins", "efm");

        final Connection conn = openConnectionWithRetry(
            DB_CONN_STR_PREFIX,
            POSTGRES_CLUSTER_URL,
            AURORA_POSTGRES_PORT,
            props);
        LOGGER.finest("WrapperEfm connection is open.");


        Thread.sleep(1000);
        startLatch.countDown(); // notify that this thread is ready for work
        startLatch.await(5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

        LOGGER.finest("WrapperEfm Starting long query...");
        // Execute long query
        final Statement statement = conn.createStatement();
        try (final ResultSet result = statement.executeQuery(QUERY)) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException throwable) { // Catching executing query
          LOGGER.finest("WrapperEfm thread exception: " + throwable);

          // Calculate and add detection time
          failureTime = (System.nanoTime() - downtime.get());
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        fail("WrapperEfm thread exception: " + exception);
      } finally {
        PerfStat data = new PerfStat();
        data.paramFailoverDelayMillis = sleepDelayMillis;
        data.paramDriverName = "AWS Wrapper (PG, EFM)";
        data.failureDetectionTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTime);
        perfDataList.add(data);
        LOGGER.finest("WrapperEfm Failure detection time is " + data.failureDetectionTimeMillis + "ms");

        finishLatch.countDown();
        LOGGER.finest("WrapperEfm thread is completed.");
      }
    });
  }

  private Thread getThread_WrapperEfmFailover(
      final int sleepDelayMillis,
      final AtomicLong downtime,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      long failureTime = 0;
      try {
        final Properties props = getDefaultPropsNoTimeouts();
        props.setProperty("monitoring-connectTimeout", Integer.toString(CONNECT_TIMEOUT));
        props.setProperty("monitoring-socketTimeout", Integer.toString(TIMEOUT));
        props.setProperty("connectTimeout", Integer.toString(CONNECT_TIMEOUT));
        props.setProperty("failureDetectionTime", Integer.toString(EFM_FAILURE_DETECTION_TIME_MS));
        props.setProperty("failureDetectionInterval", Integer.toString(EFM_FAILURE_DETECTION_INTERVAL_MS));
        props.setProperty("failureDetectionCount", Integer.toString(EFM_FAILURE_DETECTION_COUNT));
        props.setProperty("failoverTimeoutMs", Integer.toString(FAILOVER_TIMEOUT_MS));
        props.setProperty("wrapperPlugins", "failover,efm");

        final Connection conn = openConnectionWithRetry(
            DB_CONN_STR_PREFIX,
            POSTGRES_CLUSTER_URL,
            AURORA_POSTGRES_PORT,
            props);
        LOGGER.finest("WrapperEfmFailover connection is open.");

        Thread.sleep(1000);
        startLatch.countDown(); // notify that this thread is ready for work
        startLatch.await(5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

        LOGGER.finest("WrapperEfmFailover Starting long query...");
        // Execute long query
        final Statement statement = conn.createStatement();
        try (final ResultSet result = statement.executeQuery(QUERY)) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException throwable) {
          LOGGER.finest("WrapperEfmFailover thread exception: " + throwable);
          if (throwable instanceof FailoverSuccessSQLException) {
            // Calculate and add detection time
            failureTime = (System.nanoTime() - downtime.get());
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        fail("WrapperEfmFailover thread exception: " + exception);
      } finally {
        PerfStat data = new PerfStat();
        data.paramFailoverDelayMillis = sleepDelayMillis;
        data.paramDriverName = "AWS Wrapper (PG, EFM, Failover)";
        data.reconnectTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTime);
        perfDataList.add(data);
        LOGGER.finest("WrapperEfmFailover Reconnect time is " + data.reconnectTimeMillis + "ms");

        finishLatch.countDown();
        LOGGER.finest("WrapperEfmFailover thread is completed.");
      }
    });
  }

  private Thread getThread_DNS(
      final int sleepDelayMillis,
      final AtomicLong downtime,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      long failureTime = 0;
      String currentClusterIpAddress;

      try {
        currentClusterIpAddress = InetAddress.getByName(POSTGRES_CLUSTER_URL).getHostAddress();
        LOGGER.finest("Cluster Endpoint resolves to " + currentClusterIpAddress);

        Thread.sleep(1000);
        startLatch.countDown(); // notify that this thread is ready for work
        startLatch.await(5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

        String clusterIpAddress = InetAddress.getByName(POSTGRES_CLUSTER_URL).getHostAddress();

        long startTimeNano = System.nanoTime();
        while (clusterIpAddress.equals(currentClusterIpAddress)
            && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startTimeNano) < 5) {
          Thread.sleep(1000);
          clusterIpAddress = InetAddress.getByName(POSTGRES_CLUSTER_URL).getHostAddress();
          LOGGER.finest("Cluster Endpoint resolves to " + currentClusterIpAddress);
        }

        // DNS data has changed
        if (!clusterIpAddress.equals(currentClusterIpAddress)) {
          failureTime = (System.nanoTime() - downtime.get());
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        fail("Failover thread exception: " + exception);
      } finally {
        PerfStat data = new PerfStat();
        data.paramFailoverDelayMillis = sleepDelayMillis;
        data.paramDriverName = "DNS";
        data.dnsUpdateTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTime);
        perfDataList.add(data);
        LOGGER.finest("DNS Update time is " + data.dnsUpdateTimeMillis + "ms");

        finishLatch.countDown();
        LOGGER.finest("DNS thread is completed.");
      }
    });
  }

  private Connection openConnectionWithRetry(String protocol, String host, int port, Properties props) {
    Connection conn = null;
    int connectCount = 0;
    while (conn == null && connectCount < 10) {
      try {
        final String completeUrl = protocol + host + ":" + port + "/" + AURORA_POSTGRES_DB;
        conn = DriverManager.getConnection(completeUrl, props);

      } catch (SQLException sqlEx) {
        // ignore, try to connect again
      }
      connectCount++;
    }

    if (conn == null) {
      fail("Can't connect to " + host);
    }
    return conn;
  }

  private static Stream<Arguments> generateParams() {

    ArrayList<Arguments> args = new ArrayList<>();

    for (int i = 1; i <= REPEAT_TIMES; i++) {
      args.add(Arguments.of(10000, i));
    }
    for (int i = 1; i <= REPEAT_TIMES; i++) {
      args.add(Arguments.of(20000, i));
    }
    for (int i = 1; i <= REPEAT_TIMES; i++) {
      args.add(Arguments.of(30000, i));
    }
    for (int i = 1; i <= REPEAT_TIMES; i++) {
      args.add(Arguments.of(40000, i));
    }
    for (int i = 1; i <= REPEAT_TIMES; i++) {
      args.add(Arguments.of(50000, i));
    }
    for (int i = 1; i <= REPEAT_TIMES; i++) {
      args.add(Arguments.of(60000, i));
    }

    return Stream.of(args.toArray(new Arguments[0]));
  }

  protected static Properties getDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), AURORA_POSTGRES_USERNAME);
    props.setProperty(PGProperty.PASSWORD.getName(), AURORA_POSTGRES_PASSWORD);
    props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), Boolean.FALSE.toString());

    return props;
  }

  private static class PerfStat {

    public String paramDriverName;
    public int paramFailoverDelayMillis;
    public long failureDetectionTimeMillis;
    public long reconnectTimeMillis;
    public long dnsUpdateTimeMillis;

    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("Driver Configuration");
      cell = row.createCell(1);
      cell.setCellValue("Failover Delay Millis");
      cell = row.createCell(2);
      cell.setCellValue("Failure Detection Time Millis");
      cell = row.createCell(3);
      cell.setCellValue("Reconnect Time Millis");
      cell = row.createCell(4);
      cell.setCellValue("DNS Update Time Millis");
    }

    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.paramDriverName);
      cell = row.createCell(1);
      cell.setCellValue(this.paramFailoverDelayMillis);
      cell = row.createCell(2);
      cell.setCellValue(this.failureDetectionTimeMillis);
      cell = row.createCell(3);
      cell.setCellValue(this.reconnectTimeMillis);
      cell = row.createCell(4);
      cell.setCellValue(this.dnsUpdateTimeMillis);
    }
  }
}
