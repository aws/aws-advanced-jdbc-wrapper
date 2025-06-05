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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static software.amazon.jdbc.PropertyDefinition.CONNECT_TIMEOUT;
import static software.amazon.jdbc.PropertyDefinition.PLUGINS;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME;
import static software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS;

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.aurora.TestAuroraHostListProvider;
import integration.container.aurora.TestPluginServiceImpl;
import integration.container.condition.EnableOnTestFeature;
import integration.util.AuroraTestUtility;
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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.efm.MonitorThreadContainer;
import software.amazon.jdbc.plugin.efm2.MonitorServiceImpl;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.util.StringUtils;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature({
  TestEnvironmentFeatures.PERFORMANCE,
  TestEnvironmentFeatures.FAILOVER_SUPPORTED
})
@Tag("advanced")
@Order(1)
public class AdvancedPerformanceTest {

  private static final Logger LOGGER = Logger.getLogger(AdvancedPerformanceTest.class.getName());

  private static final String MONITORING_CONNECTION_PREFIX = "monitoring-";

  private static final int REPEAT_TIMES =
      StringUtils.isNullOrEmpty(System.getenv("REPEAT_TIMES"))
          ? 5
          : Integer.parseInt(System.getenv("REPEAT_TIMES"));

  private static final int TIMEOUT_SEC = 5;
  private static final int CONNECT_TIMEOUT_SEC = 5;
  private static final int EFM_FAILOVER_TIMEOUT_MS = 300000;
  private static final int EFM_FAILURE_DETECTION_TIME_MS = 30000;
  private static final int EFM_FAILURE_DETECTION_INTERVAL_MS = 5000;
  private static final int EFM_FAILURE_DETECTION_COUNT = 3;
  private static final String QUERY = "SELECT pg_sleep(600)"; // 600s -> 10min

  private static final ConcurrentLinkedQueue<PerfStat> perfDataList = new ConcurrentLinkedQueue<>();

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private static void doWritePerfDataToFile(
      String fileName, ConcurrentLinkedQueue<PerfStat> dataList) throws IOException {

    if (dataList.isEmpty()) {
      return;
    }

    LOGGER.finest(() -> "File name: " + fileName);

    List<PerfStat> sortedData =
        dataList.stream()
            .sorted(
                (d1, d2) ->
                    d1.paramFailoverDelayMillis == d2.paramFailoverDelayMillis
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

  @TestTemplate
  public void test_AdvancedPerformance() throws IOException {

    perfDataList.clear();

    try {
      Stream<Arguments> argsStream = generateParams();
      argsStream.forEach(
          a -> {
            try {
              ensureClusterHealthy();
              LOGGER.finest("DB cluster is healthy.");
              ensureDnsHealthy();
              LOGGER.finest("DNS is healthy.");

              Object[] args = a.get();
              int failoverDelayTimeMillis = (int) args[0];
              int runNumber = (int) args[1];

              LOGGER.finest(
                  "Iteration "
                      + runNumber
                      + "/"
                      + REPEAT_TIMES
                      + " for "
                      + failoverDelayTimeMillis
                      + "ms delay");

              doMeasurePerformance(failoverDelayTimeMillis);

            } catch (InterruptedException ex) {
              throw new RuntimeException(ex);
            } catch (UnknownHostException e) {
              throw new RuntimeException(e);
            }
          });

    } finally {
      doWritePerfDataToFile(
          String.format(
              "./build/reports/tests/AdvancedPerformanceResults_"
              + "Db_%s_Driver_%s_Instances_%d.xlsx",
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getCurrentDriver(),
              TestEnvironment.getCurrent().getInfo().getRequest().getNumOfInstances()),
          perfDataList);
      perfDataList.clear();
    }
  }

  private void doMeasurePerformance(int sleepDelayMillis) throws InterruptedException {

    final AtomicLong downtimeNano = new AtomicLong();
    final CountDownLatch startLatch = new CountDownLatch(5);
    final CountDownLatch finishLatch = new CountDownLatch(5);

    downtimeNano.set(0);

    final Thread failoverThread =
        getThread_Failover(sleepDelayMillis, downtimeNano, startLatch, finishLatch);
    final Thread pgThread =
        getThread_DirectDriver(sleepDelayMillis, downtimeNano, startLatch, finishLatch);
    final Thread wrapperEfmThread =
        getThread_WrapperEfm(sleepDelayMillis, downtimeNano, startLatch, finishLatch);
    final Thread wrapperEfmFailoverThread =
        getThread_WrapperEfmFailover(sleepDelayMillis, downtimeNano, startLatch, finishLatch);
    final Thread dnsThread = getThread_DNS(sleepDelayMillis, downtimeNano, startLatch, finishLatch);

    failoverThread.start();
    pgThread.start();
    wrapperEfmThread.start();
    wrapperEfmFailoverThread.start();
    dnsThread.start();

    LOGGER.finest("All threads started.");

    finishLatch.await(5, TimeUnit.MINUTES); // wait for all threads to complete

    LOGGER.finest("Test is over.");

    assertTrue(downtimeNano.get() > 0);

    failoverThread.interrupt();
    pgThread.interrupt();
    wrapperEfmThread.interrupt();
    wrapperEfmFailoverThread.interrupt();
    dnsThread.interrupt();
  }

  private void ensureDnsHealthy() throws UnknownHostException, InterruptedException {
    LOGGER.finest(
        "Writer is "
            + TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getInstanceId());
    final String writerIpAddress =
        InetAddress.getByName(
                TestEnvironment.getCurrent()
                    .getInfo()
                    .getDatabaseInfo()
                    .getInstances()
                    .get(0)
                    .getHost())
            .getHostAddress();
    LOGGER.finest("Writer resolves to " + writerIpAddress);
    LOGGER.finest(
        "Cluster Endpoint is "
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint());
    String clusterIpAddress =
        InetAddress.getByName(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint())
            .getHostAddress();
    LOGGER.finest("Cluster Endpoint resolves to " + clusterIpAddress);

    long startTimeNano = System.nanoTime();
    while (!clusterIpAddress.equals(writerIpAddress)
        && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startTimeNano) < 5) {
      Thread.sleep(1000);
      clusterIpAddress =
          InetAddress.getByName(
                  TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint())
              .getHostAddress();
      LOGGER.finest("Cluster Endpoint resolves to " + clusterIpAddress);
    }

    if (!clusterIpAddress.equals(writerIpAddress)) {
      fail("DNS has stale data");
    }
  }

  private Thread getThread_Failover(
      final int sleepDelayMillis,
      final AtomicLong downtimeNano,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(
        () -> {
          try {
            Thread.sleep(1000);
            startLatch.countDown(); // notify that this thread is ready for work
            startLatch.await(
                5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

            LOGGER.finest("Waiting " + sleepDelayMillis + "ms...");
            Thread.sleep(sleepDelayMillis);
            LOGGER.finest("Trigger failover...");

            // trigger failover
            failoverCluster();
            downtimeNano.set(System.nanoTime());
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

  private Thread getThread_DirectDriver(
      final int sleepDelayMillis,
      final AtomicLong downtimeNano,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(
        () -> {
          long failureTimeNano = 0;
          try {
            // DB_CONN_STR_PREFIX
            final Properties props = ConnectionStringHelper.getDefaultProperties();
            final Connection conn =
                openConnectionWithRetry(
                    ConnectionStringHelper.getUrl(
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpoint(),
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpointPort(),
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getDefaultDbName()),
                    props);
            LOGGER.finest("DirectDriver connection is open.");

            Thread.sleep(1000);
            startLatch.countDown(); // notify that this thread is ready for work
            startLatch.await(
                5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

            LOGGER.finest("DirectDriver Starting long query...");
            // Execute long query
            final Statement statement = conn.createStatement();
            try (final ResultSet result = statement.executeQuery(QUERY)) {
              fail("Sleep query finished, should not be possible with network downed.");
            } catch (SQLException throwable) { // Catching executing query
              LOGGER.finest("DirectDriver thread exception: " + throwable);
              // Calculate and add detection time
              assertTrue(downtimeNano.get() > 0);
              failureTimeNano = System.nanoTime() - downtimeNano.get();
            }

          } catch (InterruptedException interruptedException) {
            // Ignore, stop the thread
          } catch (Exception exception) {
            fail("PG thread exception: " + exception);
          } finally {
            PerfStat data = new PerfStat();
            data.paramFailoverDelayMillis = sleepDelayMillis;
            data.paramDriverName =
                "DirectDriver - " + TestEnvironment.getCurrent().getCurrentDriver();
            data.failureDetectionTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTimeNano);
            LOGGER.finest("DirectDriver Collected data: " + data);
            perfDataList.add(data);
            LOGGER.finest(
                "DirectDriver Failure detection time is " + data.failureDetectionTimeMillis + "ms");

            finishLatch.countDown();
            LOGGER.finest("DirectDriver thread is completed.");
          }
        });
  }

  private Thread getThread_WrapperEfm(
      final int sleepDelayMillis,
      final AtomicLong downtimeNano,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(
        () -> {
          long failureTimeNano = 0;
          try {
            final Properties props = ConnectionStringHelper.getDefaultProperties();

            props.setProperty(
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name,
                String.valueOf(TimeUnit.SECONDS.toMillis(CONNECT_TIMEOUT_SEC)));
            props.setProperty(
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name,
                String.valueOf(TimeUnit.SECONDS.toMillis(TIMEOUT_SEC)));
            CONNECT_TIMEOUT.set(props, String.valueOf(TimeUnit.SECONDS.toMillis(CONNECT_TIMEOUT_SEC)));

            FAILURE_DETECTION_TIME.set(props, Integer.toString(EFM_FAILURE_DETECTION_TIME_MS));
            FAILURE_DETECTION_INTERVAL.set(props, Integer.toString(EFM_FAILURE_DETECTION_INTERVAL_MS));
            FAILURE_DETECTION_COUNT.set(props, Integer.toString(EFM_FAILURE_DETECTION_COUNT));
            PLUGINS.set(props, "efm");

            final Connection conn =
                openConnectionWithRetry(
                    ConnectionStringHelper.getWrapperUrl(
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpoint(),
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpointPort(),
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getDefaultDbName()),
                    props);
            LOGGER.finest("WrapperEfm connection is open.");

            Thread.sleep(1000);
            startLatch.countDown(); // notify that this thread is ready for work
            startLatch.await(
                5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

            LOGGER.finest("WrapperEfm Starting long query...");
            // Execute long query
            final Statement statement = conn.createStatement();
            try (final ResultSet result = statement.executeQuery(QUERY)) {
              fail("Sleep query finished, should not be possible with network downed.");
            } catch (SQLException throwable) { // Catching executing query
              LOGGER.finest("WrapperEfm thread exception: " + throwable);

              // Calculate and add detection time
              assertTrue(downtimeNano.get() > 0);
              failureTimeNano = System.nanoTime() - downtimeNano.get();
            }

          } catch (InterruptedException interruptedException) {
            // Ignore, stop the thread
          } catch (Exception exception) {
            fail("WrapperEfm thread exception: " + exception);
          } finally {
            PerfStat data = new PerfStat();
            data.paramFailoverDelayMillis = sleepDelayMillis;
            data.paramDriverName =
                String.format(
                    "AWS Wrapper (%s, EFM)", TestEnvironment.getCurrent().getCurrentDriver());
            data.failureDetectionTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTimeNano);
            LOGGER.finest("WrapperEfm Collected data: " + data);
            perfDataList.add(data);
            LOGGER.finest(
                "WrapperEfm Failure detection time is " + data.failureDetectionTimeMillis + "ms");

            finishLatch.countDown();
            LOGGER.finest("WrapperEfm thread is completed.");
          }
        });
  }

  private Thread getThread_WrapperEfmFailover(
      final int sleepDelayMillis,
      final AtomicLong downtimeNano,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(
        () -> {
          long failureTimeNano = 0;
          try {
            final Properties props = ConnectionStringHelper.getDefaultProperties();

            props.setProperty(
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name,
                String.valueOf(TimeUnit.SECONDS.toMillis(CONNECT_TIMEOUT_SEC)));
            props.setProperty(
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name,
                String.valueOf(TimeUnit.SECONDS.toMillis(TIMEOUT_SEC)));
            CONNECT_TIMEOUT.set(props, String.valueOf(TimeUnit.SECONDS.toMillis(CONNECT_TIMEOUT_SEC)));

            FAILURE_DETECTION_TIME.set(props, Integer.toString(EFM_FAILURE_DETECTION_TIME_MS));
            FAILURE_DETECTION_INTERVAL.set(props, Integer.toString(EFM_FAILURE_DETECTION_TIME_MS));
            FAILURE_DETECTION_COUNT.set(props, Integer.toString(EFM_FAILURE_DETECTION_COUNT));
            FAILOVER_TIMEOUT_MS.set(props, Integer.toString(EFM_FAILOVER_TIMEOUT_MS));
            PLUGINS.set(props, "failover,efm");

            final Connection conn =
                openConnectionWithRetry(
                    ConnectionStringHelper.getWrapperUrl(
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpoint(),
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpointPort(),
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getDefaultDbName()),
                    props);
            LOGGER.finest("WrapperEfmFailover connection is open.");

            Thread.sleep(1000);
            startLatch.countDown(); // notify that this thread is ready for work
            startLatch.await(
                5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

            LOGGER.finest("WrapperEfmFailover Starting long query...");
            // Execute long query
            final Statement statement = conn.createStatement();
            try (final ResultSet result = statement.executeQuery(QUERY)) {
              fail("Sleep query finished, should not be possible with network downed.");
            } catch (SQLException throwable) {
              LOGGER.finest("WrapperEfmFailover thread exception: " + throwable);
              if (throwable instanceof FailoverSuccessSQLException) {
                // Calculate and add detection time
                assertTrue(downtimeNano.get() > 0);
                failureTimeNano = System.nanoTime() - downtimeNano.get();
              }
            }

          } catch (InterruptedException interruptedException) {
            // Ignore, stop the thread
          } catch (Exception exception) {
            fail("WrapperEfmFailover thread exception: " + exception);
          } finally {
            PerfStat data = new PerfStat();
            data.paramFailoverDelayMillis = sleepDelayMillis;
            data.paramDriverName =
                String.format(
                    "AWS Wrapper (%s, EFM, Failover)",
                    TestEnvironment.getCurrent().getCurrentDriver());
            data.reconnectTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTimeNano);
            LOGGER.finest("WrapperEfmFailover Collected data: " + data);
            perfDataList.add(data);
            LOGGER.finest(
                "WrapperEfmFailover Reconnect time is " + data.reconnectTimeMillis + "ms");

            finishLatch.countDown();
            LOGGER.finest("WrapperEfmFailover thread is completed.");
          }
        });
  }

  private Thread getThread_DNS(
      final int sleepDelayMillis,
      final AtomicLong downtimeNano,
      final CountDownLatch startLatch,
      final CountDownLatch finishLatch) {

    return new Thread(
        () -> {
          long failureTimeNano = 0;
          String currentClusterIpAddress;

          try {
            currentClusterIpAddress =
                InetAddress.getByName(
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpoint())
                    .getHostAddress();
            LOGGER.finest("Cluster Endpoint resolves to " + currentClusterIpAddress);

            Thread.sleep(1000);
            startLatch.countDown(); // notify that this thread is ready for work
            startLatch.await(
                5, TimeUnit.MINUTES); // wait for another threads to be ready to start the test

            String clusterIpAddress =
                InetAddress.getByName(
                        TestEnvironment.getCurrent()
                            .getInfo()
                            .getDatabaseInfo()
                            .getClusterEndpoint())
                    .getHostAddress();

            long startTimeNano = System.nanoTime();
            while (clusterIpAddress.equals(currentClusterIpAddress)
                && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startTimeNano) < 5) {
              Thread.sleep(1000);
              clusterIpAddress =
                  InetAddress.getByName(
                          TestEnvironment.getCurrent()
                              .getInfo()
                              .getDatabaseInfo()
                              .getClusterEndpoint())
                      .getHostAddress();
              LOGGER.finest("Cluster Endpoint resolves to " + currentClusterIpAddress);
            }

            // DNS data has changed
            if (!clusterIpAddress.equals(currentClusterIpAddress)) {
              assertTrue(downtimeNano.get() > 0);
              failureTimeNano = System.nanoTime() - downtimeNano.get();
            }

          } catch (InterruptedException interruptedException) {
            // Ignore, stop the thread
          } catch (Exception exception) {
            fail("Failover thread exception: " + exception);
          } finally {
            PerfStat data = new PerfStat();
            data.paramFailoverDelayMillis = sleepDelayMillis;
            data.paramDriverName = "DNS";
            data.dnsUpdateTimeMillis = TimeUnit.NANOSECONDS.toMillis(failureTimeNano);
            LOGGER.finest("DNS Collected data: " + data);
            perfDataList.add(data);
            LOGGER.finest("DNS Update time is " + data.dnsUpdateTimeMillis + "ms");

            finishLatch.countDown();
            LOGGER.finest("DNS thread is completed.");
          }
        });
  }

  private Connection openConnectionWithRetry(String url, Properties props) {
    Connection conn = null;
    int connectCount = 0;
    while (conn == null && connectCount < 10) {
      try {
        conn = DriverManager.getConnection(url, props);

      } catch (SQLException sqlEx) {
        // ignore, try to connect again
      }
      connectCount++;
    }

    if (conn == null) {
      fail("Can't connect to " + url);
    }
    return conn;
  }

  private void failoverCluster() throws InterruptedException {
    String clusterId = TestEnvironment.getCurrent().getInfo().getRdsDbName();
    String randomNode = auroraUtil.getRandomDBClusterReaderInstanceId(clusterId);
    auroraUtil.failoverClusterToTarget(clusterId, randomNode);
  }

  private void ensureClusterHealthy() throws InterruptedException {

    auroraUtil.waitUntilClusterHasRightState(
        TestEnvironment.getCurrent().getInfo().getRdsDbName());

    // Always get the latest topology info with writer as first
    List<String> latestTopology = new ArrayList<>();

    // Need to ensure that cluster details through API matches topology fetched through SQL
    // Wait up to 5min
    long startTimeNano = System.nanoTime();
    while ((latestTopology.size()
                != TestEnvironment.getCurrent().getInfo().getRequest().getNumOfInstances()
            || !auroraUtil.isDBInstanceWriter(latestTopology.get(0)))
        && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startTimeNano) < 5) {

      Thread.sleep(5000);

      try {
        latestTopology = auroraUtil.getAuroraInstanceIds();
      } catch (SQLException ex) {
        latestTopology = new ArrayList<>();
      }
    }
    assertTrue(
        auroraUtil.isDBInstanceWriter(
            TestEnvironment.getCurrent().getInfo().getRdsDbName(), latestTopology.get(0)));
    String currentWriter = latestTopology.get(0);

    // Adjust database info to reflect a current writer and to move corresponding instance to
    // position 0.
    TestEnvironment.getCurrent().getInfo().getDatabaseInfo().moveInstanceFirst(currentWriter);
    TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().moveInstanceFirst(currentWriter);

    auroraUtil.makeSureInstancesUp(TimeUnit.MINUTES.toSeconds(5));

    TestAuroraHostListProvider.clearCache();
    TestPluginServiceImpl.clearHostAvailabilityCache();
    MonitorThreadContainer.releaseInstance();
    MonitorServiceImpl.closeAllMonitors();
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

    @Override
    public String toString() {
      return String.format("%s [\nparamDriverName=%s,\nparamFailoverDelayMillis=%d,\n"
              + "failureDetectionTimeMillis=%d,\nreconnectTimeMillis=%d,\ndnsUpdateTimeMillis=%d ]",
          super.toString(),
          this.paramDriverName,
          this.paramFailoverDelayMillis,
          this.failureDetectionTimeMillis,
          this.reconnectTimeMillis,
          this.dnsUpdateTimeMillis);
    }
  }
}
