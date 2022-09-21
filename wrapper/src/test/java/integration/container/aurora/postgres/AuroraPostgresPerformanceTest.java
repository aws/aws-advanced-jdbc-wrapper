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

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.util.StringUtils;

public class AuroraPostgresPerformanceTest extends AuroraPostgresBaseTest {

  private static final int REPEAT_TIMES = StringUtils.isNullOrEmpty(System.getenv("REPEAT_TIMES"))
      ? 5
      : Integer.parseInt(System.getenv("REPEAT_TIMES"));
  private static final int TIMEOUT = 1;
  private static final int CONNECT_TIMEOUT = 3;
  private static final int FAILOVER_TIMEOUT_MS = 40000;
  private static final List<PerfStatMonitoring> enhancedFailureMonitoringPerfDataList =
      new ArrayList<>();
  private static final List<PerfStatMonitoring> failoverWithEfmPerfDataList = new ArrayList<>();
  private static final List<PerfStatSocketTimeout> failoverWithSocketTimeoutPerfDataList =
      new ArrayList<>();

  @AfterAll
  public static void cleanUp() throws IOException {
    doWritePerfDataToFile(
        "./build/reports/tests/PostgresSQL_FailureDetectionPerformanceResults_EnhancedMonitoringEnabled.xlsx",
        enhancedFailureMonitoringPerfDataList);
    doWritePerfDataToFile(
        "./build/reports/tests/"
            + "PostgresSQL_FailureDetectionPerformanceResults_FailoverAndEnhancedMonitoringEnabled.xlsx",
        failoverWithEfmPerfDataList);
    doWritePerfDataToFile(
        "./build/reports/tests/PostgresSQL_FailoverPerformanceResults_SocketTimeout.xlsx",
        failoverWithSocketTimeoutPerfDataList);
  }

  private static void doWritePerfDataToFile(
      String fileName,
      List<? extends PerfStatBase> dataList)
      throws IOException {
    if (dataList.isEmpty()) {
      return;
    }

    try (XSSFWorkbook workbook = new XSSFWorkbook()) {

      final XSSFSheet sheet = workbook.createSheet("PerformanceResults");

      for (int rows = 0; rows < dataList.size(); rows++) {
        PerfStatBase perfStat = dataList.get(rows);
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

  @ParameterizedTest
  @MethodSource("generateFailureDetectionTimeParams")
  public void test_FailureDetectionTime_EnhancedMonitoringEnabled(
      int detectionTime,
      int detectionInterval,
      int detectionCount,
      int sleepDelayMillis)
      throws SQLException {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty("monitoring-connectTimeout", Integer.toString(TIMEOUT));
    props.setProperty("monitoring-socketTimeout", Integer.toString(TIMEOUT));
    // this performance test measures efm failure detection time after disconnecting the network
    props.setProperty("failureDetectionTime", Integer.toString(detectionTime));
    props.setProperty("failureDetectionInterval", Integer.toString(detectionInterval));
    props.setProperty("failureDetectionCount", Integer.toString(detectionCount));
    props.setProperty("wrapperPlugins", "efm");

    final PerfStatMonitoring data = new PerfStatMonitoring();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, false, data);
    data.paramDetectionTime = detectionTime;
    data.paramDetectionInterval = detectionInterval;
    data.paramDetectionCount = detectionCount;
    enhancedFailureMonitoringPerfDataList.add(data);
  }

  @ParameterizedTest
  @MethodSource("generateFailureDetectionTimeParams")
  public void test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled(
      int detectionTime,
      int detectionInterval,
      int detectionCount,
      int sleepDelayMillis)
      throws SQLException {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty("monitoring-connectTimeout", Integer.toString(TIMEOUT));
    props.setProperty("monitoring-socketTimeout", Integer.toString(TIMEOUT));
    props.setProperty("socketTimeout", Integer.toString(TIMEOUT));
    props.setProperty("connectTimeout", Integer.toString(CONNECT_TIMEOUT));

    // this performance test measures failover and efm failure detection time after disconnecting the network
    props.setProperty("failureDetectionTime", Integer.toString(detectionTime));
    props.setProperty("failureDetectionInterval", Integer.toString(detectionInterval));
    props.setProperty("failureDetectionCount", Integer.toString(detectionCount));
    props.setProperty("wrapperPlugins", "failover,efm");
    props.setProperty("failoverTimeoutMs", Integer.toString(FAILOVER_TIMEOUT_MS));

    final PerfStatMonitoring data = new PerfStatMonitoring();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, true, data);
    data.paramDetectionTime = detectionTime;
    data.paramDetectionInterval = detectionInterval;
    data.paramDetectionCount = detectionCount;
    failoverWithEfmPerfDataList.add(data);
  }

  @ParameterizedTest
  @MethodSource("generateFailoverSocketTimeoutTimeParams")
  public void test_FailoverTime_SocketTimeout(int socketTimeout, int sleepDelayMillis)
      throws SQLException {
    final Properties props = initDefaultPropsNoTimeouts();
    // this performance test measures how socket timeout changes the overall failover time
    props.setProperty("socketTimeout", Integer.toString(socketTimeout));
    props.setProperty("connectTimeout", Integer.toString(CONNECT_TIMEOUT));

    // Loads just failover plugin; don't load Enhanced Failure Monitoring plugin
    props.setProperty("failoverTimeoutMs", Integer.toString(FAILOVER_TIMEOUT_MS));
    props.setProperty("wrapperPlugins", "failover");

    final PerfStatSocketTimeout data = new PerfStatSocketTimeout();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, true, data);
    data.paramSocketTimeout = socketTimeout;
    failoverWithSocketTimeoutPerfDataList.add(data);
  }

  private void doMeasurePerformance(
      int sleepDelayMillis,
      int repeatTimes,
      Properties props,
      boolean openReadOnlyConnection,
      PerfStatBase data)
      throws SQLException {

    final String QUERY = "SELECT pg_sleep(600)"; // 600s -> 10min
    final AtomicLong downtime = new AtomicLong();
    final List<Long> elapsedTimes = new ArrayList<>(repeatTimes);

    for (int i = 0; i < repeatTimes; i++) {
      downtime.set(0);

      // Thread to stop network
      final Thread thread = new Thread(() -> {
        try {
          Thread.sleep(sleepDelayMillis);
          // Kill network
          containerHelper.disableConnectivity(proxyInstance_1);
          downtime.set(System.nanoTime());
        } catch (IOException ioException) {
          fail("Toxics were already set, should not happen");
        } catch (InterruptedException interruptedException) {
          // Ignore, stop the thread
        }
      });

      try (final Connection conn = openConnectionWithRetry(
          POSTGRES_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX,
          POSTGRES_PROXY_PORT,
          props);
          final Statement statement = conn.createStatement()) {

        conn.setReadOnly(openReadOnlyConnection);
        thread.start();

        // Execute long query
        try (final ResultSet result = statement.executeQuery(QUERY)) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException throwables) { // Catching executing query
          // Calculate and add detection time
          final long failureTime = (System.nanoTime() - downtime.get());
          elapsedTimes.add(failureTime);
        }

      } finally {
        thread.interrupt(); // Ensure thread has stopped running
        containerHelper.enableConnectivity(proxyInstance_1);
      }
    }

    final long min = elapsedTimes.stream().min(Long::compare).orElse(0L);
    final long max = elapsedTimes.stream().max(Long::compare).orElse(0L);
    final long avg =
        (long) elapsedTimes.stream().mapToLong(a -> a).summaryStatistics().getAverage();

    data.paramNetworkOutageDelayMillis = sleepDelayMillis;
    data.minFailureDetectionTimeMillis = TimeUnit.NANOSECONDS.toMillis(min);
    data.maxFailureDetectionTimeMillis = TimeUnit.NANOSECONDS.toMillis(max);
    data.avgFailureDetectionTimeMillis = TimeUnit.NANOSECONDS.toMillis(avg);
  }

  private Connection openConnectionWithRetry(String url, int port, Properties props) {
    Connection conn = null;
    int connectCount = 0;
    while (conn == null && connectCount < 10) {
      try {
        conn = connectToInstance(url, port, props);
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

  private static Stream<Arguments> generateFailureDetectionTimeParams() {
    // detectionTime, detectionInterval, detectionCount, sleepDelayMS
    return Stream.of(
        // Defaults
        Arguments.of(30000, 5000, 3, 5000),
        Arguments.of(30000, 5000, 3, 10000),
        Arguments.of(30000, 5000, 3, 15000),
        Arguments.of(30000, 5000, 3, 20000),
        Arguments.of(30000, 5000, 3, 25000),
        Arguments.of(30000, 5000, 3, 30000),
        Arguments.of(30000, 5000, 3, 35000),
        Arguments.of(30000, 5000, 3, 40000),
        Arguments.of(30000, 5000, 3, 50000),
        Arguments.of(30000, 5000, 3, 60000),

        // Aggressive detection scheme
        Arguments.of(6000, 1000, 1, 1000),
        Arguments.of(6000, 1000, 1, 2000),
        Arguments.of(6000, 1000, 1, 3000),
        Arguments.of(6000, 1000, 1, 4000),
        Arguments.of(6000, 1000, 1, 5000),
        Arguments.of(6000, 1000, 1, 6000),
        Arguments.of(6000, 1000, 1, 7000),
        Arguments.of(6000, 1000, 1, 8000),
        Arguments.of(6000, 1000, 1, 9000),
        Arguments.of(6000, 1000, 1, 10000));
  }

  private static Stream<Arguments> generateFailoverSocketTimeoutTimeParams() {
    // socketTimeout (seconds), sleepDelayMS
    return Stream.of(
        Arguments.of(30, 5000),
        Arguments.of(30, 10000),
        Arguments.of(30, 15000),
        Arguments.of(30, 25000),
        Arguments.of(30, 20000),
        Arguments.of(30, 30000));
  }

  private abstract class PerfStatBase {

    public int paramNetworkOutageDelayMillis;
    public long minFailureDetectionTimeMillis;
    public long maxFailureDetectionTimeMillis;
    public long avgFailureDetectionTimeMillis;

    public abstract void writeHeader(Row row);

    public abstract void writeData(Row row);
  }

  private class PerfStatMonitoring extends PerfStatBase {

    public int paramDetectionTime;
    public int paramDetectionInterval;
    public int paramDetectionCount;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("FailureDetectionGraceTime");
      cell = row.createCell(1);
      cell.setCellValue("FailureDetectionInterval");
      cell = row.createCell(2);
      cell.setCellValue("FailureDetectionCount");
      cell = row.createCell(3);
      cell.setCellValue("NetworkOutageDelayMillis");
      cell = row.createCell(4);
      cell.setCellValue("MinFailureDetectionTimeMillis");
      cell = row.createCell(5);
      cell.setCellValue("MaxFailureDetectionTimeMillis");
      cell = row.createCell(6);
      cell.setCellValue("AvgFailureDetectionTimeMillis");
    }

    @Override
    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.paramDetectionTime);
      cell = row.createCell(1);
      cell.setCellValue(this.paramDetectionInterval);
      cell = row.createCell(2);
      cell.setCellValue(this.paramDetectionCount);
      cell = row.createCell(3);
      cell.setCellValue(this.paramNetworkOutageDelayMillis);
      cell = row.createCell(4);
      cell.setCellValue(this.minFailureDetectionTimeMillis);
      cell = row.createCell(5);
      cell.setCellValue(this.maxFailureDetectionTimeMillis);
      cell = row.createCell(6);
      cell.setCellValue(this.avgFailureDetectionTimeMillis);
    }
  }

  private class PerfStatSocketTimeout extends PerfStatBase {

    public int paramSocketTimeout;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("SocketTimeout");
      cell = row.createCell(1);
      cell.setCellValue("NetworkOutageDelayMillis");
      cell = row.createCell(2);
      cell.setCellValue("MinFailureDetectionTimeMillis");
      cell = row.createCell(3);
      cell.setCellValue("MaxFailureDetectionTimeMillis");
      cell = row.createCell(4);
      cell.setCellValue("AvgFailureDetectionTimeMillis");
    }

    @Override
    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.paramSocketTimeout);
      cell = row.createCell(1);
      cell.setCellValue(this.paramNetworkOutageDelayMillis);
      cell = row.createCell(2);
      cell.setCellValue(this.minFailureDetectionTimeMillis);
      cell = row.createCell(3);
      cell.setCellValue(this.maxFailureDetectionTimeMillis);
      cell = row.createCell(4);
      cell.setCellValue(this.avgFailureDetectionTimeMillis);
    }
  }
}
