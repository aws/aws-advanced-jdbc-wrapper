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

package integration.refactored.container.tests;

import static org.junit.jupiter.api.Assertions.fail;

import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.ProxyHelper;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.EnableOnTestFeature;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;
import software.amazon.jdbc.util.StringUtils;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature({
  TestEnvironmentFeatures.PERFORMANCE,
  TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
  TestEnvironmentFeatures.FAILOVER_SUPPORTED
})
public class PerformanceTest {

  private static final Logger LOGGER = Logger.getLogger(PerformanceTest.class.getName());

  private static final int REPEAT_TIMES =
      StringUtils.isNullOrEmpty(System.getenv("REPEAT_TIMES"))
          ? 5
          : Integer.parseInt(System.getenv("REPEAT_TIMES"));

  private static final int TIMEOUT_SEC = 1;
  private static final int CONNECT_TIMEOUT_SEC = 3;
  private static final int FAILOVER_TIMEOUT_MS = 120000;

  private static final List<PerfStatMonitoring> enhancedFailureMonitoringPerfDataList =
      new ArrayList<>();
  private static final List<PerfStatMonitoring> failoverWithEfmPerfDataList = new ArrayList<>();
  private static final List<PerfStatSocketTimeout> failoverWithSocketTimeoutPerfDataList =
      new ArrayList<>();

  private void doWritePerfDataToFile(String fileName, List<? extends PerfStatBase> dataList)
      throws IOException {

    if (dataList.isEmpty()) {
      return;
    }

    LOGGER.finest(() -> "File name: " + fileName);

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

  @TestTemplate
  public void test_FailureDetectionTime_EnhancedMonitoringEnabled() throws IOException {

    enhancedFailureMonitoringPerfDataList.clear();

    try {
      Stream<Arguments> argsStream = generateFailureDetectionTimeParams();
      argsStream.forEach(
          a -> {
            try {
              Object[] args = a.get();
              execute_FailureDetectionTime_EnhancedMonitoringEnabled(
                  (int) args[0], (int) args[1], (int) args[2], (int) args[3]);
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          });

    } finally {
      doWritePerfDataToFile(
          String.format(
              "./build/reports/tests/"
                + "DbEngine_%s_Driver_%s_"
                + "FailureDetectionPerformanceResults_EnhancedMonitoringEnabled.xlsx",
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getCurrentDriver()),
          enhancedFailureMonitoringPerfDataList);
      enhancedFailureMonitoringPerfDataList.clear();
    }
  }

  private void execute_FailureDetectionTime_EnhancedMonitoringEnabled(
      int detectionTime, int detectionInterval, int detectionCount, int sleepDelayMillis)
      throws SQLException {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setMonitoringConnectTimeout(props, CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);
    DriverHelper.setMonitoringSocketTimeout(props, TIMEOUT_SEC, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(props, CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);
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

  @TestTemplate
  public void test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled() throws IOException {

    failoverWithEfmPerfDataList.clear();

    try {
      Stream<Arguments> argsStream = generateFailureDetectionTimeParams();
      argsStream.forEach(
          a -> {
            try {
              Object[] args = a.get();
              execute_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled(
                  (int) args[0], (int) args[1], (int) args[2], (int) args[3]);
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          });

    } finally {
      doWritePerfDataToFile(
          String.format(
              "./build/reports/tests/"
              + "DbEngine_%s_Driver_%s_"
              + "FailureDetectionPerformanceResults_FailoverAndEnhancedMonitoringEnabled.xlsx",
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getCurrentDriver()),
          failoverWithEfmPerfDataList);
      failoverWithEfmPerfDataList.clear();
    }
  }

  private void execute_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled(
      int detectionTime, int detectionInterval, int detectionCount, int sleepDelayMillis)
      throws SQLException {

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setMonitoringConnectTimeout(props, CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);
    DriverHelper.setMonitoringSocketTimeout(props, TIMEOUT_SEC, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(props, CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);

    // this performance test measures failover and efm failure detection time after disconnecting
    // the network
    props.setProperty("failureDetectionTime", Integer.toString(detectionTime));
    props.setProperty("failureDetectionInterval", Integer.toString(detectionInterval));
    props.setProperty("failureDetectionCount", Integer.toString(detectionCount));
    props.setProperty("wrapperPlugins", "failover,efm");
    props.setProperty("failoverTimeoutMs", Integer.toString(FAILOVER_TIMEOUT_MS));
    props.setProperty(
        "clusterInstanceHostPattern",
        "?."
            + TestEnvironment.getCurrent()
                .getInfo()
                .getProxyDatabaseInfo()
                .getInstanceEndpointSuffix());

    final PerfStatMonitoring data = new PerfStatMonitoring();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, true, data);
    data.paramDetectionTime = detectionTime;
    data.paramDetectionInterval = detectionInterval;
    data.paramDetectionCount = detectionCount;
    failoverWithEfmPerfDataList.add(data);
  }

  @TestTemplate
  public void test_FailoverTime_SocketTimeout() throws IOException {

    failoverWithSocketTimeoutPerfDataList.clear();

    try {
      Stream<Arguments> argsStream = generateFailoverSocketTimeoutTimeParams();
      argsStream.forEach(
          a -> {
            try {
              Object[] args = a.get();
              execute_FailoverTime_SocketTimeout((int) args[0], (int) args[1]);
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          });

    } finally {
      doWritePerfDataToFile(
          String.format(
              "./build/reports/tests/DbEngine_%s_Driver_%s_FailoverPerformanceResults_SocketTimeout.xlsx",
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getCurrentDriver()),
          failoverWithSocketTimeoutPerfDataList);
      failoverWithSocketTimeoutPerfDataList.clear();
    }
  }

  private void execute_FailoverTime_SocketTimeout(int socketTimeout, int sleepDelayMillis)
      throws SQLException {

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    // this performance test measures how socket timeout changes the overall failover time
    DriverHelper.setSocketTimeout(props, socketTimeout, TimeUnit.SECONDS);
    DriverHelper.setConnectTimeout(props, CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);

    // Loads just failover plugin; don't load Enhanced Failure Monitoring plugin
    props.setProperty("wrapperPlugins", "failover");
    props.setProperty(
        "clusterInstanceHostPattern",
        "?."
            + TestEnvironment.getCurrent()
                .getInfo()
                .getProxyDatabaseInfo()
                .getInstanceEndpointSuffix());
    props.setProperty("failoverTimeoutMs", Integer.toString(FAILOVER_TIMEOUT_MS));

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
      final Thread thread =
          new Thread(
              () -> {
                try {
                  Thread.sleep(sleepDelayMillis);
                  // Kill network
                  ProxyHelper.disableConnectivity(
                      TestEnvironment.getCurrent()
                          .getInfo()
                          .getProxyDatabaseInfo()
                          .getInstances()
                          .get(0)
                          .getInstanceName());
                  downtime.set(System.nanoTime());
                } catch (InterruptedException interruptedException) {
                  // Ignore, stop the thread
                }
              });

      try (final Connection conn = openConnectionWithRetry(props);
          final Statement statement = conn.createStatement()) {

        conn.setReadOnly(openReadOnlyConnection);
        thread.start();

        // Execute long query
        try (final ResultSet result = statement.executeQuery(QUERY)) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException ex) { // Catching executing query
          // Calculate and add detection time
          final long failureTime = (System.nanoTime() - downtime.get());
          elapsedTimes.add(failureTime);
        }

      } finally {
        thread.interrupt(); // Ensure thread has stopped running
        ProxyHelper.enableConnectivity(
            TestEnvironment.getCurrent()
                .getInfo()
                .getProxyDatabaseInfo()
                .getInstances()
                .get(0)
                .getInstanceName());
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

  private Connection openConnectionWithRetry(Properties props) {
    Connection conn = null;
    int connectCount = 0;
    while (conn == null && connectCount < 10) {
      try {
        conn = connectToInstance(props);
      } catch (SQLException sqlEx) {
        // ignore, try to connect again
      }
      connectCount++;
    }

    if (conn == null) {
      fail(
          "Can't connect to "
              + TestEnvironment.getCurrent()
                  .getInfo()
                  .getProxyDatabaseInfo()
                  .getInstances()
                  .get(0)
                  .getEndpoint());
    }
    return conn;
  }

  private Connection connectToInstance(Properties props) throws SQLException {
    final String url = ConnectionStringHelper.getProxyWrapperUrl();
    return DriverManager.getConnection(url, props);
  }

  private Stream<Arguments> generateFailureDetectionTimeParams() {
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

  private Stream<Arguments> generateFailoverSocketTimeoutTimeParams() {
    // socketTimeout (seconds), sleepDelayMS
    return Stream.of(
        Arguments.of(30, 5000),
        Arguments.of(30, 10000),
        Arguments.of(30, 15000),
        Arguments.of(30, 20000),
        Arguments.of(30, 25000),
        Arguments.of(30, 30000));
  }

  private abstract static class PerfStatBase {

    public int paramNetworkOutageDelayMillis;
    public long minFailureDetectionTimeMillis;
    public long maxFailureDetectionTimeMillis;
    public long avgFailureDetectionTimeMillis;

    public abstract void writeHeader(Row row);

    public abstract void writeData(Row row);
  }

  private static class PerfStatMonitoring extends PerfStatBase {

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

  private static class PerfStatSocketTimeout extends PerfStatBase {

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
