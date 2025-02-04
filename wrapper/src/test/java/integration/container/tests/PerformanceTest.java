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

import static org.junit.jupiter.api.Assertions.fail;
import static software.amazon.jdbc.PropertyDefinition.CONNECT_TIMEOUT;
import static software.amazon.jdbc.PropertyDefinition.PLUGINS;
import static software.amazon.jdbc.PropertyDefinition.SOCKET_TIMEOUT;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME;
import static software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS;

import integration.DatabaseEngine;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnTestFeature;
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
import java.util.LongSummaryStatistics;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
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
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.plugin.OpenedConnectionTracker;
import software.amazon.jdbc.plugin.efm.MonitorThreadContainer;
import software.amazon.jdbc.plugin.efm2.MonitorServiceImpl;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.util.StringUtils;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature({
  TestEnvironmentFeatures.PERFORMANCE,
  TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
  TestEnvironmentFeatures.FAILOVER_SUPPORTED
})
@Order(10)
public class PerformanceTest {

  private static final Logger LOGGER = Logger.getLogger(PerformanceTest.class.getName());

  private static final String MONITORING_CONNECTION_PREFIX = "monitoring-";

  private static final int REPEAT_TIMES =
      StringUtils.isNullOrEmpty(System.getenv("REPEAT_TIMES"))
          ? 5
          : Integer.parseInt(System.getenv("REPEAT_TIMES"));

  private static final int TIMEOUT_MS = 3000;
  private static final int CONNECT_TIMEOUT_MS = 3000;
  private static final int PERF_FAILOVER_TIMEOUT_MS = 120000;

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
  @Tag("efm")
  public void test_FailureDetectionTime_EnhancedMonitoringEnabled() throws IOException {
    test_FailureDetectionTime_EnhancedMonitoringEnabled("efm");
    test_FailureDetectionTime_EnhancedMonitoringEnabled("efm2");
  }

  public void test_FailureDetectionTime_EnhancedMonitoringEnabled(final String efmPlugin)
        throws IOException {

    OpenedConnectionTracker.clearCache();
    MonitorThreadContainer.releaseInstance();
    MonitorServiceImpl.closeAllMonitors();
    AuroraHostListProvider.clearAll();
    RdsHostListProvider.clearAll();

    enhancedFailureMonitoringPerfDataList.clear();

    LOGGER.finest("Test round with plugins: " + efmPlugin);

    try {
      Stream<Arguments> argsStream = generateFailureDetectionTimeParams();
      argsStream.forEach(
          a -> {
            try {
              Object[] args = a.get();
              execute_FailureDetectionTime_EnhancedMonitoringEnabled(
                  efmPlugin, (int) args[0], (int) args[1], (int) args[2], (int) args[3]);
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          });

    } finally {
      doWritePerfDataToFile(
          String.format(
              "./build/reports/tests/EnhancedMonitoringOnly_"
                + "Db_%s_Driver_%s_Instances_%d_Plugin_%s.xlsx",
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getCurrentDriver(),
              TestEnvironment.getCurrent().getInfo().getRequest().getNumOfInstances(),
              efmPlugin),
          enhancedFailureMonitoringPerfDataList);
      enhancedFailureMonitoringPerfDataList.clear();
    }
  }

  private void execute_FailureDetectionTime_EnhancedMonitoringEnabled(
      final String efmPlugin,
      int detectionTimeMillis,
      int detectionIntervalMillis,
      int detectionCount,
      int sleepDelayMillis)
      throws SQLException {

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(
        MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name,
        String.valueOf(CONNECT_TIMEOUT_MS));
    props.setProperty(
        MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name,
        String.valueOf(TIMEOUT_MS));
    CONNECT_TIMEOUT.set(props, String.valueOf(CONNECT_TIMEOUT_MS));

    // this performance test measures efm failure detection time after disconnecting the network
    FAILURE_DETECTION_TIME.set(props, Integer.toString(detectionTimeMillis));
    FAILURE_DETECTION_INTERVAL.set(props, Integer.toString(detectionIntervalMillis));
    FAILURE_DETECTION_COUNT.set(props, Integer.toString(detectionCount));
    PLUGINS.set(props, efmPlugin);
    RdsHostListProvider.CLUSTER_ID.set(props, "test-cluster-id");

    final PerfStatMonitoring data = new PerfStatMonitoring();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, data);
    data.paramDetectionTime = detectionTimeMillis;
    data.paramDetectionInterval = detectionIntervalMillis;
    data.paramDetectionCount = detectionCount;
    enhancedFailureMonitoringPerfDataList.add(data);
  }

  @TestTemplate
  @Tag("efm")
  @Tag("failover")
  public void test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled() throws IOException {
    test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled("failover,efm");
    test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled("failover,efm2");
    test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled("failover2,efm");
    test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled("failover2,efm2");
  }

  public void test_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled(final String plugins)
      throws IOException {

    OpenedConnectionTracker.clearCache();
    MonitorThreadContainer.releaseInstance();
    MonitorServiceImpl.closeAllMonitors();
    AuroraHostListProvider.clearAll();
    RdsHostListProvider.clearAll();

    failoverWithEfmPerfDataList.clear();

    LOGGER.finest("Test round with plugins: " + plugins);

    try {
      Stream<Arguments> argsStream = generateFailureDetectionTimeParams();
      argsStream.forEach(
          a -> {
            try {
              Object[] args = a.get();
              execute_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled(
                  plugins, (int) args[0], (int) args[1], (int) args[2], (int) args[3]);
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          });

    } finally {
      doWritePerfDataToFile(
          String.format(
              "./build/reports/tests/FailoverWithEnhancedMonitoring_"
              + "Db_%s_Driver_%s_Instances_%d_Plugin_%s.xlsx",
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getCurrentDriver(),
              TestEnvironment.getCurrent().getInfo().getRequest().getNumOfInstances(),
              plugins.replace(",", "_")),
          failoverWithEfmPerfDataList);
      failoverWithEfmPerfDataList.clear();
    }
  }

  private void execute_FailureDetectionTime_FailoverAndEnhancedMonitoringEnabled(
      final String plugins,
      int detectionTime,
      int detectionInterval,
      int detectionCount,
      int sleepDelayMillis)
      throws SQLException {

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(
        MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name,
        String.valueOf(CONNECT_TIMEOUT_MS));
    props.setProperty(
        MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name,
        String.valueOf(TIMEOUT_MS));
    CONNECT_TIMEOUT.set(props, String.valueOf(CONNECT_TIMEOUT_MS));

    // this performance test measures failover and efm failure detection time after disconnecting
    // the network
    FAILURE_DETECTION_TIME.set(props, Integer.toString(detectionTime));
    FAILURE_DETECTION_INTERVAL.set(props, Integer.toString(detectionInterval));
    FAILURE_DETECTION_COUNT.set(props, Integer.toString(detectionCount));
    PLUGINS.set(props, plugins);
    FAILOVER_TIMEOUT_MS.set(props, Integer.toString(PERF_FAILOVER_TIMEOUT_MS));
    props.setProperty(
        "clusterInstanceHostPattern",
        "?."
            + TestEnvironment.getCurrent()
                .getInfo()
                .getProxyDatabaseInfo()
                .getInstanceEndpointSuffix());
    FailoverConnectionPlugin.FAILOVER_MODE.set(props, "strict-reader");
    RdsHostListProvider.CLUSTER_ID.set(props, "test-cluster-id");

    final PerfStatMonitoring data = new PerfStatMonitoring();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, data);
    data.paramDetectionTime = detectionTime;
    data.paramDetectionInterval = detectionInterval;
    data.paramDetectionCount = detectionCount;
    failoverWithEfmPerfDataList.add(data);
  }

  @TestTemplate
  @Tag("failover")
  public void test_FailoverTime_SocketTimeout() throws IOException {
    test_FailoverTime_SocketTimeout("failover");
    test_FailoverTime_SocketTimeout("failover2");
  }

  private void test_FailoverTime_SocketTimeout(final String plugins) throws IOException {

    OpenedConnectionTracker.clearCache();
    MonitorThreadContainer.releaseInstance();
    MonitorServiceImpl.closeAllMonitors();
    AuroraHostListProvider.clearAll();
    RdsHostListProvider.clearAll();

    failoverWithSocketTimeoutPerfDataList.clear();

    LOGGER.finest("Test round with plugins: " + plugins);

    try {
      Stream<Arguments> argsStream = generateFailoverSocketTimeoutTimeParams();
      argsStream.forEach(
          a -> {
            try {
              Object[] args = a.get();
              execute_FailoverTime_SocketTimeout(plugins, (int) args[0], (int) args[1]);
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          });

    } finally {
      doWritePerfDataToFile(
          String.format(
              "./build/reports/tests/FailoverWithSocketTimeout_"
              + "Db_%s_Driver_%s_Instances_%d_Plugins_%s.xlsx",
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getCurrentDriver(),
              TestEnvironment.getCurrent().getInfo().getRequest().getNumOfInstances(),
              plugins.replace(",", "_")),
          failoverWithSocketTimeoutPerfDataList);
      failoverWithSocketTimeoutPerfDataList.clear();
    }
  }

  private void execute_FailoverTime_SocketTimeout(final String plugins, int socketTimeout, int sleepDelayMillis)
      throws SQLException {

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    // this performance test measures how socket timeout changes the overall failover time
    SOCKET_TIMEOUT.set(props, String.valueOf(TimeUnit.SECONDS.toMillis(socketTimeout)));
    CONNECT_TIMEOUT.set(props, String.valueOf(CONNECT_TIMEOUT_MS));

    // Loads just failover plugin; don't load Enhanced Failure Monitoring plugin
    props.setProperty("wrapperPlugins", plugins);
    props.setProperty(
        "clusterInstanceHostPattern",
        "?."
            + TestEnvironment.getCurrent()
                .getInfo()
                .getProxyDatabaseInfo()
                .getInstanceEndpointSuffix());
    props.setProperty("failoverTimeoutMs", Integer.toString(PERF_FAILOVER_TIMEOUT_MS));
    FailoverConnectionPlugin.FAILOVER_MODE.set(props, "strict-reader");
    RdsHostListProvider.CLUSTER_ID.set(props, "test-cluster-id");

    final PerfStatSocketTimeout data = new PerfStatSocketTimeout();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, data);
    data.paramSocketTimeout = socketTimeout;
    failoverWithSocketTimeoutPerfDataList.add(data);
  }

  private void doMeasurePerformance(
      int sleepDelayMillis,
      int repeatTimes,
      Properties props,
      PerfStatBase data)
      throws SQLException {

    ProxyHelper.enableAllConnectivity();

    final AtomicLong downtimeNanos = new AtomicLong();
    final List<Long> elapsedTimeMillis = new ArrayList<>(repeatTimes);

    for (int i = 0; i < repeatTimes; i++) {

      try {
        LOGGER.finest("Resting 15s...");
        TimeUnit.SECONDS.sleep(15); // let monitoring threads to recover from previous network outage
      } catch (InterruptedException ex) {
        // do nothing
      }

      LOGGER.finest("Iteration: " + i);
      downtimeNanos.set(0);

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
                          .getInstanceId());
                  downtimeNanos.set(System.nanoTime());
                  LOGGER.finest("Network outages started.");
                } catch (InterruptedException interruptedException) {
                  // Ignore, stop the thread
                }
              });

      try (final Connection conn = openConnectionWithRetry(props);
          final Statement statement = conn.createStatement()) {

        thread.start();

        LOGGER.finest("Start long query.");

        // Execute long query
        try (final ResultSet result = statement.executeQuery(getQuerySql(600))) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException ex) { // Catching executing query
          // Calculate and add detection time
          final long downtime = downtimeNanos.get();
          if (downtime == 0) {
            LOGGER.warning("Network outages start time is undefined!");
            elapsedTimeMillis.add(0L);
          } else {
            final long failureTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - downtime);
            elapsedTimeMillis.add(failureTimeMillis);
            LOGGER.finest("Elapsed: " + failureTimeMillis);
          }
        }
      } finally {
        thread.interrupt(); // Ensure thread has stopped running
        ProxyHelper.enableConnectivity(
            TestEnvironment.getCurrent()
                .getInfo()
                .getProxyDatabaseInfo()
                .getInstances()
                .get(0)
                .getInstanceId());
      }
    }

    final LongSummaryStatistics stats = elapsedTimeMillis.stream().mapToLong(a -> a).summaryStatistics();

    data.paramNetworkOutageDelayMillis = sleepDelayMillis;
    data.minFailureDetectionTimeMillis = stats.getMin();
    data.maxFailureDetectionTimeMillis = stats.getMax();
    data.avgFailureDetectionTimeMillis = Math.round(stats.getAverage());
    LOGGER.finest("Collected data: " + data);
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
                  .getHost());
    }
    return conn;
  }

  private Connection connectToInstance(Properties props) throws SQLException {
    final String url = ConnectionStringHelper.getProxyWrapperUrl();
    return DriverManager.getConnection(url, props);
  }

  private String getQuerySql(final int seconds) {
    final DatabaseEngine databaseEngine =
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    switch (databaseEngine) {
      case PG:
        return String.format("SELECT pg_sleep(%d)", seconds);
      case MYSQL:
      case MARIADB:
        return String.format("SELECT sleep(%d)", seconds);
      default:
        throw new UnsupportedOperationException(databaseEngine.name());
    }
  }

  private Stream<Arguments> generateFailureDetectionTimeParams() {
    // detectionTimeMs, detectionIntervalMs, detectionCount, sleepDelayMs
    return Stream.of(
        // Defaults
        Arguments.of(30000, 5000, 3, 10000),
        Arguments.of(30000, 5000, 3, 20000),
        Arguments.of(30000, 5000, 3, 30000),
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
        Arguments.of(6000, 1000, 1, 9000)
    );
  }

  private Stream<Arguments> generateFailoverSocketTimeoutTimeParams() {
    // socketTimeout (seconds), sleepDelayMS
    return Stream.of(
        Arguments.of(30, 10000),
        Arguments.of(30, 20000),
        Arguments.of(30, 30000),
        Arguments.of(30, 40000));
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

    @Override
    public String toString() {
      return String.format("%s [\nparamDetectionTime=%d,\nparamDetectionInterval=%d,\nparamDetectionCount=%d,\n"
          + "paramNetworkOutageDelayMillis=%d,\nmin=%d,\nmax=%d,\navg=%d ]",
          super.toString(),
          this.paramDetectionTime,
          this.paramDetectionInterval,
          this.paramDetectionCount,
          this.paramNetworkOutageDelayMillis,
          this.minFailureDetectionTimeMillis,
          this.maxFailureDetectionTimeMillis,
          this.avgFailureDetectionTimeMillis);
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

    @Override
    public String toString() {
      return String.format("%s [\nparamSocketTimeout=%d,\nparamNetworkOutageDelayMillis=%d,\n"
              + "min=%d,\nmax=%d,\navg=%d ]",
          super.toString(),
          this.paramSocketTimeout,
          this.paramNetworkOutageDelayMillis,
          this.minFailureDetectionTimeMillis,
          this.maxFailureDetectionTimeMillis,
          this.avgFailureDetectionTimeMillis);
    }
  }
}
