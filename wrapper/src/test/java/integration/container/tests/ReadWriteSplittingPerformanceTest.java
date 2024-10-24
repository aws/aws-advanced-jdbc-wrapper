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

import com.zaxxer.hikari.HikariConfig;
import integration.TestDatabaseInfo;
import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
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
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.ConnectTimeConnectionPlugin;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPlugin;
import software.amazon.jdbc.util.StringUtils;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.PERFORMANCE)
@EnableOnNumOfInstances(min = 5)
@Tag("rw-splitting")
@Order(11)
public class ReadWriteSplittingPerformanceTest {

  private static final Logger LOGGER =
      Logger.getLogger(ReadWriteSplittingPerformanceTest.class.getName());

  private static final int REPEAT_TIMES = StringUtils.isNullOrEmpty(System.getenv("REPEAT_TIMES"))
      ? 10
      : Integer.parseInt(System.getenv("REPEAT_TIMES"));

  private static final int TIMEOUT_SEC = 5;
  private static final int CONNECT_TIMEOUT_SEC = 5;

  private static final List<PerfStatSwitchConnection> setReadOnlyPerfDataList = new ArrayList<>();

  @TestTemplate
  public void test_switchReaderWriterConnection()
      throws SQLException, IOException {

    setReadOnlyPerfDataList.clear();

    // This test measures the time to switch connections between the reader and writer.
    final Properties propsWithoutPlugin = initNoPluginPropsWithTimeouts();
    final Result resultsWithoutPlugin = getSetReadOnlyResults(propsWithoutPlugin);
    final Properties propsWithPlugin = initReadWritePluginProps();
    final Result resultsWithPlugin = getSetReadOnlyResults(propsWithPlugin);

    final HikariPooledConnectionProvider connProvider =
        new HikariPooledConnectionProvider((hostSpec, props) -> new HikariConfig());
    ConnectionProviderManager.setConnectionProvider(connProvider);
    Result resultsWithPools = getSetReadOnlyResults(propsWithPlugin);
    ConnectionProviderManager.releaseResources();
    ConnectionProviderManager.resetProvider();

    final long switchToReaderMinOverhead =
        resultsWithPlugin.switchToReaderMin - resultsWithoutPlugin.switchToReaderMin;
    final long switchToReaderMaxOverhead =
        resultsWithPlugin.switchToReaderMax - resultsWithoutPlugin.switchToReaderMax;
    final long switchToReaderAvgOverhead =
        resultsWithPlugin.switchToReaderAvg - resultsWithoutPlugin.switchToReaderAvg;
    final long switchToReaderAvgOverheadWithConnect =
        resultsWithPlugin.switchToReaderWithConnectTimeAvg - resultsWithoutPlugin.switchToReaderWithConnectTimeAvg;

    final long switchToWriterMinOverhead =
        resultsWithPlugin.switchToWriterMin - resultsWithoutPlugin.switchToWriterMin;
    final long switchToWriterMaxOverhead =
        resultsWithPlugin.switchToWriterMax - resultsWithoutPlugin.switchToWriterMax;
    final long switchToWriterAvgOverhead =
        resultsWithPlugin.switchToWriterAvg - resultsWithoutPlugin.switchToWriterAvg;
    final long switchToWriterAvgOverheadWithConnect =
        resultsWithPlugin.switchToWriterWithConnectTimeAvg - resultsWithoutPlugin.switchToWriterWithConnectTimeAvg;

    final PerfStatSwitchConnection connectReaderData = new PerfStatSwitchConnection();
    connectReaderData.connectionSwitch = "Switch to reader";
    connectReaderData.minOverheadTime = switchToReaderMinOverhead;
    connectReaderData.maxOverheadTime = switchToReaderMaxOverhead;
    connectReaderData.avgOverheadTime = switchToReaderAvgOverhead;
    connectReaderData.avgOverheadIncludingConnectTime = switchToReaderAvgOverheadWithConnect;
    setReadOnlyPerfDataList.add(connectReaderData);

    final PerfStatSwitchConnection connectWriterData = new PerfStatSwitchConnection();
    connectWriterData.connectionSwitch = "Switch back to writer (use cached connection)";
    connectWriterData.minOverheadTime = switchToWriterMinOverhead;
    connectWriterData.maxOverheadTime = switchToWriterMaxOverhead;
    connectWriterData.avgOverheadTime = switchToWriterAvgOverhead;
    connectWriterData.avgOverheadIncludingConnectTime = switchToWriterAvgOverheadWithConnect;
    setReadOnlyPerfDataList.add(connectWriterData);

    doWritePerfDataToFile(
        String.format(
            "./build/reports/tests/"
                + "DbEngine_%s_Driver_%s_ReadWriteSplittingPerformanceResults_"
                + "SwitchReaderWriterConnection.xlsx",
            TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
            TestEnvironment.getCurrent().getCurrentDriver())
    );

    setReadOnlyPerfDataList.clear();

    // internal connection pool results
    long connPoolSwitchToReaderMinOverhead =
        resultsWithPools.switchToReaderMin - resultsWithoutPlugin.switchToReaderMin;
    long connPoolSwitchToReaderMaxOverhead =
        resultsWithPools.switchToReaderMax - resultsWithoutPlugin.switchToReaderMax;
    long connPoolSwitchToReaderAvgOverhead =
        resultsWithPools.switchToReaderAvg - resultsWithoutPlugin.switchToReaderAvg;
    long connPoolSwitchToReaderAvgOverheadWithConnect =
        resultsWithPools.switchToReaderWithConnectTimeAvg - resultsWithoutPlugin.switchToReaderWithConnectTimeAvg;

    long connPoolSwitchToWriterMinOverhead =
        resultsWithPools.switchToWriterMin - resultsWithoutPlugin.switchToWriterMin;
    long connPoolSwitchToWriterMaxOverhead =
        resultsWithPools.switchToWriterMax - resultsWithoutPlugin.switchToWriterMax;
    long connPoolSwitchToWriterAvgOverhead =
        resultsWithPools.switchToWriterAvg - resultsWithoutPlugin.switchToWriterAvg;
    long connPoolSwitchToWriterAvgOverheadWithConnect =
        resultsWithPools.switchToWriterWithConnectTimeAvg - resultsWithoutPlugin.switchToWriterWithConnectTimeAvg;

    PerfStatSwitchConnection connPoolsConnectReaderData = new PerfStatSwitchConnection();
    connPoolsConnectReaderData.connectionSwitch = "Switch to reader";
    connPoolsConnectReaderData.minOverheadTime = connPoolSwitchToReaderMinOverhead;
    connPoolsConnectReaderData.maxOverheadTime = connPoolSwitchToReaderMaxOverhead;
    connPoolsConnectReaderData.avgOverheadTime = connPoolSwitchToReaderAvgOverhead;
    connPoolsConnectReaderData.avgOverheadIncludingConnectTime = connPoolSwitchToReaderAvgOverheadWithConnect;
    setReadOnlyPerfDataList.add(connPoolsConnectReaderData);

    PerfStatSwitchConnection connPoolsConnectWriterData = new PerfStatSwitchConnection();
    connPoolsConnectWriterData.connectionSwitch = "Switch back to writer (use cached connection)";
    connPoolsConnectWriterData.minOverheadTime = connPoolSwitchToWriterMinOverhead;
    connPoolsConnectWriterData.maxOverheadTime = connPoolSwitchToWriterMaxOverhead;
    connPoolsConnectWriterData.avgOverheadTime = connPoolSwitchToWriterAvgOverhead;
    connPoolsConnectWriterData.avgOverheadIncludingConnectTime = connPoolSwitchToWriterAvgOverheadWithConnect;
    setReadOnlyPerfDataList.add(connPoolsConnectWriterData);

    doWritePerfDataToFile(
        String.format(
            "./build/reports/tests/"
                + "DbEngine_%s_Driver_%s_ReadWriteSplittingPerformanceResults_"
                + "InternalConnectionPools_SwitchReaderWriterConnection.xlsx",
            TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
            TestEnvironment.getCurrent().getCurrentDriver())
    );

    // internal connection pool results with warm up

    final HikariPooledConnectionProvider connProvider2 =
        new HikariPooledConnectionProvider((hostSpec, props) -> new HikariConfig());
    ConnectionProviderManager.setConnectionProvider(connProvider);
    // Initialize a pool for each instance in the topology
    final TestDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getDatabaseInfo();
    for (TestInstanceInfo instance : info.getInstances()) {
      try (final Connection conn = DriverManager.getConnection(
          ConnectionStringHelper.getUrl(instance.getHost(), instance.getPort(), info.getDefaultDbName()),
          propsWithPlugin)) {
        // do nothing
      }
    }

    resultsWithPools = getSetReadOnlyResults(propsWithPlugin);
    ConnectionProviderManager.releaseResources();
    ConnectionProviderManager.resetProvider();

    connPoolSwitchToReaderMinOverhead =
        resultsWithPools.switchToReaderMin - resultsWithoutPlugin.switchToReaderMin;
    connPoolSwitchToReaderMaxOverhead =
        resultsWithPools.switchToReaderMax - resultsWithoutPlugin.switchToReaderMax;
    connPoolSwitchToReaderAvgOverhead =
        resultsWithPools.switchToReaderAvg - resultsWithoutPlugin.switchToReaderAvg;
    connPoolSwitchToReaderAvgOverheadWithConnect =
        resultsWithPools.switchToReaderWithConnectTimeAvg - resultsWithoutPlugin.switchToReaderWithConnectTimeAvg;

    connPoolSwitchToWriterMinOverhead =
        resultsWithPools.switchToWriterMin - resultsWithoutPlugin.switchToWriterMin;
    connPoolSwitchToWriterMaxOverhead =
        resultsWithPools.switchToWriterMax - resultsWithoutPlugin.switchToWriterMax;
    connPoolSwitchToWriterAvgOverhead =
        resultsWithPools.switchToWriterAvg - resultsWithoutPlugin.switchToWriterAvg;
    connPoolSwitchToWriterAvgOverheadWithConnect =
        resultsWithPools.switchToWriterWithConnectTimeAvg - resultsWithoutPlugin.switchToWriterWithConnectTimeAvg;

    connPoolsConnectReaderData = new PerfStatSwitchConnection();
    connPoolsConnectReaderData.connectionSwitch = "Switch to reader";
    connPoolsConnectReaderData.minOverheadTime = connPoolSwitchToReaderMinOverhead;
    connPoolsConnectReaderData.maxOverheadTime = connPoolSwitchToReaderMaxOverhead;
    connPoolsConnectReaderData.avgOverheadTime = connPoolSwitchToReaderAvgOverhead;
    connPoolsConnectReaderData.avgOverheadIncludingConnectTime = connPoolSwitchToReaderAvgOverheadWithConnect;
    setReadOnlyPerfDataList.add(connPoolsConnectReaderData);

    connPoolsConnectWriterData = new PerfStatSwitchConnection();
    connPoolsConnectWriterData.connectionSwitch = "Switch back to writer (use cached connection)";
    connPoolsConnectWriterData.minOverheadTime = connPoolSwitchToWriterMinOverhead;
    connPoolsConnectWriterData.maxOverheadTime = connPoolSwitchToWriterMaxOverhead;
    connPoolsConnectWriterData.avgOverheadTime = connPoolSwitchToWriterAvgOverhead;
    connPoolsConnectWriterData.avgOverheadIncludingConnectTime = connPoolSwitchToWriterAvgOverheadWithConnect;
    setReadOnlyPerfDataList.add(connPoolsConnectWriterData);

    doWritePerfDataToFile(
        String.format(
            "./build/reports/tests/"
                + "DbEngine_%s_Driver_%s_ReadWriteSplittingPerformanceResults_"
                + "InternalConnectionPools_WithWarmUp_SwitchReaderWriterConnection.xlsx",
            TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
            TestEnvironment.getCurrent().getCurrentDriver())
    );
  }

  private Connection connectToInstance(final Properties props) throws SQLException {
    final String url = ConnectionStringHelper.getWrapperClusterEndpointUrl();
    return DriverManager.getConnection(url, props);
  }

  private void doWritePerfDataToFile(final String fileName) throws IOException {
    if (setReadOnlyPerfDataList.isEmpty()) {
      return;
    }

    LOGGER.finest(() -> "File name: " + fileName);

    try (XSSFWorkbook workbook = new XSSFWorkbook()) {

      final XSSFSheet sheet = workbook.createSheet("PerformanceResults");

      for (int rows = 0; rows < setReadOnlyPerfDataList.size(); rows++) {
        final PerfStatBase perfStat = ((List<? extends PerfStatBase>) setReadOnlyPerfDataList).get(rows);
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

  protected Properties initNoPluginPropsWithTimeouts() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.CONNECT_TIMEOUT.set(props, String.valueOf(TimeUnit.SECONDS.toMillis(CONNECT_TIMEOUT_SEC)));
    PropertyDefinition.SOCKET_TIMEOUT.set(props, String.valueOf(TimeUnit.SECONDS.toMillis(TIMEOUT_SEC)));
    props.setProperty(PropertyDefinition.PLUGINS.name, "connectTime,executionTime");

    return props;
  }

  protected Properties initReadWritePluginProps() {
    final Properties props = initNoPluginPropsWithTimeouts();
    props.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,connectTime,executionTime");
    return props;
  }

  private Result getSetReadOnlyResults(final Properties props) throws SQLException {

    long switchToReaderStartTime;
    final List<Long> elapsedSwitchToReaderTimes = new ArrayList<>(REPEAT_TIMES);
    final List<Long> elapsedSwitchToReaderWithConnectTimes = new ArrayList<>(REPEAT_TIMES);
    long switchToWriterStartTime;
    final List<Long> elapsedSwitchToWriterTimes = new ArrayList<>(REPEAT_TIMES);
    final List<Long> elapsedSwitchToWriterWithConnectTimes = new ArrayList<>(REPEAT_TIMES);
    final List<Long> elapsedConnectTimes = new ArrayList<>(REPEAT_TIMES);
    final Result result = new Result();

    for (int i = 0; i < REPEAT_TIMES; i++) {

      try (final Connection conn = connectToInstance(props)) {

        ConnectTimeConnectionPlugin.resetConnectTime();
        ExecutionTimeConnectionPlugin.resetExecutionTime();
        switchToReaderStartTime = System.nanoTime();
        conn.setReadOnly(true);
        long connectTime = ConnectTimeConnectionPlugin.getTotalConnectTime();
        long executionTime = ExecutionTimeConnectionPlugin.getTotalExecutionTime();

        final long switchToReaderElapsedTime = (System.nanoTime() - switchToReaderStartTime);

        elapsedSwitchToReaderTimes.add(switchToReaderElapsedTime - connectTime - executionTime);
        elapsedSwitchToReaderWithConnectTimes.add(switchToReaderElapsedTime - executionTime);
        elapsedConnectTimes.add(connectTime);

        switchToWriterStartTime = System.nanoTime();
        ConnectTimeConnectionPlugin.resetConnectTime();
        ExecutionTimeConnectionPlugin.resetExecutionTime();
        conn.setReadOnly(false);
        connectTime = ConnectTimeConnectionPlugin.getTotalConnectTime();
        executionTime = ExecutionTimeConnectionPlugin.getTotalExecutionTime();

        final long switchToWriterElapsedTime =
            (System.nanoTime() - switchToWriterStartTime - connectTime - executionTime);

        elapsedSwitchToWriterTimes.add(switchToWriterElapsedTime);
        elapsedSwitchToWriterWithConnectTimes.add(switchToWriterStartTime - executionTime);
      }
    }

    final LongSummaryStatistics switchToReaderStats =
        elapsedSwitchToReaderTimes.stream().mapToLong(a -> a).summaryStatistics();
    result.switchToReaderMin = switchToReaderStats.getMin();
    result.switchToReaderMax = switchToReaderStats.getMax();
    result.switchToReaderAvg = (long) switchToReaderStats.getAverage();
    result.switchToReaderWithConnectTimeAvg = (long) elapsedSwitchToReaderWithConnectTimes.stream().mapToLong(a -> a)
        .average().orElse(0);

    final LongSummaryStatistics switchToWriterStats =
        elapsedSwitchToWriterTimes.stream().mapToLong(a -> a).summaryStatistics();
    result.switchToWriterMin = switchToWriterStats.getMin();
    result.switchToWriterMax = switchToWriterStats.getMax();
    result.switchToWriterAvg = (long) switchToWriterStats.getAverage();
    result.switchToWriterWithConnectTimeAvg = (long) elapsedSwitchToWriterWithConnectTimes.stream().mapToLong(a -> a)
        .average().orElse(0);

    return result;
  }

  private static class Result {

    public long switchToReaderMin;
    public long switchToReaderMax;
    public long switchToReaderAvg;
    public long switchToReaderWithConnectTimeAvg;

    public long switchToWriterMin;
    public long switchToWriterMax;
    public long switchToWriterAvg;
    public long switchToWriterWithConnectTimeAvg;
  }

  private abstract static class PerfStatBase {

    public abstract void writeHeader(Row row);

    public abstract void writeData(Row row);
  }

  private static class PerfStatSwitchConnection extends PerfStatBase {

    public String connectionSwitch;
    public long minOverheadTime;
    public long maxOverheadTime;
    public long avgOverheadTime;
    public long avgOverheadIncludingConnectTime;

    @Override
    public void writeHeader(final Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("");
      cell = row.createCell(1);
      cell.setCellValue("minOverheadTimeNanos");
      cell = row.createCell(2);
      cell.setCellValue("maxOverheadTimeNanos");
      cell = row.createCell(3);
      cell.setCellValue("avgOverheadTimeNanos");
      cell.setCellValue("avgOverheadIncludingConnectTimeNanos");
    }

    @Override
    public void writeData(final Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.connectionSwitch);
      cell = row.createCell(1);
      cell.setCellValue(this.minOverheadTime);
      cell = row.createCell(2);
      cell.setCellValue(this.maxOverheadTime);
      cell = row.createCell(3);
      cell.setCellValue(this.avgOverheadTime);
      cell = row.createCell(4);
      cell.setCellValue(this.avgOverheadIncludingConnectTime);
    }
  }
}
