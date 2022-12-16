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

import static org.apache.commons.math3.util.Precision.round;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.PGProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.util.StringUtils;

public class AuroraPostgresReadWriteSplittingPerformanceTest extends AuroraPostgresBaseTest {

  private static final int REPEAT_TIMES = StringUtils.isNullOrEmpty(System.getenv("REPEAT_TIMES"))
      ? 100
      : Integer.parseInt(System.getenv("REPEAT_TIMES"));

  private static final int EXECUTE_QUERY_TIMES = StringUtils.isNullOrEmpty(System.getenv("EXECUTE_QUERY_TIMES"))
      ? 1000
      : Integer.parseInt(System.getenv("EXECUTE_QUERY_TIMES"));

  private static final double NANOS_TO_MILLIS = (double) TimeUnit.MILLISECONDS.toNanos(1);

  private static final List<PerfStatSwitchConnection> setReadOnlyPerfDataList = new ArrayList<>();
  private static final List<PerfStatExecuteQueries> createStatementPerfDataList = new ArrayList<>();

  private static Stream<Arguments> createStatementParameters() {
    return Stream.of(
        Arguments.of(initReadWritePluginProps()),
        Arguments.of(initNoPluginPropsWithTimeouts())
    );
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    doWritePerfDataToFile(
        "./build/reports/tests/PostgresSQL_ReadWriteSplittingPerformanceResults_SwitchReaderWriterConnection.xlsx",
        setReadOnlyPerfDataList);
    doWritePerfDataToFile(
        "./build/reports/tests/PostgresSQL_ReadWriteSplittingPerformanceResults_ReaderLoadBalancing.xlsx",
        createStatementPerfDataList);
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
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    return props;
  }

  Result getSetReadOnlyResults(final Properties props)
      throws SQLException {
    long switchToReaderStartTime;
    final List<Long> elapsedSwitchToReaderTimes = new ArrayList<>(REPEAT_TIMES);
    long switchToWriterStartTime;
    final List<Long> elapsedSwitchToWriterTimes = new ArrayList<>(REPEAT_TIMES);
    final Result result = new Result();

    for (int i = 0; i < REPEAT_TIMES; i++) {

      try (final Connection conn = connectToInstance(POSTGRES_CLUSTER_URL, AURORA_POSTGRES_PORT, props)) {

        switchToReaderStartTime = System.nanoTime();
        conn.setReadOnly(true);
        final long switchToReaderElapsedTime = (System.nanoTime() - switchToReaderStartTime);
        elapsedSwitchToReaderTimes.add(switchToReaderElapsedTime);

        switchToWriterStartTime = System.nanoTime();
        conn.setReadOnly(false);
        final long switchToWriterElapsedTime = (System.nanoTime() - switchToWriterStartTime);
        elapsedSwitchToWriterTimes.add(switchToWriterElapsedTime);
      }
    }

    final LongSummaryStatistics switchToReaderStats =
        elapsedSwitchToReaderTimes.stream().mapToLong(a -> a).summaryStatistics();
    result.switchToReaderMin = switchToReaderStats.getMin();
    result.switchToReaderMax = switchToReaderStats.getMax();
    result.switchToReaderAvg = (long) switchToReaderStats.getAverage();

    final LongSummaryStatistics switchToWriterStats =
        elapsedSwitchToWriterTimes.stream().mapToLong(a -> a).summaryStatistics();
    result.switchToWriterMin = switchToWriterStats.getMin();
    result.switchToWriterMax = switchToWriterStats.getMax();
    result.switchToWriterAvg = (long) switchToWriterStats.getAverage();

    return result;
  }

  @Test
  public void test_switchReaderWriterConnection()
      throws SQLException {
    // This test measures the time to switch connections between the reader and writer.
    final Properties propsWithoutPlugin = initNoPluginPropsWithTimeouts();
    Result resultsWithoutPlugin = getSetReadOnlyResults(propsWithoutPlugin);

    Properties propsWithPlugin = initReadWritePluginProps();
    Result resultsWithPlugin = getSetReadOnlyResults(propsWithPlugin);

    final long switchToReaderMinOverhead =
        resultsWithPlugin.switchToReaderMin - resultsWithoutPlugin.switchToReaderMin;
    final long switchToReaderMaxOverhead =
        resultsWithPlugin.switchToReaderMax - resultsWithoutPlugin.switchToReaderMax;
    final long switchToReaderAvgOverhead =
        resultsWithPlugin.switchToReaderAvg - resultsWithoutPlugin.switchToReaderAvg;

    final long switchToWriterMinOverhead =
        resultsWithPlugin.switchToWriterMin - resultsWithoutPlugin.switchToWriterMin;
    final long switchToWriterMaxOverhead =
        resultsWithPlugin.switchToWriterMax - resultsWithoutPlugin.switchToWriterMax;
    final long switchToWriterAvgOverhead =
        resultsWithPlugin.switchToWriterAvg - resultsWithoutPlugin.switchToWriterAvg;

    final PerfStatSwitchConnection connectReaderData = new PerfStatSwitchConnection();
    connectReaderData.connectionSwitch = "Switch to reader (open new connection)";
    connectReaderData.minOverheadTime = TimeUnit.NANOSECONDS.toMillis(switchToReaderMinOverhead);
    connectReaderData.maxOverheadTime = TimeUnit.NANOSECONDS.toMillis(switchToReaderMaxOverhead);
    connectReaderData.avgOverheadTime = TimeUnit.NANOSECONDS.toMillis(switchToReaderAvgOverhead);
    setReadOnlyPerfDataList.add(connectReaderData);

    final PerfStatSwitchConnection connectWriterData = new PerfStatSwitchConnection();
    connectWriterData.connectionSwitch = "Switch back to writer (use cached connection)";
    connectWriterData.minOverheadTime = TimeUnit.NANOSECONDS.toMillis(switchToWriterMinOverhead);
    connectWriterData.maxOverheadTime = TimeUnit.NANOSECONDS.toMillis(switchToWriterMaxOverhead);
    connectWriterData.avgOverheadTime = TimeUnit.NANOSECONDS.toMillis(switchToWriterAvgOverhead);
    setReadOnlyPerfDataList.add(connectWriterData);
  }

  @ParameterizedTest
  @MethodSource("createStatementParameters")
  public void test_readerLoadBalancing_createStatement(final Properties props)
      throws SQLException {
    // This test isolates how much overhead is caused by reader load-balancing.
    long readerSwitchCreateStatementStartTime;
    final List<Long> elapsedReaderSwitchCreateStatementTimes = new ArrayList<>(REPEAT_TIMES);
    final PerfStatExecuteQueries results = new PerfStatExecuteQueries();

    final String plugins = props.getProperty(PropertyDefinition.PLUGINS.name);

    if (plugins != null && plugins.contains("readWriteSplitting")) {
      results.pluginEnabled = "Enabled (load balances readers)";
      props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    } else {
      results.pluginEnabled = "Disabled (no load balancing)";
    }

    for (int i = 0; i < REPEAT_TIMES; i++) {
      try (final Connection conn = connectToInstance(
          POSTGRES_CLUSTER_URL,
          AURORA_POSTGRES_PORT,
          props)) {

        conn.setReadOnly(true);
        try (final Statement stmt1 = conn.createStatement()) {
          stmt1.executeQuery(QUERY_FOR_INSTANCE);
        }

        // The plugin does not switch readers on the first execute, so we'll start the timer after
        for (int j = 0; j < EXECUTE_QUERY_TIMES; j++) {
          // timer start
          readerSwitchCreateStatementStartTime = System.nanoTime();
          try (Statement stmt2 = conn.createStatement()) {

            // timer end
            final long readerSwitchCreateStatementTime =
                (System.nanoTime() - readerSwitchCreateStatementStartTime);
            elapsedReaderSwitchCreateStatementTimes.add(readerSwitchCreateStatementTime);

            stmt2.executeQuery(QUERY_FOR_INSTANCE);
          }
        }
      }
    }

    final LongSummaryStatistics createStatementStats =
        elapsedReaderSwitchCreateStatementTimes.stream().mapToLong(a -> a).summaryStatistics();
    results.minAverageCreateStatementTime = round(createStatementStats.getMin() / NANOS_TO_MILLIS, 4);
    results.maxAverageCreateStatementTime = round(createStatementStats.getMax() / NANOS_TO_MILLIS, 4);
    results.avgCreateStatementTime = round(createStatementStats.getAverage() / NANOS_TO_MILLIS, 4);
    createStatementPerfDataList.add(results);
  }

  private static class Result {
    public long switchToReaderMin;
    public long switchToReaderMax;
    public long switchToReaderAvg;

    public long switchToWriterMin;
    public long switchToWriterMax;
    public long switchToWriterAvg;
  }

  private abstract static class PerfStatBase {

    public abstract void writeHeader(Row row);

    public abstract void writeData(Row row);
  }

  private class PerfStatSwitchConnection extends PerfStatBase {

    public String connectionSwitch;
    public long minOverheadTime;
    public long maxOverheadTime;
    public long avgOverheadTime;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("");
      cell = row.createCell(1);
      cell.setCellValue("minOverheadTimeMillis");
      cell = row.createCell(2);
      cell.setCellValue("maxOverheadTimeMillis");
      cell = row.createCell(3);
      cell.setCellValue("avgOverheadTimeMillis");
    }

    @Override
    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.connectionSwitch);
      cell = row.createCell(1);
      cell.setCellValue(this.minOverheadTime);
      cell = row.createCell(2);
      cell.setCellValue(this.maxOverheadTime);
      cell = row.createCell(3);
      cell.setCellValue(this.avgOverheadTime);
    }
  }

  private class PerfStatExecuteQueries extends PerfStatBase {

    public String pluginEnabled;
    public double minAverageCreateStatementTime;
    public double maxAverageCreateStatementTime;
    public double avgCreateStatementTime;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("readWriteSplittingPlugin");
      cell = row.createCell(1);
      cell.setCellValue("minAverageCreateStatementTimeMillis");
      cell = row.createCell(2);
      cell.setCellValue("maxAverageCreateStatementTimeMillis");
      cell = row.createCell(3);
      cell.setCellValue("avgCreateStatementTimeMillis");
    }

    @Override
    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.pluginEnabled);
      cell = row.createCell(1);
      cell.setCellValue(this.minAverageCreateStatementTime);
      cell = row.createCell(2);
      cell.setCellValue(this.maxAverageCreateStatementTime);
      cell = row.createCell(3);
      cell.setCellValue(this.avgCreateStatementTime);
    }
  }
}
