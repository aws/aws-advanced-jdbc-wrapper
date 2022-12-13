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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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

  private static final double NANO_TO_MILLIS_CONVERSION = 1E6;

  private static final List<PerfStatSetReadOnly> setReadOnlyPerfDataList = new ArrayList<>();
  private static final List<PerfStatExecuteQueries> executeStatementsPerfDataList = new ArrayList<>();

  private static Stream<Arguments> executeStatementsParameters() {
    return Stream.of(
        Arguments.of(initNoPluginPropsWithTimeouts()),
        Arguments.of(initReadWritePluginProps())
    );
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    doWritePerfDataToFile(
        "./build/reports/tests/PostgresSQL_ReadWriteSplittingPerformanceResults_SetReadOnly.xlsx",
        setReadOnlyPerfDataList);
    doWritePerfDataToFile(
        "./build/reports/tests/PostgresSQL_ReadWriteSplittingPerformanceResults_ReaderLoadBalancing_largedataset_nanoseconds.xlsx",
        executeStatementsPerfDataList);
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
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), "30");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), "30");

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
    Long setReadOnlyTrueStartTime;
    final List<Long> elapsedSetReadOnlyTrueTimes = new ArrayList<>(REPEAT_TIMES);
    Long setReadOnlyFalseStartTime;
    final List<Long> elapsedSetReadOnlyFalseTimes = new ArrayList<>(REPEAT_TIMES);
    final Result result = new Result();

    for (int i = 0; i < REPEAT_TIMES; i++) {

      try (final Connection conn = connectToInstance(POSTGRES_CLUSTER_URL, AURORA_POSTGRES_PORT, props)) {

        setReadOnlyTrueStartTime = System.nanoTime();
        conn.setReadOnly(true);
        final long setReadOnlyTrueElapsedTime = (System.nanoTime() - setReadOnlyTrueStartTime);
        elapsedSetReadOnlyTrueTimes.add(setReadOnlyTrueElapsedTime);

        setReadOnlyFalseStartTime = System.nanoTime();
        conn.setReadOnly(false);
        final long setReadOnlyFalseElapsedTime = (System.nanoTime() - setReadOnlyFalseStartTime);
        elapsedSetReadOnlyFalseTimes.add(setReadOnlyFalseElapsedTime);
      }
    }
    final long setReadOnlyTrueMin = elapsedSetReadOnlyTrueTimes.stream().min(Long::compare).orElse(0L);
    final long setReadOnlyTrueMax = elapsedSetReadOnlyTrueTimes.stream().max(Long::compare).orElse(0L);
    final long setReadOnlyTrueAvg =
        (long) elapsedSetReadOnlyTrueTimes.stream().mapToLong(a -> a).summaryStatistics().getAverage();

    final long setReadOnlyFalseMin = elapsedSetReadOnlyFalseTimes.stream().min(Long::compare).orElse(0L);
    final long setReadOnlyFalseMax = elapsedSetReadOnlyFalseTimes.stream().max(Long::compare).orElse(0L);
    final long setReadOnlyFalseAvg =
        (long) elapsedSetReadOnlyFalseTimes.stream().mapToLong(a -> a).summaryStatistics().getAverage();

    result.setReadOnlyTrueMin = setReadOnlyTrueMin;
    result.setReadOnlyTrueMax = setReadOnlyTrueMax;
    result.setReadOnlyTrueAvg = setReadOnlyTrueAvg;

    result.setReadOnlyFalseMin = setReadOnlyFalseMin;
    result.setReadOnlyFalseMax = setReadOnlyFalseMax;
    result.setReadOnlyFalseAvg = setReadOnlyFalseAvg;

    return result;
  }

  @Test
  public void test_setReadOnly()
      throws SQLException {
    // This test measures the time to switch connections between the reader and writer.
    final Properties propsWithoutPlugin = initNoPluginPropsWithTimeouts();
    Result resultsWithoutPlugin = getSetReadOnlyResults(propsWithoutPlugin);

    Properties propsWithPlugin = initReadWritePluginProps();
    Result resultsWithPlugin = getSetReadOnlyResults(propsWithPlugin);

    final long setReadOnlyTrueMinOverhead =
        resultsWithPlugin.setReadOnlyTrueMin - resultsWithoutPlugin.setReadOnlyTrueMin;
    final long setReadOnlyTrueMaxOverhead =
        resultsWithPlugin.setReadOnlyTrueMax - resultsWithoutPlugin.setReadOnlyTrueMax;
    final long setReadOnlyTrueAvgOverhead =
        resultsWithPlugin.setReadOnlyTrueAvg - resultsWithoutPlugin.setReadOnlyTrueAvg;

    final long setReadOnlyFalseMinOverhead =
        resultsWithPlugin.setReadOnlyFalseMin - resultsWithoutPlugin.setReadOnlyFalseMin;
    final long setReadOnlyFalseMaxOverhead =
        resultsWithPlugin.setReadOnlyFalseMax - resultsWithoutPlugin.setReadOnlyFalseMax;
    final long setReadOnlyFalseAvgOverhead =
        resultsWithPlugin.setReadOnlyFalseAvg - resultsWithoutPlugin.setReadOnlyFalseAvg;

    final PerfStatSetReadOnly dataTrue = new PerfStatSetReadOnly();
    dataTrue.setReadOnly = "True";
    dataTrue.minOverheadTime = TimeUnit.NANOSECONDS.toMillis(setReadOnlyTrueMinOverhead);
    dataTrue.maxOverheadTime = TimeUnit.NANOSECONDS.toMillis(setReadOnlyTrueMaxOverhead);
    dataTrue.avgOverheadTime = TimeUnit.NANOSECONDS.toMillis(setReadOnlyTrueAvgOverhead);
    setReadOnlyPerfDataList.add(dataTrue);

    final PerfStatSetReadOnly dataFalse = new PerfStatSetReadOnly();
    dataFalse.setReadOnly = "False";
    dataFalse.minOverheadTime = TimeUnit.NANOSECONDS.toMillis(setReadOnlyFalseMinOverhead);
    dataFalse.maxOverheadTime = TimeUnit.NANOSECONDS.toMillis(setReadOnlyFalseMaxOverhead);
    dataFalse.avgOverheadTime = TimeUnit.NANOSECONDS.toMillis(setReadOnlyFalseAvgOverhead);
    setReadOnlyPerfDataList.add(dataFalse);
  }

  @ParameterizedTest
  @MethodSource("executeStatementsParameters")
  public void test_readerLoadBalancing_executeStatements(final Properties props)
      throws SQLException {
    // This test isolates how much overhead is caused by reader load-balancing.
    Long readerSwitchExecuteStatementsStartTime;
    final List<Long> elapsedReaderSwitchExecuteStatementsTimes = new ArrayList<>(REPEAT_TIMES);
    final PerfStatExecuteQueries data = new PerfStatExecuteQueries();

    final String plugins = props.getProperty(PropertyDefinition.PLUGINS.name);

    if (plugins != null && plugins.contains("readWriteSplitting")) {
      data.pluginEnabled = "Enabled";
      props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
    } else {
      data.pluginEnabled = "Disabled";
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
          readerSwitchExecuteStatementsStartTime = System.nanoTime();
          try (Statement stmt2 = conn.createStatement()) {

            // timer end
            final long readerSwitchExecuteStatementsTime =
                (System.nanoTime() - readerSwitchExecuteStatementsStartTime);
            elapsedReaderSwitchExecuteStatementsTimes.add(readerSwitchExecuteStatementsTime);

            stmt2.executeQuery(QUERY_FOR_INSTANCE);
          }
        }
      }
    }

    final long minAverageExecuteStatementTime =
        elapsedReaderSwitchExecuteStatementsTimes.stream().min(Long::compare).orElse(0L);
    final long maxAverageReaderSwitchExecuteStatementTime =
        elapsedReaderSwitchExecuteStatementsTimes.stream().max(Long::compare).orElse(0L);
    final long avgAverageExecuteStatementTime =
        (long) elapsedReaderSwitchExecuteStatementsTimes.stream().mapToLong(a -> a).summaryStatistics().getAverage();

    data.minAverageExecuteStatementTime = TimeUnit.NANOSECONDS.toNanos(minAverageExecuteStatementTime);
    data.maxAverageExecuteStatementTime = TimeUnit.NANOSECONDS.toNanos(maxAverageReaderSwitchExecuteStatementTime);
    data.avgExecuteStatementTime = TimeUnit.NANOSECONDS.toNanos(avgAverageExecuteStatementTime);
    executeStatementsPerfDataList.add(data);
  }

  private class Result {
    public long setReadOnlyTrueMin;
    public long setReadOnlyTrueMax;
    public long setReadOnlyTrueAvg;

    public long setReadOnlyFalseMin;
    public long setReadOnlyFalseMax;
    public long setReadOnlyFalseAvg;
  }

  private abstract class PerfStatBase {

    public abstract void writeHeader(Row row);

    public abstract void writeData(Row row);
  }

  private class PerfStatSetReadOnly extends PerfStatBase {

    public String setReadOnly;
    public double minOverheadTime;
    public double maxOverheadTime;
    public double avgOverheadTime;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("setReadOnly");
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
      cell.setCellValue(this.setReadOnly);
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
    public long minAverageExecuteStatementTime;
    public long maxAverageExecuteStatementTime;
    public long avgExecuteStatementTime;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("readWriteSplittingPlugin");
      cell = row.createCell(1);
      cell.setCellValue("minAverageExecuteStatementTime");
      cell = row.createCell(2);
      cell.setCellValue("maxAverageExecuteStatementTime");
      cell = row.createCell(3);
      cell.setCellValue("avgExecuteStatementTime");
    }

    @Override
    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.pluginEnabled);
      cell = row.createCell(1);
      cell.setCellValue(this.minAverageExecuteStatementTime);
      cell = row.createCell(2);
      cell.setCellValue(this.maxAverageExecuteStatementTime);
      cell = row.createCell(3);
      cell.setCellValue(this.avgExecuteStatementTime);
    }
  }
}
