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

package software.amazon.jdbc.benchmarks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.benchmarks.testplugin.TestConnectionWrapper;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@State(Scope.Benchmark)
@Fork(3)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PluginBenchmarks {

  private static final String WRITER_SESSION_ID = "MASTER_SESSION_ID";
  private static final String FIELD_SERVER_ID = "SERVER_ID";
  private static final String FIELD_SESSION_ID = "SESSION_ID";
  private static final String CONNECTION_STRING = "jdbc:postgresql://my.domain.com";
  private static final String PG_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://instance-0.XYZ.us-east-2.rds.amazonaws.com";
  private static final String TEST_HOST = "instance-0";
  private static final int TEST_PORT = 5432;
  private final HostSpec writerHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(TEST_HOST).port(TEST_PORT).build();

  @Mock private PluginService mockPluginService;
  @Mock private Dialect mockDialect;
  @Mock private ConnectionPluginManager mockConnectionPluginManager;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private PluginManagerService mockPluginManagerService;
  @Mock ConnectionProvider mockConnectionProvider;
  @Mock Connection mockConnection;
  @Mock Statement mockStatement;
  @Mock ResultSet mockResultSet;
  private AutoCloseable closeable;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(PluginBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Iteration)
  public void setUpIteration() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockConnectionPluginManager.connect(any(), any(), any(Properties.class), anyBoolean()))
        .thenReturn(mockConnection);
    when(mockConnectionPluginManager.execute(
        any(), any(), any(), eq("Connection.createStatement"), any(), any()))
        .thenReturn(mockStatement);
    when(mockConnectionPluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    // noinspection unchecked
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);
    when(mockConnectionProvider.connect(
        anyString(),
        any(Dialect.class),
        any(TargetDriverDialect.class),
        any(HostSpec.class),
        any(Properties.class))).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(eq(FIELD_SESSION_ID))).thenReturn(WRITER_SESSION_ID);
    when(mockResultSet.getString(eq(FIELD_SERVER_ID)))
        .thenReturn("instance-0", "instance-1");
    when(mockResultSet.getStatement()).thenReturn(mockStatement);
    when(mockStatement.getConnection()).thenReturn(mockConnection);
    when(this.mockPluginService.acceptsStrategy(any(), eq("random"))).thenReturn(true);
    when(this.mockPluginService.getCurrentHostSpec()).thenReturn(writerHostSpec);
    when(this.mockPluginService.getDialect()).thenReturn(mockDialect);
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration() throws Exception {
    closeable.close();
  }

  @Benchmark
  public void initAndReleaseBaseLine() {
  }

  @Benchmark
  public ConnectionWrapper initAndReleaseWithExecutionTimePlugin() throws SQLException {
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useExecutionTimePlugin(),
        CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService)) {
      wrapper.releaseResources();
      return wrapper;
    }
  }

  @Benchmark
  public ConnectionWrapper initAndReleaseWithAuroraHostListPlugin() throws SQLException {
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useAuroraHostListPlugin(),
        CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService)) {
      wrapper.releaseResources();
      return wrapper;
    }
  }

  @Benchmark
  public ConnectionWrapper initAndReleaseWithExecutionTimeAndAuroraHostListPlugins() throws SQLException {
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useExecutionTimeAndAuroraHostListPlugins(),
        CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService)) {
      wrapper.releaseResources();
      return wrapper;
    }
  }

  @Benchmark
  public ConnectionWrapper initAndReleaseWithReadWriteSplittingPlugin() throws SQLException {
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useReadWriteSplittingPlugin(),
        CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService)) {
      wrapper.releaseResources();
      return wrapper;
    }
  }

  @Benchmark
  public ConnectionWrapper initAndReleaseWithAuroraHostListAndReadWriteSplittingPlugin()
      throws SQLException {
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useAuroraHostListAndReadWriteSplittingPlugin(),
        PG_CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService)) {
      wrapper.releaseResources();
      return wrapper;
    }
  }

  @Benchmark
  public ConnectionWrapper initAndReleaseWithReadWriteSplittingPlugin_internalConnectionPools() throws SQLException {
    HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider((hostSpec, props) -> new HikariConfig());
    ConnectionProviderManager.setConnectionProvider(provider);
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useReadWriteSplittingPlugin(),
        CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService)) {
      wrapper.releaseResources();
      ConnectionProviderManager.releaseResources();
      ConnectionProviderManager.resetProvider();
      return wrapper;
    }
  }

  @Benchmark
  public ConnectionWrapper initAndReleaseWithAuroraHostListAndReadWriteSplittingPlugin_internalConnectionPools()
      throws SQLException {
    HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider((hostSpec, props) -> new HikariConfig());
    ConnectionProviderManager.setConnectionProvider(provider);
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useAuroraHostListAndReadWriteSplittingPlugin(),
        PG_CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService)) {
      wrapper.releaseResources();
      ConnectionProviderManager.releaseResources();
      ConnectionProviderManager.resetProvider();
      return wrapper;
    }
  }

  @Benchmark
  public Statement executeStatementBaseline() throws SQLException {
    try (ConnectionWrapper wrapper = new TestConnectionWrapper(
        useExecutionTimePlugin(),
        CONNECTION_STRING,
        mockConnectionPluginManager,
        mockTelemetryFactory,
        mockPluginService,
        mockHostListProviderService,
        mockPluginManagerService);
         Statement statement = wrapper.createStatement()) {
      return statement;
    }
  }

  @Benchmark
  public ResultSet executeStatementWithExecutionTimePlugin() throws SQLException {
    try (
        ConnectionWrapper wrapper = new TestConnectionWrapper(
            useExecutionTimePlugin(),
            CONNECTION_STRING,
            mockConnectionPluginManager,
            mockTelemetryFactory,
            mockPluginService,
            mockHostListProviderService,
            mockPluginManagerService);
        Statement statement = wrapper.createStatement();
        ResultSet resultSet = statement.executeQuery("some sql")) {
      return resultSet;
    }
  }

  @Benchmark
  public ResultSet executeStatementWithTelemetryDisabled() throws SQLException {
    try (
        ConnectionWrapper wrapper = new TestConnectionWrapper(
            disabledTelemetry(),
            CONNECTION_STRING,
            mockConnectionPluginManager,
            mockTelemetryFactory,
            mockPluginService,
            mockHostListProviderService,
            mockPluginManagerService);
        Statement statement = wrapper.createStatement();
        ResultSet resultSet = statement.executeQuery("some sql")) {
      return resultSet;
    }
  }

  @Benchmark
  public ResultSet executeStatementWithTelemetry() throws SQLException {
    try (
        ConnectionWrapper wrapper = new TestConnectionWrapper(
            useTelemetry(),
            CONNECTION_STRING,
            mockConnectionPluginManager,
            mockTelemetryFactory,
            mockPluginService,
            mockHostListProviderService,
            mockPluginManagerService);
        Statement statement = wrapper.createStatement();
        ResultSet resultSet = statement.executeQuery("some sql")) {
      return resultSet;
    }
  }

  Properties useExecutionTimePlugin() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "executionTime");
    return properties;
  }

  Properties useAuroraHostListPlugin() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "auroraHostList");
    return properties;
  }

  Properties useExecutionTimeAndAuroraHostListPlugins() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "executionTime,auroraHostList");
    return properties;
  }

  Properties useReadWriteSplittingPlugin() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "readWriteSplitting");
    return properties;
  }

  Properties useAuroraHostListAndReadWriteSplittingPlugin() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "auroraHostList,readWriteSplitting");
    return properties;
  }

  Properties useReadWriteSplittingPluginWithReaderLoadBalancing() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "readWriteSplitting");
    properties.setProperty("loadBalanceReadOnlyTraffic", "true");
    return properties;
  }

  Properties useAuroraHostListAndReadWriteSplittingPluginWithReaderLoadBalancing() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "auroraHostList,readWriteSplitting");
    properties.setProperty("loadBalanceReadOnlyTraffic", "true");
    return properties;
  }

  Properties useTelemetry() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "dataCache,auroraHostList,efm2");
    properties.setProperty("enableTelemetry", "true");
    properties.setProperty("telemetryMetricsBackend", "none");
    properties.setProperty("telemetryTracesBackend", "none");
    return properties;
  }

  Properties disabledTelemetry() {
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "dataCache,auroraHostList,efm2");
    properties.setProperty("enableTelemetry", "false");
    return properties;
  }
}
