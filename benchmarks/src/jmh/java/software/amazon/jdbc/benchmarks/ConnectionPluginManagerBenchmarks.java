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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
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
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.benchmarks.testplugin.BenchmarkPluginFactory;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@State(Scope.Benchmark)
@Fork(3)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConnectionPluginManagerBenchmarks {

  private static final String WRITER_SESSION_ID = "MASTER_SESSION_ID";
  private static final String FIELD_SERVER_ID = "SERVER_ID";
  private static final String FIELD_SESSION_ID = "SESSION_ID";
  private Properties propertiesWithoutPlugins;
  private Properties propertiesWithPlugins;
  private ConnectionPluginManager pluginManager;
  private ConnectionPluginManager pluginManagerWithNoPlugins;

  @Mock ConnectionProvider mockConnectionProvider;
  @Mock ConnectionWrapper mockConnectionWrapper;
  @Mock PluginService mockPluginService;
  @Mock PluginManagerService mockPluginManagerService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock HostListProviderService mockHostListProvider;
  @Mock Connection mockConnection;
  @Mock Statement mockStatement;
  @Mock ResultSet mockResultSet;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;
  ConfigurationProfile configurationProfile;
  private AutoCloseable closeable;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(software.amazon.jdbc.benchmarks.PluginBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Iteration)
  public void setUpIteration() throws Exception {
    closeable = openMocks(this);

    when(mockConnectionProvider.connect(
        anyString(),
        any(Dialect.class),
        any(TargetDriverDialect.class),
        any(HostSpec.class),
        any(Properties.class))).thenReturn(mockConnection);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(eq(FIELD_SESSION_ID))).thenReturn(WRITER_SESSION_ID);
    when(mockResultSet.getString(eq(FIELD_SERVER_ID)))
        .thenReturn("myInstance1.domain.com", "myInstance2.domain.com", "myInstance3.domain.com");
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);

    // Create a plugin chain with 10 custom test plugins.
    final List<Class<? extends ConnectionPluginFactory>> pluginFactories = new ArrayList<>(
        Collections.nCopies(10, BenchmarkPluginFactory.class));

    configurationProfile = ConfigurationProfileBuilder.get()
        .withName("benchmark")
        .withPluginFactories(pluginFactories)
        .build();

    propertiesWithoutPlugins = new Properties();
    propertiesWithoutPlugins.setProperty(PropertyDefinition.PLUGINS.name, "");

    propertiesWithPlugins = new Properties();
    propertiesWithPlugins.setProperty(PropertyDefinition.PROFILE_NAME.name, "benchmark");
    propertiesWithPlugins.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, "false");

    TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(propertiesWithPlugins);

    pluginManager = new ConnectionPluginManager(mockConnectionProvider,
        null,
        mockConnectionWrapper,
        telemetryFactory);
    pluginManager.init(mockPluginService, propertiesWithPlugins, mockPluginManagerService, configurationProfile);

    pluginManagerWithNoPlugins = new ConnectionPluginManager(mockConnectionProvider, null,
        mockConnectionWrapper, telemetryFactory);
    pluginManagerWithNoPlugins.init(mockPluginService, propertiesWithoutPlugins, mockPluginManagerService, null);
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration() throws Exception {
    closeable.close();
  }

  @Benchmark
  public ConnectionPluginManager initConnectionPluginManagerWithNoPlugins() throws SQLException {
    final ConnectionPluginManager manager = new ConnectionPluginManager(mockConnectionProvider, null,
        mockConnectionWrapper, mockTelemetryFactory);
    manager.init(mockPluginService, propertiesWithoutPlugins, mockPluginManagerService, configurationProfile);
    return manager;
  }

  @Benchmark
  public ConnectionPluginManager initConnectionPluginManagerWithPlugins() throws SQLException {
    final ConnectionPluginManager manager = new ConnectionPluginManager(mockConnectionProvider, null,
        mockConnectionWrapper, mockTelemetryFactory);
    manager.init(mockPluginService, propertiesWithPlugins, mockPluginManagerService, configurationProfile);
    return manager;
  }

  @Benchmark
  public Connection connectWithPlugins() throws SQLException {
    return pluginManager.connect(
        "driverProtocol",
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host").build(),
        propertiesWithPlugins,
        true);
  }

  @Benchmark
  public Connection connectWithNoPlugins() throws SQLException {
    return pluginManagerWithNoPlugins.connect(
        "driverProtocol",
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host").build(),
        propertiesWithoutPlugins,
        true);
  }

  @Benchmark
  public Integer executeWithPlugins() {
    return pluginManager.execute(
        int.class,
        RuntimeException.class,
        mockStatement,
        "Statement.execute",
        () -> 1,
        new Object[] {1}
    );
  }

  @Benchmark
  public Integer executeWithNoPlugins() {
    return pluginManagerWithNoPlugins.execute(
        int.class,
        RuntimeException.class,
        mockStatement,
        "Statement.execute",
        () -> 1,
        new Object[] {1}
    );
  }

  @Benchmark
  public ConnectionPluginManager initHostProvidersWithPlugins() throws SQLException {
    pluginManager.initHostProvider(
        "protocol",
        "url",
        propertiesWithPlugins,
        mockHostListProvider);
    return pluginManager;
  }

  @Benchmark
  public ConnectionPluginManager initHostProvidersWithNoPlugins() throws SQLException {
    pluginManagerWithNoPlugins.initHostProvider(
        "protocol",
        "url",
        propertiesWithoutPlugins,
        mockHostListProvider);
    return pluginManager;
  }

  @Benchmark
  public EnumSet<OldConnectionSuggestedAction> notifyConnectionChangedWithPlugins() {
    return pluginManager.notifyConnectionChanged(
        EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION),
        null);
  }

  @Benchmark
  public EnumSet<OldConnectionSuggestedAction> notifyConnectionChangedWithNoPlugins() {
    return pluginManagerWithNoPlugins.notifyConnectionChanged(
        EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION),
        null);
  }

  @Benchmark
  public ConnectionPluginManager releaseResourcesWithPlugins() {
    pluginManager.releaseResources();
    return pluginManager;
  }

  @Benchmark
  public ConnectionPluginManager releaseResourcesWithNoPlugins() {
    pluginManagerWithNoPlugins.releaseResources();
    return pluginManager;
  }
}
