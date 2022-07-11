/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.benchmarks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

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
import software.aws.rds.benchmarks.testplugin.BenchmarkPluginFactory;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginFactory;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.ConnectionProvider;
import software.aws.rds.jdbc.proxydriver.HostListProviderService;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.NodeChangeOptions;
import software.aws.rds.jdbc.proxydriver.PluginManagerService;
import software.aws.rds.jdbc.proxydriver.PluginService;
import software.aws.rds.jdbc.proxydriver.PropertyDefinition;
import software.aws.rds.jdbc.proxydriver.plugin.AuroraHostListConnectionPluginFactory;
import software.aws.rds.jdbc.proxydriver.plugin.ExecutionTimeConnectionPluginFactory;
import software.aws.rds.jdbc.proxydriver.profile.DriverConfigurationProfiles;

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
  private final Properties emptyProperties = new Properties();
  private Properties propertiesWithPlugins;
  private ConnectionPluginManager pluginManager;
  private ConnectionPluginManager pluginManagerWithNoPlugins;

  @Mock ConnectionProvider mockConnectionProvider;
  @Mock PluginService mockPluginService;
  @Mock PluginManagerService mockPluginManagerService;
  @Mock HostListProviderService mockHostListProvider;
  @Mock Connection mockConnection;
  @Mock Statement mockStatement;
  @Mock ResultSet mockResultSet;
  private AutoCloseable closeable;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(software.aws.rds.benchmarks.PluginBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Iteration)
  public void setUpIteration() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockConnectionProvider.connect(anyString(), any(Properties.class))).thenReturn(
        mockConnection);
    when(mockConnectionProvider.connect(anyString(), any(HostSpec.class), any(Properties.class)))
        .thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(eq(FIELD_SESSION_ID))).thenReturn(WRITER_SESSION_ID);
    when(mockResultSet.getString(eq(FIELD_SERVER_ID)))
        .thenReturn("myInstance1.domain.com", "myInstance2.domain.com", "myInstance3.domain.com");

    // Create a plugin chain with 10 custom test plugins.
    final List<Class<? extends ConnectionPluginFactory>> pluginFactories = new ArrayList<>(
        Collections.nCopies(10, BenchmarkPluginFactory.class));

    DriverConfigurationProfiles.addOrReplaceProfile(
        "benchmark",
        pluginFactories);
    propertiesWithPlugins = new Properties();
    propertiesWithPlugins.setProperty(PropertyDefinition.PROFILE_NAME.name, "benchmark");

    pluginManager = new ConnectionPluginManager(mockConnectionProvider);
    pluginManager.init(mockPluginService, propertiesWithPlugins, mockPluginManagerService);

    pluginManagerWithNoPlugins = new ConnectionPluginManager(mockConnectionProvider);
    pluginManagerWithNoPlugins.init(mockPluginService, emptyProperties, mockPluginManagerService);
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration() throws Exception {
    closeable.close();
  }

  @Benchmark
  public void initConnectionPluginManagerWithNoPlugins() throws SQLException {
    final ConnectionPluginManager manager = new ConnectionPluginManager(mockConnectionProvider);
    manager.init(mockPluginService, emptyProperties, mockPluginManagerService);
  }

  @Benchmark
  public void initConnectionPluginManagerWithPlugins() throws SQLException {
    final ConnectionPluginManager manager = new ConnectionPluginManager(mockConnectionProvider);
    manager.init(mockPluginService, propertiesWithPlugins, mockPluginManagerService);
  }

  @Benchmark
  public void connectWithPlugins() throws SQLException {
    pluginManager.connect(
        "driverProtocol",
        new HostSpec("host"),
        propertiesWithPlugins,
        true);
  }

  @Benchmark
  public void connectWithNoPlugins() throws SQLException {
    pluginManagerWithNoPlugins.connect(
        "driverProtocol",
        new HostSpec("host"),
        emptyProperties,
        true);
  }

  @Benchmark
  public void executeWithPlugins() {
    pluginManager.execute(
        int.class,
        RuntimeException.class,
        mockStatement,
        "Statement.execute",
        () -> 1,
        new Object[] {1}
    );
  }

  @Benchmark
  public void executeWithNoPlugins() {
    pluginManagerWithNoPlugins.execute(
        int.class,
        RuntimeException.class,
        mockStatement,
        "Statement.execute",
        () -> 1,
        new Object[] {1}
    );
  }

  @Benchmark
  public void initHostProvidersWithPlugins() throws SQLException {
    pluginManager.initHostProvider(
        "protocol",
        "url",
        propertiesWithPlugins,
        mockHostListProvider);
  }

  @Benchmark
  public void initHostProvidersWithNoPlugins() throws SQLException {
    pluginManagerWithNoPlugins.initHostProvider(
        "protocol",
        "url",
        emptyProperties,
        mockHostListProvider);
  }

  @Benchmark
  public void notifyConnectionChangedWithPlugins() {
    pluginManager.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION), null);
  }

  @Benchmark
  public void notifyConnectionChangedWithNoPlugins() {
    pluginManagerWithNoPlugins.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION), null);
  }

  @Benchmark
  public void releaseResourcesWithPlugins() {
    pluginManager.releaseResources();
  }

  @Benchmark
  public void releaseResourcesWithNoPlugins() {
    pluginManagerWithNoPlugins.releaseResources();
  }
}
