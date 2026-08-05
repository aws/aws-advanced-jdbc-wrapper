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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.benchmarks.support.BenchmarkServices;
import software.amazon.jdbc.benchmarks.support.FakeConnectionProvider;
import software.amazon.jdbc.benchmarks.support.FakeJdbc;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Per-call cost of the driver's real plugins.
 *
 * <p>Previously no benchmark instantiated a real plugin. {@code ConnectionPluginManagerBenchmarks}
 * builds chains of a no-op {@code BenchmarkPlugin}, and {@code PluginBenchmarks} passes
 * {@code wrapperPlugins} values to a mocked plugin manager, where they have no effect. So the
 * question "what does enabling failover2 cost per query" had no answer.
 *
 * <p>Each benchmark runs {@code ConnectionPluginManager.execute} for {@code Statement.executeQuery}
 * through a chain containing exactly one real plugin, against a {@code noPlugins} baseline that
 * contains only the terminal {@code DefaultConnectionPlugin}. The difference is that plugin's
 * per-call cost. {@code allObservability} combines the plugins commonly enabled together, to show
 * whether their costs simply add up.
 *
 * <p>What this does and does not measure:
 *
 * <ul>
 *   <li>It measures the steady-state {@code execute} path - the cost a plugin adds to every query
 *       while nothing is going wrong. That is the cost customers pay continuously.
 *   <li>It does not measure failover, monitor probes, topology refreshes or token fetches. Those are
 *       triggered by network events and background threads, not by {@code execute}, and cannot be
 *       driven from a benchmark without real infrastructure.
 *   <li>Plugins that only act on {@code connect} (iam, awsSecretsManager, federatedAuth, okta) are
 *       excluded: they would show a flat zero here and their real cost is a network call.
 * </ul>
 *
 * <h2>Reading the results: there are two regimes, not a gradient</h2>
 *
 * <p>{@code ConnectionPluginManager.makePluginChainFunc} computes {@code isSubscribed} while
 * explicitly excluding {@code DefaultConnectionPlugin}. So when no plugin subscribes to the method
 * being called, {@code executeWithSubscribedPlugins} bypasses the entire pipeline - including the
 * default plugin and its SQL analysis - and invokes the JDBC callable directly.
 *
 * <p>The consequence is that benchmarks here fall into two groups:
 *
 * <ul>
 *   <li>Plugins that do not subscribe to {@code Statement.executeQuery} score the same as
 *       {@code noPlugins}, because the pipeline never runs. That is a real result confirming the
 *       subscription filter works, not a broken benchmark.
 *   <li>The moment any plugin subscribes, the full pipeline runs and the default plugin's per-call
 *       SQL analysis is paid. So the interesting quantity is the step from {@code noPlugins} to the
 *       first subscribing plugin, which is mostly not the plugin's own work - compare it against
 *       {@code SqlMethodAnalyzerBenchmarks}. Adding further plugins on top costs comparatively
 *       little, which is what {@code typicalAurora} and {@code allObservability} show.
 * </ul>
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RealPluginChainBenchmarks {

  private static final String SQL = "SELECT id, name FROM users WHERE id = 42";

  /**
   * Plugin codes measured here, in the order they are reported. Each entry becomes one benchmark via
   * the corresponding method below; the map keeps the code and the chain together so adding a plugin
   * means touching one line plus one method.
   */
  private static final Map<String, String> CHAINS = new LinkedHashMap<>();

  static {
    CHAINS.put("noPlugins", "");
    CHAINS.put("executionTime", "executionTime");
    CHAINS.put("connectTime", "connectTime");
    CHAINS.put("logQuery", "logQuery");
    CHAINS.put("driverMetaData", "driverMetaData");
    CHAINS.put("dataCache", "dataCache");
    CHAINS.put("sqlParser", "sqlParser");
    CHAINS.put("auroraConnectionTracker", "auroraConnectionTracker");
    CHAINS.put("auroraStaleDns", "auroraStaleDns");
    CHAINS.put("efm2", "efm2");
    CHAINS.put("failover2", "failover2");
    CHAINS.put("dev", "dev");
    CHAINS.put("allObservability", "executionTime,connectTime,logQuery,driverMetaData");
    CHAINS.put("typicalAurora", "auroraConnectionTracker,failover2,efm2");
  }

  private final Map<String, ConnectionPluginManager> managers = new LinkedHashMap<>();
  private final List<StorageService> storageServices = new ArrayList<>();

  private Statement targetStatement;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(RealPluginChainBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() throws SQLException {
    final Connection target = FakeJdbc.connection(true, 1);
    this.targetStatement = target.createStatement();

    for (final Map.Entry<String, String> entry : CHAINS.entrySet()) {
      this.managers.put(entry.getKey(), buildManager(entry.getValue()));
    }
  }

  private ConnectionPluginManager buildManager(final String pluginCodes) throws SQLException {
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, pluginCodes);
    props.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, "false");
    // Plugin order is fixed by the chain under test, so leave it alone rather than letting the
    // builder re-sort it - a reordered chain would not be the chain the benchmark name claims.
    props.setProperty(PropertyDefinition.AUTO_SORT_PLUGIN_ORDER.name, "false");

    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);
    final BenchmarkServices.State state =
        BenchmarkServices.state(FakeJdbc.connection(true, 1), telemetryFactory);
    state.props = props;

    final StorageService storageService = BenchmarkServices.storageService();
    this.storageServices.add(storageService);

    final ConnectionPluginManager manager = new ConnectionPluginManager(
        props, telemetryFactory, new FakeConnectionProvider(state.currentConnection), null);
    manager.initPlugins(
        BenchmarkServices.servicesContainer(
            state,
            BenchmarkServices.pluginService(state),
            BenchmarkServices.pluginManagerService(state),
            manager,
            storageService),
        null);
    return manager;
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    for (final ConnectionPluginManager manager : this.managers.values()) {
      manager.releaseResources();
    }
    for (final StorageService storageService : this.storageServices) {
      BenchmarkServices.releaseStorage(storageService);
    }
  }

  private Integer executeThrough(final String chain) {
    return this.managers.get(chain).execute(
        Integer.class,
        RuntimeException.class,
        this.targetStatement,
        JdbcMethod.STATEMENT_EXECUTEQUERY,
        () -> 1,
        new Object[] {SQL});
  }

  @Benchmark
  public Integer noPlugins() {
    return executeThrough("noPlugins");
  }

  @Benchmark
  public Integer executionTime() {
    return executeThrough("executionTime");
  }

  @Benchmark
  public Integer connectTime() {
    return executeThrough("connectTime");
  }

  @Benchmark
  public Integer logQuery() {
    return executeThrough("logQuery");
  }

  @Benchmark
  public Integer driverMetaData() {
    return executeThrough("driverMetaData");
  }

  @Benchmark
  public Integer dataCache() {
    return executeThrough("dataCache");
  }

  @Benchmark
  public Integer sqlParser() {
    return executeThrough("sqlParser");
  }

  @Benchmark
  public Integer auroraConnectionTracker() {
    return executeThrough("auroraConnectionTracker");
  }

  @Benchmark
  public Integer auroraStaleDns() {
    return executeThrough("auroraStaleDns");
  }

  @Benchmark
  public Integer efm2() {
    return executeThrough("efm2");
  }

  @Benchmark
  public Integer failover2() {
    return executeThrough("failover2");
  }

  @Benchmark
  public Integer dev() {
    return executeThrough("dev");
  }

  @Benchmark
  public Integer allObservability() {
    return executeThrough("allObservability");
  }

  /** The plugin set a typical Aurora deployment enables, measured as one chain. */
  @Benchmark
  public Integer typicalAurora() {
    return executeThrough("typicalAurora");
  }
}
