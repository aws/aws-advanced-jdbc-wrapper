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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.benchmarks.support.BenchmarkServices;
import software.amazon.jdbc.benchmarks.support.FakeConnectionProvider;
import software.amazon.jdbc.benchmarks.support.FakeJdbc;
import software.amazon.jdbc.benchmarks.support.FixedDialectProvider;
import software.amazon.jdbc.benchmarks.support.StaticTopologyProvider;
import software.amazon.jdbc.dialect.UnknownDialect;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.states.SessionStateServiceImpl;
import software.amazon.jdbc.targetdriverdialect.GenericTargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Benchmarks for the real {@link PluginServiceImpl} and {@link SessionStateServiceImpl}.
 *
 * <p>{@code PluginServiceImpl} is the busiest object in the driver - every plugin and every wrapper
 * call reaches it - and it was mocked out of every previous benchmark, so its own cost was invisible.
 * The real class is constructed here; only dialect detection is bypassed, via a fixed
 * {@link FixedDialectProvider}, because detection requires a live database and happens once at
 * connect rather than on any measured path.
 *
 * <p>The methods measured are the ones called repeatedly:
 *
 * <ul>
 *   <li>{@code getCurrentConnection} and {@code getAllHosts} are field reads, included as the floor.
 *   <li>{@code getHosts} is not. It looks up {@link AllowedAndBlockedHosts} in the storage service on
 *       every call and, when a permission entry exists, filters the topology through two stream
 *       pipelines. It runs on every host selection, so it is measured both without permissions (the
 *       common case) and with an allow-list (the custom-endpoint and blue/green case).
 *   <li>{@code setAvailability} streams the whole topology and may notify plugins; it runs whenever a
 *       connection succeeds or fails.
 *   <li>{@code resetCallContext} runs once per JDBC call.
 * </ul>
 *
 * <p>{@link SessionStateServiceImpl} is measured separately: {@code begin}/{@code complete}/
 * {@code reset} run on every connection close, and {@code applyPristineSessionState} runs on close
 * and on every failover, reading and restoring up to eight session attributes.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PluginServiceBenchmarks {

  private static final String URL = "jdbc:aws-wrapper:postgresql://instance-0.XYZ.us-east-2.rds.amazonaws.com";
  private static final String PROTOCOL = "jdbc:postgresql://";
  private static final String DOMAIN = ".XYZ.us-east-2.rds.amazonaws.com";
  private static final int HOST_COUNT = 5;

  private PluginServiceImpl pluginService;
  private PluginServiceImpl pluginServiceWithHostPermissions;
  private SessionStateService sessionStateService;
  private Connection targetConnection;
  private HostSpec readerHostSpec;

  private final List<ConnectionPluginManager> pluginManagers = new ArrayList<>();
  private final List<StorageService> storageServices = new ArrayList<>();

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(PluginServiceBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() throws SQLException {
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "");
    props.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, "false");

    this.targetConnection = FakeJdbc.connection(true, 1);

    final List<HostSpec> hosts = topology();
    this.readerHostSpec = hosts.get(1);

    this.pluginService = newPluginService(props, hosts, null);

    // An allow-list entry forces getHosts() down its filtering path, which is what the custom
    // endpoint and blue/green plugins produce.
    this.pluginServiceWithHostPermissions = newPluginService(
        props,
        hosts,
        new AllowedAndBlockedHosts(
            new HashSet<>(Arrays.asList("instance-1", "instance-2", "instance-3")), null, null));

    this.sessionStateService = new SessionStateServiceImpl(this.pluginService, props);
  }

  private List<HostSpec> topology() {
    final List<HostSpec> hosts = new ArrayList<>(HOST_COUNT);
    hosts.add(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-0" + DOMAIN).hostId("instance-0").port(5432).role(HostRole.WRITER).build());
    for (int i = 1; i < HOST_COUNT; i++) {
      hosts.add(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("instance-" + i + DOMAIN).hostId("instance-" + i).port(5432).role(HostRole.READER).build());
    }
    return hosts;
  }

  private PluginServiceImpl newPluginService(
      final Properties props,
      final List<HostSpec> hosts,
      final AllowedAndBlockedHosts hostPermissions) throws SQLException {

    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);
    final BenchmarkServices.State state =
        BenchmarkServices.state(this.targetConnection, telemetryFactory);

    final StorageService storageService = BenchmarkServices.storageService();
    this.storageServices.add(storageService);

    final ConnectionPluginManager pluginManager = new ConnectionPluginManager(
        props, telemetryFactory, new FakeConnectionProvider(this.targetConnection), null);
    this.pluginManagers.add(pluginManager);

    final FullServicesContainer container = BenchmarkServices.servicesContainer(
        state,
        BenchmarkServices.pluginService(state),
        BenchmarkServices.pluginManagerService(state),
        pluginManager,
        storageService);
    pluginManager.initPlugins(container, null);

    final PluginServiceImpl service = new PluginServiceImpl(
        container,
        new ExceptionManager(),
        props,
        URL,
        PROTOCOL,
        new FixedDialectProvider(new UnknownDialect()),
        new GenericTargetDriverDialect(),
        null,
        null);

    service.setInitialConnectionHostSpec(hosts.get(0));
    // Populate the host list through the real refresh path rather than reaching into the field, so
    // setNodeList runs and the service ends up in the state production would leave it in.
    service.setHostListProvider(new StaticTopologyProvider(hosts));
    service.refreshHostList();
    service.setCurrentConnection(this.targetConnection, hosts.get(0));

    if (hostPermissions != null) {
      storageService.set(hosts.get(0).getUrl(), hostPermissions);
    }

    return service;
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    for (final ConnectionPluginManager pluginManager : this.pluginManagers) {
      pluginManager.releaseResources();
    }
    for (final StorageService storageService : this.storageServices) {
      BenchmarkServices.releaseStorage(storageService);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // PluginServiceImpl
  // ---------------------------------------------------------------------------------------------

  /** Field read; the floor for everything else here. */
  @Benchmark
  public Connection getCurrentConnection() {
    return this.pluginService.getCurrentConnection();
  }

  @Benchmark
  public HostSpec getCurrentHostSpec() {
    return this.pluginService.getCurrentHostSpec();
  }

  @Benchmark
  public List<HostSpec> getAllHosts() {
    return this.pluginService.getAllHosts();
  }

  /** Storage lookup on every call, but no filtering because no permissions are registered. */
  @Benchmark
  public List<HostSpec> getHostsNoPermissions() {
    return this.pluginService.getHosts();
  }

  /** Storage lookup plus two stream pipelines over the topology. */
  @Benchmark
  public List<HostSpec> getHostsWithAllowList() {
    return this.pluginServiceWithHostPermissions.getHosts();
  }

  @Benchmark
  public boolean isInTransaction() {
    return this.pluginService.isInTransaction();
  }

  @Benchmark
  public PluginCallContext getCallContext() {
    return this.pluginService.getCallContext();
  }

  /** Once per JDBC call. */
  @Benchmark
  public PluginService resetCallContext() {
    this.pluginService.resetCallContext();
    return this.pluginService;
  }

  /**
   * Streams the topology looking for matching hosts and writes the availability cache. Called
   * whenever a connection attempt succeeds or fails, so it is on the connect path rather than the
   * query path. Availability is set to the value the host already has, so this measures the lookup
   * without triggering the plugin notification branch.
   */
  @Benchmark
  public PluginService setAvailability() {
    this.pluginService.setAvailability(this.readerHostSpec, HostAvailability.AVAILABLE);
    return this.pluginService;
  }

  @Benchmark
  public boolean isNetworkExceptionBySqlState() {
    return this.pluginService.isNetworkException("08006");
  }

  @Benchmark
  public boolean isNetworkExceptionNotMatching() {
    return this.pluginService.isNetworkException("00000");
  }

  // ---------------------------------------------------------------------------------------------
  // SessionStateServiceImpl
  // ---------------------------------------------------------------------------------------------

  /** The pair run on every connection close. */
  @Benchmark
  public SessionStateService beginAndComplete() throws SQLException {
    this.sessionStateService.begin();
    this.sessionStateService.complete();
    return this.sessionStateService;
  }

  @Benchmark
  public SessionStateService reset() {
    this.sessionStateService.reset();
    return this.sessionStateService;
  }

  /**
   * Runs on every close and on every failover. With no pristine values recorded this is the
   * short-circuit path; the {@code Pristine} variant records them first so the restore actually runs.
   */
  @Benchmark
  public SessionStateService applyPristineSessionState() throws SQLException {
    this.sessionStateService.begin();
    try {
      this.sessionStateService.applyPristineSessionState(this.targetConnection);
    } finally {
      this.sessionStateService.complete();
      this.sessionStateService.reset();
    }
    return this.sessionStateService;
  }

  @Benchmark
  public SessionStateService applyPristineSessionStateWithRecordedState() throws SQLException {
    this.sessionStateService.setupPristineAutoCommit(true);
    this.sessionStateService.setupPristineReadOnly(false);
    this.sessionStateService.setupPristineCatalog("benchmark");
    this.sessionStateService.setupPristineSchema("public");
    this.sessionStateService.setupPristineTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    this.sessionStateService.begin();
    try {
      this.sessionStateService.applyPristineSessionState(this.targetConnection);
    } finally {
      this.sessionStateService.complete();
      this.sessionStateService.reset();
    }
    return this.sessionStateService;
  }
}
