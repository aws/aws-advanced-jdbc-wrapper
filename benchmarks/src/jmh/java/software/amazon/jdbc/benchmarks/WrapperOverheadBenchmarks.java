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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.benchmarks.support.BenchmarkServices;
import software.amazon.jdbc.benchmarks.support.FakeConnectionProvider;
import software.amazon.jdbc.benchmarks.support.FakeJdbc;
import software.amazon.jdbc.benchmarks.testplugin.TestConnectionWrapper;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

/**
 * The wrapper's own overhead, measured against the target driver it wraps.
 *
 * <p>This is the number the module's README claims to provide and previously could not: every other
 * benchmark here compares the wrapper against itself with a different plugin list, which cannot
 * answer "what does the wrapper cost". Each {@code wrapped*} benchmark has a {@code raw*} twin that
 * performs the identical operation directly against the same {@link FakeJdbc} target, so the
 * difference between the pair is the wrapper's contribution and the target's cost cancels.
 *
 * <p>Read the pairs, not the absolute values. The target is a {@link java.lang.reflect.Proxy}, not a
 * real driver, so absolute numbers are not a database round trip; but the target is identical on
 * both sides of every pair, which is what makes the difference meaningful.
 *
 * <p>Three cost centres are covered, in increasing order of how often they run:
 *
 * <ul>
 *   <li><b>Per statement</b> - {@code createStatement}, {@code prepareStatement}, {@code executeQuery}.
 *       Each goes through {@code WrapperUtils.executeWithPlugins}: a telemetry context, a call-context
 *       reset, a bound-connection check, the plugin chain, then a proxy wrapper allocation for the
 *       result.
 *   <li><b>Per parameter</b> - {@code PreparedStatement} setters. A statement with many parameters
 *       pays the wrapper's dispatch once per parameter, which had no coverage at all.
 *   <li><b>Per row</b> - {@code ResultSet.next()} plus column reads. This is the highest-multiplier
 *       path in the driver and the largest previous gap: a query returning 10,000 rows pays it
 *       10,000 times. Measured over 1, 100 and 1,000 rows so the fixed and per-row parts separate.
 * </ul>
 *
 * <p>Plugins are deliberately excluded ({@code wrapperPlugins} is empty) so this isolates the
 * wrapper's own machinery. Plugin costs are measured in {@code RealPluginChainBenchmarks}.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class WrapperOverheadBenchmarks {

  private static final String SQL = "SELECT id, name FROM users WHERE id = 42";
  private static final String URL = "jdbc:aws-wrapper:postgresql://instance-0.XYZ.us-east-2.rds.amazonaws.com";
  private static final String PROTOCOL = "jdbc:postgresql://";
  private static final int SMALL_ROWS = 1;
  private static final int MEDIUM_ROWS = 100;
  private static final int LARGE_ROWS = 1000;

  private Connection rawConnection;
  private ConnectionWrapper wrappedConnection;
  private ConnectionWrapper wrappedConnectionNoEvents;
  private Statement rawStatement;
  private Statement wrappedStatement;
  private Statement wrappedStatementNoEvents;
  private PreparedStatement rawPreparedStatement;
  private PreparedStatement wrappedPreparedStatement;

  private final List<ConnectionPluginManager> pluginManagers = new ArrayList<>();
  private final List<StorageService> storageServices = new ArrayList<>();

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(WrapperOverheadBenchmarks.class.getSimpleName())
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

    // The row count is a property of the target connection, so a separate raw target is created for
    // each row-count variant below; this one backs the statement-level benchmarks.
    this.rawConnection = FakeJdbc.connection(true, LARGE_ROWS);
    this.rawStatement = this.rawConnection.createStatement();
    this.rawPreparedStatement = this.rawConnection.prepareStatement(SQL);

    this.wrappedConnection = newWrappedConnection(props, true);
    this.wrappedStatement = this.wrappedConnection.createStatement();
    this.wrappedPreparedStatement = this.wrappedConnection.prepareStatement(SQL);

    this.wrappedConnectionNoEvents = newWrappedConnection(props, false);
    this.wrappedStatementNoEvents = this.wrappedConnectionNoEvents.createStatement();
  }

  /**
   * Builds a wrapper over a {@link FakeJdbc} target with a real, plugin-free
   * {@link ConnectionPluginManager}.
   *
   * @param importantEventsEnabled matches production default when true
   */
  private ConnectionWrapper newWrappedConnection(
      final Properties props, final boolean importantEventsEnabled) throws SQLException {
    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);
    final BenchmarkServices.State state =
        BenchmarkServices.state(FakeJdbc.connection(true, LARGE_ROWS), telemetryFactory);

    final StorageService storageService = BenchmarkServices.storageService();
    this.storageServices.add(storageService);

    final ConnectionPluginManager pluginManager = new ConnectionPluginManager(
        props, telemetryFactory, new FakeConnectionProvider(state.currentConnection), null);
    this.pluginManagers.add(pluginManager);

    final FullServicesContainer container = BenchmarkServices.servicesContainer(
        state,
        BenchmarkServices.pluginService(state),
        BenchmarkServices.pluginManagerService(state),
        pluginManager,
        storageService,
        importantEventsEnabled);
    pluginManager.initPlugins(container, null);

    return new TestConnectionWrapper(container, props, URL, PROTOCOL);
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
  // Per statement
  // ---------------------------------------------------------------------------------------------

  @Benchmark
  public Statement rawCreateStatement() throws SQLException {
    return this.rawConnection.createStatement();
  }

  @Benchmark
  public Statement wrappedCreateStatement() throws SQLException {
    return this.wrappedConnection.createStatement();
  }

  @Benchmark
  public PreparedStatement rawPrepareStatement() throws SQLException {
    return this.rawConnection.prepareStatement(SQL);
  }

  @Benchmark
  public PreparedStatement wrappedPrepareStatement() throws SQLException {
    return this.wrappedConnection.prepareStatement(SQL);
  }

  @Benchmark
  public ResultSet rawExecuteQuery() throws SQLException {
    return this.rawStatement.executeQuery(SQL);
  }

  @Benchmark
  public ResultSet wrappedExecuteQuery() throws SQLException {
    return this.wrappedStatement.executeQuery(SQL);
  }

  /**
   * Same call with {@code ImportantEventService} disabled. It is on by default, and
   * {@code DefaultConnectionPlugin.execute} records an event - allocating an {@code ImportantEvent}
   * and an {@code Instant}, and sweeping the expiry queue - on every JDBC call. The gap against
   * {@link #wrappedExecuteQuery()} is what that costs per query.
   */
  @Benchmark
  public ResultSet wrappedExecuteQueryEventsDisabled() throws SQLException {
    return this.wrappedStatementNoEvents.executeQuery(SQL);
  }

  @Benchmark
  public boolean rawGetAutoCommit() throws SQLException {
    return this.rawConnection.getAutoCommit();
  }

  @Benchmark
  public boolean wrappedGetAutoCommit() throws SQLException {
    return this.wrappedConnection.getAutoCommit();
  }

  // ---------------------------------------------------------------------------------------------
  // Per parameter
  // ---------------------------------------------------------------------------------------------

  @Benchmark
  public PreparedStatement rawSetParameters() throws SQLException {
    final PreparedStatement statement = this.rawPreparedStatement;
    for (int i = 1; i <= 10; i++) {
      statement.setInt(i, i);
    }
    return statement;
  }

  @Benchmark
  public PreparedStatement wrappedSetParameters() throws SQLException {
    final PreparedStatement statement = this.wrappedPreparedStatement;
    for (int i = 1; i <= 10; i++) {
      statement.setInt(i, i);
    }
    return statement;
  }

  @Benchmark
  public PreparedStatement rawSetStringParameters() throws SQLException {
    final PreparedStatement statement = this.rawPreparedStatement;
    for (int i = 1; i <= 10; i++) {
      statement.setString(i, FakeJdbc.STRING_VALUE);
    }
    return statement;
  }

  @Benchmark
  public PreparedStatement wrappedSetStringParameters() throws SQLException {
    final PreparedStatement statement = this.wrappedPreparedStatement;
    for (int i = 1; i <= 10; i++) {
      statement.setString(i, FakeJdbc.STRING_VALUE);
    }
    return statement;
  }

  // ---------------------------------------------------------------------------------------------
  // Per row
  // ---------------------------------------------------------------------------------------------

  @Benchmark
  public void rawTraverseOneRow(final Blackhole blackhole) throws SQLException {
    traverse(this.rawStatement.executeQuery(SQL), blackhole, SMALL_ROWS);
  }

  @Benchmark
  public void wrappedTraverseOneRow(final Blackhole blackhole) throws SQLException {
    traverse(this.wrappedStatement.executeQuery(SQL), blackhole, SMALL_ROWS);
  }

  @Benchmark
  public void rawTraverseHundredRows(final Blackhole blackhole) throws SQLException {
    traverse(this.rawStatement.executeQuery(SQL), blackhole, MEDIUM_ROWS);
  }

  @Benchmark
  public void wrappedTraverseHundredRows(final Blackhole blackhole) throws SQLException {
    traverse(this.wrappedStatement.executeQuery(SQL), blackhole, MEDIUM_ROWS);
  }

  @Benchmark
  public void rawTraverseThousandRows(final Blackhole blackhole) throws SQLException {
    traverse(this.rawStatement.executeQuery(SQL), blackhole, LARGE_ROWS);
  }

  @Benchmark
  public void wrappedTraverseThousandRows(final Blackhole blackhole) throws SQLException {
    traverse(this.wrappedStatement.executeQuery(SQL), blackhole, LARGE_ROWS);
  }

  /**
   * Reads at most {@code maxRows} rows and closes the result set.
   *
   * <p>Both targets are built with {@code LARGE_ROWS} available, so the row count is bounded here
   * rather than by the target. That keeps every raw/wrapped pair doing the same number of
   * {@code next()} and column reads, which is what makes the difference attributable to the wrapper.
   * The enclosing {@code executeQuery} is included on both sides so the pair stays symmetric; it is
   * priced on its own by {@code raw/wrappedExecuteQuery}.
   */
  private void traverse(final ResultSet resultSet, final Blackhole blackhole, final int maxRows)
      throws SQLException {
    int rows = 0;
    while (rows < maxRows && resultSet.next()) {
      blackhole.consume(resultSet.getInt(1));
      blackhole.consume(resultSet.getString(2));
      rows++;
    }
    resultSet.close();
  }
}
