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

package software.amazon.jdbc.benchmarks.support;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * One logical wrapper connection with a real plugin chain and a fake target connection.
 *
 * <p>Equivalent to one {@code ConnectionWrapper}: the plugin instances, the service stand-ins and the
 * fake driver connection all belong to this object alone, so a benchmark that wants N connections to
 * the same host builds N of these. The host is the single writer that {@link BenchmarkServices#state}
 * configures, so every chain built here shares the same per-host monitors, exactly as N application
 * connections to one instance would.
 */
public final class RealPluginChain implements AutoCloseable {

  public static final String SQL = "SELECT id, name FROM users WHERE id = 42";
  private static final Object[] ARGS = {SQL};

  private final ConnectionPluginManager manager;
  private final StorageService storageService;
  private final boolean ownsStorage;
  private final Statement targetStatement;

  private RealPluginChain(
      final ConnectionPluginManager manager,
      final StorageService storageService,
      final boolean ownsStorage,
      final Statement targetStatement) {
    this.manager = manager;
    this.storageService = storageService;
    this.ownsStorage = ownsStorage;
    this.targetStatement = targetStatement;
  }

  /**
   * Builds a chain for the given comma-separated plugin codes with default plugin properties.
   */
  public static RealPluginChain build(final String pluginCodes) throws SQLException {
    return build(pluginCodes, new Properties());
  }

  /**
   * Builds a chain for the given comma-separated plugin codes; {@code extraProps} are passed to the plugins
   * on top of the benchmark defaults (telemetry off, plugin order as given).
   */
  public static RealPluginChain build(final String pluginCodes, final Properties extraProps) throws SQLException {
    return build(pluginCodes, extraProps, null);
  }

  /**
   * As above, sharing the given storage service between chains (as the driver's process-wide one is shared between
   * connections) instead of creating one per chain. A shared storage service is not released by {@link #close()}.
   */
  public static RealPluginChain build(
      final String pluginCodes, final Properties extraProps, final StorageService sharedStorage) throws SQLException {
    final Properties props = new Properties();
    props.putAll(extraProps);
    props.setProperty(PropertyDefinition.PLUGINS.name, pluginCodes);
    props.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, "false");
    // Plugin order is fixed by the chain under test, so leave it alone rather than letting the
    // builder re-sort it - a reordered chain would not be the chain the benchmark name claims.
    props.setProperty(PropertyDefinition.AUTO_SORT_PLUGIN_ORDER.name, "false");

    final Connection target = FakeJdbc.connection(true, 1);
    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);
    final BenchmarkServices.State state = BenchmarkServices.state(target, telemetryFactory);
    state.props = props;
    final boolean ownsStorage = sharedStorage == null;
    final StorageService storageService = ownsStorage ? BenchmarkServices.storageService() : sharedStorage;
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
    return new RealPluginChain(manager, storageService, ownsStorage, target.createStatement());
  }

  /** Runs {@code Statement.executeQuery} through the chain; the target call itself is a constant. */
  public Integer executeQuery() {
    return this.manager.execute(
        Integer.class,
        RuntimeException.class,
        this.targetStatement,
        JdbcMethod.STATEMENT_EXECUTEQUERY,
        () -> 1,
        ARGS);
  }

  public ConnectionPluginManager getManager() {
    return this.manager;
  }

  /** Releases the plugins and the storage, as closing the wrapper connection would. */
  @Override
  public void close() {
    this.manager.releaseResources();
    this.releaseOwnStorage();
  }

  /**
   * Drops the chain the way an application leaks a connection: the plugins are never told to release their
   * resources. Only the chain's own storage (a harness artefact, not plugin state) is released.
   */
  public void dropWithoutReleasingPlugins() {
    this.releaseOwnStorage();
  }

  private void releaseOwnStorage() {
    if (this.ownsStorage) {
      BenchmarkServices.releaseStorage(this.storageService);
    }
  }
}
