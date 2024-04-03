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

package software.amazon.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPlugin;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPlugin;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin;
import software.amazon.jdbc.plugin.DataCacheConnectionPlugin;
import software.amazon.jdbc.plugin.DefaultConnectionPlugin;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPlugin;
import software.amazon.jdbc.plugin.LogQueryConnectionPlugin;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointPlugin;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.federatedauth.FederatedAuthPlugin;
import software.amazon.jdbc.plugin.federatedauth.OktaAuthPlugin;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.plugin.limitless.LimitlessConnectionPlugin;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;
import software.amazon.jdbc.plugin.staledns.AuroraStaleDnsPlugin;
import software.amazon.jdbc.plugin.strategy.fastestresponse.FastestResponseStrategyPlugin;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.util.AsynchronousMethodsHelper;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlMethodAnalyzer;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

/**
 * This class creates and handles a chain of {@link ConnectionPlugin} for each connection.
 *
 * <p>THIS CLASS IS NOT MULTI-THREADING SAFE IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER
 * JDBC CONNECTION
 */
public class ConnectionPluginManager implements CanReleaseResources, Wrapper {

  protected static final Map<Class<? extends ConnectionPlugin>, String> pluginNameByClass =
      new HashMap<Class<? extends ConnectionPlugin>, String>() {
        {
          put(LimitlessConnectionPlugin.class, "plugin:limitless");
          put(ExecutionTimeConnectionPlugin.class, "plugin:executionTime");
          put(AuroraConnectionTrackerPlugin.class, "plugin:auroraConnectionTracker");
          put(LogQueryConnectionPlugin.class, "plugin:logQuery");
          put(DataCacheConnectionPlugin.class, "plugin:dataCache");
          put(HostMonitoringConnectionPlugin.class, "plugin:efm");
          put(software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPlugin.class, "plugin:efm2");
          put(FailoverConnectionPlugin.class, "plugin:failover");
          put(software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin.class, "plugin:failover2");
          put(IamAuthConnectionPlugin.class, "plugin:iam");
          put(AwsSecretsManagerConnectionPlugin.class, "plugin:awsSecretsManager");
          put(FederatedAuthPlugin.class, "plugin:federatedAuth");
          put(OktaAuthPlugin.class, "plugin:okta");
          put(AuroraStaleDnsPlugin.class, "plugin:auroraStaleDns");
          put(ReadWriteSplittingPlugin.class, "plugin:readWriteSplitting");
          put(FastestResponseStrategyPlugin.class, "plugin:fastestResponseStrategy");
          put(DefaultConnectionPlugin.class, "plugin:targetDriver");
          put(AuroraInitialConnectionStrategyPlugin.class, "plugin:initialConnection");
          put(CustomEndpointPlugin.class, "plugin:customEndpoint");
        }
      };

  private static final Logger LOGGER = Logger.getLogger(ConnectionPluginManager.class.getName());
  protected static final String ALL_METHODS = "*";
  protected static final String CONNECT_METHOD = "connect";
  private static final String FORCE_CONNECT_METHOD = "forceConnect";
  private static final String ACCEPTS_STRATEGY_METHOD = "acceptsStrategy";
  protected static final String GET_HOST_SPEC_BY_STRATEGY_METHOD = "getHostSpecByStrategy";
  private static final String INIT_HOST_PROVIDER_METHOD = "initHostProvider";
  private static final String NOTIFY_CONNECTION_CHANGED_METHOD = "notifyConnectionChanged";
  private static final String NOTIFY_NODE_LIST_CHANGED_METHOD = "notifyNodeListChanged";
  private static final SqlMethodAnalyzer sqlMethodAnalyzer = new SqlMethodAnalyzer();

  private final ReentrantLock lock = new ReentrantLock();

  protected Properties props = new Properties();
  protected List<ConnectionPlugin> plugins;
  protected final @NonNull ConnectionProvider defaultConnProvider;
  protected final @Nullable ConnectionProvider effectiveConnProvider;
  protected final ConnectionWrapper connectionWrapper;
  protected PluginService pluginService;
  protected TelemetryFactory telemetryFactory;

  @SuppressWarnings("rawtypes")
  protected final Map<String, PluginChainJdbcCallable> pluginChainFuncMap = new HashMap<>();

  public ConnectionPluginManager(
      final @NonNull ConnectionProvider defaultConnProvider,
      final @Nullable ConnectionProvider effectiveConnProvider,
      final @NonNull ConnectionWrapper connectionWrapper,
      final @NonNull TelemetryFactory telemetryFactory) {
    this.defaultConnProvider = defaultConnProvider;
    this.effectiveConnProvider = effectiveConnProvider;
    this.connectionWrapper = connectionWrapper;
    this.telemetryFactory = telemetryFactory;
  }

  /**
   * This constructor is for testing purposes only.
   */
  ConnectionPluginManager(
      final @NonNull ConnectionProvider defaultConnProvider,
      final @Nullable ConnectionProvider effectiveConnProvider,
      final Properties props,
      final List<ConnectionPlugin> plugins,
      final ConnectionWrapper connectionWrapper,
      final PluginService pluginService,
      final TelemetryFactory telemetryFactory) {
    this(defaultConnProvider, effectiveConnProvider, props, plugins, connectionWrapper, telemetryFactory);
    this.pluginService = pluginService;
  }

  /**
   * This constructor is for testing purposes only.
   */
  ConnectionPluginManager(
      final @NonNull ConnectionProvider defaultConnProvider,
      final @Nullable ConnectionProvider effectiveConnProvider,
      final Properties props,
      final List<ConnectionPlugin> plugins,
      final ConnectionWrapper connectionWrapper,
      final TelemetryFactory telemetryFactory) {
    this.defaultConnProvider = defaultConnProvider;
    this.effectiveConnProvider = effectiveConnProvider;
    this.props = props;
    this.plugins = plugins;
    this.connectionWrapper = connectionWrapper;
    this.telemetryFactory = telemetryFactory;
  }

  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }

  public boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }

  /**
   * Initialize a chain of {@link ConnectionPlugin} using their corresponding {@link
   * ConnectionPluginFactory}. If {@code PropertyDefinition.PLUGINS} is provided by the user,
   * initialize the chain with the given connection plugins in the order they are specified.
   *
   * <p>The {@link DefaultConnectionPlugin} will always be initialized and attached as the last
   * connection plugin in the chain.
   *
   * @param pluginService        a reference to a plugin service that plugin can use
   * @param props                the configuration of the connection
   * @param pluginManagerService a reference to a plugin manager service
   * @param configurationProfile a profile configuration defined by the user
   * @throws SQLException if errors occurred during the execution
   */
  public void init(
      final PluginService pluginService,
      final Properties props,
      final PluginManagerService pluginManagerService,
      @Nullable ConfigurationProfile configurationProfile)
      throws SQLException {

    this.props = props;
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();

    ConnectionPluginChainBuilder pluginChainBuilder = new ConnectionPluginChainBuilder();
    this.plugins = pluginChainBuilder.getPlugins(
        this.pluginService,
        this.defaultConnProvider,
        this.effectiveConnProvider,
        pluginManagerService,
        props,
        configurationProfile);
  }

  protected <T, E extends Exception> T executeWithSubscribedPlugins(
      final String methodName,
      final PluginPipeline<T, E> pluginPipeline,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final @Nullable ConnectionPlugin pluginToSkip)
      throws E {

    if (pluginPipeline == null) {
      throw new IllegalArgumentException("pluginPipeline");
    }

    if (jdbcMethodFunc == null) {
      throw new IllegalArgumentException("jdbcMethodFunc");
    }

    @SuppressWarnings({"unchecked", "noinspection"})
    PluginChainJdbcCallable<T, E> pluginChainFunc = this.pluginChainFuncMap.get(methodName);

    if (pluginChainFunc == null) {
      pluginChainFunc = this.makePluginChainFunc(methodName);
      this.pluginChainFuncMap.put(methodName, pluginChainFunc);
    }

    if (pluginChainFunc == null) {
      throw new RuntimeException("Error processing this JDBC call.");
    }

    return pluginChainFunc.call(pluginPipeline, jdbcMethodFunc, pluginToSkip);
  }


  protected <T, E extends Exception> T executeWithTelemetry(
      final @NonNull JdbcCallable<T, E> execution,
      final @NonNull String pluginName) throws E {
    final TelemetryContext context = telemetryFactory.openTelemetryContext(
        pluginName, TelemetryTraceLevel.NESTED);
    try {
      return execution.call();
    } finally {
      context.closeContext();
    }
  }

  @Nullable
  protected <T, E extends Exception> PluginChainJdbcCallable<T, E> makePluginChainFunc(
      final @NonNull String methodName) {

    PluginChainJdbcCallable<T, E> pluginChainFunc = null;

    for (int i = this.plugins.size() - 1; i >= 0; i--) {
      final ConnectionPlugin plugin = this.plugins.get(i);
      final Set<String> pluginSubscribedMethods = plugin.getSubscribedMethods();
      final String pluginName = pluginNameByClass.getOrDefault(plugin.getClass(), plugin.getClass().getSimpleName());
      final boolean isSubscribed = pluginSubscribedMethods.contains(ALL_METHODS)
          || pluginSubscribedMethods.contains(methodName);

      if (isSubscribed) {
        if (pluginChainFunc == null) {
          // This case is for DefaultConnectionPlugin that always terminates the list of plugins.
          // Default plugin can't be skipped.
          pluginChainFunc = (pipelineFunc, jdbcFunc, pluginToSkip) ->
              executeWithTelemetry(() -> pipelineFunc.call(plugin, jdbcFunc), pluginName);
        } else {
          final PluginChainJdbcCallable<T, E> finalPluginChainFunc = pluginChainFunc;
          pluginChainFunc = (pipelineFunc, jdbcFunc, pluginToSkip) -> {
            if (pluginToSkip == plugin) {
              return finalPluginChainFunc.call(pipelineFunc, jdbcFunc, pluginToSkip);
            } else {
              return executeWithTelemetry(
                  () -> pipelineFunc.call(
                      plugin,
                      () -> finalPluginChainFunc.call(pipelineFunc, jdbcFunc, pluginToSkip)),
                  pluginName);
            }
          };
        }
      }
    }
    return pluginChainFunc;
  }

  protected <E extends Exception> void notifySubscribedPlugins(
      final String methodName,
      final PluginPipeline<Void, E> pluginPipeline,
      final ConnectionPlugin skipNotificationForThisPlugin)
      throws E {

    if (pluginPipeline == null) {
      throw new IllegalArgumentException("pluginPipeline");
    }

    for (final ConnectionPlugin plugin : this.plugins) {
      if (plugin == skipNotificationForThisPlugin) {
        continue;
      }
      final Set<String> pluginSubscribedMethods = plugin.getSubscribedMethods();
      final boolean isSubscribed =
          pluginSubscribedMethods.contains(ALL_METHODS)
              || pluginSubscribedMethods.contains(methodName);

      if (isSubscribed) {
        pluginPipeline.call(plugin, null);
      }
    }
  }

  public ConnectionWrapper getConnectionWrapper() {
    return this.connectionWrapper;
  }

  public TelemetryFactory getTelemetryFactory() {
    return this.telemetryFactory;
  }

  public <T, E extends Exception> T execute(
      final Class<T> resultType,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    // The target driver may block on Statement.getConnection().
    if (!AsynchronousMethodsHelper.ASYNCHRONOUS_METHODS.contains(methodName)) {
      final Connection conn = WrapperUtils.getConnectionFromSqlObject(methodInvokeOn);
      if (conn != null
          && conn != this.pluginService.getCurrentConnection()
          && !sqlMethodAnalyzer.isMethodClosingSqlObject(methodName)) {
        throw WrapperUtils.wrapExceptionIfNeeded(
            exceptionClass,
            new SQLException(
                Messages.get("ConnectionPluginManager.invokedAgainstOldConnection", new Object[] {methodInvokeOn})));
      }
    }

    return executeWithSubscribedPlugins(
        methodName,
        (plugin, func) ->
            plugin.execute(
                resultType, exceptionClass, methodInvokeOn, methodName, func, jdbcMethodArgs),
        jdbcMethodFunc,
        null);
  }

  /**
   * Establishes a connection to the given host using the given driver protocol and properties. If a
   * non-default {@link ConnectionProvider} has been set with
   * {@link Driver#setCustomConnectionProvider(ConnectionProvider)} and
   * {@link ConnectionProvider#acceptsUrl(String, HostSpec, Properties)} returns true for the given
   * protocol, host, and properties, the connection will be created by the non-default
   * ConnectionProvider. Otherwise, the connection will be created by the default
   * ConnectionProvider. The default ConnectionProvider will be {@link DriverConnectionProvider} for
   * connections requested via the {@link java.sql.DriverManager} and
   * {@link DataSourceConnectionProvider} for connections requested via an
   * {@link software.amazon.jdbc.ds.AwsWrapperDataSource}.
   *
   * @param driverProtocol      the driver protocol that should be used to establish the connection
   * @param hostSpec            the host details for the desired connection
   * @param props               the connection properties
   * @param isInitialConnection a boolean indicating whether the current {@link Connection} is
   *                            establishing an initial physical connection to the database or has
   *                            already established a physical connection in the past
   * @param pluginToSkip        the plugin that needs to be skipped while executing this pipeline
   * @return a {@link Connection} to the requested host
   * @throws SQLException if there was an error establishing a {@link Connection} to the requested
   *                      host
   */
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @Nullable ConnectionPlugin pluginToSkip)
      throws SQLException {

    TelemetryContext context = telemetryFactory.openTelemetryContext("connect", TelemetryTraceLevel.NESTED);
    try {
      return executeWithSubscribedPlugins(
          CONNECT_METHOD,
          (plugin, func) ->
              plugin.connect(driverProtocol, hostSpec, props, isInitialConnection, func),
          () -> {
            throw new SQLException("Shouldn't be called.");
          },
          pluginToSkip);
    } catch (final SQLException | RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new SQLException(e);
    } finally {
      context.closeContext();
    }
  }

  /**
   * Establishes a connection to the given host using the given driver protocol and properties. This
   * call differs from {@link ConnectionPlugin#connect} in that the default
   * {@link ConnectionProvider} will be used to establish the connection even if a non-default
   * ConnectionProvider has been set via {@link Driver#setCustomConnectionProvider(ConnectionProvider)}.
   * The default ConnectionProvider will be {@link DriverConnectionProvider} for connections
   * requested via the {@link java.sql.DriverManager} and {@link DataSourceConnectionProvider} for
   * connections requested via an {@link software.amazon.jdbc.ds.AwsWrapperDataSource}.
   *
   * @param driverProtocol      the driver protocol that should be used to establish the connection
   * @param hostSpec            the host details for the desired connection
   * @param props               the connection properties
   * @param isInitialConnection a boolean indicating whether the current {@link Connection} is
   *                            establishing an initial physical connection to the database or has
   *                            already established a physical connection in the past
   * @param pluginToSkip        the plugin that needs to be skipped while executing this pipeline
   * @return a {@link Connection} to the requested host
   * @throws SQLException if there was an error establishing a {@link Connection} to the requested
   *                      host
   */
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @Nullable ConnectionPlugin pluginToSkip)
      throws SQLException {

    try {
      return executeWithSubscribedPlugins(
          FORCE_CONNECT_METHOD,
          (plugin, func) ->
              plugin.forceConnect(driverProtocol, hostSpec, props, isInitialConnection, func),
          () -> {
            throw new SQLException("Shouldn't be called.");
          },
          pluginToSkip);
    } catch (SQLException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  /**
   * Returns a boolean indicating if the available {@link ConnectionProvider} or
   * {@link ConnectionPlugin} instances implement the selection of a host with the requested role
   * and strategy via {@link #getHostSpecByStrategy}.
   *
   * @param role     the desired host role
   * @param strategy the strategy that should be used to pick a host (eg "random")
   * @return true if the available {@link ConnectionProvider} or {@link ConnectionPlugin} instances
   *     support the selection of a host with the requested role and strategy via
   *     {@link #getHostSpecByStrategy}. Otherwise, return false.
   */
  public boolean acceptsStrategy(HostRole role, String strategy) throws SQLException {
    try {
      for (ConnectionPlugin plugin : this.plugins) {
        Set<String> pluginSubscribedMethods = plugin.getSubscribedMethods();
        boolean isSubscribed =
            pluginSubscribedMethods.contains(ALL_METHODS)
                || pluginSubscribedMethods.contains(ACCEPTS_STRATEGY_METHOD);

        if (isSubscribed) {
          if (plugin.acceptsStrategy(role, strategy)) {
            return true;
          }
        }
      }

      return false;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  /**
   * Selects a {@link HostSpec} with the requested role from available hosts using the requested
   * strategy. {@link #acceptsStrategy} should be called first to evaluate if the available
   * {@link ConnectionProvider} or {@link ConnectionPlugin} instances support the selection of a
   * host with the requested role and strategy.
   *
   * @param role     the desired role of the host - either a writer or a reader
   * @param strategy the strategy that should be used to select a {@link HostSpec} from the
   *                 available hosts (eg "random")
   * @return a {@link HostSpec} with the requested role
   * @throws SQLException                  if the available hosts do not contain any hosts matching
   *                                       the requested role or an error occurs while selecting a
   *                                       host
   * @throws UnsupportedOperationException if the available {@link ConnectionProvider} or
   *                                       {@link ConnectionPlugin} instances do not support the
   *                                       requested strategy
   */
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy)
      throws SQLException, UnsupportedOperationException {
    return getHostSpecByStrategy(null, role, strategy);
  }

  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy)
      throws SQLException, UnsupportedOperationException {
    try {
      for (ConnectionPlugin plugin : this.plugins) {
        Set<String> pluginSubscribedMethods = plugin.getSubscribedMethods();
        boolean isSubscribed =
            pluginSubscribedMethods.contains(ALL_METHODS)
                || pluginSubscribedMethods.contains(GET_HOST_SPEC_BY_STRATEGY_METHOD);

        if (isSubscribed) {
          try {
            final HostSpec host = Utils.isNullOrEmpty(hosts)
                ? plugin.getHostSpecByStrategy(role, strategy)
                : plugin.getHostSpecByStrategy(hosts, role, strategy);

            if (host != null) {
              return host;
            }
          } catch (UnsupportedOperationException e) {
            // This plugin does not support the provided strategy, ignore the exception and move on
          }
        }
      }

      throw new UnsupportedOperationException(
          "The driver does not support the requested host selection strategy: " + strategy);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService)
      throws SQLException {
    TelemetryContext context = telemetryFactory.openTelemetryContext(
        "initHostProvider", TelemetryTraceLevel.NESTED);
    try {
      executeWithSubscribedPlugins(
          INIT_HOST_PROVIDER_METHOD,
          (PluginPipeline<Void, SQLException>)
              (plugin, func) -> {
                plugin.initHostProvider(
                    driverProtocol, initialUrl, props, hostListProviderService, func);
                return null;
              },
          () -> {
            throw new SQLException("Shouldn't be called.");
          },
          null);
    } finally {
      context.closeContext();
    }
  }

  public EnumSet<OldConnectionSuggestedAction> notifyConnectionChanged(
      @NonNull final EnumSet<NodeChangeOptions> changes,
      @Nullable final ConnectionPlugin skipNotificationForThisPlugin) {

    final EnumSet<OldConnectionSuggestedAction> result =
        EnumSet.noneOf(OldConnectionSuggestedAction.class);

    notifySubscribedPlugins(
        NOTIFY_CONNECTION_CHANGED_METHOD,
        (plugin, func) -> {
          final OldConnectionSuggestedAction pluginOpinion = plugin.notifyConnectionChanged(changes);
          result.add(pluginOpinion);
          return null;
        },
        skipNotificationForThisPlugin);

    return result;
  }

  public void notifyNodeListChanged(@NonNull final Map<String, EnumSet<NodeChangeOptions>> changes) {

    notifySubscribedPlugins(
        NOTIFY_NODE_LIST_CHANGED_METHOD,
        (plugin, func) -> {
          plugin.notifyNodeListChanged(changes);
          return null;
        },
        null);
  }

  /**
   * Release all dangling resources held by the connection plugins associated with a single
   * connection.
   */
  public void releaseResources() {
    LOGGER.finest(() -> Messages.get("ConnectionPluginManager.releaseResources"));

    // This step allows all connection plugins a chance to clean up any dangling resources or
    // perform any
    // last tasks before shutting down.

    this.plugins.forEach(
        (plugin) -> {
          if (plugin instanceof CanReleaseResources) {
            ((CanReleaseResources) plugin).releaseResources();
          }
        });
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == ConnectionPluginManager.class) {
      return iface.cast(this);
    }
    if (iface == PluginService.class) {
      return iface.cast(this.pluginService);
    }
    if (this.plugins == null) {
      return null;
    }

    for (ConnectionPlugin p : this.plugins) {
      if (iface.isAssignableFrom(p.getClass())) {
        return iface.cast(p);
      }
    }
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (this.plugins == null) {
      return false;
    }

    for (ConnectionPlugin p : this.plugins) {
      if (iface.isAssignableFrom(p.getClass())) {
        return true;
      }
    }
    return false;
  }

  public @NonNull ConnectionProvider getDefaultConnProvider() {
    return this.defaultConnProvider;
  }

  public ConnectionProvider getEffectiveConnProvider() {
    return this.effectiveConnProvider;
  }

  protected interface PluginPipeline<T, E extends Exception> {

    T call(final @NonNull ConnectionPlugin plugin, final @Nullable JdbcCallable<T, E> jdbcMethodFunc) throws E;
  }

  protected interface PluginChainJdbcCallable<T, E extends Exception> {

    T call(
        final @NonNull PluginPipeline<T, E> pipelineFunc,
        final @NonNull JdbcCallable<T, E> jdbcMethodFunc,
        final @Nullable ConnectionPlugin pluginToSkip) throws E;
  }
}
