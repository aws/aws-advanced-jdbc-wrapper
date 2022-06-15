/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.plugin.DefaultConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.util.SqlState;
import software.aws.rds.jdbc.proxydriver.util.StringUtils;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

/**
 * This class creates and handles a chain of {@link ConnectionPlugin} for each connection.
 *
 * <p>THIS CLASS IS NOT MULTI-THREADING SAFE IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER
 * JDBC CONNECTION
 */
public class ConnectionPluginManager {

  protected static final String DEFAULT_PLUGIN_FACTORIES = "";

  // TODO: use weak pointers
  protected static final Queue<ConnectionPluginManager> instances = new ConcurrentLinkedQueue<>();

  private static final transient Logger LOGGER =
      Logger.getLogger(ConnectionPluginManager.class.getName());
  private static final String ALL_METHODS = "*";
  private static final String CONNECT_METHOD = "connect";
  private static final String INIT_HOST_PROVIDER_METHOD = "initHostProvider";
  private static final String NOTIFY_CONNECTION_CHANGED_METHOD = "notifyConnectionChanged";
  private static final String NOTIFY_HOST_LIST_CHANGED_METHOD = "notifyNodeListChanged";

  protected Properties props = new Properties();
  protected ArrayList<ConnectionPlugin> plugins;
  protected final ConnectionProvider connectionProvider;

  public ConnectionPluginManager(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  public ConnectionPluginManager(java.sql.Driver targetDriver) {
    this.connectionProvider = new DriverConnectionProvider(targetDriver);
  }

  public ConnectionPluginManager(DataSource targetDataSource) {
    this.connectionProvider = new DataSourceConnectionProvider(targetDataSource);
  }

  /**
   * This constructor is for testing purposes only.
   */
  ConnectionPluginManager(
      ConnectionProvider connectionProvider,
      Properties props,
      ArrayList<ConnectionPlugin> plugins) {
    this.connectionProvider = connectionProvider;
    this.props = props;
    this.plugins = plugins;
    instances.add(this);
  }

  /**
   * Release all dangling resources for all connection plugin managers.
   */
  public static void releaseAllResources() {
    instances.forEach(ConnectionPluginManager::releaseResources);
  }

  /**
   * Initialize a chain of {@link ConnectionPlugin} using their corresponding {@link
   * ConnectionPluginFactory}. If {@code PropertyKey.connectionPluginFactories} is provided by the
   * user, initialize the chain with the given connection plugins in the order they are specified.
   *
   * <p>The {@link DefaultConnectionPlugin} will always be initialized and attached as the last
   * connection plugin in the chain.
   *
   * @param pluginService A reference to a plugin service that plugin can use.
   * @param props         The configuration of the connection.
   * @throws SQLException if errors occurred during the execution.
   */
  public void init(PluginService pluginService, Properties props) throws SQLException {

    instances.add(this);
    this.props = props;

    String factoryClazzNames = PropertyDefinition.PLUGIN_FACTORIES.get(props);

    if (factoryClazzNames == null) {
      factoryClazzNames = DEFAULT_PLUGIN_FACTORIES;
    }

    if (!StringUtils.isNullOrEmpty(factoryClazzNames)) {
      try {
        ConnectionPluginFactory[] factories =
            WrapperUtils.loadClasses(
                    factoryClazzNames,
                    ConnectionPluginFactory.class,
                    "Unable to load connection plugin factory '%s'.")
                .toArray(new ConnectionPluginFactory[0]);

        // make a chain of connection plugins

        this.plugins = new ArrayList<>(factories.length + 1);

        for (ConnectionPluginFactory factory : factories) {
          this.plugins.add(factory.getInstance(pluginService, this.props));
        }

      } catch (InstantiationException instEx) {
        throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getCode(), instEx);
      }
    } else {
      this.plugins = new ArrayList<>(1); // one spot for default connection plugin
    }

    // add default connection plugin to the tail

    ConnectionPlugin defaultPlugin =
        new DefaultConnectionPlugin(pluginService, this.connectionProvider);
    this.plugins.add(defaultPlugin);
  }

  protected <T, E extends Exception> T executeWithSubscribedPlugins(
      final String methodName,
      final PluginPipeline<T, E> pluginPipeline,
      final JdbcCallable<T, E> jdbcMethodFunc)
      throws E {

    if (pluginPipeline == null) {
      throw new IllegalArgumentException("pluginPipeline");
    }

    if (jdbcMethodFunc == null) {
      throw new IllegalArgumentException("jdbcMethodFunc");
    }

    JdbcCallable<T, E> func = jdbcMethodFunc;

    for (int i = this.plugins.size() - 1; i >= 0; i--) {
      ConnectionPlugin plugin = this.plugins.get(i);
      Set<String> pluginSubscribedMethods = plugin.getSubscribedMethods();
      boolean isSubscribed =
          pluginSubscribedMethods.contains(ALL_METHODS)
              || pluginSubscribedMethods.contains(methodName);

      if (isSubscribed) {
        final JdbcCallable<T, E> finalFunc = func;
        final JdbcCallable<T, E> nextLevelFunc = () -> pluginPipeline.call(plugin, finalFunc);
        func = nextLevelFunc;
      }
    }

    return func.call();
  }

  protected <E extends Exception> void notifySubscribedPlugins(
      final String methodName,
      final PluginPipeline<Void, E> pluginPipeline,
      final ConnectionPlugin skipNotificationForThisPlugin) throws E {

    if (pluginPipeline == null) {
      throw new IllegalArgumentException("pluginPipeline");
    }

    for (int i = 0; i < this.plugins.size(); i++) {
      ConnectionPlugin plugin = this.plugins.get(i);
      if (plugin == skipNotificationForThisPlugin) {
        continue;
      }
      Set<String> pluginSubscribedMethods = plugin.getSubscribedMethods();
      boolean isSubscribed = pluginSubscribedMethods.contains(ALL_METHODS)
          || pluginSubscribedMethods.contains(methodName);

      if (isSubscribed) {
        pluginPipeline.call(plugin, null);
      }
    }
  }

  public <T, E extends Exception> T execute(
      final Class<T> resultType,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    return executeWithSubscribedPlugins(
        methodName,
        (plugin, func) ->
            plugin.execute(
                resultType, exceptionClass, methodInvokeOn, methodName, func, jdbcMethodArgs),
        jdbcMethodFunc);
  }

  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection)
      throws SQLException {

    try {
      return executeWithSubscribedPlugins(
          CONNECT_METHOD,
          (plugin, func) ->
              plugin.connect(driverProtocol, hostSpec, props, isInitialConnection, func),
          () -> {
            throw new SQLException("Shouldn't be called.");
          });
    } catch (SQLException | RuntimeException e) {
      throw e;
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
        });
  }

  public EnumSet<OldConnectionSuggestedAction> notifyConnectionChanged(
      @NonNull EnumSet<NodeChangeOptions> changes,
      @Nullable ConnectionPlugin skipNotificationForThisPlugin) {

    final EnumSet<OldConnectionSuggestedAction> result = EnumSet.noneOf(
        OldConnectionSuggestedAction.class);

    notifySubscribedPlugins(
        NOTIFY_CONNECTION_CHANGED_METHOD,
        (plugin, func) -> {
          OldConnectionSuggestedAction pluginOpinion = plugin.notifyConnectionChanged(changes);
          result.add(pluginOpinion);
          return null;
        },
        skipNotificationForThisPlugin);

    return result;
  }

  public void notifyNodeListChanged(@NonNull Map<String, EnumSet<NodeChangeOptions>> changes) {

    notifySubscribedPlugins(
        NOTIFY_HOST_LIST_CHANGED_METHOD,
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
    instances.remove(this);
    LOGGER.log(Level.FINE, "releasing resources");

    // This step allows all connection plugins a chance to clean up any dangling resources or
    // perform any
    // last tasks before shutting down.
    this.plugins.forEach(ConnectionPlugin::releaseResources);
  }

  private interface PluginPipeline<T, E extends Exception> {

    T call(final ConnectionPlugin plugin, JdbcCallable<T, E> func) throws E;
  }
}
