/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import software.aws.rds.jdbc.proxydriver.plugin.DefaultConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.util.SqlState;
import software.aws.rds.jdbc.proxydriver.util.StringUtils;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class creates and handles a chain of {@link ConnectionPlugin} for each connection.
 *
 * THIS CLASS IS NOT MULTI-THREADING SAFE
 * IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER JDBC CONNECTION
 *
 */
public class ConnectionPluginManager {

    protected static final String DEFAULT_PLUGIN_FACTORIES = "";
    protected static final Queue<ConnectionPluginManager> instances = new ConcurrentLinkedQueue<>();

    private static final transient Logger LOGGER =
            Logger.getLogger(ConnectionPluginManager.class.getName());
    private static final String ALL_METHODS = "*";
    private static final String OPEN_INITIAL_CONNECTION_METHOD = "openInitialConnection";

    protected Properties props = new Properties();
    protected ArrayList<ConnectionPlugin> plugins;
    protected CurrentConnectionProvider currentConnectionProvider;
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
     * Release all dangling resources for all connection plugin managers.
     */
    public static void releaseAllResources() {
        instances.forEach(ConnectionPluginManager::releaseResources);
    }

    // For testing purposes only
    ConnectionPluginManager(ConnectionProvider connectionProvider,
                            Properties props,
                            ArrayList<ConnectionPlugin> plugins) {
        this.connectionProvider = connectionProvider;
        this.props = props;
        this.plugins = plugins;
        instances.add(this);
    }

    /**
     * Initialize a chain of {@link ConnectionPlugin} using their corresponding
     * {@link ConnectionPluginFactory}.
     * If {@code PropertyKey.connectionPluginFactories} is provided by the user, initialize
     * the chain with the given connection plugins in the order they are specified.
     *
     * <p>The {@link DefaultConnectionPlugin} will always be initialized and attached as the
     * last connection plugin in the chain.
     *
     * @param currentConnectionProvider The connection the plugins are associated with.
     * @param props                     The configuration of the connection.
     * @throws SQLException if errors occurred during the execution.
     */
    public void init(CurrentConnectionProvider currentConnectionProvider, Properties props)
            throws SQLException {
        instances.add(this);
        this.currentConnectionProvider = currentConnectionProvider;
        this.props = props;

        String factoryClazzNames = PropertyDefinition.PLUGIN_FACTORIES.get(props);

        if (factoryClazzNames == null) {
            factoryClazzNames = DEFAULT_PLUGIN_FACTORIES;
        }

        if (!StringUtils.isNullOrEmpty(factoryClazzNames)) {
            try {
                ConnectionPluginFactory[] factories = WrapperUtils.loadClasses(
                                factoryClazzNames,
                                ConnectionPluginFactory.class,
                                "Unable to load connection plugin factory '%s'.")
                        .toArray(new ConnectionPluginFactory[0]);

                // make a chain of connection plugins

                this.plugins = new ArrayList<>(factories.length + 1);

                for (ConnectionPluginFactory factory : factories) {
                    this.plugins.add(factory.getInstance(
                            this.currentConnectionProvider,
                            this.props));
                }

            } catch (InstantiationException instEx) {
                throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getCode(), instEx);
            }
        } else {
            this.plugins = new ArrayList<>(1); // one spot for default connection plugin
        }

        // add default connection plugin to the tail

        ConnectionPlugin defaultPlugin = new DefaultConnectionPlugin(
                this.currentConnectionProvider, this.connectionProvider);
        this.plugins.add(defaultPlugin);
    }

    protected <T, E extends Exception> T executeWithSubscribedPlugins(
            final Class<T> resultClass,
            final Class<E> exceptionClass,
            final String methodName,
            final PluginPipeline<T, E> pluginPipeline,
            final Callable<T> executeSqlFunc) throws E {

        if (pluginPipeline == null) {
            throw new IllegalArgumentException("pluginPipeline");
        }

        if (executeSqlFunc == null) {
            throw new IllegalArgumentException("executeSqlFunc");
        }

        Callable<T> func = executeSqlFunc;

        for (int i = this.plugins.size() - 1; i >= 0; i--) {
            ConnectionPlugin plugin = this.plugins.get(i);
            Set<String> pluginSubscribedMethods = plugin.getSubscribedMethods();
            boolean isSubscribed = pluginSubscribedMethods.contains(ALL_METHODS)
                    || pluginSubscribedMethods.contains(methodName);

            if (isSubscribed) {
                final Callable<T> finalFunc = func;
                final Callable<T> nextLevelFunc = () -> pluginPipeline.call(plugin, finalFunc);
                func = nextLevelFunc;
            }
        }

        try {
            return func.call();
        } catch(Exception e) {
            if (exceptionClass.isInstance(e)) {
                throw exceptionClass.cast(e);
            }
            throw new RuntimeException(e);
        }
    }

    public <T> T execute(
            final Class<T> resultType,
            final Class<?> methodInvokeOn,
            final String methodName,
            final Callable<T> executeSqlFunc,
            final Object[] args) throws Exception {

        //noinspection unchecked
        return executeWithSubscribedPlugins(
                resultType,
                Exception.class,
                methodName,
                (plugin, func) -> (T) plugin.execute(methodInvokeOn, methodName, func, args),
                executeSqlFunc);
    }

    public void openInitialConnection(
            final HostSpec[] hostSpecs,
            final Properties props,
            final String url) throws SQLException {

        executeWithSubscribedPlugins(
                Void.TYPE,
                SQLException.class,
                "openInitialConnection",
                (plugin, func) -> {
                    plugin.openInitialConnection(hostSpecs, props, url, func);
                    return null;
                },
                () -> null);
    }

    /**
     * Release all dangling resources held by the connection plugins associated with
     * a single connection.
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
        T call(final ConnectionPlugin plugin, Callable<T> func) throws Exception;
    }
}

