/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin;

import software.aws.rds.jdbc.proxydriver.*;
import software.aws.rds.jdbc.proxydriver.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This connection plugin will always be the last plugin in the connection plugin chain,
 * and will invoke the JDBC method passed down the chain.
 */
public final class DefaultConnectionPlugin implements ConnectionPlugin {

    private static final transient Logger LOGGER =
            Logger.getLogger(DefaultConnectionPlugin.class.getName());
    private static final Set<String> subscribedMethods =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("*", "openInitialConnection")));

    protected final ConnectionProvider connectionProvider;
    protected final CurrentConnectionProvider currentConnectionProvider;

    public DefaultConnectionPlugin(CurrentConnectionProvider currentConnectionProvider,
                                   ConnectionProvider connectionProvider) {
        if (currentConnectionProvider == null) {
            throw new IllegalArgumentException("currentConnectionProvider");
        }
        if (connectionProvider == null) {
            throw new IllegalArgumentException("connectionProvider");
        }

        this.currentConnectionProvider = currentConnectionProvider;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public Set<String> getSubscribedMethods() {
        return subscribedMethods;
    }

    @Override
    public <T, E extends Exception> T execute(
            Class<T> resultClass,
            Class<E> exceptionClass,
            Class<?> methodInvokeOn,
            String methodName,
            JdbcCallable<T, E> jdbcMethodFunc,
            Object[] args) throws E {

        LOGGER.log(Level.FINEST, String.format("method=%s", methodName));
        return jdbcMethodFunc.call();
    }

    @Override
    public void openInitialConnection(
            HostSpec[] hostSpecs,
            Properties props,
            String url,
            JdbcCallable<Void, Exception> openInitialConnectionFunc) throws Exception {

        if (this.currentConnectionProvider.getCurrentConnection() != null) {
            // Connection has already opened by a prior plugin in a plugin chain
            // Execution of openInitialConnectionFunc can be skipped since this plugin is guaranteed
            // the last one in the plugin chain
            return;
        }

        Connection conn = this.connectionProvider.connect(hostSpecs, props, url);
        this.currentConnectionProvider.setCurrentConnection(conn, null);

        // Execution of openInitialConnectionFunc can be skipped since this plugin is guaranteed the
        // last one in the plugin chain
    }

    @Override
    public void releaseResources() {
        // do nothing
    }
}

