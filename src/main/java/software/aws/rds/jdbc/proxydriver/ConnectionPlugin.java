/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

/**
 * Interface for connection plugins. This class implements ways to execute a JDBC method
 * and to clean up resources used before closing the plugin.
 */
public interface ConnectionPlugin {

    Set<String> getSubscribedMethods();

    <T, E extends Exception> T execute(
            final Class<T> resultClass,
            final Class<E> exceptionClass,
            final Object methodInvokeOn,
            final String methodName,
            final JdbcCallable<T, E> jdbcMethodFunc,
            final Object[] jdbcMethodArgs) throws E;

    Connection connect(
            final String driverProtocol,
            final HostSpec hostSpec,
            final Properties props,
            final boolean isInitialConnection,
            final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException;

    void initHostProvider(
            final String driverProtocol,
            final String initialUrl,
            final Properties props,
            final HostListProviderService hostListProviderService,
            final JdbcCallable<Void, SQLException> initHostProviderFunc) throws SQLException;

    void releaseResources();
}
