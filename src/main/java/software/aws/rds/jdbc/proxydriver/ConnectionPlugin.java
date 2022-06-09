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
            Class<T> resultClass,
            Class<E> exceptionClass,
            Class<?> methodInvokeOn,
            String methodName,
            JdbcCallable<T, E> jdbcMethodFunc,
            Object[] jdbcMethodArgs) throws E;

    Connection connect(
            String driverProtocol,
            HostSpec hostSpec,
            Properties props,
            boolean isInitialConnection,
            JdbcCallable<Connection, SQLException> connectFunc) throws SQLException;

    void releaseResources();
}
