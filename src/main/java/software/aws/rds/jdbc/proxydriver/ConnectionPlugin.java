/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

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

    void openInitialConnection(
            HostSpec[] hostSpecs,
            Properties props,
            String url,
            JdbcCallable<Void, Exception> openInitialConnectionFunc) throws Exception;

    void releaseResources();
}
