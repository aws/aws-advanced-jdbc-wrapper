/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.util.Properties;

/**
 * Interface for connection plugin factories. This class implements ways to initialize a
 * connection plugin.
 */
public interface ConnectionPluginFactory {
    ConnectionPlugin getInstance(CurrentConnectionProvider currentConnectionProvider,
                                 Properties props);
}
