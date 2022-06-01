/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin;

import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginFactory;
import software.aws.rds.jdbc.proxydriver.CurrentConnectionProvider;

import java.util.Properties;

/**
 * Initialize a {@link DefaultConnectionPlugin}.
 */
public final class DefaultConnectionPluginFactory implements ConnectionPluginFactory {
    @Override
    public ConnectionPlugin getInstance(
            CurrentConnectionProvider currentConnectionProvider,
            Properties props) {
        return new DefaultConnectionPlugin(currentConnectionProvider);
    }
}