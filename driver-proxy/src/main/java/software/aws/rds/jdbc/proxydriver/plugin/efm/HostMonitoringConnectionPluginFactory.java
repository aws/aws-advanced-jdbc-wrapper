/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

import java.util.Properties;
import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginFactory;
import software.aws.rds.jdbc.proxydriver.PluginService;

/** Class initializing a {@link HostMonitoringConnectionPlugin}. */
public class HostMonitoringConnectionPluginFactory implements ConnectionPluginFactory {
  @Override
  public ConnectionPlugin getInstance(PluginService pluginService, Properties props) {
    return new HostMonitoringConnectionPlugin(pluginService, props);
  }
}
