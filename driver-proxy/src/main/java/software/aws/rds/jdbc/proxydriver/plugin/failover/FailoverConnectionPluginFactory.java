/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.failover;

import java.util.Properties;
import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginFactory;
import software.aws.rds.jdbc.proxydriver.PluginService;

public class FailoverConnectionPluginFactory implements ConnectionPluginFactory {

  @Override
  public ConnectionPlugin getInstance(PluginService pluginService, Properties props) {
    return new FailoverConnectionPlugin(pluginService, props);
  }
}
