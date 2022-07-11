/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.benchmarks.testplugin;

import java.util.Properties;
import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginFactory;
import software.aws.rds.jdbc.proxydriver.PluginService;

public class BenchmarkPluginFactory implements ConnectionPluginFactory {

  @Override
  public ConnectionPlugin getInstance(PluginService pluginService, Properties props) {
    return new BenchmarkPlugin();
  }
}
