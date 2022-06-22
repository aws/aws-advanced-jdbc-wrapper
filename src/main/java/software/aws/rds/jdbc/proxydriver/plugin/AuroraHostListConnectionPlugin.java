/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import software.aws.rds.jdbc.proxydriver.HostListProviderService;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;
import software.aws.rds.jdbc.proxydriver.PluginService;
import software.aws.rds.jdbc.proxydriver.hostlistprovider.AuroraHostListProvider;

public class AuroraHostListConnectionPlugin extends AbstractConnectionPlugin {

  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Collections.singletonList("initHostProvider")));
  private final PluginService pluginService;
  private final Properties properties;

  public AuroraHostListConnectionPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
    this.properties = properties;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public void initHostProvider(
      String driverProtocol,
      String initialUrl,
      Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc) throws SQLException {
    if (hostListProviderService.getHostListProvider() != null) {
      if (hostListProviderService.isStaticHostListProvider()) {
        hostListProviderService.setHostListProvider(
            new AuroraHostListProvider(driverProtocol, pluginService, props, initialUrl));
      } else {
        throw new SQLException("A dynamic host list provider has already been set.");
      }
    }
    initHostProviderFunc.call();
  }
}
