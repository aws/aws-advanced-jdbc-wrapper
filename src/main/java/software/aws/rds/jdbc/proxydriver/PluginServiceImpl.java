/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import software.aws.rds.jdbc.proxydriver.nodelistprovider.ConnectionStringHostListProvider;

public class PluginServiceImpl implements PluginService, HostListProviderService {

  protected final ConnectionPluginManager pluginManager;
  protected volatile HostListProvider hostListProvider;

  protected HostSpec[] hostSpecs;
  protected Connection currentConnection;
  protected HostSpec currentHostSpec;

  public PluginServiceImpl(@NonNull ConnectionPluginManager pluginManager) {
    this.pluginManager = pluginManager;
  }

  @Override
  public Connection getCurrentConnection() {
    return this.currentConnection;
  }

  @Override
  public HostSpec getCurrentHostSpec() {
    return this.currentHostSpec;
  }

  @Override
  public void setCurrentConnection(final @NonNull Connection connection, final @NonNull HostSpec hostSpec) {
    this.currentConnection = connection;
    this.currentHostSpec = hostSpec;
  }

  @Override
  public HostSpec[] getHosts() {
    return new HostSpec[0];
  }

  @Override
  public boolean isExplicitReadOnly() {
    return false;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public boolean isInTransaction() {
    return false;
  }

  @Override
  public HostListProvider getHostListProvider() {
    if (this.hostListProvider == null) {
      synchronized (this) {
        if (this.hostListProvider == null) {
          this.hostListProvider = new ConnectionStringHostListProvider();
        }
      }
    }
    return this.hostListProvider;
  }

  @Override
  public void refreshHostList() {

  }

  @Override
  public boolean isDefaultHostListProvider() {
    return this.hostListProvider instanceof ConnectionStringHostListProvider;
  }

  @Override
  public void setHostListProvider(HostListProvider hostListProvider) {
    this.hostListProvider = hostListProvider;
  }

  @Override
  public Connection connect(HostSpec hostSpec, Properties props) throws SQLException {
    return null;
  }
}
