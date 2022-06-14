/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.util.EnumSet;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

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
    public void setCurrentConnection(
        final @NonNull Connection connection,
        final @NonNull HostSpec hostSpec) throws SQLException {

        setCurrentConnection(connection, hostSpec, null);
    }

    @Override
    public synchronized EnumSet<NodeChangeOptions> setCurrentConnection(
        final @NonNull Connection connection,
        final @NonNull HostSpec hostSpec,
        @Nullable ConnectionPlugin skipNotificationForThisPlugin) throws SQLException {

        EnumSet<NodeChangeOptions> changes = EnumSet.noneOf(NodeChangeOptions.class);

        if (this.currentConnection != connection) {
            changes.add(NodeChangeOptions.CONNECTION_OBJECT_CHANGED);
        }

        if (!this.currentHostSpec.getHost().equals(hostSpec.getHost())
            || this.currentHostSpec.getPort() != hostSpec.getPort()) {
            changes.add(NodeChangeOptions.HOSTNAME);
        }

        if (this.currentHostSpec.getRole() != hostSpec.getRole()) {
            if (hostSpec.getRole() == HostRole.WRITER) {
                changes.add(NodeChangeOptions.PROMOTED_TO_WRITER);
            } else if (hostSpec.getRole() == HostRole.READER) {
                changes.add(NodeChangeOptions.PROMOTED_TO_READER);
            }
        }

        if (this.currentHostSpec.getAvailability() != hostSpec.getAvailability()) {
            if (hostSpec.getAvailability() == HostAvailability.AVAILABLE) {
                changes.add(NodeChangeOptions.WENT_UP);
            } else if (hostSpec.getAvailability() == HostAvailability.NOT_AVAILABLE) {
                changes.add(NodeChangeOptions.WENT_DOWN);
            }
        }

        if (!changes.isEmpty()) {
            changes.add(NodeChangeOptions.NODE_CHANGED);
        }

        final Connection oldConnection = this.currentConnection;

        this.currentConnection = connection;
        this.currentHostSpec = hostSpec;

        EnumSet<OldConnectionSuggestedAction> pluginOpinions = this.pluginManager.notifyConnectionChanged(changes, skipNotificationForThisPlugin);

        if (oldConnection != null && !oldConnection.isClosed()) {

            pluginOpinions.remove(OldConnectionSuggestedAction.NO_OPINION);

            boolean shouldCloseConnection = pluginOpinions.contains(OldConnectionSuggestedAction.DISPOSE)
                || pluginOpinions.isEmpty();

            if (shouldCloseConnection) {
                try {
                    oldConnection.close();
                } catch (SQLException e) {
                    // Ignore any exception
                }
            }
        }

        return changes;
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
