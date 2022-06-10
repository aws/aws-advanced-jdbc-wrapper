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

public class PluginServiceImpl implements PluginService, HostListProviderService {

    protected final ConnectionPluginManager pluginManager;
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
        return null;
    }

    @Override
    public void refreshHostList() {

    }

    @Override
    public boolean isDefaultHostListProvider() {
        return false;
    }

    @Override
    public void setHostListProvider(HostListProvider hostListProvider) {

    }

    @Override
    public Connection connect(HostSpec hostSpec, Properties props) throws SQLException {
        return null;
    }
}
