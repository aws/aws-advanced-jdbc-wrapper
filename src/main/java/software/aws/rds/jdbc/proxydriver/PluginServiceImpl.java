package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class PluginServiceImpl implements PluginService, HostListProviderService {

    protected final ConnectionPluginManager pluginManager;
    protected HostSpec hostSpec;
    protected HostSpec[] hostSpecs;

    public PluginServiceImpl(ConnectionPluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    @Override
    public Connection getCurrentConnection() {
        return null;
    }

    @Override
    public HostSpec getCurrentHostSpec() {
        return null;
    }

    @Override
    public void setCurrentConnection(Connection connection, HostSpec hostSpec) {

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
