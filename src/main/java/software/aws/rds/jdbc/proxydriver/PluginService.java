/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Interface for retrieving the current active {@link Connection} and its {@link HostSpec}.
 */
public interface PluginService {

    Connection getCurrentConnection();
    HostSpec getCurrentHostSpec();
    void setCurrentConnection(Connection connection, HostSpec hostSpec);

    HostSpec[] getHosts();

    boolean isExplicitReadOnly();
    boolean isReadOnly();
    boolean isInTransaction();

    HostListProvider getHostListProvider();
    void refreshHostList();

    Connection connect(HostSpec hostSpec, Properties props) throws SQLException;
}
