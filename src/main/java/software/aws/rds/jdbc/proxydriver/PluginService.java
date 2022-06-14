/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.EnumSet;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Interface for retrieving the current active {@link Connection} and its {@link HostSpec}.
 */
public interface PluginService {

    Connection getCurrentConnection();
    HostSpec getCurrentHostSpec();
    void setCurrentConnection(
        final @NonNull Connection connection,
        final @NonNull HostSpec hostSpec) throws SQLException;
    EnumSet<NodeChangeOptions> setCurrentConnection(
        final @NonNull Connection connection,
        final @NonNull HostSpec hostSpec,
        @Nullable ConnectionPlugin skipNotificationForThisPlugin) throws SQLException;

    List<HostSpec> getHosts();

    boolean isExplicitReadOnly();
    boolean isReadOnly();
    boolean isInTransaction();

    HostListProvider getHostListProvider();
    void refreshHostList() throws SQLException;

    Connection connect(HostSpec hostSpec, Properties props) throws SQLException;
}
