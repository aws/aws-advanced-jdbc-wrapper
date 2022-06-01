/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Implement this interface in order to handle physical connection creation process.
 */
public interface ConnectionProvider {
    /**
     * Called once per connection that needs to be created.
     *
     * @param hostSpecs The HostSpec containing the host-port information for the host to connect to
     * @param props     The Properties to use for the connection
     * @param url       The connection URL
     * @return {@link Connection} resulting from the given connection information
     * @throws SQLException if an error occurs
     */
    Connection connect(HostSpec[] hostSpecs, Properties props, @Nullable String url)
            throws SQLException;
}
