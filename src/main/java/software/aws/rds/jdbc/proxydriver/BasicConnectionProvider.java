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
 * This class is a basic implementation of IConnectionProvider interface. It creates and returns an
 * instance of PgConnection.
 */
public class BasicConnectionProvider implements ConnectionProvider {

    /**
     * Called to create a connection.
     *
     * @param hostSpecs The HostSpec containing the host-port information for the host to connect to
     * @param props     The Properties to use for the connection
     * @param url       The connection URL
     * @return {@link Connection} resulting from the given connection information
     * @throws SQLException if an error occurs
     */
    @Override
    public Connection connect(HostSpec[] hostSpecs, Properties props, @Nullable String url)
            throws SQLException {
        //TODO
        //return new PgConnection(hostSpecs, props, url == null ? "" : url);
        return null;
    }
}
