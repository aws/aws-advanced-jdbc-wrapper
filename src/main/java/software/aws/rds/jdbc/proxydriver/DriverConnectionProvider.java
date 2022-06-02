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
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and returns an
 * instance of PgConnection.
 */
public class DriverConnectionProvider implements ConnectionProvider {

    private java.sql.Driver driver;

    public DriverConnectionProvider(java.sql.Driver driver) {
        this.driver = driver;
    }

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
        return this.driver.connect(url, props);
    }
}
