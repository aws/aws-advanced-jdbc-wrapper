/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.util.PropertyUtils;
import software.aws.rds.jdbc.proxydriver.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and returns an
 * instance of PgConnection.
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

    private DataSource dataSource;

    public DataSourceConnectionProvider(DataSource dataSource) {
        this.dataSource = dataSource;
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

        //TODO: make it configurable since every data source has its own interface
        // For now it's hardcoded for MysqlDataSource
        Properties copy = PropertyUtils.copyProperties(props);
        if(!StringUtils.isNullOrEmpty(url)) {
            // If url is provided then use it
            copy.setProperty("url", url);
        } else if(hostSpecs != null && hostSpecs.length > 0) {
            // Otherwise, set up a host and a port and let the data source to handle it
            copy.setProperty("serverName", hostSpecs[0].host);
            copy.put("port", hostSpecs[0].port);
        }

        PropertyUtils.applyProperties(this.dataSource, props);
        return this.dataSource.getConnection();
    }
}
