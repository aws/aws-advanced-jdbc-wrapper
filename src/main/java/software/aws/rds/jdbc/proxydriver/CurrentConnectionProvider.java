/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;

/**
 * Interface for retrieving the current active {@link Connection} and its {@link HostSpec}.
 */
public interface CurrentConnectionProvider {
    Connection getCurrentConnection();

    HostSpec getCurrentHostSpec();

    void setCurrentConnection(Connection connection, HostSpec hostSpec);
}
