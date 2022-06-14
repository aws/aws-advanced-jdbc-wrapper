/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.SQLException;
import java.util.List;

public interface HostListProvider {
    List<HostSpec> getHostList();

    void refresh() throws SQLException;
}
