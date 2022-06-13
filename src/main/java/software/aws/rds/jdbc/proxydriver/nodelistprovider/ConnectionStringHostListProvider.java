/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.nodelistprovider;

import java.sql.SQLException;
import software.aws.rds.jdbc.proxydriver.HostListProvider;

public class ConnectionStringHostListProvider implements HostListProvider {

  @Override
  public void refresh() throws SQLException {
    // do nothing
  }
}
