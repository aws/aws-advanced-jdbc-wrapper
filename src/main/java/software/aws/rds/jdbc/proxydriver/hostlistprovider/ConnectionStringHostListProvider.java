/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.hostlistprovider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import software.aws.rds.jdbc.proxydriver.HostListProvider;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.util.ConnectionUrlParser;

public class ConnectionStringHostListProvider implements HostListProvider {

  final List<HostSpec> hostList = new ArrayList<>();
  Properties properties;

  public ConnectionStringHostListProvider(final Properties properties, final String initialUrl) {
    this(properties, initialUrl, new ConnectionUrlParser());
  }

  ConnectionStringHostListProvider(
      final Properties properties,
      final String initialUrl,
      final ConnectionUrlParser connectionUrlParser) {
    // TODO: check properties for relevant parameters

    hostList.addAll(connectionUrlParser.getHostsFromConnectionUrl(initialUrl));
  }

  @Override
  public List<HostSpec> refresh() throws SQLException {
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    return Collections.unmodifiableList(hostList);
  }
}
