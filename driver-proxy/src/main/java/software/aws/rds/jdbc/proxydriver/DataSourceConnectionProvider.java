/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.util.PropertyUtils;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and
 * returns an instance of PgConnection.
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

  private final DataSource dataSource;

  public DataSourceConnectionProvider(final DataSource dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Called once per connection that needs to be created.
   *
   * @param protocol The connection protocol (example "jdbc:mysql://")
   * @param hostSpec The HostSpec containing the host-port information for the host to connect to
   * @param props    The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  @Override
  public Connection connect(
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props)
      throws SQLException {

    // TODO: make it configurable since every data source has its own interface
    // For now it's hardcoded for MysqlDataSource

    Properties copy = PropertyUtils.copyProperties(props);
    copy.setProperty("serverName", hostSpec.getHost());
    if (hostSpec.isPortSpecified()) {
      copy.put("port", hostSpec.getPort());
    }

    PropertyUtils.applyProperties(this.dataSource, props);
    return this.dataSource.getConnection();
  }

  /**
   * Called once per connection that needs to be created.
   *
   * @param url   The connection URL
   * @param props The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  public Connection connect(final @NonNull String url, final @NonNull Properties props)
      throws SQLException {

    // TODO: make it configurable since every data source has its own interface
    // For now it's hardcoded for MysqlDataSource

    Properties copy = PropertyUtils.copyProperties(props);
    copy.setProperty("url", url);
    PropertyUtils.applyProperties(this.dataSource, props);

    return this.dataSource.getConnection();
  }
}
