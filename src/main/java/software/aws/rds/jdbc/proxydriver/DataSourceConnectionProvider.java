/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
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
   * @param props The Properties to use for the connection
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
   * @param url The connection URL
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
