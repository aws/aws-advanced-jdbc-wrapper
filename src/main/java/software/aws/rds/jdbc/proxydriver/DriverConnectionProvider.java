/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and
 * returns an instance of PgConnection.
 */
public class DriverConnectionProvider implements ConnectionProvider {

  private final java.sql.Driver driver;

  public DriverConnectionProvider(final java.sql.Driver driver) {
    this.driver = driver;
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

    final String url = protocol + hostSpec.getUrl();
    return this.driver.connect(url, props);
  }

  /**
   * Called once per connection that needs to be created.
   *
   * @param url The connection URL
   * @param props The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  public Connection connect(@NonNull String url, @NonNull Properties props) throws SQLException {

    return this.driver.connect(url, props);
  }
}
