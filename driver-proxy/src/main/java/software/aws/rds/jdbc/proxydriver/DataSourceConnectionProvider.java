/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import static software.aws.rds.jdbc.proxydriver.ConnectionPropertyNames.DATABASE_PROPERTY_NAME;
import static software.aws.rds.jdbc.proxydriver.ConnectionPropertyNames.PASSWORD_PROPERTY_NAME;
import static software.aws.rds.jdbc.proxydriver.ConnectionPropertyNames.USER_PROPERTY_NAME;
import static software.aws.rds.jdbc.proxydriver.util.ConnectionUrlBuilder.buildUrl;
import static software.aws.rds.jdbc.proxydriver.util.StringUtils.isNullOrEmpty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.util.PropertyUtils;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and
 * returns an instance of PgConnection.
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

  private final @NonNull DataSource dataSource;
  private final @Nullable String serverPropertyName;
  private final @Nullable String portPropertyName;
  private final @Nullable String urlPropertyName;
  private final @Nullable String databasePropertyName;
  private final @Nullable String userPropertyName;
  private final @Nullable String passwordPropertyName;

  public DataSourceConnectionProvider(final DataSource dataSource) {
    this(dataSource, null, null, null, null, null, null);
  }

  public DataSourceConnectionProvider(final @NonNull DataSource dataSource,
                                      final @Nullable String serverPropertyName,
                                      final @Nullable String portPropertyName,
                                      final @Nullable String urlPropertyName,
                                      final @Nullable String databasePropertyName,
                                      final @Nullable String usernamePropertyName,
                                      final @Nullable String passwordPropertyName) {
    this.dataSource = dataSource;
    this.serverPropertyName = serverPropertyName;
    this.portPropertyName = portPropertyName;
    this.urlPropertyName = urlPropertyName;
    this.databasePropertyName = databasePropertyName;
    this.userPropertyName = usernamePropertyName;
    this.passwordPropertyName = passwordPropertyName;
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

    Properties copy = PropertyUtils.copyProperties(props);

    if (!isNullOrEmpty(this.serverPropertyName)) {
      copy.setProperty(this.serverPropertyName, hostSpec.getHost());
    }

    if (hostSpec.isPortSpecified() && !isNullOrEmpty(this.portPropertyName)) {
      copy.put(this.portPropertyName, hostSpec.getPort());
    }

    if (!isNullOrEmpty(this.databasePropertyName) && !isNullOrEmpty(props.getProperty(DATABASE_PROPERTY_NAME))) {
      copy.setProperty(this.databasePropertyName, props.getProperty(DATABASE_PROPERTY_NAME));
    }

    if (!isNullOrEmpty(this.userPropertyName) && !isNullOrEmpty(props.getProperty(USER_PROPERTY_NAME))) {
      copy.setProperty(this.userPropertyName, props.getProperty(USER_PROPERTY_NAME));
    }

    if (!isNullOrEmpty(this.passwordPropertyName) && !isNullOrEmpty(props.getProperty(PASSWORD_PROPERTY_NAME))) {
      copy.setProperty(this.passwordPropertyName, props.getProperty(PASSWORD_PROPERTY_NAME));
    }

    if (!isNullOrEmpty(this.urlPropertyName) && !isNullOrEmpty(props.getProperty(this.urlPropertyName))) {
      Properties urlProperties = PropertyUtils.copyProperties(copy);
      // Remove the current url property to replace with a new url built from updated HostSpec and properties
      urlProperties.remove(this.urlPropertyName);

      copy.setProperty(
          this.urlPropertyName,
          buildUrl(
              protocol,
              hostSpec,
              this.serverPropertyName,
              this.portPropertyName,
              this.databasePropertyName,
              this.userPropertyName,
              this.passwordPropertyName,
              urlProperties));
    }

    PropertyUtils.applyProperties(this.dataSource, copy);
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
  public Connection connect(final @NonNull String url, final @NonNull Properties props) throws SQLException {
    Properties copy = PropertyUtils.copyProperties(props);

    if (!isNullOrEmpty(this.urlPropertyName)) {
      copy.setProperty(this.urlPropertyName, url);
    }

    if (!isNullOrEmpty(this.userPropertyName) && !isNullOrEmpty(props.getProperty(USER_PROPERTY_NAME))) {
      copy.put(this.userPropertyName, props.getProperty(USER_PROPERTY_NAME));
    }

    if (!isNullOrEmpty(this.passwordPropertyName) && !isNullOrEmpty(props.getProperty(PASSWORD_PROPERTY_NAME))) {
      copy.put(this.passwordPropertyName, props.getProperty(PASSWORD_PROPERTY_NAME));
    }

    if (!isNullOrEmpty(this.databasePropertyName) && !isNullOrEmpty(props.getProperty(DATABASE_PROPERTY_NAME))) {
      copy.put(this.databasePropertyName, props.getProperty(DATABASE_PROPERTY_NAME));
    }

    PropertyUtils.applyProperties(this.dataSource, copy);
    return this.dataSource.getConnection();
  }
}
