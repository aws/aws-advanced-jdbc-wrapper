/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc;

import static software.amazon.jdbc.util.ConnectionUrlBuilder.buildUrl;
import static software.amazon.jdbc.util.StringUtils.isNullOrEmpty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.util.PropertyUtils;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and
 * returns an instance of PgConnection.
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

  private static final Logger LOGGER = Logger.getLogger(DataSourceConnectionProvider.class.getName());

  private final @NonNull DataSource dataSource;
  private final @Nullable String serverPropertyName;
  private final @Nullable String portPropertyName;
  private final @Nullable String urlPropertyName;
  private final @Nullable String databasePropertyName;

  public DataSourceConnectionProvider(
      final @NonNull DataSource dataSource,
      final @Nullable String serverPropertyName,
      final @Nullable String portPropertyName,
      final @Nullable String urlPropertyName,
      final @Nullable String databasePropertyName) {
    this.dataSource = dataSource;
    this.serverPropertyName = serverPropertyName;
    this.portPropertyName = portPropertyName;
    this.urlPropertyName = urlPropertyName;
    this.databasePropertyName = databasePropertyName;
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
      final @NonNull Dialect dialect,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props)
      throws SQLException {

    final Properties copy = PropertyUtils.copyProperties(props);

    if (!isNullOrEmpty(this.serverPropertyName)) {
      copy.setProperty(this.serverPropertyName, hostSpec.getHost());
    }

    if (hostSpec.isPortSpecified() && !isNullOrEmpty(this.portPropertyName)) {
      copy.put(this.portPropertyName, hostSpec.getPort());
    }

    if (!isNullOrEmpty(this.databasePropertyName)
        && !isNullOrEmpty(PropertyDefinition.DATABASE.getString(props))) {
      copy.setProperty(this.databasePropertyName, PropertyDefinition.DATABASE.getString(props));
    }

    if (!isNullOrEmpty(this.urlPropertyName)) {
      final Properties urlProperties = PropertyUtils.copyProperties(copy);

      if (!isNullOrEmpty(props.getProperty(this.urlPropertyName))) {
        // Remove the current url property to replace with a new url built from updated HostSpec and properties
        urlProperties.remove(this.urlPropertyName);
      }

      String finalUrl = buildUrl(
          protocol,
          hostSpec,
          this.serverPropertyName,
          this.portPropertyName,
          this.databasePropertyName,
          urlProperties);
      LOGGER.finest(() -> "Connecting to " + finalUrl);
      copy.setProperty(this.urlPropertyName, finalUrl);
    }

    PropertyDefinition.removeAllExceptCredentials(copy);
    PropertyUtils.applyProperties(this.dataSource, copy);

    LOGGER.finest(() -> PropertyUtils.logProperties(copy, "Connecting with properties: \n"));

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
  public Connection connect(final @NonNull String url, final @NonNull Properties props) throws SQLException {
    final Properties copy = PropertyUtils.copyProperties(props);

    if (!isNullOrEmpty(this.urlPropertyName)) {
      copy.setProperty(this.urlPropertyName, url);
    }

    if (!isNullOrEmpty(this.databasePropertyName)
        && !isNullOrEmpty(PropertyDefinition.DATABASE.getString(props))) {
      copy.put(this.databasePropertyName, PropertyDefinition.DATABASE.getString(props));
    }

    PropertyDefinition.removeAllExceptCredentials(copy);
    PropertyUtils.applyProperties(this.dataSource, copy);
    return this.dataSource.getConnection();
  }
}
