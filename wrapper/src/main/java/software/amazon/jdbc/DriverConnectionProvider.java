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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and
 * returns a connection provided by a target driver or a data source.
 */
public class DriverConnectionProvider implements ConnectionProvider {

  private static final Logger LOGGER = Logger.getLogger(DriverConnectionProvider.class.getName());

  private static final Map<String, HostSelector> acceptedStrategies =
      Collections.unmodifiableMap(new HashMap<String, HostSelector>() {
        {
          put("random", new RandomHostSelector());
        }
      });

  private final java.sql.Driver driver;

  public DriverConnectionProvider(final java.sql.Driver driver) {
    this.driver = driver;
  }

  /**
   * Indicates whether this ConnectionProvider can provide connections for the given host and
   * properties. Some ConnectionProvider implementations may not be able to handle certain URL
   * types or properties.
   *
   * @param protocol The connection protocol (example "jdbc:mysql://")
   * @param hostSpec The HostSpec containing the host-port information for the host to connect to
   * @param props    The Properties to use for the connection
   * @return true if this ConnectionProvider can provide connections for the given URL, otherwise
   *         return false
   */
  @Override
  public boolean acceptsUrl(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props) {
    return true;
  }

  @Override
  public boolean acceptsStrategy(@NonNull HostRole role, @NonNull String strategy) {
    return acceptedStrategies.containsKey(strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(
      @NonNull List<HostSpec> hosts, @NonNull HostRole role, @NonNull String strategy)
      throws SQLException {
    if (!acceptedStrategies.containsKey(strategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "ConnectionProvider.unsupportedHostSpecSelectorStrategy",
              new Object[] {strategy, DriverConnectionProvider.class}));
    }

    return acceptedStrategies.get(strategy).getHost(hosts, role);
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
    final String databaseName =
        PropertyDefinition.DATABASE.getString(props) != null
            ? PropertyDefinition.DATABASE.getString(props)
            : "";
    final StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(protocol).append(hostSpec.getUrl()).append(databaseName);

    // In the case where we are connecting to MySQL using MariaDB driver,
    // we need to append "?permitMysqlScheme" to the connection URL
    if (protocol.startsWith("jdbc:mysql:")
        && props.stringPropertyNames().contains("permitMysqlScheme")) {
      urlBuilder.append("?permitMysqlScheme");
    }

    LOGGER.finest(() -> "Connecting to " + urlBuilder);
    LOGGER.finest(() -> PropertyUtils.logProperties(props, "Connecting with properties: \n"));

    return this.driver.connect(urlBuilder.toString(), props);
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

    LOGGER.finest(() -> "Connecting to " + url);
    return this.driver.connect(url, props);
  }
}
