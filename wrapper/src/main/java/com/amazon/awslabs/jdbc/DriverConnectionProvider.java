/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc;

import static com.amazon.awslabs.jdbc.ConnectionPropertyNames.DATABASE_PROPERTY_NAME;
import static com.amazon.awslabs.jdbc.ConnectionPropertyNames.PASSWORD_PROPERTY_NAME;
import static com.amazon.awslabs.jdbc.ConnectionPropertyNames.USER_PROPERTY_NAME;
import static com.amazon.awslabs.jdbc.util.StringUtils.isNullOrEmpty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and returns a connection
 * provided by a target driver or a data source.
 */
public class DriverConnectionProvider implements ConnectionProvider {

  private final java.sql.Driver driver;
  private final String userPropertyName;
  private final String passwordPropertyName;

  public DriverConnectionProvider(final java.sql.Driver driver) {
    this(driver, null, null);
  }

  public DriverConnectionProvider(final java.sql.Driver driver, String userPropertyName, String passwordPropertyName) {
    this.driver = driver;
    this.userPropertyName = userPropertyName;
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

    final String databaseName =
        props.getProperty(DATABASE_PROPERTY_NAME) != null ? props.getProperty(DATABASE_PROPERTY_NAME) : "";
    final StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(protocol).append(hostSpec.getUrl()).append(databaseName);

    if (!isNullOrEmpty(this.userPropertyName) && !isNullOrEmpty(props.getProperty(USER_PROPERTY_NAME))) {
      props.setProperty(this.userPropertyName, props.getProperty(USER_PROPERTY_NAME));
    }

    if (!isNullOrEmpty(this.passwordPropertyName) && !isNullOrEmpty(props.getProperty(PASSWORD_PROPERTY_NAME))) {
      props.setProperty(this.passwordPropertyName, props.getProperty(PASSWORD_PROPERTY_NAME));
    }

    return this.driver.connect(urlBuilder.toString(), props);
  }

  /**
   * Called once per connection that needs to be created.
   *
   * @param url   The connection URL
   * @param props The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  public Connection connect(@NonNull String url, @NonNull Properties props) throws SQLException {

    return this.driver.connect(url, props);
  }
}
