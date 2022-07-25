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

package com.amazon.awslabs.jdbc;

import static com.amazon.awslabs.jdbc.util.ConnectionUrlBuilder.buildUrl;
import static com.amazon.awslabs.jdbc.util.StringUtils.isNullOrEmpty;

import com.amazon.awslabs.jdbc.util.PropertyUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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

    Properties copy = PropertyUtils.copyProperties(props);

    if (!isNullOrEmpty(this.serverPropertyName)) {
      copy.setProperty(this.serverPropertyName, hostSpec.getHost());
    }

    if (hostSpec.isPortSpecified() && !isNullOrEmpty(this.portPropertyName)) {
      copy.put(this.portPropertyName, hostSpec.getPort());
    }

    if (!isNullOrEmpty(this.databasePropertyName) && !isNullOrEmpty(PropertyDefinition.DATABASE_NAME.getString(props))) {
      copy.setProperty(this.databasePropertyName, PropertyDefinition.DATABASE_NAME.getString(props));
    }

    if (!isNullOrEmpty(this.userPropertyName) && !isNullOrEmpty(PropertyDefinition.USER.getString(props))) {
      copy.setProperty(this.userPropertyName, PropertyDefinition.USER.getString(props));
    }

    if (!isNullOrEmpty(this.passwordPropertyName) && !isNullOrEmpty(PropertyDefinition.PASSWORD.getString(props))) {
      copy.setProperty(this.passwordPropertyName, PropertyDefinition.PASSWORD.getString(props));
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

    copy.remove(PropertyDefinition.DATABASE_NAME.name);
    copy.remove(PropertyDefinition.USER.name);
    copy.remove(PropertyDefinition.PASSWORD.name);

    PropertyUtils.applyProperties(this.dataSource, copy);
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
    Properties copy = PropertyUtils.copyProperties(props);

    if (!isNullOrEmpty(this.urlPropertyName)) {
      copy.setProperty(this.urlPropertyName, url);
    }

    if (!isNullOrEmpty(this.userPropertyName) && !isNullOrEmpty(PropertyDefinition.USER.getString(props))) {
      copy.put(this.userPropertyName, PropertyDefinition.USER.getString(props));
    }

    if (!isNullOrEmpty(this.passwordPropertyName) && !isNullOrEmpty(PropertyDefinition.PASSWORD.getString(props))) {
      copy.put(this.passwordPropertyName, PropertyDefinition.PASSWORD.getString(props));
    }

    if (!isNullOrEmpty(this.databasePropertyName) && !isNullOrEmpty(PropertyDefinition.DATABASE_NAME.getString(props))) {
      copy.put(this.databasePropertyName, PropertyDefinition.DATABASE_NAME.getString(props));
    }

    copy.remove(PropertyDefinition.DATABASE_NAME.name);
    copy.remove(PropertyDefinition.USER.name);
    copy.remove(PropertyDefinition.PASSWORD.name);

    PropertyUtils.applyProperties(this.dataSource, copy);
    return this.dataSource.getConnection();
  }
}
