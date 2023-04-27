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

package software.amazon.jdbc.ds;

import static software.amazon.jdbc.util.ConnectionUrlBuilder.buildUrl;
import static software.amazon.jdbc.util.ConnectionUrlParser.parsePropertiesFromUrl;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.DataSourceConnectionProvider;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.DriverConnectionProvider;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class AwsWrapperDataSource implements DataSource, Referenceable, Serializable {

  private static final Logger LOGGER = Logger.getLogger(AwsWrapperDataSource.class.getName());

  static {
    try {
      if (!Driver.isRegistered()) {
        Driver.register();
      }
    } catch (final SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  protected transient @Nullable PrintWriter logWriter;
  protected @Nullable String user;
  protected @Nullable String password;
  protected @Nullable String jdbcUrl;
  protected @Nullable String targetDataSourceClassName;
  protected @Nullable Properties targetDataSourceProperties;
  protected @Nullable String jdbcProtocol;
  protected @Nullable String serverPropertyName;
  protected @Nullable String portPropertyName;
  protected @Nullable String urlPropertyName;
  protected @Nullable String databasePropertyName;

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(this.user, this.password);
  }

  @Override
  public Connection getConnection(final String username, final String password) throws SQLException {
    this.user = username;
    this.password = password;

    if (StringUtils.isNullOrEmpty(this.targetDataSourceClassName) && StringUtils.isNullOrEmpty(this.jdbcUrl)) {
      throw new SQLException(Messages.get("AwsWrapperDataSource.missingTarget"));
    }

    final Properties props = PropertyUtils.copyProperties(this.targetDataSourceProperties);
    setCredentialProperties(props);

    if (!StringUtils.isNullOrEmpty(this.targetDataSourceClassName)) {
      final DataSource targetDataSource = createTargetDataSource();

      if (!StringUtils.isNullOrEmpty(this.databasePropertyName)
          && !StringUtils.isNullOrEmpty(props.getProperty(this.databasePropertyName))) {
        PropertyDefinition.DATABASE.set(props, props.getProperty(this.databasePropertyName));
      }

      // If the url is set explicitly through setJdbcUrl or the connection properties.
      if (!StringUtils.isNullOrEmpty(this.jdbcUrl)
          || (!StringUtils.isNullOrEmpty(this.urlPropertyName)
          && !StringUtils.isNullOrEmpty(props.getProperty(this.urlPropertyName)))) {
        final Properties parsedProperties = new Properties();
        if (!StringUtils.isNullOrEmpty(this.jdbcUrl)) {
          parsePropertiesFromUrl(this.jdbcUrl, parsedProperties);
        } else {
          parsePropertiesFromUrl(props.getProperty(this.urlPropertyName), parsedProperties);
        }
        parsedProperties.forEach(props::putIfAbsent);
        setJdbcUrlOrUrlProperty(props);
        setDatabasePropertyFromUrl(props);
        if (StringUtils.isNullOrEmpty(this.user) || StringUtils.isNullOrEmpty(this.password)) {
          setCredentialPropertiesFromUrl(props);
        }

      } else {
        this.jdbcUrl = buildUrl(
            this.jdbcProtocol,
            null,
            this.serverPropertyName,
            this.portPropertyName,
            this.databasePropertyName,
            props);
      }

      if (StringUtils.isNullOrEmpty(this.jdbcUrl)) {
        throw new SQLException(Messages.get("AwsWrapperDataSource.missingUrl"));
      }

      return createConnectionWrapper(
          props,
          this.jdbcUrl,
          new DataSourceConnectionProvider(
              targetDataSource,
              this.serverPropertyName,
              this.portPropertyName,
              this.urlPropertyName,
              this.databasePropertyName));

    } else {

      final java.sql.Driver targetDriver = DriverManager.getDriver(this.jdbcUrl);

      if (targetDriver == null) {
        throw new SQLException(Messages.get("AwsWrapperDataSource.missingDriver", new Object[] {this.jdbcUrl}));
      }

      parsePropertiesFromUrl(this.jdbcUrl, props);
      setCredentialProperties(props);
      setDatabasePropertyFromUrl(props);

      return createConnectionWrapper(props, this.jdbcUrl, new DriverConnectionProvider(targetDriver));
    }
  }

  ConnectionWrapper createConnectionWrapper(
      final Properties props,
      final String url,
      final ConnectionProvider provider) throws SQLException {
    return new ConnectionWrapper(props, url, provider);
  }

  public void setTargetDataSourceClassName(@Nullable final String dataSourceClassName) {
    this.targetDataSourceClassName = dataSourceClassName;
  }

  public @Nullable String getTargetDataSourceClassName() {
    return this.targetDataSourceClassName;
  }

  public void setServerPropertyName(@NonNull final String serverPropertyName) {
    this.serverPropertyName = serverPropertyName;
  }

  public @Nullable String getServerPropertyName() {
    return this.serverPropertyName;
  }

  public void setPortPropertyName(@NonNull final String portPropertyName) {
    this.portPropertyName = portPropertyName;
  }

  public @Nullable String getPortPropertyName() {
    return this.portPropertyName;
  }

  public void setUrlPropertyName(@NonNull final String urlPropertyName) {
    this.urlPropertyName = urlPropertyName;
  }

  public @Nullable String getUrlPropertyName() {
    return this.urlPropertyName;
  }

  public void setDatabasePropertyName(@NonNull final String databasePropertyName) {
    this.databasePropertyName = databasePropertyName;
  }

  public @Nullable String getDatabasePropertyName() {
    return this.databasePropertyName;
  }

  public void setJdbcUrl(@Nullable final String url) {
    this.jdbcUrl = url;
  }

  public @Nullable String getJdbcUrl() {
    return this.jdbcUrl;
  }

  public void setJdbcProtocol(@NonNull final String jdbcProtocol) {
    this.jdbcProtocol = jdbcProtocol;
  }

  public @Nullable String getJdbcProtocol() {
    return this.jdbcProtocol;
  }

  public void setTargetDataSourceProperties(final Properties dataSourceProps) {
    this.targetDataSourceProperties = dataSourceProps;
  }

  public Properties getTargetDataSourceProperties() {
    return this.targetDataSourceProperties;
  }

  public void setUser(final String user) {
    this.user = user;
  }

  public String getUser() {
    return this.user;
  }

  public void setPassword(final String password) {
    this.password = password;
  }

  public String getPassword() {
    return this.password;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.logWriter;
  }

  @Override
  public void setLogWriter(final PrintWriter out) throws SQLException {
    this.logWriter = out;
  }

  @Override
  public void setLoginTimeout(final int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

  @Override
  public Reference getReference() throws NamingException {
    final Reference reference =
        new Reference(getClass().getName(), AwsWrapperDataSourceFactory.class.getName(), null);
    reference.add(new StringRefAddr("user", getUser()));
    reference.add(new StringRefAddr("password", getPassword()));
    reference.add(new StringRefAddr("jdbcUrl", getJdbcUrl()));
    reference.add(new StringRefAddr("targetDataSourceClassName", getTargetDataSourceClassName()));
    reference.add(new StringRefAddr("jdbcProtocol", getJdbcProtocol()));
    reference.add(new StringRefAddr("serverPropertyName", getServerPropertyName()));
    reference.add(new StringRefAddr("portPropertyName", getPortPropertyName()));
    reference.add(new StringRefAddr("urlPropertyName", getUrlPropertyName()));
    reference.add(new StringRefAddr("databasePropertyName", getDatabasePropertyName()));

    if (this.targetDataSourceProperties != null) {
      for (final Map.Entry<Object, Object> entry : this.targetDataSourceProperties.entrySet()) {
        reference.add(new StringRefAddr(entry.getKey().toString(), entry.getValue().toString()));
      }
    }

    return reference;
  }

  private void setCredentialProperties(final Properties props) {
    // If username was provided as a get connection parameter.
    if (!StringUtils.isNullOrEmpty(this.user)) {
      PropertyDefinition.USER.set(props, this.user);
    }

    if (!StringUtils.isNullOrEmpty(this.password)) {
      PropertyDefinition.PASSWORD.set(props, this.password);
    }
  }

  private DataSource createTargetDataSource() throws SQLException {
    try {
      return WrapperUtils.createInstance(this.targetDataSourceClassName, DataSource.class);
    } catch (final InstantiationException instEx) {
      throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getState(), instEx);
    }
  }

  private void setDatabasePropertyFromUrl(final Properties props) {
    final String databaseName = ConnectionUrlParser.parseDatabaseFromUrl(this.jdbcUrl);
    if (!StringUtils.isNullOrEmpty(databaseName)) {
      PropertyDefinition.DATABASE.set(props, databaseName);
    }
  }

  private void setCredentialPropertiesFromUrl(final Properties props) {
    final String userFromUrl = ConnectionUrlParser.parseUserFromUrl(this.jdbcUrl);
    if (StringUtils.isNullOrEmpty(this.user) && !StringUtils.isNullOrEmpty(userFromUrl)) {
      this.user = userFromUrl;
      PropertyDefinition.USER.set(props, this.user);
    }

    final String passwordFromUrl = ConnectionUrlParser.parsePasswordFromUrl(this.jdbcUrl);
    if (StringUtils.isNullOrEmpty(this.password) && !StringUtils.isNullOrEmpty(passwordFromUrl)) {
      this.password = passwordFromUrl;
      PropertyDefinition.PASSWORD.set(props, this.password);
    }
  }

  private void setJdbcUrlOrUrlProperty(final Properties props) {
    // If the jdbc url wasn't set, use the url property if it exists.
    if (StringUtils.isNullOrEmpty(this.jdbcUrl)
        && (!StringUtils.isNullOrEmpty(this.urlPropertyName)
        && !StringUtils.isNullOrEmpty(props.getProperty(this.urlPropertyName)))) {
      this.jdbcUrl = props.getProperty(this.urlPropertyName);

      // If the url property wasn't set, use the provided jdbc url.
    } else if (!StringUtils.isNullOrEmpty(this.urlPropertyName)) {
      props.setProperty(this.urlPropertyName, this.jdbcUrl);
    }
  }
}
