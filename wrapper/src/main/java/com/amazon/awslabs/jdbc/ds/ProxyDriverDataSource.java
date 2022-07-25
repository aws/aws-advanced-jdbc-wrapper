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

package com.amazon.awslabs.jdbc.ds;

import static com.amazon.awslabs.jdbc.ConnectionPropertyNames.DATABASE_PROPERTY_NAME;
import static com.amazon.awslabs.jdbc.ConnectionPropertyNames.PASSWORD_PROPERTY_NAME;
import static com.amazon.awslabs.jdbc.ConnectionPropertyNames.USER_PROPERTY_NAME;
import static com.amazon.awslabs.jdbc.util.ConnectionUrlBuilder.buildUrl;
import static com.amazon.awslabs.jdbc.util.ConnectionUrlParser.parsePropertiesFromUrl;

import com.amazon.awslabs.jdbc.DataSourceConnectionProvider;
import com.amazon.awslabs.jdbc.Driver;
import com.amazon.awslabs.jdbc.DriverConnectionProvider;
import com.amazon.awslabs.jdbc.util.ConnectionUrlParser;
import com.amazon.awslabs.jdbc.util.PropertyUtils;
import com.amazon.awslabs.jdbc.util.SqlState;
import com.amazon.awslabs.jdbc.util.WrapperUtils;
import com.amazon.awslabs.jdbc.wrapper.ConnectionWrapper;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ProxyDriverDataSource implements DataSource, Referenceable, Serializable {

  private static final Logger LOGGER = Logger.getLogger(ProxyDriverDataSource.class.getName());

  static {
    try {
      if (!Driver.isRegistered()) {
        Driver.register();
      }
    } catch (SQLException e) {
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
  protected @Nullable String userPropertyName;
  protected @Nullable String passwordPropertyName;

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(this.user, this.password);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    this.user = username;
    this.password = password;

    if (isNullOrEmpty(this.targetDataSourceClassName) && isNullOrEmpty(this.jdbcUrl)) {
      throw new SQLException("Target data source class name or JDBC url is required.");
    }

    Properties props = PropertyUtils.copyProperties(this.targetDataSourceProperties);
    setCredentialProperties(props);
    if (!isNullOrEmpty(this.targetDataSourceClassName)) {
      final DataSource targetDataSource = createTargetDataSource();

      if (!isNullOrEmpty(this.databasePropertyName) && !isNullOrEmpty(props.getProperty(this.databasePropertyName))) {
        props.put(DATABASE_PROPERTY_NAME, props.getProperty(this.databasePropertyName));
      }

      // If the url is set explicitly through setJdbcUrl or the connection properties.
      if (!isNullOrEmpty(this.jdbcUrl)
          || (!isNullOrEmpty(this.urlPropertyName) && !isNullOrEmpty(props.getProperty(this.urlPropertyName)))) {
        setJdbcUrlOrUrlProperty(props);
        setDatabasePropertyFromUrl(props);
        if (isNullOrEmpty(this.user) || isNullOrEmpty(this.password)) {
          setCredentialPropertiesFromUrl(props);
        }

      } else {
        this.jdbcUrl = buildUrl(
            this.jdbcProtocol,
            null,
            this.serverPropertyName,
            this.portPropertyName,
            this.databasePropertyName,
            this.userPropertyName,
            this.passwordPropertyName,
            props);
      }

      if (isNullOrEmpty(this.jdbcUrl)) {
        throw new SQLException(
            "No JDBC URL was provided, or a JDBC URL couldn't be built from the provided information.");
      }
      PropertyUtils.applyProperties(targetDataSource, props);

      return new ConnectionWrapper(
          props,
          this.jdbcUrl,
          new DataSourceConnectionProvider(
              targetDataSource,
              this.serverPropertyName,
              this.portPropertyName,
              this.urlPropertyName,
              this.databasePropertyName,
              this.userPropertyName,
              this.passwordPropertyName));

    } else {

      java.sql.Driver targetDriver = DriverManager.getDriver(this.jdbcUrl);

      if (targetDriver == null) {
        throw new SQLException("Can't find a suitable driver for " + this.jdbcUrl);
      }

      setDatabasePropertyFromUrl(props);
      parsePropertiesFromUrl(this.jdbcUrl, props);

      return new ConnectionWrapper(
          props,
          this.jdbcUrl,
          new DriverConnectionProvider(
              targetDriver,
              this.userPropertyName,
              this.passwordPropertyName));
    }
  }

  public void setTargetDataSourceClassName(@Nullable String dataSourceClassName) {
    this.targetDataSourceClassName = dataSourceClassName;
  }

  public @Nullable String getTargetDataSourceClassName() {
    return this.targetDataSourceClassName;
  }

  public void setServerPropertyName(@NonNull String serverPropertyName) {
    this.serverPropertyName = serverPropertyName;
  }

  public @Nullable String getServerPropertyName() {
    return this.serverPropertyName;
  }

  public void setPortPropertyName(@NonNull String portPropertyName) {
    this.portPropertyName = portPropertyName;
  }

  public @Nullable String getPortPropertyName() {
    return this.portPropertyName;
  }

  public void setUrlPropertyName(@NonNull String urlPropertyName) {
    this.urlPropertyName = urlPropertyName;
  }

  public @Nullable String getUrlPropertyName() {
    return this.urlPropertyName;
  }

  public void setDatabasePropertyName(@NonNull String databasePropertyName) {
    this.databasePropertyName = databasePropertyName;
  }

  public @Nullable String getDatabasePropertyName() {
    return this.databasePropertyName;
  }

  public void setUserPropertyName(@NonNull String usernamePropertyName) {
    this.userPropertyName = usernamePropertyName;
  }

  public @Nullable String getUserPropertyName() {
    return this.userPropertyName;
  }

  public void setPasswordPropertyName(@NonNull String passwordPropertyName) {
    this.passwordPropertyName = passwordPropertyName;
  }

  public @Nullable String getPasswordPropertyName() {
    return this.passwordPropertyName;
  }

  public void setJdbcUrl(@Nullable String url) {
    this.jdbcUrl = url;
  }

  public @Nullable String getJdbcUrl() {
    return this.jdbcUrl;
  }

  public void setJdbcProtocol(@NonNull String jdbcProtocol) {
    this.jdbcProtocol = jdbcProtocol;
  }

  public @Nullable String getJdbcProtocol() {
    return this.jdbcProtocol;
  }

  public void setTargetDataSourceProperties(Properties dataSourceProps) {
    this.targetDataSourceProperties = dataSourceProps;
  }

  public Properties getTargetDataSourceProperties() {
    return this.targetDataSourceProperties;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getUser() {
    return this.user;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPassword() {
    return this.password;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    this.logWriter = out;
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
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
    // TODO
    return null;
  }

  protected boolean isNullOrEmpty(final String str) {
    return str == null || str.isEmpty();
  }

  private void setCredentialProperties(Properties props) {
    // If username was provided as a get connection parameter and a userPropertyName is set.
    if (!isNullOrEmpty(this.user) && !isNullOrEmpty(this.userPropertyName)) {
      props.setProperty(USER_PROPERTY_NAME, this.user);

      // If username was provided in targetDataSourceProperties and a userPropertyName is set.
    } else if (!isNullOrEmpty(this.userPropertyName) && !isNullOrEmpty(props.getProperty(this.userPropertyName))) {
      props.setProperty(USER_PROPERTY_NAME, props.getProperty(this.userPropertyName));
      this.user = props.getProperty(this.userPropertyName);
    }

    if (!isNullOrEmpty(this.password) && !isNullOrEmpty(this.passwordPropertyName)) {
      props.setProperty(PASSWORD_PROPERTY_NAME, this.password);

    } else if (!isNullOrEmpty(this.passwordPropertyName)
        && !isNullOrEmpty(props.getProperty(this.passwordPropertyName))) {
      props.setProperty(PASSWORD_PROPERTY_NAME, props.getProperty(this.passwordPropertyName));
      this.password = props.getProperty(this.passwordPropertyName);
    }
  }

  private DataSource createTargetDataSource() throws SQLException {
    try {
      return WrapperUtils.createInstance(this.targetDataSourceClassName, DataSource.class);
    } catch (InstantiationException instEx) {
      throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getState(), instEx);
    }
  }

  private void setDatabasePropertyFromUrl(Properties props) {
    final String databaseName = ConnectionUrlParser.parseDatabaseFromUrl(this.jdbcUrl);
    if (!isNullOrEmpty(databaseName)) {
      props.setProperty(DATABASE_PROPERTY_NAME, databaseName);
    }
  }

  private void setCredentialPropertiesFromUrl(Properties props) {
    final String userFromUrl = ConnectionUrlParser.parseUserFromUrl(this.jdbcUrl, this.userPropertyName);
    if (isNullOrEmpty(this.user) && !isNullOrEmpty(userFromUrl)) {
      props.setProperty(USER_PROPERTY_NAME, userFromUrl);
      this.user = userFromUrl;
    }

    final String passwordFromUrl = ConnectionUrlParser.parsePasswordFromUrl(this.jdbcUrl, this.passwordPropertyName);
    if (isNullOrEmpty(this.password) && !isNullOrEmpty(passwordFromUrl)) {
      props.setProperty(PASSWORD_PROPERTY_NAME, passwordFromUrl);
      this.password = passwordFromUrl;
    }
  }

  private void setJdbcUrlOrUrlProperty(Properties props) {
    // If the jdbc url wasn't set, use the url property if it exists.
    if (isNullOrEmpty(this.jdbcUrl)
        && (!isNullOrEmpty(this.urlPropertyName) && !isNullOrEmpty(props.getProperty(this.urlPropertyName)))) {
      this.jdbcUrl = props.getProperty(this.urlPropertyName);

      // If the jdbc url and the url property have both been set, use the provided jdbc url.
    } else if ((!isNullOrEmpty(this.urlPropertyName) && !isNullOrEmpty(props.getProperty(this.urlPropertyName)))) {
      props.setProperty(this.urlPropertyName, this.jdbcUrl);
    }
  }
}
