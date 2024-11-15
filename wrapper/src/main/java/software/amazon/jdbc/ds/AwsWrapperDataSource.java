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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.TargetDriverHelper;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.DriverConfigurationProfiles;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialectManager;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class AwsWrapperDataSource implements DataSource, Referenceable, Serializable {

  private static final Logger LOGGER = Logger.getLogger(AwsWrapperDataSource.class.getName());

  private static final String PROTOCOL_PREFIX = "jdbc:aws-wrapper:";

  private static final String SERVER_NAME = "serverName";
  private static final String SERVER_PORT = "serverPort";

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
  protected @Nullable String serverName;
  protected @Nullable String serverPort;
  protected @Nullable String database;
  private int loginTimeout = 0;

  @Override
  public Connection getConnection() throws SQLException {
    setCredentialPropertiesFromUrl(this.jdbcUrl);
    return getConnection(this.user, this.password);
  }

  @Override
  public Connection getConnection(final String username, final String password) throws SQLException {
    this.user = username;
    this.password = password;

    final Properties props = PropertyUtils.copyProperties(this.targetDataSourceProperties);

    final String profileName = PropertyDefinition.PROFILE_NAME.getString(props);
    ConfigurationProfile configurationProfile = null;
    if (!StringUtils.isNullOrEmpty(profileName)) {
      configurationProfile = DriverConfigurationProfiles.getProfileConfiguration(profileName);
      if (configurationProfile != null) {
        PropertyUtils.addProperties(props, configurationProfile.getProperties());
        if (configurationProfile.getAwsCredentialsProviderHandler() != null) {
          AwsCredentialsManager.setCustomHandler(configurationProfile.getAwsCredentialsProviderHandler());
        }
      } else {
        throw new SQLException(
            Messages.get(
                "AwsWrapperDataSource.configurationProfileNotFound",
                new Object[] {profileName}));
      }
    }

    String finalUrl;

    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);
    final TelemetryContext context = telemetryFactory.openTelemetryContext(
        "software.amazon.jdbc.ds.AwsWrapperDataSource.getConnection",
        TelemetryTraceLevel.TOP_LEVEL);

    try {
      // Identify the URL for connection.
      if (!StringUtils.isNullOrEmpty(this.jdbcUrl)) {
        finalUrl = this.jdbcUrl.replaceFirst(PROTOCOL_PREFIX, "jdbc:");

        parsePropertiesFromUrl(this.jdbcUrl, props);
        setDatabasePropertyFromUrl(props);

        // Override credentials with the ones provided through the data source property.
        setCredentialProperties(props);

        // Override database with the one provided through the data source property.
        if (!StringUtils.isNullOrEmpty(this.database)) {
          PropertyDefinition.DATABASE.set(props, this.database);
        }

      } else {
        final String serverName = !StringUtils.isNullOrEmpty(this.serverName)
            ? this.serverName
            : props.getProperty(SERVER_NAME);
        final String serverPort = !StringUtils.isNullOrEmpty(this.serverPort)
            ? this.serverPort
            : props.getProperty(SERVER_PORT);
        final String databaseName = !StringUtils.isNullOrEmpty(this.database)
            ? this.database
            : PropertyDefinition.DATABASE.getString(props);

        if (StringUtils.isNullOrEmpty(serverName)) {
          throw new SQLException(Messages.get("AwsWrapperDataSource.missingTarget"));
        }
        if (StringUtils.isNullOrEmpty(this.jdbcProtocol)) {
          throw new SQLException(Messages.get("AwsWrapperDataSource.missingJdbcProtocol"));
        }

        int port = HostSpec.NO_PORT;
        if (!StringUtils.isNullOrEmpty(serverPort)) {
          port = Integer.parseInt(serverPort);
        }

        finalUrl = buildUrl(this.jdbcProtocol, serverName, port, databaseName);

        // Override credentials with the ones provided through the data source property.
        setCredentialProperties(props);

        // Override database with the one provided through the data source property.
        if (!StringUtils.isNullOrEmpty(databaseName)) {
          PropertyDefinition.DATABASE.set(props, databaseName);
        }
      }

      TargetDriverDialect targetDriverDialect = configurationProfile == null
          ? null
          : configurationProfile.getTargetDriverDialect();

      ConnectionProvider effectiveConnectionProvider = null;
      if (configurationProfile != null) {
        effectiveConnectionProvider = configurationProfile.getConnectionProvider();
      }

      // Identify what connection provider to use.
      if (!StringUtils.isNullOrEmpty(this.targetDataSourceClassName)) {

        final DataSource targetDataSource = createTargetDataSource();

        try {
          targetDataSource.setLoginTimeout(loginTimeout);
        } catch (Exception ex) {
          LOGGER.finest(
              () ->
                  Messages.get(
                      "DataSource.failedToSetProperty",
                      new Object[] {"loginTimeout", targetDataSource.getClass(), ex.getCause().getMessage()}));
        }

        if (targetDriverDialect == null) {
          final TargetDriverDialectManager targetDriverDialectManager = new TargetDriverDialectManager();
          targetDriverDialect = targetDriverDialectManager.getDialect(this.targetDataSourceClassName, props);
        }

        ConnectionProvider defaultConnectionProvider = new DataSourceConnectionProvider(targetDataSource);

        return createConnectionWrapper(
            props,
            finalUrl,
            defaultConnectionProvider,
            effectiveConnectionProvider,
            targetDriverDialect,
            configurationProfile,
            telemetryFactory);
      } else {
        TargetDriverHelper helper = new TargetDriverHelper();
        final java.sql.Driver targetDriver = helper.getTargetDriver(finalUrl, props);

        if (targetDriverDialect == null) {
          final TargetDriverDialectManager targetDriverDialectManager = new TargetDriverDialectManager();
          targetDriverDialect = targetDriverDialectManager.getDialect(targetDriver, props);
        }

        ConnectionProvider defaultConnectionProvider = new DriverConnectionProvider(targetDriver);

        return createConnectionWrapper(
            props,
            finalUrl,
            defaultConnectionProvider,
            effectiveConnectionProvider,
            targetDriverDialect,
            configurationProfile,
            telemetryFactory);
      }
    } catch (Exception ex) {
      context.setException(ex);
      context.setSuccess(false);
      throw ex;
    } finally {
      context.closeContext();
    }
  }

  ConnectionWrapper createConnectionWrapper(
      final Properties props,
      final String url,
      final @NonNull ConnectionProvider defaultProvider,
      final @Nullable ConnectionProvider effectiveProvider,
      final @NonNull TargetDriverDialect targetDriverDialect,
      final @Nullable ConfigurationProfile configurationProfile,
      final TelemetryFactory telemetryFactory) throws SQLException {
    return new ConnectionWrapper(
        props,
        url,
        defaultProvider,
        effectiveProvider,
        targetDriverDialect,
        configurationProfile,
        telemetryFactory);
  }

  public void setTargetDataSourceClassName(@Nullable final String dataSourceClassName) {
    this.targetDataSourceClassName = dataSourceClassName;
  }

  public @Nullable String getTargetDataSourceClassName() {
    return this.targetDataSourceClassName;
  }

  public void setServerName(@NonNull final String serverName) {
    this.serverName = serverName;
  }

  public @Nullable String getServerName() {
    return this.serverName;
  }

  public void setServerPort(@NonNull final String serverPort) {
    this.serverPort = serverPort;
  }

  public @Nullable String getServerPort() {
    return this.serverPort;
  }

  public void setDatabase(@NonNull final String database) {
    this.database = database;
  }

  public @Nullable String getDatabase() {
    return this.database;
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

  public void setTargetDataSourceProperties(final @Nullable Properties dataSourceProps) {
    this.targetDataSourceProperties = dataSourceProps;
  }

  public @Nullable Properties getTargetDataSourceProperties() {
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
    if (seconds < 0) {
      throw new SQLException("Login timeout cannot be a negative value.");
    }
    loginTimeout = seconds;
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return loginTimeout;
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
    reference.add(new StringRefAddr("serverName", getServerName()));
    reference.add(new StringRefAddr("serverPort", getServerPort()));
    reference.add(new StringRefAddr("database", getDatabase()));

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

  DataSource createTargetDataSource() throws SQLException {
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

  private void setCredentialPropertiesFromUrl(final String jdbcUrl) {
    if (StringUtils.isNullOrEmpty(jdbcUrl)) {
      return;
    }

    if (StringUtils.isNullOrEmpty(this.user)) {
      this.user = ConnectionUrlParser.parseUserFromUrl(jdbcUrl);
    }

    if (StringUtils.isNullOrEmpty(this.password)) {
      this.password = ConnectionUrlParser.parsePasswordFromUrl(jdbcUrl);
    }
  }
}
