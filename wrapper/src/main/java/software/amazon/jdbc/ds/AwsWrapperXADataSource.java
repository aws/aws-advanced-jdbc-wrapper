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

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.ds.xa.XAConnectionWrapper;
import software.amazon.jdbc.ds.xa.XADataSourceConnectionProvider;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.DriverConfigurationProfiles;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialectManager;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.ServiceUtility;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

/**
 * An {@link XADataSource} entry point for the AWS Advanced JDBC Wrapper. It wraps a target
 * driver-specific {@code XADataSource} (for example {@code org.postgresql.xa.PGXADataSource} or
 * {@code com.mysql.cj.jdbc.MysqlXADataSource}) so the wrapper's plugin pipeline applies to the
 * application-facing {@link java.sql.Connection}, while the XA control surface is delegated to the
 * target's {@code XAResource}.
 *
 * <p>Connection-switching plugins (read/write splitting, failover) are not supported on the XA
 * path; see the XA documentation for the recommended two-datasource pattern. The wrapper's internal
 * connection pool is also not applicable to the XA path (the transaction manager / application
 * server provides XA connection pooling).
 */
public class AwsWrapperXADataSource implements XADataSource, Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = Logger.getLogger(AwsWrapperXADataSource.class.getName());

  private static final String PROTOCOL_PREFIX = "jdbc:aws-wrapper:";

  private static final Set<String> RW_SPLITTING_PLUGIN_CODES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          "readWriteSplitting",
          "autoReadWriteSplitting",
          "srw",
          "gdbReadWriteSplitting",
          "autoGdbReadWriteSplitting",
          "autoSrwReadWriteSplitting")));

  private final transient ConnectionUrlParser urlParser = new ConnectionUrlParser();
  private final transient StorageService storageService;
  private final transient MonitorService monitorService;
  private final transient EventPublisher eventPublisher;

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

  public AwsWrapperXADataSource() {
    this(CoreServicesContainer.getInstance());
  }

  public AwsWrapperXADataSource(final CoreServicesContainer coreServicesContainer) {
    this.storageService = coreServicesContainer.getStorageService();
    this.monitorService = coreServicesContainer.getMonitorService();
    this.eventPublisher = coreServicesContainer.getEventPublisher();
  }

  public void setTargetDataSourceClassName(final @Nullable String className) {
    this.targetDataSourceClassName = className;
  }

  public @Nullable String getTargetDataSourceClassName() {
    return this.targetDataSourceClassName;
  }

  public void setServerName(final @NonNull String serverName) {
    this.serverName = serverName;
  }

  public @Nullable String getServerName() {
    return this.serverName;
  }

  public void setServerPort(final @NonNull String serverPort) {
    this.serverPort = serverPort;
  }

  public @Nullable String getServerPort() {
    return this.serverPort;
  }

  public void setDatabase(final @NonNull String database) {
    this.database = database;
  }

  public @Nullable String getDatabase() {
    return this.database;
  }

  public void setJdbcUrl(final @Nullable String url) {
    this.jdbcUrl = url;
  }

  public @Nullable String getJdbcUrl() {
    return this.jdbcUrl;
  }

  public void setJdbcProtocol(final @NonNull String jdbcProtocol) {
    this.jdbcProtocol = jdbcProtocol;
  }

  public @Nullable String getJdbcProtocol() {
    return this.jdbcProtocol;
  }

  public void setTargetDataSourceProperties(final @Nullable Properties props) {
    this.targetDataSourceProperties = props;
  }

  public @Nullable Properties getTargetDataSourceProperties() {
    return this.targetDataSourceProperties;
  }

  public void setUser(final @Nullable String user) {
    this.user = user;
  }

  public @Nullable String getUser() {
    return this.user;
  }

  public void setPassword(final @Nullable String password) {
    this.password = password;
  }

  public @Nullable String getPassword() {
    return this.password;
  }

  @Override
  public XAConnection getXAConnection() throws SQLException {
    return getXAConnection(this.user, this.password);
  }

  @Override
  public XAConnection getXAConnection(final @Nullable String username, final @Nullable String password)
      throws SQLException {
    this.user = username;
    this.password = password;

    // Reject the wrapper's internal connection pool on the XA path: those providers pool plain
    // Connections and cannot expose an XAResource. XA connection pooling is provided by the
    // transaction manager / application server.
    final Properties rawProps = PropertyUtils.copyProperties(
        this.targetDataSourceProperties != null ? this.targetDataSourceProperties : new Properties());
    if (PropertyDefinition.CONNECTION_POOL_TYPE.getString(rawProps) != null) {
      throw new SQLException(
          Messages.get(
              "AwsWrapperXADataSource.internalConnectionPoolNotSupportedWithXa",
              new Object[] {PropertyDefinition.CONNECTION_POOL_TYPE.getString(rawProps)}),
          SqlState.UNKNOWN_STATE.getState());
    }

    // A globally-set custom connection provider (e.g. an internal pooled provider) would be used by
    // the connect pipeline instead of the XA provider, silently opening a non-XA connection. That is
    // not compatible with the XA path, so reject it.
    if (Driver.getCustomConnectionProvider() != null) {
      throw new SQLException(
          Messages.get("AwsWrapperXADataSource.customConnectionProviderNotSupportedWithXa"),
          SqlState.UNKNOWN_STATE.getState());
    }

    final XADataSource targetXaDataSource = createTargetXADataSource();
    final ResolvedConfig config = resolveConfig();
    warnIfConnectionSwitchingPluginsConfigured(config.props);
    configureTargetXaDataSource(targetXaDataSource, config);

    // The provider re-applies a wrapper-parameter-free URL to the target at connect time, when the
    // configured plugin classes are loaded and all wrapper property names are registered (so the
    // URL cleaning is complete, unlike the best-effort cleaning done above at getXAConnection time).
    final XADataSourceConnectionProvider provider =
        new XADataSourceConnectionProvider(targetXaDataSource, config.finalUrl);

    // Each getConnection() on the returned XAConnection builds a fresh logical connection with a
    // fresh plugin pipeline over the same physical XA connection (Option B).
    final XAConnectionWrapper.LogicalConnectionBuilder handleBuilder =
        () -> buildLogicalConnection(provider, config);

    return new XAConnectionWrapper(provider, handleBuilder);
  }

  /**
   * Builds one logical connection: a {@link ConnectionWrapper} with a fresh plugin pipeline whose
   * physical connection is obtained (and reused) from the shared {@link XADataSourceConnectionProvider}.
   */
  ConnectionWrapper buildLogicalConnection(
      final XADataSourceConnectionProvider provider, final ResolvedConfig config) throws SQLException {

    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(config.props);
    final String targetProtocol = this.urlParser.getProtocol(config.finalUrl);

    final FullServicesContainer servicesContainer = ServiceUtility.getInstance().createStandardServiceContainer(
        this.storageService,
        this.monitorService,
        this.eventPublisher,
        provider,
        null,
        telemetryFactory,
        config.finalUrl,
        targetProtocol,
        config.targetDriverDialect,
        config.props,
        config.configurationProfile);

    return new ConnectionWrapper(
        servicesContainer,
        config.props,
        config.finalUrl,
        targetProtocol,
        config.configurationProfile);
  }

  /**
   * Read/write splitting (and other connection-switching plugins that route work to a different
   * physical session) have no valid use on the XA datasource: the {@code XAResource} is pinned to
   * one session, so routing a statement elsewhere would run it outside the transaction. Warn if such
   * a plugin is configured, and point users to the two-datasource pattern.
   */
  void warnIfConnectionSwitchingPluginsConfigured(final Properties props) {
    final String plugins = PropertyDefinition.PLUGINS.getString(props);
    if (StringUtils.isNullOrEmpty(plugins)) {
      return;
    }
    for (final String rawCode : plugins.split(",")) {
      final String code = rawCode.trim();
      if (RW_SPLITTING_PLUGIN_CODES.contains(code)) {
        LOGGER.warning(() -> Messages.get(
            "AwsWrapperXADataSource.readWriteSplittingNotSupportedWithXa", new Object[] {code}));
      }
    }
  }

  XADataSource createTargetXADataSource() throws SQLException {
    if (StringUtils.isNullOrEmpty(this.targetDataSourceClassName)) {
      throw new SQLException(
          Messages.get("AwsWrapperXADataSource.missingTarget"), SqlState.UNKNOWN_STATE.getState());
    }
    final String className = this.targetDataSourceClassName;
    final Class<?> loaded;
    try {
      loaded = Class.forName(className);
    } catch (final ClassNotFoundException e) {
      throw new SQLException(
          Messages.get("AwsWrapperXADataSource.missingTarget"), SqlState.UNKNOWN_STATE.getState(), e);
    }
    if (!XADataSource.class.isAssignableFrom(loaded)) {
      throw new SQLException(
          Messages.get("AwsWrapperXADataSource.targetNotXaDataSource", new Object[] {className}),
          SqlState.UNKNOWN_STATE.getState());
    }
    try {
      return WrapperUtils.createInstance(className, XADataSource.class);
    } catch (final InstantiationException instEx) {
      throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getState(), instEx);
    }
  }

  /**
   * Configures the target {@link XADataSource} via bean setters from
   * {@code targetDataSourceProperties}, plus the resolved connection URL and user/password.
   *
   * <p>AWS Wrapper-specific properties are stripped before anything reaches the target: from the
   * property map via {@link PropertyDefinition#removeAll(Properties)}, and from the URL query string
   * via {@link DataSourceConfigHelper#removeWrapperPropertiesFromUrl(String)}. This prevents wrapper
   * parameters (e.g. {@code wrapperPlugins}) from leaking into the target driver's URL, which some
   * drivers reject as unknown parameters.
   */
  void configureTargetXaDataSource(final XADataSource xaDataSource, final ResolvedConfig config) {
    final Properties targetProps = new Properties();

    if (this.targetDataSourceProperties != null) {
      targetProps.putAll(this.targetDataSourceProperties);
    }

    // A computed URL lets server/port/database flow to the target XADataSource that supports setUrl.
    // Strip wrapper-specific query parameters so they are not passed to the target driver.
    final String targetUrl = DataSourceConfigHelper.removeWrapperPropertiesFromUrl(config.finalUrl);
    if (!StringUtils.isNullOrEmpty(targetUrl)) {
      targetProps.put("url", targetUrl);
    }
    if (!StringUtils.isNullOrEmpty(this.user)) {
      targetProps.put(PropertyDefinition.USER.name, this.user);
    }
    if (!StringUtils.isNullOrEmpty(this.password)) {
      targetProps.put(PropertyDefinition.PASSWORD.name, this.password);
    }

    // "user"/"password" are AWS Wrapper property names but are also standard target-datasource bean
    // setters, so capture their effective values and re-apply them after removeAll strips
    // wrapper-specific keys.
    final String effectiveUser = targetProps.getProperty(PropertyDefinition.USER.name);
    final String effectivePassword = targetProps.getProperty(PropertyDefinition.PASSWORD.name);

    // Remove AWS Wrapper-specific properties so only target-relevant properties are applied.
    PropertyDefinition.removeAll(targetProps);

    if (!StringUtils.isNullOrEmpty(effectiveUser)) {
      targetProps.put(PropertyDefinition.USER.name, effectiveUser);
    }
    if (!StringUtils.isNullOrEmpty(effectivePassword)) {
      targetProps.put(PropertyDefinition.PASSWORD.name, effectivePassword);
    }

    PropertyUtils.applyProperties(xaDataSource, targetProps);
  }

  /**
   * Resolves the final target URL, effective properties, target driver dialect, and configuration
   * profile from the configured server/port/database or JDBC URL. (Kept local to the XA datasource
   * for now; a shared resolver with {@link AwsWrapperDataSource} is a later optimization.)
   */
  ResolvedConfig resolveConfig() throws SQLException {
    final Properties props = PropertyUtils.copyProperties(
        this.targetDataSourceProperties != null ? this.targetDataSourceProperties : new Properties());

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
            Messages.get("AwsWrapperDataSource.configurationProfileNotFound", new Object[] {profileName}));
      }
    }

    final String finalUrl = DataSourceConfigHelper.resolveFinalUrl(
        props,
        PROTOCOL_PREFIX,
        this.jdbcUrl,
        this.serverName,
        this.serverPort,
        this.database,
        this.jdbcProtocol,
        this.user,
        this.password);

    TargetDriverDialect targetDriverDialect = configurationProfile == null
        ? null : configurationProfile.getTargetDriverDialect();
    if (targetDriverDialect == null) {
      final TargetDriverDialectManager manager = new TargetDriverDialectManager();
      targetDriverDialect = manager.getDialect(this.targetDataSourceClassName, props);
    }

    return new ResolvedConfig(finalUrl, props, targetDriverDialect, configurationProfile);
  }

  /** Resolved configuration used to open the target XA connection and build logical handles. */
  static final class ResolvedConfig {
    final String finalUrl;
    final Properties props;
    final TargetDriverDialect targetDriverDialect;
    final @Nullable ConfigurationProfile configurationProfile;

    ResolvedConfig(
        final String finalUrl,
        final Properties props,
        final TargetDriverDialect targetDriverDialect,
        final @Nullable ConfigurationProfile configurationProfile) {
      this.finalUrl = finalUrl;
      this.props = props;
      this.targetDriverDialect = targetDriverDialect;
      this.configurationProfile = configurationProfile;
    }
  }

  @Override
  // The JDK stub declares a @NonNull return, but the log writer is unset (null) by default.
  @SuppressWarnings("override.return")
  public @Nullable PrintWriter getLogWriter() throws SQLException {
    return this.logWriter;
  }

  @Override
  public void setLogWriter(final PrintWriter out) throws SQLException {
    this.logWriter = out;
  }

  @Override
  public void setLoginTimeout(final int seconds) throws SQLException {
    if (seconds < 0) {
      throw new SQLException(Messages.get("AwsWrapperXADataSource.negativeLoginTimeout"));
    }
    this.loginTimeout = seconds;
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return this.loginTimeout;
  }

  @Override
  // Returning null is existing behavior for this data source family.
  @SuppressWarnings("return")
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
