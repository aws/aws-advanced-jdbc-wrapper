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
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionProviderManager.ConnectionInitFunc;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsHostListProvider;
import software.amazon.jdbc.plugin.AwsSecretsManagerCacheHolder;
import software.amazon.jdbc.plugin.DataCacheConnectionPlugin;
import software.amazon.jdbc.plugin.OpenedConnectionTracker;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointMonitorImpl;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointPlugin;
import software.amazon.jdbc.plugin.efm.MonitorThreadContainer;
import software.amazon.jdbc.plugin.federatedauth.FederatedAuthCacheHolder;
import software.amazon.jdbc.plugin.federatedauth.OktaAuthCacheHolder;
import software.amazon.jdbc.plugin.iam.IamAuthCacheHolder;
import software.amazon.jdbc.plugin.limitless.LimitlessRouterServiceImpl;
import software.amazon.jdbc.plugin.strategy.fastestresponse.FastestResponseStrategyPlugin;
import software.amazon.jdbc.plugin.strategy.fastestresponse.HostResponseTimeServiceImpl;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.DriverConfigurationProfiles;
import software.amazon.jdbc.states.ResetSessionStateOnCloseCallable;
import software.amazon.jdbc.states.TransferSessionStateOnSwitchCallable;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialectManager;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class Driver implements java.sql.Driver {

  private static final String PROTOCOL_PREFIX = "jdbc:aws-wrapper:";
  private static final Logger PARENT_LOGGER = Logger.getLogger("software.amazon.jdbc");
  private static final Logger LOGGER = Logger.getLogger("software.amazon.jdbc.Driver");
  private static @Nullable Driver registeredDriver;

  private static final AtomicReference<ResetSessionStateOnCloseCallable> resetSessionStateOnCloseCallable =
      new AtomicReference<>(null);
  private static final AtomicReference<TransferSessionStateOnSwitchCallable> transferSessionStateOnSwitchCallable =
      new AtomicReference<>(null);

  private static final AtomicReference<ExceptionHandler> customExceptionHandler =
      new AtomicReference<>(null);

  private static final AtomicReference<Dialect> customDialect =
      new AtomicReference<>(null);

  private static final AtomicReference<TargetDriverDialect> customTargetDriverDialect =
      new AtomicReference<>(null);

  private static final AtomicReference<ConnectionProvider> customConnectionProvider =
      new AtomicReference<>(null);

  private static final AtomicReference<ConnectionInitFunc> connectionInitFunc =
      new AtomicReference<>(null);

  static {
    try {
      register();
    } catch (final SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static void register() throws SQLException {
    if (isRegistered()) {
      throw new IllegalStateException(
          Messages.get("Driver.alreadyRegistered"));
    }
    final Driver driver = new Driver();
    DriverManager.registerDriver(driver);
    registeredDriver = driver;
  }

  public static void deregister() throws SQLException {
    if (registeredDriver == null) {
      throw new IllegalStateException(
          Messages.get("Driver.notRegistered"));
    }
    DriverManager.deregisterDriver(registeredDriver);
    registeredDriver = null;
  }

  public static boolean isRegistered() {
    if (registeredDriver != null) {
      final List<java.sql.Driver> registeredDrivers = Collections.list(DriverManager.getDrivers());
      for (final java.sql.Driver d : registeredDrivers) {
        if (d == registeredDriver) {
          return true;
        }
      }

      // Driver isn't registered with DriverManager.
      registeredDriver = null;
    }
    return false;
  }

  @Override
  public Connection connect(final String url, final Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    LOGGER.finest("Opening connection to " + url);

    ConnectionUrlParser.parsePropertiesFromUrl(url, info);
    final Properties props = PropertyUtils.copyProperties(info);

    final String databaseName = ConnectionUrlParser.parseDatabaseFromUrl(url);
    if (!StringUtils.isNullOrEmpty(databaseName)) {
      PropertyDefinition.DATABASE.set(props, databaseName);
    }

    LOGGER.finest(() -> PropertyUtils.logProperties(
        PropertyUtils.maskProperties(props), "Connecting with properties: \n"));

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
                "Driver.configurationProfileNotFound",
                new Object[] {profileName}));
      }
    }

    TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);
    TelemetryContext context = telemetryFactory.openTelemetryContext(
        "software.amazon.jdbc.Driver.connect", TelemetryTraceLevel.TOP_LEVEL);

    try {
      final String driverUrl = url.replaceFirst(PROTOCOL_PREFIX, "jdbc:");

      TargetDriverHelper helper = new TargetDriverHelper();
      java.sql.Driver driver = helper.getTargetDriver(driverUrl, props);

      final String logLevelStr = PropertyDefinition.LOGGER_LEVEL.getString(props);
      if (!StringUtils.isNullOrEmpty(logLevelStr)) {
        final Level logLevel = Level.parse(logLevelStr.toUpperCase());
        final Logger rootLogger = Logger.getLogger("");
        for (final Handler handler : rootLogger.getHandlers()) {
          if (handler instanceof ConsoleHandler) {
            if (handler.getLevel().intValue() > logLevel.intValue()) {
              // Set higher (more detailed) level as requested
              handler.setLevel(logLevel);
            }
          }
        }
        PARENT_LOGGER.setLevel(logLevel);
      }

      TargetDriverDialect targetDriverDialect = configurationProfile == null
          ? null
          : configurationProfile.getTargetDriverDialect();

      if (targetDriverDialect == null) {
        final TargetDriverDialectManager targetDriverDialectManager = new TargetDriverDialectManager();
        targetDriverDialect = targetDriverDialectManager.getDialect(driver, props);
      }

      final ConnectionProvider defaultConnectionProvider = new DriverConnectionProvider(driver);

      ConnectionProvider effectiveConnectionProvider = null;
      if (configurationProfile != null) {
        effectiveConnectionProvider = configurationProfile.getConnectionProvider();
      }

      return new ConnectionWrapper(
          props,
          driverUrl,
          defaultConnectionProvider,
          effectiveConnectionProvider,
          targetDriverDialect,
          configurationProfile,
          telemetryFactory);

    } catch (Exception ex) {
      context.setException(ex);
      context.setSuccess(false);
      throw ex;
    } finally {
      context.closeContext();
    }
  }

  @Override
  public boolean acceptsURL(final String url) throws SQLException {
    if (url == null) {
      throw new SQLException(Messages.get("Driver.nullUrl"));
    }

    return url.startsWith(PROTOCOL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
    final Properties copy = new Properties(info);
    final String databaseName = ConnectionUrlParser.parseDatabaseFromUrl(url);
    if (!StringUtils.isNullOrEmpty(databaseName)) {
      PropertyDefinition.DATABASE.set(copy, databaseName);
    }
    ConnectionUrlParser.parsePropertiesFromUrl(url, copy);

    final Collection<AwsWrapperProperty> knownProperties = PropertyDefinition.allProperties();
    final DriverPropertyInfo[] props = new DriverPropertyInfo[knownProperties.size()];
    int i = 0;
    for (final AwsWrapperProperty prop : knownProperties) {
      props[i++] = prop.toDriverPropertyInfo(copy);
    }

    return props;
  }

  @Override
  public int getMajorVersion() {
    return DriverInfo.MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return DriverInfo.MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return PARENT_LOGGER;
  }

  public static void setResetSessionStateOnCloseFunc(final @NonNull ResetSessionStateOnCloseCallable func) {
    resetSessionStateOnCloseCallable.set(func);
  }

  public static void resetResetSessionStateOnCloseFunc() {
    resetSessionStateOnCloseCallable.set(null);
  }

  public static ResetSessionStateOnCloseCallable getResetSessionStateOnCloseFunc() {
    return resetSessionStateOnCloseCallable.get();
  }

  public static void setTransferSessionStateOnSwitchFunc(final @NonNull TransferSessionStateOnSwitchCallable func) {
    transferSessionStateOnSwitchCallable.set(func);
  }

  public static void resetTransferSessionStateOnSwitchFunc() {
    transferSessionStateOnSwitchCallable.set(null);
  }

  public static TransferSessionStateOnSwitchCallable getTransferSessionStateOnSwitchFunc() {
    return transferSessionStateOnSwitchCallable.get();
  }

  public static void setPrepareHostFunc(final Function<String, String> func) {
    RdsUtils.setPrepareHostFunc(func);
  }

  public static void resetPrepareHostFunc() {
    RdsUtils.resetPrepareHostFunc();
  }

  public static void setCustomExceptionHandler(final ExceptionHandler exceptionHandler) {
    customExceptionHandler.set(exceptionHandler);
  }

  public static ExceptionHandler getCustomExceptionHandler() {
    return customExceptionHandler.get();
  }

  public static void resetCustomExceptionHandler() {
    customExceptionHandler.set(null);
  }

  public static void setCustomDialect(final @NonNull Dialect dialect) {
    customDialect.set(dialect);
  }

  public static Dialect getCustomDialect() {
    return customDialect.get();
  }

  public static void resetCustomDialect() {
    customDialect.set(null);
  }

  public static void setCustomTargetDriverDialect(final @NonNull TargetDriverDialect targetDriverDialect) {
    customTargetDriverDialect.set(targetDriverDialect);
  }

  public static TargetDriverDialect getCustomTargetDriverDialect() {
    return customTargetDriverDialect.get();
  }

  public static void resetCustomTargetDriverDialect() {
    customTargetDriverDialect.set(null);
  }

  /**
   * Setter that can optionally be called to request a non-default {@link ConnectionProvider}. The
   * requested ConnectionProvider will be used to establish future connections unless it does not
   * support a requested URL, in which case the default ConnectionProvider will be used. See
   * {@link ConnectionProvider#acceptsUrl} for more info.
   *
   * @param connProvider the {@link ConnectionProvider} to use to establish new connections
   */
  public static void setCustomConnectionProvider(ConnectionProvider connProvider) {
    customConnectionProvider.set(connProvider);
  }

  public static ConnectionProvider getCustomConnectionProvider() {
    return customConnectionProvider.get();
  }

  /**
   * Clears the non-default {@link ConnectionProvider} if it has been set. The default
   * ConnectionProvider will be used if the non-default ConnectionProvider has not been set or has
   * been cleared.
   */
  public static void resetCustomConnectionProvider() {
    customConnectionProvider.set(null);
  }

  public static void setConnectionInitFunc(final @NonNull ConnectionInitFunc func) {
    connectionInitFunc.set(func);
  }

  public static ConnectionInitFunc getConnectionInitFunc() {
    return connectionInitFunc.get();
  }

  public static void resetConnectionInitFunc() {
    connectionInitFunc.set(null);
  }

  public static void clearCaches() {
    RdsUtils.clearCache();
    RdsHostListProvider.clearAll();
    PluginServiceImpl.clearCache();
    DialectManager.resetEndpointCache();
    CustomEndpointMonitorImpl.clearCache();
    OpenedConnectionTracker.clearCache();
    AwsSecretsManagerCacheHolder.clearCache();
    DataCacheConnectionPlugin.clearCache();
    FederatedAuthCacheHolder.clearCache();
    OktaAuthCacheHolder.clearCache();
    IamAuthCacheHolder.clearCache();
    LimitlessRouterServiceImpl.clearCache();
    RoundRobinHostSelector.clearCache();
    FastestResponseStrategyPlugin.clearCache();
  }

  public static void releaseResources() {
    software.amazon.jdbc.plugin.efm2.MonitorServiceImpl.closeAllMonitors();
    MonitorThreadContainer.releaseInstance();
    ConnectionProviderManager.releaseResources();
    CustomEndpointPlugin.closeMonitors();
    HikariPoolsHolder.closeAllPools();
    HostResponseTimeServiceImpl.closeAllMonitors();
    MonitoringRdsHostListProvider.closeAllMonitors();
    clearCaches();
  }
}
