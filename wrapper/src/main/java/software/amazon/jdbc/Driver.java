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
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialectManager;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.Messages;
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

    final String databaseName = ConnectionUrlParser.parseDatabaseFromUrl(url);
    if (!StringUtils.isNullOrEmpty(databaseName)) {
      PropertyDefinition.DATABASE.set(info, databaseName);
    }
    ConnectionUrlParser.parsePropertiesFromUrl(url, info);

    TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(info);
    TelemetryContext context = telemetryFactory.openTelemetryContext(
        "software.amazon.jdbc.Driver.connect", TelemetryTraceLevel.TOP_LEVEL);

    try {
      final String driverUrl = url.replaceFirst(PROTOCOL_PREFIX, "jdbc:");

      java.sql.Driver driver;
      try {
        driver = DriverManager.getDriver(driverUrl);
      } catch (SQLException e) {
        final List<String> registeredDrivers = Collections.list(DriverManager.getDrivers())
            .stream()
            .map(x -> x.getClass().getName())
            .collect(Collectors.toList());
        throw new SQLException(
            Messages.get("Driver.missingDriver", new Object[] {driverUrl, registeredDrivers}), e);
      }

      if (driver == null) {
        final List<String> registeredDrivers = Collections.list(DriverManager.getDrivers())
            .stream()
            .map(x -> x.getClass().getName())
            .collect(Collectors.toList());
        LOGGER.severe(() -> Messages.get("Driver.missingDriver", new Object[] {driverUrl, registeredDrivers}));
        return null;
      }

      final String logLevelStr = PropertyDefinition.LOGGER_LEVEL.getString(info);
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

      final TargetDriverDialectManager targetDriverDialectManager = new TargetDriverDialectManager();
      final TargetDriverDialect targetDriverDialect = targetDriverDialectManager.getDialect(driver, info);

      final ConnectionProvider connectionProvider = new DriverConnectionProvider(driver, targetDriverDialect);

      return new ConnectionWrapper(info, driverUrl, connectionProvider, telemetryFactory);

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
}
