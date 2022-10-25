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
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class Driver implements java.sql.Driver {

  private static final String PROTOCOL_PREFIX = "jdbc:aws-wrapper:";
  private static final Logger PARENT_LOGGER = Logger.getLogger("software.amazon.jdbc");
  private static final Logger LOGGER = Logger.getLogger("software.amazon.jdbc.Driver");
  private static @Nullable Driver registeredDriver;

  static {
    try {
      register();
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static void register() throws SQLException {
    if (isRegistered()) {
      throw new IllegalStateException(
          Messages.get("Driver.alreadyRegistered"));
    }
    Driver driver = new Driver();
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
    return registeredDriver != null;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    String driverUrl = url.replaceFirst(PROTOCOL_PREFIX, "jdbc:");
    java.sql.Driver driver = DriverManager.getDriver(driverUrl);

    if (driver == null) {
      LOGGER.warning(() -> Messages.get("Driver.missingDriver", new Object[] {driverUrl}));
      return null;
    }

    Properties props = PropertyUtils.parseProperties(url, info);

    String logLevelStr = PropertyDefinition.LOGGER_LEVEL.getString(props);
    if (!StringUtils.isNullOrEmpty(logLevelStr)) {
      Level logLevel = Level.parse(logLevelStr);
      Logger rootLogger = Logger.getLogger("");
      for (Handler handler : rootLogger.getHandlers()) {
        if (handler instanceof ConsoleHandler) {
          if (handler.getLevel().intValue() > logLevel.intValue()) {
            // Set higher (more detailed) level as requested
            handler.setLevel(logLevel);
          }
        }
      }
      PARENT_LOGGER.setLevel(logLevel);
    }

    ConnectionProvider connectionProvider = new DriverConnectionProvider(driver);

    return new ConnectionWrapper(props, driverUrl, connectionProvider);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url == null) {
      throw new SQLException(Messages.get("Driver.nullUrl"));
    }
    // get defaults
    Properties defaults;

    if (!url.startsWith(PROTOCOL_PREFIX)) {
      return false;
    }

    return true;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Properties copy = new Properties(info);
    Properties parse = PropertyUtils.parseProperties(url, copy);
    if (parse != null) {
      copy = parse;
    }

    Collection<AwsWrapperProperty> knownProperties = PropertyDefinition.allProperties();
    DriverPropertyInfo[] props = new DriverPropertyInfo[knownProperties.size()];
    int i = 0;
    for (AwsWrapperProperty prop : knownProperties) {
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
