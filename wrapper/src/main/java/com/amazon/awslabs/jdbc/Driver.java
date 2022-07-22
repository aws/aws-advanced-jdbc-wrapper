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

import com.amazon.awslabs.jdbc.util.DriverInfo;
import com.amazon.awslabs.jdbc.util.StringUtils;
import com.amazon.awslabs.jdbc.wrapper.ConnectionWrapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

public class Driver implements java.sql.Driver {

  private static final String PROTOCOL_PREFIX = "aws-proxy-jdbc:";
  private static final Logger PARENT_LOGGER = Logger.getLogger("com.amazon.awslabs.jdbc");
  private static final Logger LOGGER = Logger.getLogger("com.amazon.awslabs.jdbc.Driver");
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
          "Driver is already registered. It can only be registered once.");
    }
    Driver driver = new Driver();
    DriverManager.registerDriver(driver);
    registeredDriver = driver;
  }

  public static void deregister() throws SQLException {
    if (registeredDriver == null) {
      throw new IllegalStateException(
          "Driver is not registered (or it has not been registered using Driver.register() method)");
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

    String logLevelStr = PropertyDefinition.LOGGER_LEVEL.get(info);
    if (logLevelStr != null && !logLevelStr.isEmpty()) {
      LOGGER.setLevel(Level.parse(logLevelStr));
    }

    String driverUrl = url.replaceFirst(PROTOCOL_PREFIX, "jdbc:");
    java.sql.Driver driver = DriverManager.getDriver(driverUrl);

    if (driver == null) {
      LOGGER.log(Level.WARNING, "No suitable driver found for " + driverUrl);
      return null;
    }

    Properties props = parseProperties(url, info);
    return new ConnectionWrapper(props, driverUrl, new DriverConnectionProvider(driver));
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url == null) {
      throw new SQLException("url is null");
    }
    // get defaults
    Properties defaults;

    if (!url.startsWith(PROTOCOL_PREFIX)) {
      return false;
    }

    return true;
  }

  @SuppressWarnings("checkstyle:LocalVariableName")
  private @Nullable Properties parseProperties(String url, @Nullable Properties defaults) {

    String urlServer = url;
    String urlArgs = "";

    int qPos = url.indexOf('?');
    if (qPos != -1) {
      urlServer = url.substring(0, qPos);
      urlArgs = url.substring(qPos + 1);
    }

    Properties propertiesFromUrl = new Properties();

    // TODO: allow user to disable database parsing
    int protocolPos = urlServer.indexOf("//");
    if (protocolPos != -1) {
      protocolPos += 2;
    }
    int dPos = urlServer.indexOf("/", protocolPos);
    if (dPos != -1) {
      String database = urlServer.substring(dPos + 1);
      if (!database.isEmpty()) {
        propertiesFromUrl.setProperty(DATABASE_PROPERTY_NAME, database);
      }
    }

    String[] args = urlArgs.split("&");

    for (String token : args) {
      if (token.isEmpty()) {
        continue;
      }
      int pos = token.indexOf('=');
      if (pos == -1) {
        propertiesFromUrl.setProperty(token, "");
      } else {
        String pName = token.substring(0, pos);
        String pValue = urlDecode(token.substring(pos + 1));
        if (pValue == null) {
          return null;
        }
        propertiesFromUrl.setProperty(pName, pValue);
      }
    }

    Properties result = new Properties();
    result.putAll(propertiesFromUrl);
    if (defaults != null) {
      defaults.forEach(result::putIfAbsent);
    }

    return result;
  }

  private @Nullable String urlDecode(String url) {
    try {
      return StringUtils.decode(url);
    } catch (IllegalArgumentException e) {
      LOGGER.log(
          Level.FINE,
          "Url [{0}] parsing failed with error [{1}]",
          new Object[] {url, e.getMessage()});
    }
    return null;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Properties copy = new Properties(info);
    Properties parse = parseProperties(url, copy);
    if (parse != null) {
      copy = parse;
    }

    Collection<ProxyDriverProperty> knownProperties = PropertyDefinition.allProperties();
    DriverPropertyInfo[] props = new DriverPropertyInfo[knownProperties.size()];
    int i = 0;
    for (ProxyDriverProperty prop : knownProperties) {
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
