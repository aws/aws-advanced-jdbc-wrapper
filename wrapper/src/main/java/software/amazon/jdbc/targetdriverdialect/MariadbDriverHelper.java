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

package software.amazon.jdbc.targetdriverdialect;

import static software.amazon.jdbc.util.ConnectionUrlBuilder.buildUrl;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

public class MariadbDriverHelper {

  private static final Logger LOGGER =
      Logger.getLogger(MariadbDriverHelper.class.getName());

  private static final String DS_CLASS_NAME = MariaDbDataSource.class.getName();
  private static final String DS_CP_CLASS_NAME = MariaDbPoolDataSource.class.getName();

  // MariaDbDataSource/MariaDbPoolDataSource #setUser/#setPassword are declared with @NonNull
  // parameters in the driver stubs, but they accept null (meaning "no value configured"). The
  // wrapper USER/PASSWORD properties may legitimately be null here, so the arguments are safe.
  @SuppressWarnings("argument")
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    if (dataSource instanceof MariaDbDataSource) {
      final MariaDbDataSource mariaDbDataSource = (MariaDbDataSource) dataSource;

      mariaDbDataSource.setUser(PropertyDefinition.USER.getString(props));
      mariaDbDataSource.setPassword(PropertyDefinition.PASSWORD.getString(props));

      final Integer connectTimeoutSeconds = getConnectTimeoutSeconds(props);
      if (connectTimeoutSeconds != null) {
        mariaDbDataSource.setLoginTimeout(connectTimeoutSeconds);
      }

      // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
      // and include them to connect URL.
      PropertyDefinition.removeAllExcept(props,
          PropertyDefinition.DATABASE.name,
          PropertyDefinition.TCP_KEEP_ALIVE.name,
          PropertyDefinition.CONNECT_TIMEOUT.name,
          PropertyDefinition.SOCKET_TIMEOUT.name);

      String finalUrl = buildUrl(protocol, hostSpec, props);
      LOGGER.finest(() -> "Connecting to " + finalUrl);
      mariaDbDataSource.setUrl(finalUrl);

    } else if (dataSource instanceof MariaDbPoolDataSource) {

      final MariaDbPoolDataSource mariaDbPoolDataSource = (MariaDbPoolDataSource) dataSource;

      mariaDbPoolDataSource.setUser(PropertyDefinition.USER.getString(props));
      mariaDbPoolDataSource.setPassword(PropertyDefinition.PASSWORD.getString(props));

      final Integer connectTimeoutSeconds = getConnectTimeoutSeconds(props);
      if (connectTimeoutSeconds != null) {
        mariaDbPoolDataSource.setLoginTimeout(connectTimeoutSeconds);
      }

      // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
      // and include them to connect URL.
      PropertyDefinition.removeAllExcept(props,
          PropertyDefinition.DATABASE.name,
          PropertyDefinition.TCP_KEEP_ALIVE.name,
          PropertyDefinition.CONNECT_TIMEOUT.name,
          PropertyDefinition.SOCKET_TIMEOUT.name);

      String finalUrl = buildUrl(protocol, hostSpec, props);
      LOGGER.finest(() -> "Connecting to " + finalUrl);
      mariaDbPoolDataSource.setUrl(finalUrl);

    } else {
      throw new SQLException(Messages.get(
          "TargetDriverDialectManager.unexpectedClass",
          new Object[] { DS_CLASS_NAME + ", " + DS_CP_CLASS_NAME, dataSource.getClass().getName() }));
    }
  }

  /**
   * Returns the connect timeout to apply to a MariaDB data source, in whole seconds, or null when
   * neither {@code connectTimeout} nor {@code loginTimeout} is set.
   *
   * <p>Both wrapper properties are in milliseconds. An explicitly set {@code connectTimeout} takes
   * precedence over {@code loginTimeout}. MariaDB Connector/J applies
   * {@code setLoginTimeout(seconds)} as {@code connectTimeout = seconds * 1000}, and that value
   * overrides the {@code connectTimeout} URL parameter. {@code AwsWrapperDataSource} has already
   * called {@code setLoginTimeout} with its JDBC login timeout, so {@code setLoginTimeout} is the
   * only way to make the wrapper value take effect. The milliseconds are converted to whole
   * seconds by truncating, through {@link TimeoutConverter}; a value from 1 to 999 becomes 0, which
   * MariaDB Connector/J treats as no connect timeout, and that case is logged once as a warning.
   *
   * @param props the connection properties.
   * @return the connect timeout in seconds, or null if no timeout property is set.
   */
  private static @Nullable Integer getConnectTimeoutSeconds(final Properties props) {
    Integer millis = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.CONNECT_TIMEOUT);
    String propertyName = PropertyDefinition.CONNECT_TIMEOUT.name;
    if (millis == null) {
      millis = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.LOGIN_TIMEOUT);
      propertyName = PropertyDefinition.LOGIN_TIMEOUT.name;
    }
    if (millis == null) {
      return null;
    }
    return TimeoutConverter.millisToSeconds(TimeoutConverter.MARIADB_CONNECTOR_J, propertyName, millis);
  }

  public boolean isDriverRegistered() throws SQLException {
    return Collections.list(DriverManager.getDrivers())
        .stream()
        .filter(x -> x instanceof org.mariadb.jdbc.Driver)
        .map(x -> true)
        .findAny()
        .orElse(false);
  }

  public void registerDriver() throws SQLException {
    try {
      DriverManager.registerDriver(new org.mariadb.jdbc.Driver());
    } catch (SQLException e) {
      throw new SQLException(Messages.get("MariadbDriverHelper.canNotRegister"), e);
    }
  }
}
