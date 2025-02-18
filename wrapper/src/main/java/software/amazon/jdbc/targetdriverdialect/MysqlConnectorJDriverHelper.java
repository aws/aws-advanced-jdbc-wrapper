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

import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.jdbc.MysqlDataSource;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

public class MysqlConnectorJDriverHelper {

  private static final Logger LOGGER =
      Logger.getLogger(MysqlConnectorJDriverHelper.class.getName());

  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    if (!(dataSource instanceof MysqlDataSource)) {
      throw new SQLException(Messages.get(
          "TargetDriverDialectManager.unexpectedClass",
          new Object[] {
              "com.mysql.cj.jdbc.MysqlDataSource, com.mysql.cj.jdbc.MysqlConnectionPoolDataSource",
              dataSource.getClass().getName()}));
    }

    final MysqlDataSource baseDataSource = (MysqlDataSource) dataSource;

    baseDataSource.setDatabaseName(PropertyDefinition.DATABASE.getString(props));
    baseDataSource.setUser(PropertyDefinition.USER.getString(props));
    baseDataSource.setPassword(PropertyDefinition.PASSWORD.getString(props));
    baseDataSource.setServerName(hostSpec.getHost());

    if (hostSpec.isPortSpecified()) {
      baseDataSource.setPortNumber(hostSpec.getPort());
    }

    Integer loginTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.LOGIN_TIMEOUT);
    if (loginTimeout != null) {
      baseDataSource.setLoginTimeout((int) TimeUnit.MILLISECONDS.toSeconds(loginTimeout));
    }

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and try to apply them to data source
    PropertyDefinition.removeAllExcept(props,
        PropertyDefinition.USER.name,
        PropertyDefinition.PASSWORD.name,
        PropertyDefinition.TCP_KEEP_ALIVE.name,
        PropertyDefinition.SOCKET_TIMEOUT.name,
        PropertyDefinition.CONNECT_TIMEOUT.name);

    PropertyUtils.applyProperties(dataSource, props);
  }

  public boolean isDriverRegistered() throws SQLException {
    return Collections.list(DriverManager.getDrivers())
        .stream()
        .filter(x -> x instanceof com.mysql.cj.jdbc.Driver)
        .map(x -> true)
        .findAny()
        .orElse(false);
  }

  public void registerDriver() throws SQLException {
    try {
      DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
    } catch (SQLException e) {
      throw new SQLException(Messages.get("MysqlConnectorJDriverHelper.canNotRegister"), e);
    }
  }

  public String getSQLState(final Throwable throwable) {
    return throwable instanceof CJException ? ((CJException) throwable).getSQLState() : null;
  }
}
