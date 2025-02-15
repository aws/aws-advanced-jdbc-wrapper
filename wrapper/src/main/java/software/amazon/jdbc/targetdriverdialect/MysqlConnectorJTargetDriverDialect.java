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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

public class MysqlConnectorJTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final String DS_CLASS_NAME = "com.mysql.cj.jdbc.MysqlDataSource";
  private static final String CP_DS_CLASS_NAME = "com.mysql.cj.jdbc.MysqlConnectionPoolDataSource";

  @Override
  public boolean isDialect(Driver driver) {
    return DRIVER_CLASS_NAME.equals(driver.getClass().getName());
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return DS_CLASS_NAME.equals(dataSourceClass)
        || CP_DS_CLASS_NAME.equals(dataSourceClass);
  }

  @Override
  public ConnectInfo prepareConnectInfo(final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    final String databaseName =
        PropertyDefinition.DATABASE.getString(props) != null
            ? PropertyDefinition.DATABASE.getString(props)
            : "";
    String urlBuilder = protocol + hostSpec.getUrl() + databaseName;

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and use them to make a connection
    PropertyDefinition.removeAllExcept(props,
        PropertyDefinition.USER.name,
        PropertyDefinition.PASSWORD.name,
        PropertyDefinition.TCP_KEEP_ALIVE.name,
        PropertyDefinition.SOCKET_TIMEOUT.name,
        PropertyDefinition.CONNECT_TIMEOUT.name);

    return new ConnectInfo(urlBuilder, props);
  }

  @Override
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    // The logic is isolated to a separated class since it uses
    // direct reference to com.mysql.cj.jdbc.MysqlDataSource
    final MysqlConnectorJDriverHelper helper = new MysqlConnectorJDriverHelper();
    helper.prepareDataSource(dataSource, hostSpec, props);
  }

  @Override
  public boolean isDriverRegistered() throws SQLException {
    final MysqlConnectorJDriverHelper helper = new MysqlConnectorJDriverHelper();
    return helper.isDriverRegistered();
  }

  @Override
  public void registerDriver() throws SQLException {
    final MysqlConnectorJDriverHelper helper = new MysqlConnectorJDriverHelper();
    helper.registerDriver();
  }

  @Override
  public boolean ping(@NonNull Connection connection) {
    try {
      try (final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement.executeQuery("/* ping */ SELECT 1")) {
        return true;
      }
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public Set<String> getAllowedOnConnectionMethodNames() {
    return Collections.unmodifiableSet(new HashSet<String>() {
      {
        addAll(ALLOWED_ON_CLOSED_METHODS);
        add(CONN_GET_CATALOG);
        add(CONN_IS_READ_ONLY);
        add(CONN_GET_AUTO_COMMIT);
        add(CONN_GET_HOLDABILITY);
        add(CONN_GET_CLIENT_INFO);
        add(CONN_GET_NETWORK_TIMEOUT);
        add(CONN_GET_TYPE_MAP);
        add(CONN_CREATE_CLOB);
        add(CONN_CREATE_BLOB);
        add(CONN_CREATE_NCLOB);
        add(CONN_SET_HOLDABILITY);
      }
    });
  }

  @Override
  public String getSQLState(Throwable throwable) {
    final MysqlConnectorJDriverHelper helper = new MysqlConnectorJDriverHelper();
    return helper.getSQLState(throwable);
  }
}
