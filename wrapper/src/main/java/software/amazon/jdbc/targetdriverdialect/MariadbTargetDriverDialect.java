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

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

public class MariadbTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String PERMIT_MYSQL_SCHEME = "permitMysqlScheme";
  private static final String DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver";
  private static final String DS_CLASS_NAME = "org.mariadb.jdbc.MariaDbDataSource";
  private static final String CP_DS_CLASS_NAME = "org.mariadb.jdbc.MariaDbPoolDataSource";

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
    final boolean permitMysqlSchemeFlag = props.containsKey(PERMIT_MYSQL_SCHEME);

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and use them to make a connection
    props.remove(PERMIT_MYSQL_SCHEME);
    PropertyDefinition.removeAllExcept(props,
        PropertyDefinition.USER.name,
        PropertyDefinition.PASSWORD.name,
        PropertyDefinition.TCP_KEEP_ALIVE.name,
        PropertyDefinition.CONNECT_TIMEOUT.name,
        PropertyDefinition.SOCKET_TIMEOUT.name);

    // "permitMysqlScheme" should be in Url rather than in properties.
    String urlBuilder = protocol + hostSpec.getUrl() + databaseName
        + (permitMysqlSchemeFlag ? "?" + PERMIT_MYSQL_SCHEME : "");

    return new ConnectInfo(urlBuilder, props);
  }

  @Override
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    // The logic is isolated to a separated class since it uses
    // direct reference to org.mariadb.jdbc.MariaDbDataSource
    final MariadbDriverHelper helper = new MariadbDriverHelper();
    helper.prepareDataSource(dataSource, protocol, hostSpec, props);
  }

  @Override
  public boolean isDriverRegistered() throws SQLException {
    final MariadbDriverHelper helper = new MariadbDriverHelper();
    return helper.isDriverRegistered();
  }

  @Override
  public void registerDriver() throws SQLException {
    final MariadbDriverHelper helper = new MariadbDriverHelper();
    helper.registerDriver();
  }

  @Override
  public List<String> getAllowedOnConnectionMethodNames() {
    return Arrays.asList(
        CONN_GET_CATALOG,
        CONN_GET_METADATA,
        CONN_IS_READ_ONLY,
        CONN_GET_SCHEMA,
        CONN_GET_AUTO_COMMIT,
        CONN_GET_HOLDABILITY,
        CONN_GET_CLIENT_INFO,
        CONN_GET_NETWORK_TIMEOUT,
        CONN_GET_TYPE_MAP,
        CONN_CREATE_CLOB,
        CONN_CREATE_BLOB,
        CONN_CREATE_NCLOB,
        CONN_IS_CLOSED,
        CONN_CLEAR_WARNINGS,
        CONN_SET_HOLDABILITY,
        CONN_SET_SCHEMA,
        STATEMENT_CLEAR_WARNINGS,
        STATEMENT_GET_CONNECTION,
        STATEMENT_GET_FETCH_DIRECTION,
        STATEMENT_GET_FETCH_SIZE,
        STATEMENT_GET_MAX_FIELD_SIZE,
        STATEMENT_GET_RESULT_SET_HOLDABILITY,
        STATEMENT_GET_RESULT_SET_TYPE,
        STATEMENT_IS_CLOSED,
        STATEMENT_IS_CLOSE_ON_COMPLETION,
        STATEMENT_CLEAR_BATCH,
        STATEMENT_CLOSE_ON_COMPLETION,
        STATEMENT_GET_LARGE_MAX_ROWS,
        STATEMENT_GET_GENERATED_KEYS,
        STATEMENT_GET_MAX_ROWS,
        STATEMENT_GET_MORE_RESULTS,
        STATEMENT_GET_QUERY_TIMEOUT,
        STATEMENT_GET_RESULT_SET,
        STATEMENT_GET_RESULT_SET_CONCURRENCY,
        STATEMENT_GET_UPDATE_COUNT,
        STATEMENT_ADD_BATCH,
        CALL_GET_ARRAY,
        CALL_GET_BIG_DECIMAL,
        CALL_GET_BLOB,
        CALL_GET_BOOLEAN,
        CALL_GET_BYTE,
        CALL_GET_BYTES,
        CALL_GET_CHARACTER_STREAM,
        CALL_GET_CLOB,
        CALL_GET_DATE,
        CALL_GET_DOUBLE,
        CALL_GET_FLOAT,
        CALL_GET_INT,
        CALL_GET_LONG,
        CALL_GET_N_CHAR,
        CALL_GET_N_CLOB,
        CALL_GET_N_STRING,
        CALL_GET_OBJECT,
        CALL_GET_SHORT,
        CALL_GET_TIME,
        CALL_GET_STRING,
        CALL_GET_TIMESTAMP,
        CALL_GET_URL,
        CALL_WAS_NULL,
        PREP_ADD_BATCH,
        PREP_CLEAR_PARAMS
    );
  }
}
