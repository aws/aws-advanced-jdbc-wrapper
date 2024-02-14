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
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
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
  public Set<String> getAllowedOnConnectionMethodNames() {
    return Collections.unmodifiableSet(new HashSet<String>() {
      {
        addAll(ALLOWED_ON_CLOSED_METHODS);
        add(CONN_GET_CATALOG);
        add(CONN_GET_METADATA);
        add(CONN_IS_READ_ONLY);
        add(CONN_GET_SCHEMA);
        add(CONN_GET_AUTO_COMMIT);
        add(CONN_GET_HOLDABILITY);
        add(CONN_GET_CLIENT_INFO);
        add(CONN_GET_NETWORK_TIMEOUT);
        add(CONN_GET_TYPE_MAP);
        add(CONN_CREATE_CLOB);
        add(CONN_CREATE_BLOB);
        add(CONN_CREATE_NCLOB);
        add(CONN_CLEAR_WARNINGS);
        add(CONN_SET_HOLDABILITY);
        add(CONN_SET_SCHEMA);
        add(STATEMENT_CLEAR_WARNINGS);
        add(STATEMENT_GET_FETCH_SIZE);
        add(STATEMENT_GET_MAX_FIELD_SIZE);
        add(STATEMENT_GET_RESULT_SET_TYPE);
        add(STATEMENT_IS_CLOSE_ON_COMPLETION);
        add(STATEMENT_CLEAR_BATCH);
        add(STATEMENT_CLOSE_ON_COMPLETION);
        add(STATEMENT_GET_GENERATED_KEYS);
        add(STATEMENT_GET_MAX_ROWS);
        add(STATEMENT_GET_MORE_RESULTS);
        add(STATEMENT_GET_QUERY_TIMEOUT);
        add(STATEMENT_GET_RESULT_SET);
        add(STATEMENT_GET_RESULT_SET_CONCURRENCY);
        add(STATEMENT_GET_UPDATE_COUNT);
        add(STATEMENT_ADD_BATCH);
        add(CALL_GET_ARRAY);
        add(CALL_GET_BIG_DECIMAL);
        add(CALL_GET_BLOB);
        add(CALL_GET_BOOLEAN);
        add(CALL_GET_BYTE);
        add(CALL_GET_BYTES);
        add(CALL_GET_CHARACTER_STREAM);
        add(CALL_GET_CLOB);
        add(CALL_GET_DATE);
        add(CALL_GET_DOUBLE);
        add(CALL_GET_FLOAT);
        add(CALL_GET_INT);
        add(CALL_GET_LONG);
        add(CALL_GET_N_CHAR);
        add(CALL_GET_N_CLOB);
        add(CALL_GET_N_STRING);
        add(CALL_GET_OBJECT);
        add(CALL_GET_SHORT);
        add(CALL_GET_TIME);
        add(CALL_GET_STRING);
        add(CALL_GET_TIMESTAMP);
        add(CALL_GET_URL);
        add(CALL_WAS_NULL);
        add(PREP_ADD_BATCH);
        add(PREP_CLEAR_PARAMS);
      }
    });
  }
}
