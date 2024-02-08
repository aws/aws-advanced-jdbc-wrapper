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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.PropertyUtils;

public class PgTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static final String SIMPLE_DS_CLASS_NAME = "org.postgresql.ds.PGSimpleDataSource";
  private static final String POOLING_DS_CLASS_NAME = "org.postgresql.ds.PGPoolingDataSource";
  private static final String CP_DS_CLASS_NAME = "org.postgresql.ds.PGConnectionPoolDataSource";

  private static final Set<String> dataSourceClassMap = new HashSet<>(Arrays.asList(
      SIMPLE_DS_CLASS_NAME,
      POOLING_DS_CLASS_NAME,
      CP_DS_CLASS_NAME));

  @Override
  public boolean isDialect(Driver driver) {
    return DRIVER_CLASS_NAME.equals(driver.getClass().getName());
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return dataSourceClassMap.contains(dataSourceClass);
  }

  @Override
  public ConnectInfo prepareConnectInfo(final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    final String databaseName =
        PropertyDefinition.DATABASE.getString(props) != null
            ? PropertyDefinition.DATABASE.getString(props)
            : "";

    final Boolean tcpKeepAlive = PropertyUtils.getBooleanPropertyValue(props, PropertyDefinition.TCP_KEEP_ALIVE);
    final Integer loginTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.LOGIN_TIMEOUT);
    final Integer connectTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.CONNECT_TIMEOUT);
    final Integer socketTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.SOCKET_TIMEOUT);

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and use them to make a connection
    PropertyDefinition.removeAllExceptCredentials(props);

    if (tcpKeepAlive != null) {
      props.setProperty("tcpKeepAlive", String.valueOf(tcpKeepAlive));
    }

    if (loginTimeout != null) {
      props.setProperty("loginTimeout",
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(loginTimeout)));
    }
    if (connectTimeout != null) {
      props.setProperty("connectTimeout",
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(connectTimeout)));
    }
    if (socketTimeout != null) {
      props.setProperty("socketTimeout",
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(socketTimeout)));
    }

    String urlBuilder = protocol + hostSpec.getUrl() + databaseName;

    return new ConnectInfo(urlBuilder, props);
  }

  @Override
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    // The logic is isolated to a separated class since it uses
    // direct reference to org.postgresql.ds.common.BaseDataSource
    final PgDriverHelper helper = new PgDriverHelper();
    helper.prepareDataSource(dataSource, hostSpec, props);
  }

  @Override
  public boolean isDriverRegistered() throws SQLException {
    final PgDriverHelper helper = new PgDriverHelper();
    return helper.isDriverRegistered();
  }

  @Override
  public void registerDriver() throws SQLException {
    final PgDriverHelper helper = new PgDriverHelper();
    helper.registerDriver();
  }

  @Override
  public List<String> getAllowedOnConnectionMethodNames() {
    return Arrays.asList(
        CONN_IS_CLOSED,
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
        STATEMENT_GET_WARNINGS,
        STATEMENT_ADD_BATCH,
        CALL_GET_ARRAY,
        CALL_GET_BIG_DECIMAL,
        CALL_GET_BOOLEAN,
        CALL_GET_BYTE,
        CALL_GET_BYTES,
        CALL_GET_DATE,
        CALL_GET_DOUBLE,
        CALL_GET_FLOAT,
        CALL_GET_INT,
        CALL_GET_LONG,
        CALL_GET_OBJECT,
        CALL_GET_SHORT,
        CALL_GET_SQLXML,
        CALL_GET_TIME,
        CALL_GET_STRING,
        CALL_GET_TIMESTAMP,
        CALL_WAS_NULL,
        PREP_ADD_BATCH,
        PREP_CLEAR_PARAMS
    );
  }
}
