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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

public class GenericTargetDriverDialect implements TargetDriverDialect {

  private static final Logger LOGGER =
      Logger.getLogger(GenericTargetDriverDialect.class.getName());
  public static final List<String> ALLOWED_ON_CLOSED_METHODS = Arrays.asList(
      "Connection.isClosed",
      "Statement.getConnection",
      "Statement.getFetchDirection",
      "Statement.getResultSetHoldability",
      "Statement.isClosed",
      "Statement.getLargeMaxRows"
  );
  public static final String CONN_GET_AUTO_COMMIT = "Connection.getAutoCommit";
  public static final String CONN_GET_CATALOG = "Connection.getCatalog";
  public static final String CONN_GET_SCHEMA = "Connection.getSchema";
  public static final String CONN_GET_NETWORK_TIMEOUT = "Connection.getNetworkTimeout";
  public static final String CONN_GET_METADATA = "Connection.getMetaData";
  public static final String CONN_IS_READ_ONLY = "Connection.isReadOnly";
  public static final String CONN_GET_HOLDABILITY = "Connection.getHoldability";
  public static final String CONN_GET_CLIENT_INFO = "Connection.getClientInfo";
  public static final String CONN_GET_TYPE_MAP = "Connection.getTypeMap";
  public static final String CONN_CREATE_CLOB = "Connection.createClob";
  public static final String CONN_CREATE_BLOB = "Connection.createBlob";
  public static final String CONN_CREATE_NCLOB = "Connection.createNClob";
  public static final String CONN_CLEAR_WARNINGS = "Connection.clearWarnings";
  public static final String CONN_SET_HOLDABILITY = "Connection.setHoldability";
  public static final String CONN_SET_SCHEMA = "Connection.setSchema";
  public static final String STATEMENT_CLEAR_WARNINGS = "Statement.clearWarnings";
  public static final String STATEMENT_GET_FETCH_SIZE = "Statement.getFetchSize";
  public static final String STATEMENT_GET_MAX_FIELD_SIZE = "Statement.getMaxFieldSize";
  public static final String STATEMENT_GET_RESULT_SET_TYPE = "Statement.getResultSetType";
  public static final String STATEMENT_IS_CLOSE_ON_COMPLETION = "Statement.isCloseOnCompletion";
  public static final String STATEMENT_CLEAR_BATCH = "Statement.clearBatch";
  public static final String STATEMENT_CLOSE_ON_COMPLETION = "Statement.closeOnCompletion";
  public static final String STATEMENT_GET_GENERATED_KEYS = "Statement.getGeneratedKeys";
  public static final String STATEMENT_GET_MAX_ROWS = "Statement.getMaxRows";
  public static final String STATEMENT_GET_MORE_RESULTS = "Statement.getMoreResults";
  public static final String STATEMENT_GET_QUERY_TIMEOUT = "Statement.getQueryTimeout";
  public static final String STATEMENT_GET_RESULT_SET = "Statement.getResultSet";
  public static final String STATEMENT_GET_RESULT_SET_CONCURRENCY = "Statement.getResultSetConcurrency";
  public static final String STATEMENT_GET_UPDATE_COUNT = "Statement.getUpdateCount";
  public static final String STATEMENT_GET_WARNINGS = "Statement.getWarnings";
  public static final String STATEMENT_ADD_BATCH = "Statement.addBatch";
  public static final String CALL_GET_ARRAY = "CallableStatement.getArray";
  public static final String CALL_GET_BIG_DECIMAL = "CallableStatement.getBigDecimal";
  public static final String CALL_GET_BLOB = "CallableStatement.getBlob";
  public static final String CALL_GET_BOOLEAN = "CallableStatement.getBoolean";
  public static final String CALL_GET_BYTE = "CallableStatement.getByte";
  public static final String CALL_GET_BYTES = "CallableStatement.getBytes";
  public static final String CALL_GET_CHARACTER_STREAM = "CallableStatement.getCharacterStream";
  public static final String CALL_GET_CLOB = "CallableStatement.getClob";
  public static final String CALL_GET_DATE = "CallableStatement.getDate";
  public static final String CALL_GET_DOUBLE = "CallableStatement.getDouble";
  public static final String CALL_GET_FLOAT = "CallableStatement.getFloat";
  public static final String CALL_GET_INT = "CallableStatement.getInt";
  public static final String CALL_GET_LONG = "CallableStatement.getLong";
  public static final String CALL_GET_N_CLOB = "CallableStatement.getNClob";
  public static final String CALL_GET_N_CHAR = "CallableStatement.getNCharacterStream";
  public static final String CALL_GET_N_STRING = "CallableStatement.getNString";
  public static final String CALL_GET_OBJECT = "CallableStatement.getObject";
  public static final String CALL_GET_SHORT = "CallableStatement.getShort";
  public static final String CALL_GET_SQLXML = "CallableStatement.getSQLXML";
  public static final String CALL_GET_TIME = "CallableStatement.getTime";
  public static final String CALL_GET_STRING = "CallableStatement.getString";
  public static final String CALL_GET_TIMESTAMP = "CallableStatement.getTimestamp";
  public static final String CALL_GET_URL = "CallableStatement.getURL";
  public static final String CALL_WAS_NULL = "CallableStatement.wasNull";
  public static final String PREP_ADD_BATCH = "PreparedStatement.addBatch";
  public static final String PREP_CLEAR_PARAMS = "PreparedStatement.clearParameters";

  @Override
  public boolean isDialect(Driver driver) {
    return true;
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return true;
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
    PropertyDefinition.removeAllExceptCredentials(props);

    return new ConnectInfo(urlBuilder, props);
  }

  @Override
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    String finalUrl = buildUrl(
        protocol,
        hostSpec.getHost(),
        hostSpec.getPort(),
        PropertyDefinition.DATABASE.getString(props));

    LOGGER.finest(() -> "Connecting to " + finalUrl);
    props.setProperty("url", finalUrl);

    PropertyDefinition.removeAllExceptCredentials(props);
    LOGGER.finest(() -> PropertyUtils.logProperties(PropertyUtils.maskProperties(props),
        "Connecting with properties: \n"));

    if (!props.isEmpty()) {
      PropertyUtils.applyProperties(dataSource, props);
    }
  }

  public boolean isDriverRegistered() throws SQLException {
    throw new SQLException(Messages.get("TargetDriverDialect.unsupported"));
  }

  public void registerDriver() throws SQLException {
    throw new SQLException(Messages.get("TargetDriverDialect.unsupported"));
  }

  @Override
  public boolean ping(@NonNull Connection connection) {
    try {
      return connection.isValid(10); // 10s
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public Set<String> getAllowedOnConnectionMethodNames() {
    return Collections.unmodifiableSet(new HashSet<String>() {
      {
        addAll(ALLOWED_ON_CLOSED_METHODS);
      }
    });
  }

  @Override
  public String getSQLState(Throwable throwable) {
    return null;
  }
}
