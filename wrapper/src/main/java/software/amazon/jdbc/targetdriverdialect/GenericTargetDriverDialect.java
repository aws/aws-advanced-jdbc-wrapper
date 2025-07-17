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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

public class GenericTargetDriverDialect implements TargetDriverDialect {

  private static final Logger LOGGER =
      Logger.getLogger(GenericTargetDriverDialect.class.getName());
  public static final Set<String> ALLOWED_ON_CLOSED_METHODS = Collections.unmodifiableSet(
      new HashSet<>(
          Arrays.asList(
              JdbcMethod.CONNECTION_ISCLOSED.methodName,
              JdbcMethod.STATEMENT_GETCONNECTION.methodName,
              JdbcMethod.STATEMENT_GETFETCHDIRECTION.methodName,
              JdbcMethod.STATEMENT_GETRESULTSETHOLDABILITY.methodName,
              JdbcMethod.STATEMENT_ISCLOSED.methodName,
              JdbcMethod.STATEMENT_GETMAXROWS.methodName
          )
      )
  );

  private static final Set<String> NETWORK_BOUND_METHODS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          JdbcMethod.CONNECTION_COMMIT.methodName,
          JdbcMethod.CONNECT.methodName,
          JdbcMethod.FORCECONNECT.methodName,
          JdbcMethod.CONNECTION_ISVALID.methodName,
          JdbcMethod.CONNECTION_ROLLBACK.methodName,
          JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName,
          JdbcMethod.CONNECTION_SETREADONLY.methodName,
          JdbcMethod.STATEMENT_CANCEL.methodName,
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.STATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_GETPARAMETERMETADATA.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName,

          // may require fetch another chunk of data
          JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS.methodName,
          JdbcMethod.RESULTSET_NEXT.methodName,
          JdbcMethod.RESULTSET_ABSOLUTE.methodName,
          JdbcMethod.RESULTSET_AFTERLAST.methodName,
          JdbcMethod.RESULTSET_BEFOREFIRST.methodName,
          JdbcMethod.RESULTSET_FIRST.methodName,
          JdbcMethod.RESULTSET_LAST.methodName,
          JdbcMethod.RESULTSET_MOVETOCURRENTROW.methodName,
          JdbcMethod.RESULTSET_MOVETOINSERTROW.methodName,
          JdbcMethod.RESULTSET_PREVIOUS.methodName,
          JdbcMethod.RESULTSET_RELATIVE.methodName,

          // big data methods
          JdbcMethod.RESULTSET_GETASCIISTREAM.methodName,
          JdbcMethod.RESULTSET_GETBINARYSTREAM.methodName,
          JdbcMethod.RESULTSET_GETBLOB.methodName,
          JdbcMethod.RESULTSET_GETCHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_GETCLOB.methodName,
          JdbcMethod.RESULTSET_GETNCHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_GETNCLOB.methodName,
          JdbcMethod.RESULTSET_GETSQLXML.methodName,
          JdbcMethod.RESULTSET_GETUNICODESTREAM.methodName,
          JdbcMethod.RESULTSET_GETBYTES.methodName,

          // data updates
          JdbcMethod.RESULTSET_DELETEROW.methodName,
          JdbcMethod.RESULTSET_INSERTROW.methodName,
          JdbcMethod.RESULTSET_REFRESHROW.methodName,
          JdbcMethod.RESULTSET_UPDATEROW.methodName,

          // TODO: verify if these calls need network
          JdbcMethod.RESULTSET_UPDATEASCIISTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATEBLOB.methodName,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATECLOB.methodName,
          JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATENCLOB.methodName,
          JdbcMethod.RESULTSET_UPDATESQLXML.methodName,
          JdbcMethod.RESULTSET_UPDATEBYTE.methodName
      )));

  public static final Set<String> NETWORK_BOUND_METHODS_FOR_ENTIRE_RESULTSET = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          JdbcMethod.CONNECTION_COMMIT.methodName,
          JdbcMethod.CONNECT.methodName,
          JdbcMethod.FORCECONNECT.methodName,
          JdbcMethod.CONNECTION_ISVALID.methodName,
          JdbcMethod.CONNECTION_ROLLBACK.methodName,
          JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName,
          JdbcMethod.CONNECTION_SETREADONLY.methodName,
          JdbcMethod.STATEMENT_CANCEL.methodName,
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.STATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_GETPARAMETERMETADATA.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName,

          // big data methods
          JdbcMethod.RESULTSET_GETASCIISTREAM.methodName,
          JdbcMethod.RESULTSET_GETBINARYSTREAM.methodName,
          JdbcMethod.RESULTSET_GETBLOB.methodName,
          JdbcMethod.RESULTSET_GETCHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_GETCLOB.methodName,
          JdbcMethod.RESULTSET_GETNCHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_GETNCLOB.methodName,
          JdbcMethod.RESULTSET_GETSQLXML.methodName,
          JdbcMethod.RESULTSET_GETUNICODESTREAM.methodName,
          JdbcMethod.RESULTSET_GETBYTES.methodName,

          // data updates
          JdbcMethod.RESULTSET_DELETEROW.methodName,
          JdbcMethod.RESULTSET_INSERTROW.methodName,
          JdbcMethod.RESULTSET_REFRESHROW.methodName,
          JdbcMethod.RESULTSET_UPDATEROW.methodName,

          JdbcMethod.RESULTSET_UPDATEASCIISTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATEBLOB.methodName,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATECLOB.methodName,
          JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM.methodName,
          JdbcMethod.RESULTSET_UPDATENCLOB.methodName,
          JdbcMethod.RESULTSET_UPDATESQLXML.methodName,
          JdbcMethod.RESULTSET_UPDATEBYTE.methodName
      )));

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
    return ALLOWED_ON_CLOSED_METHODS;
  }

  @Override
  public String getSQLState(Throwable throwable) {
    return null;
  }

  @Override
  public Set<String> getNetworkBoundMethodNames(final @Nullable Properties properties) {
    return properties != null && PropertyDefinition.ASSUME_FETCH_ENTIRE_RESULT_SET.getBoolean(properties)
        ? NETWORK_BOUND_METHODS_FOR_ENTIRE_RESULTSET
        : NETWORK_BOUND_METHODS;
  }

  @Override
  public void setConnectTimeoutMs(Properties props, long milliseconds) {
    PropertyDefinition.CONNECT_TIMEOUT.set(props, String.valueOf(milliseconds));
  }

  @Override
  public void setSocketTimeoutMs(Properties props, long milliseconds) {
    PropertyDefinition.SOCKET_TIMEOUT.set(props, String.valueOf(milliseconds));
  }
}
