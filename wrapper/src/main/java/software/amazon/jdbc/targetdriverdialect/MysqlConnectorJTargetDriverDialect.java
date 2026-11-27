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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SynchronousExecutor;

public class MysqlConnectorJTargetDriverDialect extends MysqlCompatibleTargetDriverDialect {

  private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final String DS_CLASS_NAME = "com.mysql.cj.jdbc.MysqlDataSource";
  private static final String CP_DS_CLASS_NAME = "com.mysql.cj.jdbc.MysqlConnectionPoolDataSource";
  private static final String XA_DS_CLASS_NAME = "com.mysql.cj.jdbc.MysqlXADataSource";
  private static final String PREPARED_STATEMENT_QUERY_HEADER = "PreparedStatement:";
  private static final Pattern PREPARED_STATEMENT_HEADER_PATTERN =
      Pattern.compile("^[\\w.$]+PreparedStatement(?:\\[\\d+\\])?: ");
  private static final Logger LOGGER = Logger.getLogger(MysqlConnectorJTargetDriverDialect.class.getName());

  // Same bound as GenericTargetDriverDialect.ping(), which uses isValid(10).
  protected static final int PING_TIMEOUT_SEC = 10;
  protected static final int PING_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(PING_TIMEOUT_SEC);
  protected static final Executor networkTimeoutExecutor = new SynchronousExecutor();

  private static final Set<String> MYSQL_ALLOWED_ON_CLOSED_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<String>() {
        {
          addAll(ALLOWED_ON_CLOSED_METHODS);
          add(JdbcMethod.CONNECTION_GETCATALOG.methodName);
          add(JdbcMethod.CONNECTION_ISREADONLY.methodName);
          add(JdbcMethod.CONNECTION_GETAUTOCOMMIT.methodName);
          add(JdbcMethod.CONNECTION_GETHOLDABILITY.methodName);
          add(JdbcMethod.CONNECTION_GETCLIENTINFO.methodName);
          add(JdbcMethod.CONNECTION_GETNETWORKTIMEOUT.methodName);
          add(JdbcMethod.CONNECTION_GETTYPEMAP.methodName);
          add(JdbcMethod.CONNECTION_CREATECLOB.methodName);
          add(JdbcMethod.CONNECTION_CREATEBLOB.methodName);
          add(JdbcMethod.CONNECTION_CREATENCLOB.methodName);
          add(JdbcMethod.CONNECTION_SETHOLDABILITY.methodName);
        }
      });

  @Override
  public boolean isDialect(Driver driver) {
    return DRIVER_CLASS_NAME.equals(driver.getClass().getName());
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return DS_CLASS_NAME.equals(dataSourceClass)
        || CP_DS_CLASS_NAME.equals(dataSourceClass)
        || XA_DS_CLASS_NAME.equals(dataSourceClass);
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
  public String prepareTargetDataSource(
      final @NonNull CommonDataSource dataSource,
      final @NonNull String url,
      final @NonNull Properties props) throws SQLException {

    // The logic is isolated to a separated class since it uses
    // direct reference to com.mysql.cj.jdbc.MysqlDataSource
    final MysqlConnectorJDriverHelper helper = new MysqlConnectorJDriverHelper();
    helper.prepareTargetDataSource(dataSource, props);
    return url;
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

  /**
   * Pings the node with a lightweight query. Both a network timeout and a query timeout bound the
   * call: the query timeout alone does not free a thread blocked in a socket read on a half-open
   * connection, and the ping connection is not watched by any failure-detection plugin. A network
   * timeout the caller already set is left alone; a default of zero (no timeout) is replaced for
   * the duration of the ping and restored afterwards.
   */
  @Override
  public boolean ping(@NonNull Connection connection) {
    int originalNetworkTimeout = 0;
    boolean timeoutChanged = false;
    try {
      originalNetworkTimeout = connection.getNetworkTimeout();
      if (originalNetworkTimeout == 0) {
        connection.setNetworkTimeout(networkTimeoutExecutor, PING_TIMEOUT_MS);
        timeoutChanged = true;
      }
      try (final Statement statement = connection.createStatement()) {
        statement.setQueryTimeout(PING_TIMEOUT_SEC);
        try (final ResultSet resultSet = statement.executeQuery("/* ping */ SELECT 1")) {
          return true;
        }
      }
    } catch (SQLException e) {
      return false;
    } finally {
      if (timeoutChanged) {
        restoreNetworkTimeout(connection, originalNetworkTimeout);
      }
    }
  }

  private static void restoreNetworkTimeout(final Connection connection, final int originalNetworkTimeout) {
    try {
      if (!connection.isClosed()) {
        connection.setNetworkTimeout(networkTimeoutExecutor, originalNetworkTimeout);
      }
    } catch (SQLException e) {
      LOGGER.finest(() -> Messages.get(
          "MysqlConnectorJTargetDriverDialect.failedToRestoreNetworkTimeout", new Object[] {e.getMessage()}));
    }
  }

  @Override
  public Set<String> getAllowedOnConnectionMethodNames() {
    return MYSQL_ALLOWED_ON_CLOSED_METHOD_NAMES;
  }

  @Override
  public String getSQLState(Throwable throwable) {
    final MysqlConnectorJDriverHelper helper = new MysqlConnectorJDriverHelper();
    return helper.getSQLState(throwable);
  }

  @Override
  public @Nullable String getSQLQueryString(PreparedStatement ps) {
    // For MySQL, this gives something like "com.mysql.cj.jdbc.ClientPreparedStatement: select * from T where A=1"
    return this.findSQLQueryString(ps, PREPARED_STATEMENT_QUERY_HEADER);
  }

  @Override
  public @Nullable String getSQLStatementText(final PreparedStatement ps) {
    // "com.mysql.cj.jdbc.ClientPreparedStatement: <sql>", or
    // "com.mysql.cj.jdbc.ServerPreparedStatement[<id>]: <sql>" when useServerPrepStmts=true.
    final String rawStatementStr = ps.toString();
    if (rawStatementStr == null) {
      return null;
    }
    final Matcher matcher = PREPARED_STATEMENT_HEADER_PATTERN.matcher(rawStatementStr);
    return matcher.lookingAt() ? rawStatementStr.substring(matcher.end()) : null;
  }

  @Override
  public @Nullable Boolean isInTransaction(final @NonNull Connection connection) {
    final MysqlConnectorJDriverHelper helper = new MysqlConnectorJDriverHelper();
    return helper.isInTransaction(connection);
  }
}
