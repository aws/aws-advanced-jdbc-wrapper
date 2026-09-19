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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.wrapper.PgEncryptedDataHelper;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect.AuthorizationStateImpact;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.SqlMethodAnalyzer;
import software.amazon.jdbc.util.StringUtils;

public class PgTargetDriverDialect extends GenericTargetDriverDialect {

  private static final Logger LOGGER = Logger.getLogger(PgTargetDriverDialect.class.getName());

  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static final String SIMPLE_DS_CLASS_NAME = "org.postgresql.ds.PGSimpleDataSource";
  private static final String POOLING_DS_CLASS_NAME = "org.postgresql.ds.PGPoolingDataSource";
  private static final String CP_DS_CLASS_NAME = "org.postgresql.ds.PGConnectionPoolDataSource";
  private static final String XA_DS_CLASS_NAME = "org.postgresql.xa.PGXADataSource";
  private static final String AUTHORIZATION_SESSION_STATE_QUERY =
      "SELECT session_user, current_user, "
          + "pg_catalog.current_setting('search_path'), "
          + "pg_catalog.array_to_json(pg_catalog.current_schemas(true))::pg_catalog.text";

  private static final Pattern AUTHORIZATION_STATE_STATEMENT_PATTERN = Pattern.compile(
      "(?:^|;)\\s*(?:"
          + "SET\\s+(?:(?:SESSION|LOCAL)\\s+)?(?:"
          + "ROLE\\b|\"ROLE\""
          + "|SESSION\\s+AUTHORIZATION\\b"
          + "|SESSION_AUTHORIZATION\\b|\"SESSION_AUTHORIZATION\""
          + "|SEARCH_PATH\\b|\"SEARCH_PATH\""
          + "|SCHEMA\\b)"
          + "|RESET\\s+(?:"
          + "ROLE\\b|\"ROLE\""
          + "|SESSION\\s+AUTHORIZATION\\b"
          + "|SESSION_AUTHORIZATION\\b|\"SESSION_AUTHORIZATION\""
          + "|SEARCH_PATH\\b|\"SEARCH_PATH\""
          + "|ALL\\b)"
          + "|DISCARD\\s+(?:ALL|TEMP)\\b"
          + "|(?:CALL|DO)\\b"
          + "|(?:COMMIT|ROLLBACK|END|ABORT)\\b"
          + ")",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern SET_CONFIG_PATTERN =
      Pattern.compile(
          "(?:\\bSET_CONFIG\\b|\"SET_CONFIG\")\\s*\\(",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern UNTRACKED_AUTHORIZATION_STATE_STATEMENT_PATTERN = Pattern.compile(
      "(?:^|;)\\s*(?:"
          + "(?:CALL|DO)\\b"
          + "|CREATE\\s+(?:(?:GLOBAL|LOCAL)\\s+)?TEMP(?:ORARY)?\\b"
          + "|SELECT\\b.*?\\bINTO\\s+TEMP(?:ORARY)?(?:\\s+TABLE)?\\b"
          + "|(?:SET|RESET)\\s+(?:(?:SESSION|LOCAL)\\s+)?"
          + "(?:\"(?:[^\"]|\"\")*\\.(?:[^\"]|\"\")*\""
          + "|[A-Z_][A-Z0-9_$]*\\s*\\.\\s*[A-Z_][A-Z0-9_$]*)"
          + ")",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Set<String> dataSourceClassMap = new HashSet<>(Arrays.asList(
      SIMPLE_DS_CLASS_NAME,
      POOLING_DS_CLASS_NAME,
      CP_DS_CLASS_NAME,
      XA_DS_CLASS_NAME));

  /**
   * Properties that make the PostgreSQL driver reject a node whose role does not match what the
   * application asked for.
   *
   * <p>Whether {@code targetServerType} rejects a node depends on its value, not just on the node
   * being read-only. After authenticating, the driver establishes whether the server accepts writes
   * and compares that against the requested type: {@code primary} (and its {@code master} alias)
   * refuses a read-only server, {@code secondary} refuses a writable one, {@code preferPrimary} and
   * {@code preferSecondary} express a preference without refusing, and the default {@code any}
   * refuses nothing.
   *
   * <p>The wrapper's monitors each target one specific node deliberately, so any value that refuses
   * a node can break them in one direction or the other: under {@code primary} a monitor aimed at a
   * reader or at a not-yet-promoted Blue/Green replica is refused, and under {@code secondary} a
   * monitor aimed at the writer is. The property is therefore removed from monitoring connection
   * properties whatever its value, rather than inspected.
   */
  private static final Set<String> HOST_SELECTION_PROPERTY_NAMES =
      Collections.unmodifiableSet(new HashSet<>(Collections.singletonList("targetServerType")));

  private static final Set<String> PG_ALLOWED_ON_CLOSED_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<String>() {
        {
          addAll(ALLOWED_ON_CLOSED_METHODS);
          add(JdbcMethod.STATEMENT_CLEARWARNINGS.methodName);
          add(JdbcMethod.STATEMENT_GETFETCHSIZE.methodName);
          add(JdbcMethod.STATEMENT_GETMAXFIELDSIZE.methodName);
          add(JdbcMethod.STATEMENT_GETRESULTSETTYPE.methodName);
          add(JdbcMethod.STATEMENT_ISCLOSEONCOMPLETION.methodName);
          add(JdbcMethod.STATEMENT_CLEARBATCH.methodName);
          add(JdbcMethod.STATEMENT_CLOSEONCOMPLETION.methodName);
          add(JdbcMethod.STATEMENT_GETGENERATEDKEYS.methodName);
          add(JdbcMethod.STATEMENT_GETMAXROWS.methodName);
          add(JdbcMethod.STATEMENT_GETMORERESULTS.methodName);
          add(JdbcMethod.STATEMENT_GETQUERYTIMEOUT.methodName);
          add(JdbcMethod.STATEMENT_GETRESULTSET.methodName);
          add(JdbcMethod.STATEMENT_GETRESULTSETCONCURRENCY.methodName);
          add(JdbcMethod.STATEMENT_GETUPDATECOUNT.methodName);
          add(JdbcMethod.STATEMENT_GETWARNINGS.methodName);
          add(JdbcMethod.STATEMENT_ADDBATCH.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETARRAY.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBOOLEAN.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBYTE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBYTES.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETDATE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETDOUBLE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETFLOAT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETINT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETLONG.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETOBJECT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETSHORT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETSQLXML.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETTIME.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETSTRING.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_WASNULL.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_ADDBATCH.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_CLEARPARAMETERS.methodName);
        }
      });

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
      final @NonNull Properties props) {

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
  public String prepareTargetDataSource(
      final @NonNull CommonDataSource dataSource,
      final @NonNull String url,
      final @NonNull Properties props) {

    // The logic is isolated to a separated class since it uses
    // direct reference to org.postgresql.ds.common.BaseDataSource
    final PgDriverHelper helper = new PgDriverHelper();
    helper.prepareTargetDataSource(dataSource, props);
    return url;
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
  public Set<String> getAllowedOnConnectionMethodNames() {
    return PG_ALLOWED_ON_CLOSED_METHOD_NAMES;
  }

  @Override
  public void abortConnection(@NonNull Connection connectionToAbort, @NonNull Executor abortExecutor)
      throws SQLException {
    try {
      connectionToAbort.abort(abortExecutor);
    } catch (final SecurityException secEx) {
      // JDK 24 fully removed the Java Security Manager (deprecated since JDK 17, removed in JDK 24 per JEP 486).
      // abort() is not supported on JDK 24+ (Security Manager removed); fall back to close()
      LOGGER.warning(
          () -> Messages.get(
              "PgTargetDriverDialect.exceptionAbortingConnection",
              new Object[] {secEx.getMessage()}));

      connectionToAbort.close();
    }
  }

  @Override
  public @Nullable String getSQLQueryString(PreparedStatement ps) {
    // For PG, this gives the raw query string itself. i.e. "select * from T where A = 1".
    return this.findSQLQueryString(ps, null);
  }

  // Everything below this point is duplicated verbatim from the base
  // src/main/java PgTargetDriverDialect. A multi-release JAR replaces the class wholesale on JDK 24,
  // so anything omitted here silently degrades to the GenericTargetDriverDialect behaviour instead
  // of the PostgreSQL-specific behaviour. Keep the two variants in sync; the only intended
  // differences are abortConnection (no Security Manager on JDK 24) and prepareConnectInfo.
  // PgTargetDriverDialectVariantTest guards against renewed drift.

  @Override
  public boolean supportsAuthorizationSessionState() {
    return true;
  }

  @Override
  public Optional<AuthorizationSessionState> readAuthorizationSessionState(
      final @NonNull Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(AUTHORIZATION_SESSION_STATE_QUERY)) {
      if (!resultSet.next()) {
        return Optional.empty();
      }

      final String sessionUser = resultSet.getString(1);
      final String currentUser = resultSet.getString(2);
      final String searchPath = resultSet.getString(3);
      final String resolvedSearchPath = resultSet.getString(4);
      if (sessionUser == null
          || currentUser == null
          || searchPath == null
          || resolvedSearchPath == null) {
        return Optional.empty();
      }

      return Optional.of(new AuthorizationSessionState(
        sessionUser,
        currentUser,
        searchPath,
        resolvedSearchPath));
    }
  }

  @Override
  public AuthorizationStateImpact getAuthorizationStateImpact(final @Nullable String sql) {
    if (StringUtils.isNullOrEmpty(sql)) {
      return AuthorizationStateImpact.NONE;
    }

    final String sqlWithoutComments =
        SqlMethodAnalyzer.stripCommentsWithNestedBlockComments(sql);
    if (SET_CONFIG_PATTERN.matcher(sqlWithoutComments).find()
        || UNTRACKED_AUTHORIZATION_STATE_STATEMENT_PATTERN.matcher(sqlWithoutComments).find()) {
      return AuthorizationStateImpact.UNTRACKED;
    }

    if (AUTHORIZATION_STATE_STATEMENT_PATTERN.matcher(sqlWithoutComments).find()) {
      return AuthorizationStateImpact.TRACKED;
    }

    return AuthorizationStateImpact.NONE;
  }

  @Override
  @SuppressWarnings("deprecation")
  public void registerDataType(@NonNull Connection connection, @NonNull String typeName, @NonNull String className)
      throws SQLException {
    org.postgresql.PGConnection pgConn = connection.unwrap(org.postgresql.PGConnection.class);
    pgConn.addDataType(typeName, className);
  }

  private final ResourceLock encryptedDataHelperLock = new ResourceLock();
  private volatile PgEncryptedDataHelper pgEncryptedDataHelper;

  private PgEncryptedDataHelper getPgEncryptedDataHelper() {
    if (pgEncryptedDataHelper == null) {
      try (ResourceLock ignored = encryptedDataHelperLock.obtain()) {
        if (pgEncryptedDataHelper == null) {
          pgEncryptedDataHelper = new PgEncryptedDataHelper();
        }
      }
    }
    return pgEncryptedDataHelper;
  }

  @Override
  public void setEncryptedParameter(@NonNull PreparedStatement ps, int paramIndex, byte[] encrypted)
      throws SQLException {
    getPgEncryptedDataHelper().setEncryptedParameter(ps, paramIndex, encrypted);
  }

  @Override
  public byte @Nullable [] getEncryptedBytes(@NonNull ResultSet rs, Object columnRef)
      throws SQLException {
    return getPgEncryptedDataHelper().getEncryptedBytes(rs, columnRef);
  }

  @Override
  public void updateInternalState(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) throws SQLException {

    final String currentSchema = props.getProperty("currentSchema");
    if (!StringUtils.isNullOrEmpty(currentSchema)) {
      LOGGER.finest(() -> Messages.get(
          "PgTargetDriverDialect.transferringPropertyToSessionState",
          new Object[] {"currentSchema", currentSchema}));
      pluginService.getSessionStateService().setupPristineSchema(currentSchema);
      pluginService.getSessionStateService().setSchema(currentSchema);
    }

    final String readOnlyValue = props.getProperty("readOnly");
    if (!StringUtils.isNullOrEmpty(readOnlyValue)) {
      final boolean readOnly = Boolean.parseBoolean(readOnlyValue);
      LOGGER.finest(() -> Messages.get(
          "PgTargetDriverDialect.transferringPropertyToSessionState",
          new Object[] {"readOnly", readOnly}));
      pluginService.getSessionStateService().setupPristineReadOnly(readOnly);
      pluginService.getSessionStateService().setReadOnly(readOnly);
    }
  }

  @Override
  public Set<String> removeHostSelectionProperties(final @NonNull Properties props) {
    final Set<String> removed = new HashSet<>();
    for (final String propertyName : HOST_SELECTION_PROPERTY_NAMES) {
      if (props.remove(propertyName) != null) {
        removed.add(propertyName);
      }
    }
    return removed;
  }
}
