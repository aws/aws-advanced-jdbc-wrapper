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

package software.amazon.jdbc.plugin.encryption;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * ConnectionPlugin that provides transparent column-level encryption/decryption
 * by intercepting PreparedStatement.setXxx and ResultSet.getXxx calls directly,
 * without creating wrapper objects.
 */
public class KmsEncryptionConnectionPlugin implements ConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(KmsEncryptionConnectionPlugin.class.getName());

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName);
          add(JdbcMethod.CONNECTION_PREPARECALL.methodName);
          add(JdbcMethod.CONNECTION_CLOSE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_CLOSE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETSTRING.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETINT.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETLONG.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETDOUBLE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETFLOAT.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETSHORT.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETBYTE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETBOOLEAN.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETBIGDECIMAL.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETTIME.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETTIMESTAMP.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETOBJECT.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETBYTES.methodName);
          add(JdbcMethod.RESULTSET_GETSTRING.methodName);
          add(JdbcMethod.RESULTSET_GETINT.methodName);
          add(JdbcMethod.RESULTSET_GETLONG.methodName);
          add(JdbcMethod.RESULTSET_GETDOUBLE.methodName);
          add(JdbcMethod.RESULTSET_GETFLOAT.methodName);
          add(JdbcMethod.RESULTSET_GETSHORT.methodName);
          add(JdbcMethod.RESULTSET_GETBYTE.methodName);
          add(JdbcMethod.RESULTSET_GETBOOLEAN.methodName);
          add(JdbcMethod.RESULTSET_GETBIGDECIMAL.methodName);
          add(JdbcMethod.RESULTSET_GETDATE.methodName);
          add(JdbcMethod.RESULTSET_GETTIME.methodName);
          add(JdbcMethod.RESULTSET_GETTIMESTAMP.methodName);
          add(JdbcMethod.RESULTSET_GETOBJECT.methodName);
          add(JdbcMethod.RESULTSET_GETBYTES.methodName);
        }
      });

  private final KmsEncryptionUtility encryptionUtility;
  private final PluginService pluginService;

  // Track SQL and parameter mappings per PreparedStatement
  private final Map<PreparedStatement, StatementContext> statementContexts =
      new ConcurrentHashMap<>();

  public KmsEncryptionConnectionPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
    this.encryptionUtility = new KmsEncryptionUtility(pluginService);

    try {
      this.encryptionUtility.initialize(properties);
      LOGGER.info(() -> Messages.get("KmsEncryptionConnectionPlugin.initialized"));
    } catch (SQLException e) {
      LOGGER.severe(
          () -> Messages.get("KmsEncryptionConnectionPlugin.initFailed",
              new Object[]{e.getMessage()}));
      throw new RuntimeException(
          Messages.get("KmsEncryptionConnectionPlugin.initPluginFailed"), e);
    }
  }

  // Visible for testing
  KmsEncryptionConnectionPlugin(PluginService pluginService, KmsEncryptionUtility encryptionUtility) {
    this.pluginService = pluginService;
    this.encryptionUtility = encryptionUtility;
  }

  public KmsEncryptionUtility getEncryptionUtility() {
    return encryptionUtility;
  }

  @Override
  public <T, E extends Exception> T execute(
      Class<T> methodClass,
      Class<E> exceptionClass,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcCallable,
      Object... args)
      throws E {

    try {
      // Handle connection close — clear tracked contexts
      if ("Connection.close".equals(methodName)) {
        statementContexts.clear();
        encryptionUtility.onConnectionClosed((Connection) methodInvokeOn);
        return jdbcCallable.call();
      }

      // Handle statement close — remove statement context and clear result sets
      if ("PreparedStatement.close".equals(methodName)) {
        statementContexts.remove(methodInvokeOn);
        return jdbcCallable.call();
      }

      // Handle PreparedStatement creation — track SQL for later encryption
      if (methodName.startsWith("Connection.prepareStatement")
          || methodName.startsWith("Connection.prepareCall")) {
        return handlePrepareStatement(methodClass, exceptionClass, jdbcCallable, args);
      }

      // Handle PreparedStatement.setXxx — encrypt if needed
      if (methodName.startsWith("PreparedStatement.set") && methodInvokeOn instanceof PreparedStatement) {
        return handleSetParameter(methodClass, exceptionClass, (PreparedStatement) methodInvokeOn,
            methodName, jdbcCallable, args);
      }

      // Handle ResultSet.getXxx — decrypt if needed
      if (methodName.startsWith("ResultSet.get") && methodInvokeOn instanceof ResultSet) {
        return handleGetValue(methodClass, (ResultSet) methodInvokeOn, methodName, jdbcCallable, args);
      }
    } catch (SQLException e) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
    }

    return jdbcCallable.call();
  }

  @SuppressWarnings("unchecked")
  private <T, E extends Exception> T handlePrepareStatement(
      Class<T> methodClass, Class<E> exceptionClass,
      JdbcCallable<T, E> jdbcCallable, Object... args) throws E {

    T result = jdbcCallable.call();

    if (result instanceof PreparedStatement && args.length > 0 && args[0] instanceof String) {
      PreparedStatement ps = (PreparedStatement) result;

      try {
        ensureInitialized(ps.getConnection());
      } catch (SQLException e) {
        // Log but don't fail — encryption may still work
        LOGGER.fine(() -> Messages.get("KmsEncryptionUtility.initWithConnectionFailed",
            new Object[]{e.getMessage()}));
      }

      try {
        statementContexts.put(ps, new StatementContext(pluginService.getCallContext()));
      } catch (SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  private <T, E extends Exception> T handleSetParameter(
      Class<T> methodClass,
      Class<E> exceptionClass,
      PreparedStatement ps,
      String methodName,
      JdbcCallable<T, E> jdbcCallable,
      Object... args) throws E, SQLException {

    StatementContext ctx = statementContexts.get(ps);
    if (ctx == null || args.length < 2) {
      return jdbcCallable.call();
    }

    int paramIndex = (Integer) args[0];
    Object value = args[1];

    if (value == null) {
      return jdbcCallable.call();
    }

    String columnName = ctx.getColumnNameForParameter(paramIndex);
    if (columnName == null || ctx.tableName == null) {
      return jdbcCallable.call();
    }

    MetadataManager metadataManager = encryptionUtility.getMetadataManager();
    if (metadataManager == null || !metadataManager.isColumnEncrypted(ctx.tableName, columnName)) {
      return jdbcCallable.call();
    }

    ColumnEncryptionConfig config = metadataManager.getColumnConfig(ctx.tableName, columnName);
    if (config == null) {
      return jdbcCallable.call();
    }

    // Encrypt the value
    KeyManager keyManager = encryptionUtility.getKeyManager();
    EncryptionService encryptionService = encryptionUtility.getEncryptionService();

    byte[] dataKey = keyManager.decryptDataKey(
        config.getKeyMetadata().getEncryptedDataKey(),
        config.getKeyMetadata().getMasterKeyArn());

    byte[] hmacKey = config.getKeyMetadata().getHmacKey();
    byte[] encrypted = encryptionService.encrypt(value, dataKey, hmacKey, config.getAlgorithm());
    java.util.Arrays.fill(dataKey, (byte) 0);

    // Set encrypted bytes using database-appropriate method
    pluginService.getTargetDriverDialect().setEncryptedParameter(ps, paramIndex, encrypted);

    return (T) null; // void return for setXxx methods
  }

  @SuppressWarnings("unchecked")
  private <T, E extends Exception> T handleGetValue(
      Class<T> methodClass,
      ResultSet rs,
      String methodName,
      JdbcCallable<T, E> jdbcCallable,
      Object... args) throws E, SQLException {

    // Determine column name from args (index or label)
    String columnName = resolveColumnName(rs, args);
    String tableName = resolveTableName(rs, args);

    if (columnName == null || tableName == null) {
      return jdbcCallable.call();
    }

    MetadataManager metadataManager = encryptionUtility.getMetadataManager();
    if (metadataManager == null || !metadataManager.isColumnEncrypted(tableName, columnName)) {
      return jdbcCallable.call();
    }

    ColumnEncryptionConfig config = metadataManager.getColumnConfig(tableName, columnName);
    if (config == null) {
      return jdbcCallable.call();
    }

    // Get raw encrypted bytes
    byte[] encryptedBytes = getEncryptedBytes(rs, args);
    if (encryptedBytes == null) {
      return jdbcCallable.call();
    }

    // Decrypt
    KeyManager keyManager = encryptionUtility.getKeyManager();
    EncryptionService encryptionService = encryptionUtility.getEncryptionService();

    byte[] dataKey = keyManager.decryptDataKey(
        config.getKeyMetadata().getEncryptedDataKey(),
        config.getKeyMetadata().getMasterKeyArn());

    byte[] hmacKey = config.getKeyMetadata().getHmacKey();
    Class<?> targetType = getTargetType(methodName);
    Object decrypted = encryptionService.decrypt(
        encryptedBytes, dataKey, hmacKey, config.getAlgorithm(), targetType);
    java.util.Arrays.fill(dataKey, (byte) 0);

    return (T) decrypted;
  }

  private byte[] getEncryptedBytes(ResultSet rs, Object... args) throws SQLException {
    return pluginService.getTargetDriverDialect().getEncryptedBytes(rs, args[0]);
  }

  private String resolveColumnName(ResultSet rs, Object... args) throws SQLException {
    if (args.length == 0) {
      return null;
    }
    if (args[0] instanceof String) {
      return (String) args[0];
    }
    if (args[0] instanceof Integer) {
      return rs.getMetaData().getColumnName((Integer) args[0]);
    }
    return null;
  }

  private String resolveTableName(ResultSet rs, Object... args) {
    try {
      int colIndex;
      if (args.length > 0 && args[0] instanceof Integer) {
        colIndex = (Integer) args[0];
      } else if (args.length > 0 && args[0] instanceof String) {
        colIndex = rs.findColumn((String) args[0]);
      } else {
        return null;
      }
      return rs.getMetaData().getTableName(colIndex);
    } catch (SQLException e) {
      return null;
    }
  }

  private Class<?> getTargetType(String methodName) {
    if (methodName.endsWith("String")) {
      return String.class;
    }
    if (methodName.endsWith("Int")) {
      return Integer.class;
    }
    if (methodName.endsWith("Long")) {
      return Long.class;
    }
    if (methodName.endsWith("Double")) {
      return Double.class;
    }
    if (methodName.endsWith("Float")) {
      return Float.class;
    }
    if (methodName.endsWith("Short")) {
      return Short.class;
    }
    if (methodName.endsWith("Byte")) {
      return Byte.class;
    }
    if (methodName.endsWith("Boolean")) {
      return Boolean.class;
    }
    if (methodName.endsWith("BigDecimal")) {
      return java.math.BigDecimal.class;
    }
    if (methodName.endsWith("Date")) {
      return java.sql.Date.class;
    }
    if (methodName.endsWith("Time")) {
      return java.sql.Time.class;
    }
    if (methodName.endsWith("Timestamp")) {
      return java.sql.Timestamp.class;
    }
    if (methodName.endsWith("Bytes")) {
      return byte[].class;
    }
    return Object.class;
  }

  private void ensureInitialized(Connection conn) throws SQLException {
    encryptionUtility.ensureInitializedWithConnection(conn);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(
      String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectFunc.call();
  }

  @Override
  public void initHostProvider(
      String driverProtocol, String initialUrl, Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initFunc) throws SQLException {
    initFunc.call();
  }

  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) {
    return true;
  }

  @Override
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy) throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("KmsEncryptionConnectionPlugin.noHostSelection"));
  }

  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy)
      throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("KmsEncryptionConnectionPlugin.noHostSelection2"));
  }

  @Override
  public Connection forceConnect(
      String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectFunc.call();
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public @Nullable List<Pair<String, Object>> getSnapshotState() {
    return null;
  }

  /** Tracks SQL analysis and parameter mappings for a PreparedStatement. */
  private static class StatementContext {
    final String tableName;
    final Map<Integer, String> parameterColumnMapping;

    // Raw type casts from context — generic parameters (Set<String>, Map<Integer,String>)
    // cannot be checked at runtime due to type erasure. Types are documented in SqlContextKeys
    // and enforced by SqlParserConnectionPlugin which populates the context.
    @SuppressWarnings("unchecked")
    StatementContext(PluginCallContext ctx) throws SQLException {
      if (ctx == null) {
        throw new SQLException(Messages.get("KmsEncryptionConnectionPlugin.missingParseContext"));
      }
      Set<String> tables = ctx.getAttribute(SqlContextKeys.TABLES, Set.class);
      if (tables == null) {
        throw new SQLException(Messages.get("KmsEncryptionConnectionPlugin.missingParseContext"));
      }
      this.tableName = tables.isEmpty() ? null : (String) tables.iterator().next();

      this.parameterColumnMapping = new ConcurrentHashMap<>();
      Map<Integer, String> paramMapping = ctx.getAttribute(SqlContextKeys.PARAM_MAPPING, Map.class);
      if (paramMapping != null) {
        this.parameterColumnMapping.putAll(paramMapping);
      }
    }

    String getColumnNameForParameter(int paramIndex) {
      return parameterColumnMapping.get(paramIndex);
    }
  }
}
