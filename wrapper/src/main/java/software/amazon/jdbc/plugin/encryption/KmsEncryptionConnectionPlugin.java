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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import software.amazon.jdbc.parser.ParameterColumnMapper.UnboundColumn;
import software.amazon.jdbc.parser.ParameterMappingProblem;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.sqlparser.SqlParserConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * ConnectionPlugin that provides transparent column-level encryption/decryption
 * by intercepting PreparedStatement.setXxx and ResultSet.getXxx calls directly,
 * without creating wrapper objects.
 *
 * <p>Only bind parameters can be encrypted. A statement that writes a column configured for
 * encryption with a client constant (a literal, or an expression that reads no column and holds no
 * subquery) is refused, whether it is prepared or passed directly to {@code execute},
 * {@code executeQuery}, {@code executeUpdate} or {@code addBatch}: that value is certain to be stored
 * as plaintext. Values read from a column or a subquery, and DEFAULT, may be ciphertext the database
 * already holds, so they are left to the database's own HMAC check, as is any statement the plugin
 * cannot read that has no parameters to encrypt.
 */
public class KmsEncryptionConnectionPlugin implements ConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(KmsEncryptionConnectionPlugin.class.getName());

  /**
   * Stream and LOB setters. Their values cannot be encrypted, so they are rejected when the
   * parameter maps to an encrypted column instead of silently writing plaintext.
   */
  private static final Set<String> UNSUPPORTED_ENCRYPTION_SETTERS =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.PREPAREDSTATEMENT_SETCHARACTERSTREAM.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETNCHARACTERSTREAM.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETASCIISTREAM.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETBINARYSTREAM.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETUNICODESTREAM.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETCLOB.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETNCLOB.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_SETBLOB.methodName);
        }
      });

  /**
   * Methods that take the SQL to run as their first argument. That SQL is checked before it is sent,
   * since any value it writes is written as-is. They are the methods sqlParser parses that SQL for.
   */
  private static final Set<String> SQL_EXECUTION_METHODS = SqlParserConnectionPlugin.SQL_EXECUTION_METHODS;

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
          add(JdbcMethod.PREPAREDSTATEMENT_SETNSTRING.methodName);
          addAll(UNSUPPORTED_ENCRYPTION_SETTERS);
          addAll(SQL_EXECUTION_METHODS);
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
      final @Nullable Object... args)
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

      // Handle SQL passed directly to execute* / addBatch(String) — refuse unencryptable writes
      if (SQL_EXECUTION_METHODS.contains(methodName) && args.length > 0 && args[0] instanceof String) {
        checkExecutedSql(methodInvokeOn);
        return jdbcCallable.call();
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
      JdbcCallable<T, E> jdbcCallable, final @Nullable Object... args) throws E {

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
        final StatementContext ctx = new StatementContext(pluginService.getCallContext());
        checkWrites(ctx.problems, ctx.unboundColumns, ctx, ps);
        statementContexts.put(ps, ctx);
      } catch (SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    return result;
  }

  /**
   * Checks SQL passed directly to an execute or addBatch method, using the analysis the SQL parser
   * put into the call context for this call.
   *
   * <p>Such SQL has no bound values, so the only thing to check is the columns it writes with a
   * client constant. Almost no statement has one, so that list is read first and the call returns
   * before any metadata is looked up.
   */
  private void checkExecutedSql(final Object statement) throws SQLException {
    if (statement instanceof Statement) {
      try {
        // Also registers the driver's encrypted_data type, which reads on this connection need.
        ensureInitialized(((Statement) statement).getConnection());
      } catch (SQLException e) {
        LOGGER.fine(() -> Messages.get("KmsEncryptionUtility.initWithConnectionFailed",
            new Object[]{e.getMessage()}));
      }
    }

    final List<UnboundColumn> unboundColumns = unboundWriteColumns(pluginService.getCallContext());
    if (unboundColumns.isEmpty()) {
      return;
    }
    checkWrites(Collections.emptyList(), unboundColumns, null, null);
  }

  /**
   * The columns the parsed statement writes with a client constant, from the call context.
   *
   * @throws SQLException when the context holds no parse results, which means the sqlParser plugin
   *     is not loaded
   */
  @SuppressWarnings("unchecked")
  private static List<UnboundColumn> unboundWriteColumns(final @Nullable PluginCallContext ctx)
      throws SQLException {
    if (ctx == null || !ctx.hasAttribute(SqlContextKeys.UNBOUND_WRITE_COLUMNS)) {
      throw new SQLException(Messages.get("KmsEncryptionConnectionPlugin.missingParseContext"));
    }
    final List<UnboundColumn> columns = ctx.getAttribute(SqlContextKeys.UNBOUND_WRITE_COLUMNS, List.class);
    return columns == null ? Collections.emptyList() : columns;
  }

  /**
   * Fails closed on writes the plugin cannot encrypt. Messages name tables, columns, parameter
   * indexes and counts only, never SQL text.
   *
   * <p>A column configured for encryption written with a client constant is refused whatever
   * {@code encryptionFailOnUnmappedParameter} says: that value is certain to be stored as plaintext.
   * So is a prepared statement with an executable comment that changes placeholder numbering, on a
   * table with encrypted columns: its parameters are numbered differently by different drivers and
   * servers, so any mapping of them could bind a value to the wrong column.
   * Parameters the SQL parser could not map to columns, and statements it could not read, are
   * refused when they concern a table with encrypted columns and
   * {@code encryptionFailOnUnmappedParameter} is on; otherwise one warning is logged. That check
   * applies to prepared statements only: SQL passed directly to execute or addBatch has no bound
   * values, and a {@code ?} in it is not a parameter (for example PostgreSQL's JSONB {@code ?}
   * operator). When the encryption metadata cannot be read, which columns are encrypted is unknown,
   * so that setting decides between refusing and warning for every check.
   *
   * @param problems the parameters the SQL parser could not map; empty for directly executed SQL
   * @param unboundColumns the columns written with a client constant
   * @param ctx the parse results of the statement being prepared, whose parameter mapping and
   *     tables the executable-comment check reads; null for directly executed SQL
   * @param ps the statement being prepared, closed when it is refused; null when the SQL is being
   *     executed directly
   */
  private void checkWrites(
      final List<ParameterMappingProblem> problems,
      final List<UnboundColumn> unboundColumns,
      final @Nullable StatementContext ctx,
      final @Nullable PreparedStatement ps) throws SQLException {
    if (problems.isEmpty() && unboundColumns.isEmpty()) {
      return;
    }
    final MetadataManager metadataManager = encryptionUtility.getMetadataManager();
    if (metadataManager == null) {
      return;
    }

    final boolean strict = encryptionUtility.isFailOnUnmappedParameter();
    final Set<String> unencryptableTables = new LinkedHashSet<>();
    final Set<String> unencryptableDetails = new LinkedHashSet<>();
    final Set<String> affectedTables = new LinkedHashSet<>();
    final Set<String> details = new LinkedHashSet<>();
    final Set<String> executableCommentTables = new LinkedHashSet<>();
    try {
      collectUnencryptableValues(
          metadataManager, unboundColumns, unencryptableTables, unencryptableDetails);
      if (ctx != null) {
        collectExecutableCommentTables(metadataManager, ctx, problems, executableCommentTables);
      }
      collectRelevantProblems(metadataManager, problems, affectedTables, details);
    } catch (MetadataException e) {
      if (strict) {
        closeQuietly(ps, e);
        throw e;
      }
      LOGGER.warning(() -> Messages.get(
          "KmsEncryptionConnectionPlugin.unmappedParametersCheckFailed",
          new Object[] {e.getMessage()}));
      return;
    }

    if (!unencryptableDetails.isEmpty()) {
      final SQLException exception = new SQLException(Messages.get(
          "KmsEncryptionConnectionPlugin.unencryptableValues",
          new Object[] {String.join(", ", unencryptableTables), String.join("; ", unencryptableDetails)}));
      closeQuietly(ps, exception);
      throw exception;
    }

    if (!executableCommentTables.isEmpty()) {
      final SQLException exception = new SQLException(Messages.get(
          "KmsEncryptionConnectionPlugin.executableComment",
          new Object[] {String.join(", ", executableCommentTables)}));
      closeQuietly(ps, exception);
      throw exception;
    }

    if (details.isEmpty()) {
      return;
    }

    final String tables = String.join(", ", affectedTables);
    final String detail = String.join("; ", details);
    if (strict) {
      final SQLException exception = new SQLException(Messages.get(
          "KmsEncryptionConnectionPlugin.unmappedParameters", new Object[] {tables, detail}));
      closeQuietly(ps, exception);
      throw exception;
    }
    LOGGER.warning(() -> Messages.get(
        "KmsEncryptionConnectionPlugin.unmappedParametersWarning", new Object[] {tables, detail}));
  }

  private static void closeQuietly(final @Nullable PreparedStatement ps, final SQLException primary) {
    if (ps == null) {
      return;
    }
    try {
      ps.close();
    } catch (SQLException closeException) {
      primary.addSuppressed(closeException);
    }
  }

  /**
   * Adds every unbound column that is configured for encryption to {@code details}, and its table
   * to {@code tables}.
   */
  private static void collectUnencryptableValues(
      final MetadataManager metadataManager,
      final List<UnboundColumn> unboundColumns,
      final Set<String> tables,
      final Set<String> details) throws MetadataException {
    for (UnboundColumn column : unboundColumns) {
      final @Nullable String table = column.getTable();
      if (table != null && metadataManager.isColumnEncrypted(table, column.getColumn())) {
        tables.add(table);
        details.add(Messages.get("KmsEncryptionConnectionPlugin.unboundValue",
            new Object[] {column.getColumn(),
                Messages.get("KmsEncryptionConnectionPlugin.valueKind." + column.getKind().name())}));
      }
    }
  }

  /**
   * Adds to {@code tables} every table with encrypted columns that an executable-comment problem
   * concerns: the tables the statement writes (or reads, for a statement that does not write), and
   * the tables its annotations name or, for an annotation without a table, encrypt that column in,
   * since the annotations' parameter indexes are just as uncertain.
   */
  private static void collectExecutableCommentTables(
      final MetadataManager metadataManager,
      final StatementContext ctx,
      final List<ParameterMappingProblem> problems,
      final Set<String> tables) throws MetadataException {
    boolean found = false;
    for (ParameterMappingProblem problem : problems) {
      if (problem.getType() != ParameterMappingProblem.Type.EXECUTABLE_COMMENT) {
        continue;
      }
      found = true;
      final @Nullable String table = problem.getTable();
      if (table != null) {
        if (metadataManager.hasEncryptedColumns(table)) {
          tables.add(table);
        }
      } else if (!problem.getCandidateTables().isEmpty()) {
        tables.addAll(matchEncryptedTables(
            problem.getCandidateTables(), metadataManager.getEncryptedTableNames()));
      }
    }
    if (!found) {
      return;
    }
    // Only annotations are left in the mapping of such a statement.
    for (String identifier : ctx.parameterColumnMapping.values()) {
      final int dot = identifier.lastIndexOf('.');
      if (dot > 0) {
        if (metadataManager.hasEncryptedColumns(identifier.substring(0, dot))) {
          tables.add(identifier.substring(0, dot));
        }
        continue;
      }
      for (String table : ctx.tables) {
        if (metadataManager.isColumnEncrypted(table, identifier)) {
          tables.add(table);
        }
      }
    }
  }

  /**
   * Adds the rendered text of every problem that concerns a table with encrypted columns to
   * {@code details}, and the concerned tables to {@code affectedTables}.
   */
  private static void collectRelevantProblems(
      final MetadataManager metadataManager,
      final List<ParameterMappingProblem> problems,
      final Set<String> affectedTables,
      final Set<String> details) throws MetadataException {
    @Nullable Set<String> encryptedTableNames = null;
    for (ParameterMappingProblem problem : problems) {
      final @Nullable String table = problem.getTable();
      switch (problem.getType()) {
        case PARAMS_IN_EXPRESSION:
          if (table != null && anyColumnEncrypted(metadataManager, table, problem.getColumns())) {
            affectedTables.add(table);
            details.add(Messages.get("KmsEncryptionConnectionPlugin.paramsInExpression",
                new Object[] {String.join(", ", problem.getColumns()),
                    joinIndexes(problem.getParameterIndexes())}));
          }
          break;
        case UNKNOWN_TARGET_PARAMS:
          if (table != null && (problem.getColumns().isEmpty()
              ? metadataManager.hasEncryptedColumns(table)
              : anyColumnEncrypted(metadataManager, table, problem.getColumns()))) {
            affectedTables.add(table);
            details.add(Messages.get("KmsEncryptionConnectionPlugin.unknownTargetParams",
                new Object[] {joinIndexes(problem.getParameterIndexes())}));
          }
          break;
        case NO_COLUMN_LIST:
          if (table != null && metadataManager.hasEncryptedColumns(table)) {
            affectedTables.add(table);
            details.add(Messages.get("KmsEncryptionConnectionPlugin.noColumnList"));
          }
          break;
        case FIXED_INDEX_PARAMETER:
          if (table != null && metadataManager.hasEncryptedColumns(table)) {
            affectedTables.add(table);
            details.add(Messages.get("KmsEncryptionConnectionPlugin.fixedIndexParameter"));
          }
          break;
        case PLACEHOLDER_COUNT_MISMATCH:
          if (table != null && metadataManager.hasEncryptedColumns(table)) {
            affectedTables.add(table);
            details.add(Messages.get("KmsEncryptionConnectionPlugin.placeholderCountMismatch",
                new Object[] {String.valueOf(problem.getExpectedCount()),
                    String.valueOf(problem.getActualCount())}));
          }
          break;
        case UNSUPPORTED_STATEMENT:
          if (table != null && metadataManager.hasEncryptedColumns(table)) {
            affectedTables.add(table);
            details.add(Messages.get("KmsEncryptionConnectionPlugin.unsupportedStatement",
                new Object[] {String.valueOf(problem.getStatementKind())}));
          }
          break;
        case UNPARSED_STATEMENT:
          if (encryptedTableNames == null) {
            encryptedTableNames = metadataManager.getEncryptedTableNames();
          }
          final Set<String> matched = matchEncryptedTables(
              problem.getCandidateTables(), encryptedTableNames);
          if (!matched.isEmpty()) {
            affectedTables.addAll(matched);
            details.add(Messages.get("KmsEncryptionConnectionPlugin.unparsedStatement"));
          }
          break;
        default:
          break;
      }
    }
  }

  private static boolean anyColumnEncrypted(
      final MetadataManager metadataManager, final String table, final List<String> columns)
      throws MetadataException {
    for (String column : columns) {
      if (metadataManager.isColumnEncrypted(table, column)) {
        return true;
      }
    }
    return false;
  }

  private static Set<String> matchEncryptedTables(
      final List<String> candidates, final Set<String> encryptedTableNames) {
    final Set<String> matched = new LinkedHashSet<>();
    for (String encryptedTable : encryptedTableNames) {
      for (String candidate : candidates) {
        if (candidate.equalsIgnoreCase(encryptedTable)) {
          matched.add(encryptedTable);
          break;
        }
      }
    }
    return matched;
  }

  private static String joinIndexes(final List<Integer> indexes) {
    final StringBuilder sb = new StringBuilder();
    for (Integer index : indexes) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(index);
    }
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  private <T, E extends Exception> T handleSetParameter(
      Class<T> methodClass,
      Class<E> exceptionClass,
      PreparedStatement ps,
      String methodName,
      JdbcCallable<T, E> jdbcCallable,
      final @Nullable Object... args) throws E, SQLException {

    StatementContext ctx = statementContexts.get(ps);
    if (ctx == null || args.length < 2) {
      return jdbcCallable.call();
    }

    // args[0] is the parameter index of a PreparedStatement.setXxx call, so it is always a
    // non-null Integer; the instanceof check only narrows the type for the nullness checker.
    final @Nullable Object paramIndexArg = args[0];
    final @Nullable Object value = args[1];

    if (!(paramIndexArg instanceof Integer) || value == null) {
      return jdbcCallable.call();
    }

    final int paramIndex = (Integer) paramIndexArg;

    final @Nullable String columnIdentifier = ctx.getColumnNameForParameter(paramIndex);
    if (columnIdentifier == null) {
      return jdbcCallable.call();
    }

    MetadataManager metadataManager = encryptionUtility.getMetadataManager();
    if (metadataManager == null) {
      return jdbcCallable.call();
    }

    // A mapping value is "table.column". A bare "column" is an /*@encrypt:column*/ annotation whose
    // table is not certain; its value is not encrypted (for a write, the SQL parser reported it as
    // unmapped). MetadataManager composes its lookup key as tableName + "." + columnName, so the
    // value is split here, on the LAST dot so that a schema-qualified "schema.table.column" stays
    // intact.

    final int separatorIndex = columnIdentifier.lastIndexOf('.');
    if (separatorIndex <= 0 || separatorIndex == columnIdentifier.length() - 1) {
      return jdbcCallable.call();
    }
    final String tableName = columnIdentifier.substring(0, separatorIndex);
    final String columnName = columnIdentifier.substring(separatorIndex + 1);
    if (!metadataManager.isColumnEncrypted(tableName, columnName)) {
      return jdbcCallable.call();
    }

    if (UNSUPPORTED_ENCRYPTION_SETTERS.contains(methodName)) {
      // A stream or LOB value cannot be encrypted; passing it through would write plaintext.
      throw new SQLException(Messages.get(
          "KmsEncryptionConnectionPlugin.unsupportedSetterForEncryptedColumn",
          new Object[] {String.valueOf(paramIndex), tableName, columnName, methodName}));
    }

    ColumnEncryptionConfig config = metadataManager.getColumnConfig(tableName, columnName);
    if (config == null) {
      return jdbcCallable.call();
    }

    // Encrypt the value
    KeyManager keyManager = encryptionUtility.getKeyManager();
    EncryptionService encryptionService = encryptionUtility.getEncryptionService();
    byte[] hmacKey = config.getKeyMetadata().getHmacKey();

    byte[] dataKey = keyManager.decryptDataKey(
        config.getKeyMetadata().getEncryptedDataKey(),
        config.getKeyMetadata().getMasterKeyArn());

    final byte @Nullable [] encrypted;
    try {
      encrypted = encryptionService.encrypt(value, dataKey, hmacKey, config.getAlgorithm());
    } finally {
      java.util.Arrays.fill(dataKey, (byte) 0);
    }

    if (encrypted == null) {
      // Unreachable: encrypt() only returns null for a null value, and null values were
      // already delegated to the target driver above.
      return jdbcCallable.call();
    }

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
      final @Nullable Object... args) throws E, SQLException {

    final @Nullable Integer columnIndex = resolveColumnIndex(rs, args);
    if (columnIndex == null) {
      return jdbcCallable.call();
    }

    // Look the column up by the table column it reads rather than by its label, so an aliased
    // encrypted column is still decrypted and a plain column labelled with an encrypted column's
    // name is not. Expressions have no table, though some drivers report their label as the column
    // name, so they are returned as-is.
    final ResultSetMetaData metaData = rs.getMetaData();
    final @Nullable String tableName = metaData.getTableName(columnIndex);
    final @Nullable String columnName =
        pluginService.getTargetDriverDialect().getBaseColumnName(metaData, columnIndex);
    if (StringUtils.isNullOrEmpty(tableName) || StringUtils.isNullOrEmpty(columnName)) {
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
    byte[] hmacKey = config.getKeyMetadata().getHmacKey();
    Class<?> targetType = getTargetType(methodName);

    byte[] dataKey = keyManager.decryptDataKey(
        config.getKeyMetadata().getEncryptedDataKey(),
        config.getKeyMetadata().getMasterKeyArn());

    try {
      return (T) encryptionService.decrypt(
          encryptedBytes, dataKey, hmacKey, config.getAlgorithm(), targetType);
    } finally {
      java.util.Arrays.fill(dataKey, (byte) 0);
    }
  }

  private byte @Nullable [] getEncryptedBytes(ResultSet rs, final @Nullable Object... args)
      throws SQLException {
    final @Nullable Object columnRef = args[0];
    if (columnRef == null) {
      // Unreachable: callers only get here after resolveColumnIndex() accepted args[0] as a
      // column index or label.
      return null;
    }
    return pluginService.getTargetDriverDialect().getEncryptedBytes(rs, columnRef);
  }

  /**
   * Returns the index of the column that a {@code ResultSet.getXxx} call reads, or {@code null} if
   * the call has no column argument or names a label that is not in the result set. In the second
   * case the driver's own call reports the unknown label.
   */
  private @Nullable Integer resolveColumnIndex(ResultSet rs, final @Nullable Object... args) {
    final @Nullable Object columnRef = args.length > 0 ? args[0] : null;
    if (columnRef instanceof Integer) {
      return (Integer) columnRef;
    }
    if (columnRef instanceof String) {
      try {
        return rs.findColumn((String) columnRef);
      } catch (SQLException e) {
        return null;
      }
    }
    return null;
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
  public boolean acceptsStrategy(final @Nullable HostRole role, String strategy) {
    return true;
  }

  @Override
  public HostSpec getHostSpecByStrategy(final @Nullable HostRole role, String strategy)
      throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("KmsEncryptionConnectionPlugin.noHostSelection"));
  }

  @Override
  public HostSpec getHostSpecByStrategy(
      List<HostSpec> hosts, final @Nullable HostRole role, String strategy)
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
    /** The tables the statement names, the ones it writes first. */
    final List<String> tables;
    final Map<Integer, String> parameterColumnMapping;
    final List<ParameterMappingProblem> problems;
    final List<UnboundColumn> unboundColumns;

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
      this.tables = Collections.unmodifiableList(new ArrayList<String>(tables));

      this.parameterColumnMapping = new ConcurrentHashMap<>();
      Map<Integer, String> paramMapping = ctx.getAttribute(SqlContextKeys.PARAM_MAPPING, Map.class);
      if (paramMapping != null) {
        this.parameterColumnMapping.putAll(paramMapping);
      }

      List<ParameterMappingProblem> mappingProblems =
          ctx.getAttribute(SqlContextKeys.PARAM_MAPPING_PROBLEMS, List.class);
      this.problems = mappingProblems == null
          ? Collections.emptyList()
          : Collections.unmodifiableList(new ArrayList<>(mappingProblems));

      List<UnboundColumn> unbound = ctx.getAttribute(SqlContextKeys.UNBOUND_WRITE_COLUMNS, List.class);
      this.unboundColumns = unbound == null
          ? Collections.emptyList()
          : Collections.unmodifiableList(new ArrayList<>(unbound));
    }

    @Nullable String getColumnNameForParameter(int paramIndex) {
      return parameterColumnMapping.get(paramIndex);
    }
  }
}
