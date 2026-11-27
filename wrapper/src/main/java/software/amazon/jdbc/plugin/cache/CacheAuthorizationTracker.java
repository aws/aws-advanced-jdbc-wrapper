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

package software.amazon.jdbc.plugin.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect.AuthorizationStateImpact;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlMethodAnalyzer;
import software.amazon.jdbc.util.SqlMethodAnalyzer.SqlStatementAnalysis;
import software.amazon.jdbc.util.StringUtils;

/**
 * Tracks the database authorization session state of one connection so that a query cache can
 * partition its entries by the identity the database would evaluate a query under.
 *
 * <p>Shared by {@link RemoteQueryCachePlugin} and {@link DataLocalCacheConnectionPlugin}. The
 * tracker fails closed: when the state cannot be read, or the application ran SQL that changes
 * state the key cannot represent, {@link #getAuthorizationStateForCacheKey()} returns
 * {@code null} and the caller bypasses the cache.
 */
final class CacheAuthorizationTracker {

  private static final Logger LOGGER = Logger.getLogger(CacheAuthorizationTracker.class.getName());
  private static final SqlMethodAnalyzer sqlMethodAnalyzer = new SqlMethodAnalyzer();

  /**
   * Methods, in addition to the query methods, that a cache plugin has to see for the tracked
   * state to stay current.
   */
  private static final Set<String> TRACKED_METHODS = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList(
          JdbcMethod.CONNECTION_COMMIT.methodName,
          JdbcMethod.CONNECTION_ROLLBACK.methodName,
          JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName,
          JdbcMethod.CONNECTION_SETCATALOG.methodName,
          JdbcMethod.CONNECTION_SETSCHEMA.methodName,
          JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.STATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName)));

  private final PluginService pluginService;
  private final boolean keepKnownStateAtTransactionEnd;
  private final AtomicBoolean authorizationStateWarningLogged = new AtomicBoolean(false);
  private final AtomicBoolean untrackedAuthorizationStateWarningLogged = new AtomicBoolean(false);
  private final AtomicBoolean refreshFailureWarningLogged = new AtomicBoolean(false);
  private final AtomicLong connectionGeneration = new AtomicLong();

  /**
   * Creates a tracker for one connection.
   *
   * @param pluginService the plugin service of the connection
   * @param keepKnownStateAtTransactionEnd whether a state that is still known when a transaction
   *     ends with auto-commit off, or when auto-commit is turned back on, is kept instead of being
   *     marked unknown and read again. Keeping it avoids a query and keeps caching on, but a change
   *     the tracker does not recognize, such as a role set inside a function, then stays out of the
   *     key until the next recognized change or connection change.
   */
  CacheAuthorizationTracker(
      final PluginService pluginService, final boolean keepKnownStateAtTransactionEnd) {
    this.pluginService = pluginService;
    this.keepKnownStateAtTransactionEnd = keepKnownStateAtTransactionEnd;
  }

  static Set<String> withTrackedMethods(final Set<String> queryMethods) {
    final Set<String> methods = new HashSet<>(queryMethods);
    methods.addAll(TRACKED_METHODS);
    return Collections.unmodifiableSet(methods);
  }

  /**
   * Reads the authorization state of a new underlying connection, and advances the connection
   * generation so that a result fetched on the previous connection is not written under a key
   * read from the new one.
   */
  void notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    if (!changes.contains(NodeChangeOptions.INITIAL_CONNECTION)
        && !changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED)) {
      return;
    }

    this.connectionGeneration.incrementAndGet();

    if (!this.pluginService.getTargetDriverDialect().supportsAuthorizationSessionState()) {
      return;
    }

    final SessionStateService sessionStateService = this.pluginService.getSessionStateService();
    sessionStateService.enableAuthorizationStateTracking();

    try {
      if (!this.pluginService.getCurrentConnection().getAutoCommit() || this.isInTransaction()) {
        // Querying here would open a transaction the application did not start, or read state that
        // is local to the application's transaction. The state is read again outside it.
        sessionStateService.markAuthorizationStateUnknown();
        return;
      }
      sessionStateService.refreshAuthorizationState();
    } catch (final SQLException e) {
      sessionStateService.markAuthorizationStateUnknown();
      logAuthorizationStateUnavailable(e);
      return;
    }

    if (!sessionStateService.getAuthorizationState().isPresent()) {
      logAuthorizationStateUnavailable(null);
    }
  }

  long getConnectionGeneration() {
    return this.connectionGeneration.get();
  }

  /**
   * Returns whether a query may be looked up in, or stored into, a cache keyed on the tracked
   * authorization state. A query that changes or reads session state the key does not represent
   * is not cacheable, and neither is any query when the target driver cannot report the state.
   */
  boolean isCacheableQuery(final String methodName, final @Nullable String query) {
    if (methodName.startsWith("CallableStatement.") || !isSingleStatement(query)) {
      return false;
    }
    final TargetDriverDialect targetDriverDialect = this.pluginService.getTargetDriverDialect();
    return targetDriverDialect.supportsAuthorizationSessionState()
        && targetDriverDialect.getAuthorizationStateImpact(query) == AuthorizationStateImpact.NONE;
  }

  /**
   * Returns the current authorization state, or {@code null} when the cache must be bypassed
   * because the state is unknown or the connection has run SQL whose effect is untracked.
   */
  @Nullable AuthorizationSessionState getAuthorizationStateForCacheKey() {
    if (!this.pluginService.getTargetDriverDialect().supportsAuthorizationSessionState()) {
      return null;
    }
    final SessionStateService sessionStateService = this.pluginService.getSessionStateService();
    if (sessionStateService.hasUntrackedAuthorizationState()) {
      logUntrackedAuthorizationState();
      return null;
    }
    final Optional<AuthorizationSessionState> state = sessionStateService.getAuthorizationState();
    if (!state.isPresent()) {
      logAuthorizationStateUnavailable(null);
      return null;
    }
    return state.get();
  }

  /**
   * Runs a JDBC call and updates the tracked authorization state from what the call could have
   * changed.
   */
  <T, E extends Exception> T execute(
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final @Nullable Object[] jdbcMethodArgs)
      throws E {

    final TargetDriverDialect targetDriverDialect = this.pluginService.getTargetDriverDialect();
    final SessionStateService sessionStateService = this.pluginService.getSessionStateService();
    if (!targetDriverDialect.supportsAuthorizationSessionState()
        || !sessionStateService.isAuthorizationStateTrackingEnabled()
        || sessionStateService.hasUntrackedAuthorizationState()) {
      return jdbcMethodFunc.call();
    }

    final boolean isBatchExecution = methodName.endsWith(".executeBatch");
    if (isBatchExecution) {
      // Batch entries cannot be inspected portably and may partially succeed.
      sessionStateService.markAuthorizationStateUntracked();
      return jdbcMethodFunc.call();
    }

    // Parsed once and shared by both transaction checks below.
    final SqlStatementAnalysis sqlAnalysis = sqlMethodAnalyzer.analyze(jdbcMethodArgs);
    final boolean doesSwitchAutoCommitFalseTrue =
        sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(
            this.pluginService.getCurrentConnection(),
            methodName,
            jdbcMethodArgs,
            sqlAnalysis);
    final boolean isStatementExecution = isStatementExecutionMethod(methodName);
    final boolean isDatabaseContextSetter =
        JdbcMethod.CONNECTION_SETCATALOG.methodName.equals(methodName)
            || JdbcMethod.CONNECTION_SETSCHEMA.methodName.equals(methodName);
    final @Nullable String sql =
        isStatementExecution ? getStatementText(methodInvokeOn, jdbcMethodArgs) : null;
    final AuthorizationStateImpact impact;
    if (methodName.startsWith("CallableStatement.execute")) {
      impact = AuthorizationStateImpact.UNTRACKED;
    } else if (isStatementExecution && sql == null) {
      // The executed SQL cannot be inspected, so it may have changed session state.
      impact = AuthorizationStateImpact.UNTRACKED;
    } else {
      impact = targetDriverDialect.getAuthorizationStateImpact(sql);
    }

    T result;
    boolean executionSucceeded = false;
    try {
      result = jdbcMethodFunc.call();
      executionSucceeded = true;
    } finally {
      if (!executionSucceeded) {
        if (impact == AuthorizationStateImpact.UNTRACKED) {
          sessionStateService.markAuthorizationStateUntracked();
        } else if (impact == AuthorizationStateImpact.TRACKED) {
          sessionStateService.markAuthorizationStateUnknown();
        }
      }
    }

    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (methodInvokeOn instanceof Connection && methodInvokeOn != currentConnection) {
      return result;
    }

    if (impact == AuthorizationStateImpact.UNTRACKED) {
      // Untracked settings and opaque calls can change authorization context the key cannot
      // represent, so caching stays off for this connection.
      sessionStateService.markAuthorizationStateUntracked();
      return result;
    }

    final boolean doesCloseTransaction =
        sqlMethodAnalyzer.doesCloseTransaction(
            currentConnection,
            methodName,
            jdbcMethodArgs,
            sqlAnalysis);
    final boolean changesState = impact == AuthorizationStateImpact.TRACKED || isDatabaseContextSetter;
    if (!changesState && !doesCloseTransaction && !doesSwitchAutoCommitFalseTrue) {
      return result;
    }

    try {
      final boolean autoCommit = currentConnection.getAutoCommit();
      if (this.keepKnownStateAtTransactionEnd
          && !changesState
          && (doesSwitchAutoCommitFalseTrue || !autoCommit)
          && sessionStateService.getAuthorizationState().isPresent()) {
        // The state is never read inside a transaction, so a tracked statement in this transaction
        // would already have marked it unknown. A state that is still known has not changed.
        return result;
      }
      if (!autoCommit || this.isInTransaction()) {
        // Avoid issuing an internal query inside the application's transaction, and do not record
        // transaction-local state such as SET LOCAL ROLE. With auto-commit on, the state is read
        // again after a COMMIT or ROLLBACK statement ends the transaction. With auto-commit off, it
        // is read when auto-commit is turned back on.
        sessionStateService.markAuthorizationStateUnknown();
      } else {
        sessionStateService.refreshAuthorizationState();
      }
    } catch (final SQLException e) {
      sessionStateService.markAuthorizationStateUnknown();
      if (this.refreshFailureWarningLogged.compareAndSet(false, true)) {
        LOGGER.log(
            Level.WARNING,
            Messages.get("CacheAuthorizationTracker.errorRefreshingAuthorizationSessionState"),
            e);
      }
    }

    return result;
  }

  private boolean isInTransaction() {
    return Boolean.TRUE.equals(this.pluginService.isInTransaction());
  }

  /**
   * Appends one cache key component, prefixed with its length so that component boundaries are
   * unambiguous. A {@code null} component is written as {@code -1:}.
   */
  static void appendCacheKeyPart(final StringBuilder cacheKey, final @Nullable String value) {
    if (value == null) {
      cacheKey.append("-1:");
      return;
    }
    cacheKey.append(value.length()).append(':').append(value);
  }

  /**
   * Appends every component of the authorization session state. Both cache plugins use this, so a
   * component added to {@link AuthorizationSessionState} reaches both keys.
   */
  static void appendAuthorizationStateParts(
      final StringBuilder cacheKey, final AuthorizationSessionState state) {
    appendCacheKeyPart(cacheKey, state.getSessionUser());
    appendCacheKeyPart(cacheKey, state.getCurrentUser());
    appendCacheKeyPart(cacheKey, state.getSearchPath());
    appendCacheKeyPart(cacheKey, state.getResolvedSearchPath());
    // PostgreSQL SET ROLE changes current_user, while MySQL and MariaDB keep CURRENT_USER()
    // unchanged and expose active roles separately through CURRENT_ROLE(), so both are included.
    appendCacheKeyPart(cacheKey, state.getActiveRoles());
    appendCacheKeyPart(cacheKey, state.getCurrentDatabase());
  }

  private static boolean isStatementExecutionMethod(final String methodName) {
    return methodName.startsWith("Statement.execute")
        || methodName.startsWith("PreparedStatement.execute")
        || methodName.startsWith("CallableStatement.execute");
  }

  private static boolean isSingleStatement(final @Nullable String sql) {
    if (StringUtils.isNullOrEmpty(sql)) {
      return false;
    }

    String candidate = sql.trim();
    if (candidate.endsWith(";")) {
      candidate = candidate.substring(0, candidate.length() - 1);
    }
    // A semicolon inside a literal can cause a conservative cache bypass, which is safe.
    return candidate.indexOf(';') < 0;
  }

  // Returns the executed SQL without driver-specific formatting, for session-state analysis only.
  private @Nullable String getStatementText(
      final Object methodInvokeOn, final @Nullable Object[] jdbcMethodArgs) {
    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0 && jdbcMethodArgs[0] != null) {
      return jdbcMethodArgs[0].toString().trim();
    }

    if (methodInvokeOn instanceof PreparedStatement) {
      try {
        return this.pluginService.getTargetDriverDialect()
            .getSQLStatementText((PreparedStatement) methodInvokeOn);
      } catch (Exception e) {
        LOGGER.log(Level.FINE, Messages.get("CacheAuthorizationTracker.unableToGetStatementText"), e);
        return null;
      }
    }
    return null;
  }

  private void logAuthorizationStateUnavailable(final @Nullable SQLException exception) {
    if (!this.authorizationStateWarningLogged.compareAndSet(false, true)) {
      return;
    }
    if (exception == null) {
      LOGGER.warning(Messages.get("CacheAuthorizationTracker.authorizationStateUnavailable"));
    } else {
      LOGGER.log(
          Level.WARNING,
          Messages.get("CacheAuthorizationTracker.authorizationStateUnavailable"),
          exception);
    }
  }

  private void logUntrackedAuthorizationState() {
    if (this.untrackedAuthorizationStateWarningLogged.compareAndSet(false, true)) {
      LOGGER.warning(Messages.get("CacheAuthorizationTracker.untrackedAuthorizationState"));
    }
  }
}
