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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * Read/write splitting plugin that automatically routes queries based on SQL analysis.
 * SELECT queries are routed to a reader instance; DML/DDL queries are routed to the writer.
 * SELECT ... FOR UPDATE is detected and routed to the writer.
 *
 * <p>Routing can be overridden using SQL comment hints:
 * <ul>
 *   <li>{@code /*@reader* /} — force query to reader</li>
 *   <li>{@code /*@writer* /} — force query to writer</li>
 *   <li>{@code /*@keep* /} — run on the current connection without re-routing</li>
 * </ul>
 *
 * <p>Requires the {@code sqlParser} plugin to be loaded before this plugin.
 * Connection routing is not changed while a transaction is in progress: a transaction
 * keeps running against the connection it started on (so a read-only transaction started
 * on a reader stays on the reader, and a write transaction stays on the writer).
 */
public class AutoReadWriteSplittingPlugin extends ReadWriteSplittingPlugin {

  private static final Set<String> executeMethodNames;

  static {
    Set<String> methods = new HashSet<>();
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName);
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName);
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName);
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName);
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTE.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTEQUERY.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTEBATCH.methodName);
    methods.add(JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName);
    methods.add(JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName);
    methods.add(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName);
    methods.add(JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName);
    methods.add(JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH.methodName);
    executeMethodNames = Collections.unmodifiableSet(methods);
  }

  private final Set<String> allSubscribedMethods;

  public AutoReadWriteSplittingPlugin(
      final PluginService pluginService, final @NonNull Properties properties) {
    super(pluginService, properties);
    Set<String> combined = new HashSet<>(super.getSubscribedMethods());
    combined.addAll(executeMethodNames);
    this.allSubscribedMethods = Collections.unmodifiableSet(combined);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return allSubscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] args)
      throws E {

    // For execute methods, determine routing before delegating.
    // Re-routing is intentionally suppressed when the connection must be pinned to whatever
    // it currently is: either a transaction is already in progress, or autocommit is disabled
    // (the next statement will implicitly start a transaction). Switching the connection in
    // those cases would break the in-flight transaction, so the statement runs on the current
    // connection (writer or reader) as-is.
    // The parent's execute() only handles setReadOnly/setAutoCommit/clearWarnings —
    // it does not route on execute methods, so there is no double-switch risk.
    if (executeMethodNames.contains(methodName) && !shouldKeepCurrentConnection()) {
      try {
        boolean readOnly = shouldRouteToReader();
        switchConnectionIfRequired(readOnly);
      } catch (final SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    return super.execute(resultClass, exceptionClass, methodInvokeOn,
        methodName, jdbcMethodFunc, args);
  }

  /**
   * Returns true when re-routing must be suppressed for the current statement, pinning it to
   * the connection currently in use (whether writer or reader).
   *
   * <p>This is the case when:
   * <ul>
   *   <li>an explicit {@code /*@keep* /} routing hint is present, or</li>
   *   <li>a transaction is already open, or</li>
   *   <li>autocommit is disabled (the next statement will implicitly start a transaction).</li>
   * </ul>
   * A connection cannot be switched in the middle of a transaction without breaking it, so the
   * statement stays on the current connection regardless of its role. In the common flow the
   * current connection is the writer (autocommit is typically disabled before the first query,
   * while still on the writer), so this keeps writes on the writer.
   *
   * <p>{@link PluginService#isInTransaction()} alone is insufficient: {@code setAutoCommit(false)}
   * does not open a transaction until the first statement executes, which is too late to route
   * that first statement. The autocommit value is read from the wrapper-tracked
   * {@link SessionStateService} rather than the live connection to avoid a driver round-trip;
   * it is only treated as "off" when explicitly set to false.
   */
  // Visible for testing
  boolean shouldKeepCurrentConnection() {
    final PluginCallContext ctx = pluginService.getCallContext();
    if (ctx != null
        && ctx.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class) == RoutingHint.KEEP) {
      return true;
    }
    if (pluginService.isInTransaction()) {
      return true;
    }
    try {
      final Optional<Boolean> autoCommit = pluginService.getSessionStateService().getAutoCommit();
      return autoCommit.isPresent() && !autoCommit.get();
    } catch (final SQLException e) {
      // If autocommit state cannot be determined, fall back to normal routing.
      return false;
    }
  }

  // Visible for testing
  boolean shouldRouteToReader() {
    PluginCallContext ctx = pluginService.getCallContext();

    if (ctx != null) {
      // Check for explicit routing hint (highest priority)
      RoutingHint routingHint = ctx.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class);
      if (routingHint == RoutingHint.READER) {
        return true;
      }
      if (routingHint == RoutingHint.WRITER) {
        return false;
      }

      // Use parsed query type from context
      QueryType queryType = ctx.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class);
      if (queryType != null) {
        if (queryType != QueryType.SELECT) {
          return false;
        }
        // SELECT FOR UPDATE must go to writer (takes row locks)
        Boolean forUpdate = ctx.getAttribute(SqlContextKeys.FOR_UPDATE, Boolean.class);
        return !Boolean.TRUE.equals(forUpdate);
      }
    }

    // Fallback when no parse result is available (e.g. batch execute, parser plugin missing):
    // route to the writer to be safe.
    return false;
  }
}
