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

package software.amazon.jdbc.plugin.readwritesplitting.signal;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.util.Messages;

/**
 * SQL-based {@link RoutingSignal}. Because a JDBC statement is bound to the connection that created
 * it, this signal resolves a role only on the statement-creation methods
 * ({@code Connection.prepareStatement} / {@code Connection.prepareCall}), where a switch can still
 * rebind the statement. It also subscribes to plain {@code Statement.execute*(sql)} methods for
 * observation (reuse warning / deferred rotation / optional rebinding), but returns
 * {@link TargetRole#NO_DECISION} for their role because a bound plain statement cannot be rerouted by
 * switching the current connection.
 *
 * <p>Requires the {@code sqlParser} plugin to be ordered before the unified plugin so parse results
 * are present in the {@link PluginCallContext} during the prepare call.
 *
 * <p>Optionally (see {@code assumeWriteTransaction}) a transaction that was not declared read-only
 * is treated as a read-write transaction, so a {@code SELECT} that opens it is routed to the writer
 * instead of a reader. See {@link #isWriteTransactionAssumed(RwSplitContext)}.
 */
public class SqlRoutingSignal implements RoutingSignal {

  private static final Logger LOGGER = Logger.getLogger(SqlRoutingSignal.class.getName());

  private static final Set<String> PREPARE_METHODS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName,
          JdbcMethod.CONNECTION_PREPARECALL.methodName)));

  static final Set<String> PLAIN_STATEMENT_EXECUTE_METHODS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName)));

  private static final Set<String> SUBSCRIBED;

  static {
    Set<String> all = new HashSet<>(PREPARE_METHODS);
    all.addAll(PLAIN_STATEMENT_EXECUTE_METHODS);
    SUBSCRIBED = Collections.unmodifiableSet(all);
  }

  private final boolean assumeWriteTransaction;

  /** Routes purely on the parsed SQL, without inferring write intent from transaction state. */
  public SqlRoutingSignal() {
    this(false);
  }

  /**
   * Routes on the parsed SQL, optionally inferring write intent from the transaction state.
   *
   * @param assumeWriteTransaction when {@code true}, a transaction that carries no explicit
   *                               read-only declaration is treated as a read-write transaction and
   *                               routed to the writer (see {@code assumeWriteTransaction}).
   */
  public SqlRoutingSignal(final boolean assumeWriteTransaction) {
    this.assumeWriteTransaction = assumeWriteTransaction;
  }

  @Override
  public Set<String> extraSubscribedMethods() {
    return SUBSCRIBED;
  }

  @Override
  public TargetRole resolve(
      final RwSplitContext ctx, final String methodName, final @Nullable Object[] args) {
    if (PREPARE_METHODS.contains(methodName)) {
      return roleFromSqlContext(ctx);
    }
    // Plain Statement execute methods are observed elsewhere; the bound statement cannot be
    // rerouted by switching the current connection, so this signal abstains here.
    return TargetRole.NO_DECISION;
  }

  @Override
  public TargetRole resolveForBoundStatement(final RwSplitContext ctx) {
    return roleFromSqlContext(ctx);
  }

  /**
   * Computes the routing role from the parsed SQL analysis in the {@link PluginCallContext}:
   * an explicit {@link RoutingHint} wins; otherwise {@link QueryType} (only a non-locking
   * {@code SELECT} routes to the reader, and only when write intent is not assumed for the current
   * transaction); when no parse result is available the writer is used as a safe fallback.
   *
   * @param ctx the read/write splitting context
   * @return the resolved target role (never {@link TargetRole#NO_DECISION})
   */
  public TargetRole roleFromSqlContext(final RwSplitContext ctx) {
    final PluginCallContext callContext = ctx.pluginService().getCallContext();
    if (callContext != null) {
      final RoutingHint hint = callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class);
      if (hint == RoutingHint.READER) {
        return TargetRole.READER;
      }
      if (hint == RoutingHint.WRITER) {
        return TargetRole.WRITER;
      }
      if (hint == RoutingHint.KEEP) {
        return TargetRole.KEEP;
      }

      final QueryType queryType = callContext.getAttribute(SqlContextKeys.QUERY_TYPE, QueryType.class);
      if (queryType != null) {
        if (queryType != QueryType.SELECT) {
          return TargetRole.WRITER;
        }
        final Boolean forUpdate = callContext.getAttribute(SqlContextKeys.FOR_UPDATE, Boolean.class);
        if (Boolean.TRUE.equals(forUpdate)) {
          return TargetRole.WRITER;
        }
        // A plain read: send it to a reader unless it belongs to a transaction that has to be
        // assumed read-write, in which case the whole transaction must start on the writer.
        if (this.isWriteTransactionAssumed(ctx)) {
          // FINEST, not FINE: this fires on every read of every transaction while the setting is
          // enabled, so it belongs at the most verbose level to keep the routing path quiet. The
          // resulting host switch (or the absence of one) is already logged by the plugin.
          LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.assumedWriteTransaction"));
          return TargetRole.WRITER;
        }
        return TargetRole.READER;
      }
    }
    // No parse result (e.g. batch execute, parser plugin missing): route to the writer to be safe.
    return TargetRole.WRITER;
  }

  /**
   * Decides whether the current (or imminent) transaction has to be treated as read-write, which
   * makes a read that would otherwise go to a reader route to the writer instead.
   *
   * <p>Transaction managers only announce the read-only case: Spring calls
   * {@code Connection.setReadOnly(true)} for {@code @Transactional(readOnly = true)} but issues no
   * corresponding call for the read-write default, and plain JDBC code usually calls neither. Write
   * intent therefore has to be inferred from the absence of a read-only declaration, which is a
   * guess and is why it is opt-in via {@code assumeWriteTransaction}: the trade-off is losing read
   * offloading for transactional reads in exchange for never starting a read-write transaction on a
   * reader it cannot finish on.
   *
   * <p>Autocommit being off counts as a transaction here even before the first statement runs. That
   * is the point at which the decision has to be made: the statement being routed is the one that
   * physically opens the transaction, and once it has run on a reader the transaction can no longer
   * be moved.
   *
   * @param ctx the read/write splitting context
   * @return true if reads should be routed to the writer for the current transaction
   */
  private boolean isWriteTransactionAssumed(final RwSplitContext ctx) {
    if (!this.assumeWriteTransaction) {
      return false;
    }

    final PluginService pluginService = ctx.pluginService();
    final SessionStateService sessionStateService = pluginService.getSessionStateService();
    try {
      // An explicit read-only declaration is the application stating read intent; honor it.
      final Optional<Boolean> readOnly = sessionStateService.getReadOnly();
      if (readOnly.isPresent() && readOnly.get()) {
        return false;
      }

      if (pluginService.isInTransaction()) {
        return true;
      }

      final Optional<Boolean> autoCommit = sessionStateService.getAutoCommit();
      // An empty value means autocommit was never explicitly set, so the JDBC default (on) applies
      // and this read does not belong to a transaction.
      return autoCommit.isPresent() && !autoCommit.get();
    } catch (final SQLException e) {
      // The session state cannot be read, so it is unknown whether a transaction is involved. Fall
      // back to plain SQL routing (the behavior with this setting disabled) rather than pinning
      // every read to the writer on a connection whose state cannot be queried. A switch to a
      // reader is separately vetoed by TransactionAwareGate, which pins on an unreadable autocommit
      // state, so this cannot move an open transaction.
      LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.assumeWriteTransactionUnknownState",
          new Object[] {e.getMessage()}));
      return false;
    }
  }
}
