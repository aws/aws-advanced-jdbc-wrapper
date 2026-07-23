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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

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
 */
public class SqlRoutingSignal implements RoutingSignal {

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
   * {@code SELECT} routes to the reader); when no parse result is available the writer is used as a
   * safe fallback.
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
        return Boolean.TRUE.equals(forUpdate) ? TargetRole.WRITER : TargetRole.READER;
      }
    }
    // No parse result (e.g. batch execute, parser plugin missing): route to the writer to be safe.
    return TargetRole.WRITER;
  }
}
