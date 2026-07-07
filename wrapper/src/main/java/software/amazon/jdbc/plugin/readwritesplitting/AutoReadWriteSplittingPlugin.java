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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.util.Messages;
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

  private static final Logger LOGGER =
      Logger.getLogger(AutoReadWriteSplittingPlugin.class.getName());

  public static final AwsWrapperProperty QUERY_LEVEL_LOAD_BALANCING =
      new AwsWrapperProperty(
          "queryLevelLoadBalancing",
          "false",
          "When true, a read query that is routed to a reader triggers a per-query selection of a "
              + "reader connection (query-level load balancing) using the configured "
              + "readerHostSelectorStrategy. Connection switching is still suppressed inside a "
              + "transaction or while autocommit is disabled.");

  public static final AwsWrapperProperty LOAD_BALANCING_INCLUDE_WRITER =
      new AwsWrapperProperty(
          "loadBalancingIncludeWriter",
          "false",
          "When query-level load balancing is enabled, includes the writer instance in the pool of "
              + "load-balancing candidates. Only consulted when queryLevelLoadBalancing is true.");

  static {
    PropertyDefinition.registerPluginProperties(AutoReadWriteSplittingPlugin.class);
  }

  private final boolean queryLevelLoadBalancing;
  private final boolean loadBalancingIncludeWriter;

  // The reader connection vacated by the most recent balancing switch, pending close on the next
  // switch. Held so a connection is not closed while a Statement created before the switch may
  // still be executing on it. Never holds the writer connection.
  private Connection vacatedConnection;

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
    this.queryLevelLoadBalancing = QUERY_LEVEL_LOAD_BALANCING.getBoolean(properties);
    this.loadBalancingIncludeWriter = LOAD_BALANCING_INCLUDE_WRITER.getBoolean(properties);
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
        final boolean readOnly = shouldRouteToReader();
        if (readOnly && this.queryLevelLoadBalancing) {
          // Per-query load balancing: pick a (possibly different) balancing target for this
          // read query instead of staying on the single cached reader.
          switchToBalancedReader();
        } else {
          switchConnectionIfRequired(readOnly);
        }
      } catch (final SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    return super.execute(resultClass, exceptionClass, methodInvokeOn,
        methodName, jdbcMethodFunc, args);
  }

  /**
   * Selects a balancing target for a read query and switches the current connection to it.
   *
   * <p>The target is chosen with the configured {@code readerHostSelectorStrategy}. When
   * {@code loadBalancingIncludeWriter} is enabled, the writer instance is also eligible.
   * If no eligible target is available, or the selected target is already the current host,
   * the current connection is left in place. On connection failure the current connection is
   * kept as a fallback when it is still usable.
   *
   * <p>This method is confined to the Automatic Read/Write Splitting Plugin and does not change
   * the behavior of the other read/write splitting plugins.
   */
  protected void switchToBalancedReader() throws SQLException {
    // Close the connection vacated during the PREVIOUS balancing switch. It is only safe to close
    // it now, one query later: any Statement/ResultSet created before that switch has since been
    // closed by the caller. The connection vacated by THIS switch cannot be closed yet (a statement
    // created before this switch may still be executing on it), so its cleanup is deferred.
    releaseVacatedConnection();

    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();

    if (currentConnection == null || currentConnection.isClosed() || currentHost == null) {
      // No usable current connection; let the standard reader-routing path establish one.
      switchConnectionIfRequired(true);
      return;
    }

    final HostRole role = this.loadBalancingIncludeWriter ? null : HostRole.READER;
    final HostSpec target = this.pluginService.getHostSpecByStrategy(
        this.getReaderHostCandidates(), role, this.readerSelectorStrategy);

    if (target == null) {
      // No balancing candidate available (e.g. single-host cluster). Fall back to the standard
      // reader-routing path, which handles single-host clusters and reader fallback. Only needed
      // when we are not already in read-write-split (reader) mode.
      if (!this.inReadWriteSplit) {
        switchConnectionIfRequired(true);
      }
      return;
    }

    if (target.getHostAndPort().equals(currentHost.getHostAndPort())) {
      // Already connected to the selected host; nothing to switch.
      this.inReadWriteSplit = true;
      return;
    }

    // Preserve the writer connection so it can be reused for later write routing instead of
    // being orphaned when we balance away from it.
    if (isWriter(currentHost) && !isConnectionUsable(this.writerConnection)) {
      setWriterConnection(currentConnection, currentHost);
    }

    // Ask the plugin service for a connection to the selected host. When the user has configured
    // an internal connection pool, this reuses a pooled connection; otherwise it opens a new one.
    final Connection newConnection;
    try {
      newConnection = this.pluginService.connect(target, this.properties, this);
    } catch (final SQLException e) {
      // Could not connect to the selected target; keep the current connection when it is usable.
      if (!isConnectionUsable(currentConnection)) {
        throw e;
      }
      LOGGER.fine(() -> Messages.get(
          "ReadWriteSplittingPlugin.fallbackToCurrentConnection",
          new Object[] {currentHost.getHostAndPort(), e.getMessage()}));
      return;
    }

    this.isReaderConnFromInternalPool = Boolean.TRUE.equals(this.pluginService.isPooledConnection());
    this.inReadWriteSplit = true;
    // Store the new reader in the base reader cache (honoring cachedReaderKeepAliveTimeoutMs) so
    // notifyConnectionChanged treats it as a cached connection and does not close it.
    setReaderConnection(newConnection, target);
    switchCurrentConnectionTo(newConnection, target);
    LOGGER.finest(() -> Messages.get(
        "AutoReadWriteSplittingPlugin.switchedToBalancedReader",
        new Object[] {target.getHostAndPort(), this.readerSelectorStrategy}));

    // Defer closing the connection we just left until the next balancing switch: it may still be
    // in use by an in-flight statement created before this switch. The writer connection is never
    // queued for closing so it remains available for write routing.
    this.vacatedConnection = (currentConnection == this.writerConnection) ? null : currentConnection;
  }

  /**
   * Closes the connection that was vacated by the previous balancing switch, unless it is the
   * preserved writer connection or has become the current connection again.
   */
  private void releaseVacatedConnection() {
    final Connection toClose = this.vacatedConnection;
    this.vacatedConnection = null;
    if (toClose == null || toClose == this.writerConnection) {
      return;
    }
    try {
      if (toClose != this.pluginService.getCurrentConnection() && !toClose.isClosed()) {
        toClose.close();
      }
    } catch (final SQLException e) {
      // best-effort cleanup / pool return; ignore
    }
  }

  @Override
  public void releaseResources() {
    releaseVacatedConnection();
    super.releaseResources();
  }

  // Visible for testing
  boolean isQueryLevelLoadBalancingEnabled() {
    return this.queryLevelLoadBalancing;
  }

  // Visible for testing
  @Nullable HostRole getLoadBalancingCandidateRole() {
    return this.loadBalancingIncludeWriter ? null : HostRole.READER;
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
