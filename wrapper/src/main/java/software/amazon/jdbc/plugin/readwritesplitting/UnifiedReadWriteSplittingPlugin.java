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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.Rebindable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.balancer.LoadBalancingPolicy;
import software.amazon.jdbc.plugin.readwritesplitting.balancer.PerQueryBalancedReaderPolicy;
import software.amazon.jdbc.plugin.readwritesplitting.balancer.StickyReaderPolicy;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.WriterResolution;
import software.amazon.jdbc.plugin.readwritesplitting.signal.TargetRole;
import software.amazon.jdbc.util.CacheItem;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * Unified read/write splitting plugin. Owns the shared connection state and orchestration formerly
 * in {@code AbstractReadWriteSplittingPlugin}, delegating every variable decision to injected
 * helpers ({@link RwSplitHelpers}). The existing plugin codes are reconstructed as helper
 * assemblies by their factories.
 */
public abstract class UnifiedReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources, StateSnapshotProvider, RwSplitContext {

  private static final Logger LOGGER = Logger.getLogger(UnifiedReadWriteSplittingPlugin.class.getName());

  public static final AwsWrapperProperty CACHED_READER_KEEP_ALIVE_TIMEOUT =
      new AwsWrapperProperty(
          "cachedReaderKeepAliveTimeoutMs",
          "0",
          "The time in milliseconds to keep a reader connection alive in the cache. "
              + "Default value 0 means the Wrapper will keep reusing the same cached reader connection.");

  public static final AwsWrapperProperty ALLOW_STATEMENT_RECREATION_ON_CONNECTION_SWITCH =
      new AwsWrapperProperty(
          "allowStatementRecreationOnConnectionSwitch",
          "true",
          "When a routing decision switches the underlying connection to a different node, allow "
              + "an already-created Statement/PreparedStatement/CallableStatement to be re-created "
              + "on the new connection (replaying its recorded settings, bound parameters, and "
              + "registered OUT parameters) so the query runs there. Enabled by default; set to "
              + "false to leave reused statements on their original connection (falling back to the "
              + "bound-statement reuse warning). When re-creation is not possible (a one-shot stream "
              + "parameter or a pending batch), the same fallback applies regardless of this "
              + "setting.");

  public static final AwsWrapperProperty QUERY_LEVEL_LOAD_BALANCING =
      new AwsWrapperProperty(
          "queryLevelLoadBalancing",
          "false",
          "Select a fresh reader on each read-routing decision within an established read-only "
              + "phase (query-level load balancing) instead of reusing a single sticky reader.");

  public static final AwsWrapperProperty LOAD_BALANCING_INCLUDE_WRITER =
      new AwsWrapperProperty(
          "loadBalancingIncludeWriter",
          "false",
          "When query-level load balancing is enabled, include the writer node as an eligible "
              + "target in the reader-balancing pool.");

  static {
    PropertyDefinition.registerPluginProperties(UnifiedReadWriteSplittingPlugin.class);
  }

  /** Plain {@code Statement} execute methods that carry SQL and support rerouting via rebinding. */
  private static final Set<String> PLAIN_STATEMENT_EXECUTE_METHODS =
      Collections.unmodifiableSet(new HashSet<>(java.util.Arrays.asList(
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName)));

  /**
   * {@code PreparedStatement}/{@code CallableStatement} execute methods. Their SQL is routed at
   * prepare time, so reuse is normal; but with query-level load balancing enabled, re-executing the
   * same bound statement cannot rotate readers, which warrants a one-time reuse warning.
   */
  private static final Set<String> PREPARED_CALLABLE_EXECUTE_METHODS =
      Collections.unmodifiableSet(new HashSet<>(java.util.Arrays.asList(
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName)));

  private static final Set<String> BASE_SUBSCRIBED_METHODS =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.INITHOSTPROVIDER.methodName);
          add(JdbcMethod.CONNECT.methodName);
          add(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName);
          add(JdbcMethod.CONNECTION_SETREADONLY.methodName);
          add(JdbcMethod.CONNECTION_CLEARWARNINGS.methodName);
          add(JdbcMethod.STATEMENT_EXECUTE.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName);
        }
      });

  protected final PluginService pluginService;
  protected final Properties properties;
  protected final RwSplitHelpers helpers;
  private final Set<String> subscribedMethods;
  private final boolean allowStatementRecreationOnConnectionSwitch;
  private final boolean queryLevelLoadBalancing;

  // Tracks plain Statement objects seen for execute-with-SQL, to warn once per statement when a
  // bound statement is reused and rerouting cannot be applied.
  private final Map<Object, Boolean> seenBoundStatements =
      Collections.synchronizedMap(new WeakHashMap<>());

  protected volatile boolean inReadWriteSplit = false;
  protected @Nullable HostListProviderService hostListProviderService;
  protected @Nullable Connection writerConnection;
  protected @Nullable CacheItem<Connection> readerCacheItem;
  protected @Nullable HostSpec writerHostSpec;
  protected @Nullable HostSpec readerHostSpec;
  protected boolean isReaderConnFromInternalPool;
  protected boolean isWriterConnFromInternalPool;

  public UnifiedReadWriteSplittingPlugin(
      final PluginService pluginService, final Properties properties, final RwSplitHelpers helpers) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.helpers = helpers;

    final Set<String> methods = new HashSet<>(BASE_SUBSCRIBED_METHODS);
    methods.addAll(helpers.routingSignal.extraSubscribedMethods());
    this.subscribedMethods = Collections.unmodifiableSet(methods);
    this.allowStatementRecreationOnConnectionSwitch =
        ALLOW_STATEMENT_RECREATION_ON_CONNECTION_SWITCH.getBoolean(properties);
    this.queryLevelLoadBalancing = QUERY_LEVEL_LOAD_BALANCING.getBoolean(properties);
  }

  /**
   * Builds the reader load-balancing policy for an assembly: {@link PerQueryBalancedReaderPolicy}
   * when {@code queryLevelLoadBalancing} is enabled (honoring {@code loadBalancingIncludeWriter}),
   * otherwise a {@link StickyReaderPolicy}.
   */
  protected static LoadBalancingPolicy readerLoadBalancer(final Properties props, final String strategy) {
    if (QUERY_LEVEL_LOAD_BALANCING.getBoolean(props)) {
      return new PerQueryBalancedReaderPolicy(LOAD_BALANCING_INCLUDE_WRITER.getBoolean(props), strategy);
    }
    return new StickyReaderPolicy(strategy);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return this.subscribedMethods;
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {
    this.hostListProviderService = hostListProviderService;
    initHostProviderFunc.call();
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return this.helpers.initialConnectionHandler.onConnect(
        this, driverProtocol, hostSpec, props, isInitialConnection, connectFunc);
  }

  @Override
  public Connection connect(final HostSpec host, final Properties props) throws SQLException {
    return this.pluginService.connect(host, props, this);
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    if (changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED)) {
      final Connection currentConnection = this.pluginService.getCurrentConnection();
      final boolean isCachedConnection =
          currentConnection == this.writerConnection
              || (this.readerCacheItem != null && currentConnection == this.readerCacheItem.get());
      if (!isCachedConnection) {
        this.closeIdleConnections();
      }
    }

    try {
      this.updateInternalConnectionInfo();
    } catch (final SQLException e) {
      // ignore
    }

    if (this.inReadWriteSplit) {
      return OldConnectionSuggestedAction.PRESERVE;
    }
    return OldConnectionSuggestedAction.NO_OPINION;
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
    final Connection conn = WrapperUtils.getConnectionFromSqlObject(methodInvokeOn);
    if (conn != null && conn != this.pluginService.getCurrentConnection()) {
      LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.executingAgainstOldConnection",
          new Object[] {methodInvokeOn}));
      return jdbcMethodFunc.call();
    }

    if (JdbcMethod.CONNECTION_CLEARWARNINGS.methodName.equals(methodName)) {
      try {
        if (this.writerConnection != null && !this.writerConnection.isClosed()) {
          this.writerConnection.clearWarnings();
        }
        if (this.readerCacheItem != null && this.isConnectionUsable(this.readerCacheItem.get())) {
          this.readerCacheItem.get().clearWarnings();
        }
      } catch (final SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    try {
      final TargetRole desired = this.helpers.routingSignal.resolve(this, methodName, args);
      if (desired != TargetRole.NO_DECISION && desired != TargetRole.KEEP) {
        this.performSwitch(methodName, desired);
      }
      this.maybeHandleBoundStatement(methodName, methodInvokeOn);
      this.maybeHandlePreparedStatement(methodName, methodInvokeOn);
    } catch (final SQLException e) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
    }

    try {
      return jdbcMethodFunc.call();
    } catch (final Exception e) {
      if (e instanceof FailoverSQLException) {
        LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.failoverExceptionWhileExecutingCommand",
            new Object[] {methodName}));
        this.closeIdleConnections();
      } else {
        LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.exceptionWhileExecutingCommand",
            new Object[] {methodName}));
      }
      throw e;
    }
  }

  private void performSwitch(final String methodName, final TargetRole desired) throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection != null && currentConnection.isClosed()) {
      this.logAndThrow(Messages.get("ReadWriteSplittingPlugin.setReadOnlyOnClosedConnection"),
          SqlState.CONNECTION_NOT_OPEN);
    }

    this.helpers.topologyRefresher.refresh(this);

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (desired == TargetRole.READER) {
      if (!this.helpers.roleClassifier.isReader(currentHost)) {
        if (this.helpers.switchGate.canSwitch(this, TargetRole.READER)) {
          try {
            this.switchToReader();
          } catch (final SQLException e) {
            // No reader could be selected or reached (for example, the only reader is unavailable
            // and reader selection reports that no host matches the strategy). Fall back to the
            // writer rather than remaining on a possibly broken reader connection, matching the
            // legacy read/write splitting behavior. Only keep the current connection if the writer
            // is also unreachable.
            try {
              this.switchToWriter();
              LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.fallbackToWriterOnReaderFailure",
                  new Object[] {this.pluginService.getCurrentHostSpec().getHostAndPort(), e.getMessage()}));
            } catch (final SQLException writerException) {
              if (!this.isConnectionUsable(currentConnection)) {
                this.logAndThrowCause(
                    Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader",
                        new Object[] {e.getMessage()}), e);
                return;
              }
              LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.fallbackToCurrentConnection",
                  new Object[] {this.pluginService.getCurrentHostSpec().getHostAndPort(), e.getMessage()}));
            }
          }
        }
      } else if (this.helpers.readerResolver.isPerQuery()
          && this.helpers.switchGate.canSwitch(this, TargetRole.READER)) {
        // Query-level load balancing: already on a reader, so rotate reader-to-reader (role stays
        // READER) for this read. Failure keeps the current reader.
        try {
          this.rotateToNewReader();
        } catch (final SQLException e) {
          LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.fallbackToCurrentConnection",
              new Object[] {this.pluginService.getCurrentHostSpec().getHostAndPort(), e.getMessage()}));
        }
      }
    } else if (desired == TargetRole.WRITER) {
      if (!this.helpers.roleClassifier.isWriter(currentHost)) {
        if (JdbcMethod.CONNECTION_SETREADONLY.methodName.equals(methodName)
            && this.pluginService.isInTransaction()) {
          this.logAndThrow(Messages.get("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"),
              SqlState.ACTIVE_SQL_TRANSACTION);
        }
        if (this.helpers.switchGate.canSwitch(this, TargetRole.WRITER)) {
          try {
            this.switchToWriter();
          } catch (final SQLException e) {
            this.logAndThrowCause(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToWriter"), e);
          }
        }
      }
    }
  }

  /**
   * Handles SQL-driven routing for an already-bound plain {@code Statement}. When rebinding is
   * enabled and a reroute is required, switches the current connection to the target role and
   * re-creates the statement on it (via the {@link Rebindable} handle published on the call
   * context). When rebinding is unavailable, logs the bound-statement reuse warning once per
   * statement.
   */
  private void maybeHandleBoundStatement(final String methodName, final Object methodInvokeOn)
      throws SQLException {
    if (!PLAIN_STATEMENT_EXECUTE_METHODS.contains(methodName)) {
      return;
    }

    final TargetRole sqlRole = this.helpers.routingSignal.resolveForBoundStatement(this);
    if (sqlRole != TargetRole.READER && sqlRole != TargetRole.WRITER) {
      return;
    }

    // A transaction/autocommit/keep pin means the statement legitimately stays put; not a miss.
    if (!this.helpers.switchGate.canSwitch(this, sqlRole)) {
      return;
    }

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    final boolean alreadyOnTarget =
        (sqlRole == TargetRole.READER && this.helpers.roleClassifier.isReader(currentHost))
            || (sqlRole == TargetRole.WRITER && this.helpers.roleClassifier.isWriter(currentHost));
    if (alreadyOnTarget) {
      return;
    }

    final PluginCallContext callContext = this.pluginService.getCallContext();
    final Rebindable rebindHandle = callContext == null ? null : callContext.getRebindHandle();

    if (!this.allowStatementRecreationOnConnectionSwitch || rebindHandle == null) {
      // Rerouting is wanted but cannot be applied to this bound statement.
      warnOnceReusedBoundStatement(methodInvokeOn);
      return;
    }

    this.performSwitch(methodName, sqlRole);

    final HostSpec newHost = this.pluginService.getCurrentHostSpec();
    final boolean switched =
        (sqlRole == TargetRole.READER && this.helpers.roleClassifier.isReader(newHost))
            || (sqlRole == TargetRole.WRITER && this.helpers.roleClassifier.isWriter(newHost));
    if (!switched) {
      return;
    }

    final Connection current = this.pluginService.getCurrentConnection();
    if (current != null) {
      try {
        rebindHandle.rebind(current);
      } catch (final SQLException e) {
        // Could not re-create the statement on the routed connection; keep the current target
        // as a fallback (the query runs where the statement is currently bound).
        LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.fallbackToCurrentConnection",
            new Object[] {newHost == null ? "" : newHost.getHostAndPort(), e.getMessage()}));
      }
    }
  }

  /**
   * Applies query-level load balancing to a re-executed {@code PreparedStatement}/
   * {@code CallableStatement}. Their SQL (and thus role) is fixed at prepare time, so the first
   * execution runs on the reader chosen then; on each subsequent execution, if we are in a read
   * phase and the statement can be rebound, the plugin rotates reader-to-reader and re-creates the
   * statement on the new reader. When the statement cannot be rebound (a one-shot stream parameter,
   * a pending batch, or rebinding disabled), it warns once instead (Requirement 14.7). Writes are
   * never rotated. No-op when query-level load balancing is off.
   */
  private void maybeHandlePreparedStatement(final String methodName, final Object methodInvokeOn)
      throws SQLException {
    if (!this.queryLevelLoadBalancing || !PREPARED_CALLABLE_EXECUTE_METHODS.contains(methodName)) {
      return;
    }

    final Boolean seen = this.seenBoundStatements.get(methodInvokeOn);
    if (seen == null) {
      // First execution stays on the reader chosen at prepare time; just record the statement.
      this.seenBoundStatements.put(methodInvokeOn, Boolean.FALSE);
      return;
    }

    // Re-execution: rotate reader-to-reader (reads only; role is fixed at prepare time).
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (!this.helpers.roleClassifier.isReader(currentHost)
        || !this.helpers.readerResolver.isPerQuery()
        || !this.helpers.switchGate.canSwitch(this, TargetRole.READER)) {
      return;
    }

    final PluginCallContext callContext = this.pluginService.getCallContext();
    final Rebindable rebindHandle = callContext == null ? null : callContext.getRebindHandle();
    if (this.allowStatementRecreationOnConnectionSwitch
        && rebindHandle != null && rebindHandle.canRebind()) {
      try {
        this.rotateToNewReader();
        final Connection current = this.pluginService.getCurrentConnection();
        if (current != null) {
          rebindHandle.rebind(current);
          final HostSpec reboundHost = this.pluginService.getCurrentHostSpec();
          LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.statementRecreatedOnConnectionSwitch",
              new Object[] {reboundHost == null ? "" : reboundHost.getHostAndPort()}));
        }
      } catch (final SQLException e) {
        // Keep the current reader if rotation or re-preparation fails.
        LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.fallbackToCurrentConnection",
            new Object[] {this.pluginService.getCurrentHostSpec().getHostAndPort(), e.getMessage()}));
      }
      return;
    }

    // The bound statement cannot rotate readers (stream parameter, pending batch, or rebinding
    // disabled): surface the miss once per statement object.
    if (Boolean.FALSE.equals(seen)) {
      LOGGER.warning(() -> Messages.get("SqlRoutingSignal.reusedBoundStatement"));
      this.seenBoundStatements.put(methodInvokeOn, Boolean.TRUE);
    }
  }

  private void warnOnceReusedBoundStatement(final Object statement) {
    final Boolean seen = this.seenBoundStatements.get(statement);
    if (seen == null) {
      // First use of this statement; record it without warning.
      this.seenBoundStatements.put(statement, Boolean.FALSE);
      return;
    }
    if (Boolean.FALSE.equals(seen)) {
      LOGGER.warning(() -> Messages.get("SqlRoutingSignal.reusedBoundStatement"));
      this.seenBoundStatements.put(statement, Boolean.TRUE);
    }
  }

  private void switchToWriter() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (this.helpers.roleClassifier.isWriter(currentHost) && this.isConnectionUsable(currentConnection)) {
      return;
    }

    this.inReadWriteSplit = true;
    if (!this.isConnectionUsable(this.writerConnection)) {
      final WriterResolution wr = this.helpers.writerResolver.resolveWriter(this);
      if (wr.isConnected() && wr.getConnection() != null && wr.getHostSpec() != null) {
        this.markWriterFromPool(Boolean.TRUE.equals(this.pluginService.isPooledConnection()));
        this.bindWriter(wr.getConnection(), wr.getHostSpec());
        this.switchCurrentConnectionTo(wr.getConnection(), wr.getHostSpec());
      }
      // WriterResolution.STAY (e.g. Global Write Forwarding): remain on the current connection.
    } else {
      this.switchCurrentConnectionTo(this.writerConnection, this.writerHostSpec);
    }

    if (this.isReaderConnFromInternalPool) {
      this.closeReaderConnectionIfIdle();
    }

    final HostSpec writerHost = this.writerHostSpec;
    if (writerHost != null) {
      LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromReaderToWriter",
          new Object[] {writerHost.getHostAndPort()}));
    }
  }

  private void switchToReader() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (this.helpers.roleClassifier.isReader(currentHost) && this.isConnectionUsable(currentConnection)) {
      return;
    }

    this.helpers.readerResolver.closeStaleReaderIfNecessary(this);

    this.inReadWriteSplit = true;
    if (this.readerCacheItem == null || !this.isConnectionUsable(this.readerCacheItem.get())) {
      this.helpers.readerResolver.switchToReader(this);
    } else {
      try {
        this.switchCurrentConnectionTo(this.readerCacheItem.get(), this.readerHostSpec);
        final HostSpec readerHost = this.readerHostSpec;
        LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
            new Object[] {readerHost == null ? "" : readerHost.getHostAndPort()}));
      } catch (final SQLException e) {
        final HostSpec readerHost = this.readerHostSpec;
        if (e.getMessage() != null) {
          LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReaderWithCause",
              new Object[] {readerHost == null ? "" : readerHost.getHostAndPort(), e.getMessage()}));
        } else {
          LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReader",
              new Object[] {readerHost == null ? "" : readerHost.getHostAndPort()}));
        }
        this.closeReaderConnectionIfIdle();
        this.helpers.readerResolver.switchToReader(this);
      }
    }

    if (this.isWriterConnFromInternalPool) {
      this.closeWriterConnectionIfIdle();
    }
  }

  /**
   * Reader-to-reader rotation for query-level load balancing. The role stays READER (safe: never
   * turns a read into a write); a fresh reader is selected via the reader resolver and the current
   * connection is switched to it. The reader we rotate away from is closed to avoid leaking
   * connections, consistent with the existing sticky idle-close behavior.
   *
   * <p>Note: like the existing idle-close logic, this does not detect an open {@code ResultSet} on
   * the previous reader (Requirement 14.9); applications using query-level load balancing should
   * consume a {@code ResultSet} before issuing the next routed read. This lifecycle guard needs
   * integration-test validation before relying on it in production.
   */
  private void rotateToNewReader() throws SQLException {
    final Connection previousReader = this.pluginService.getCurrentConnection();
    this.inReadWriteSplit = true;
    this.helpers.readerResolver.switchToReader(this);

    final Connection newReader = this.pluginService.getCurrentConnection();
    final Connection cachedReader = this.readerCacheItem == null ? null : this.readerCacheItem.get();
    if (previousReader != null && previousReader != newReader && previousReader != cachedReader) {
      try {
        if (!previousReader.isClosed()) {
          previousReader.close();
        }
      } catch (final SQLException e) {
        // Do nothing.
      }
    }
  }

  private void updateInternalConnectionInfo() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (currentConnection == null || currentHost == null) {
      return;
    }
    if (this.helpers.connectionUpdatePolicy.shouldUpdateWriter(this, currentConnection, currentHost)) {
      this.bindWriter(currentConnection, currentHost);
    } else if (this.helpers.connectionUpdatePolicy.shouldUpdateReader(this, currentConnection, currentHost)) {
      this.bindReader(currentConnection, currentHost);
    }
  }

  private void closeIdleConnections() {
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.closingInternalConnections"));
    this.closeReaderConnectionIfIdle();
    this.closeWriterConnectionIfIdle();
  }

  @Override
  public void releaseResources() {
    this.closeIdleConnections();
  }

  // ---- RwSplitContext implementation ----

  @Override
  public PluginService pluginService() {
    return this.pluginService;
  }

  @Override
  public @Nullable HostListProviderService hostListProviderService() {
    return this.hostListProviderService;
  }

  @Override
  public Properties properties() {
    return this.properties;
  }

  @Override
  public @Nullable Connection currentConnection() {
    return this.pluginService.getCurrentConnection();
  }

  @Override
  public @Nullable HostSpec currentHostSpec() {
    return this.pluginService.getCurrentHostSpec();
  }

  @Override
  public @Nullable Connection writerConnection() {
    return this.writerConnection;
  }

  @Override
  public @Nullable Connection readerConnection() {
    return this.readerCacheItem == null ? null : this.readerCacheItem.get();
  }

  @Override
  public @Nullable HostSpec writerHostSpec() {
    return this.writerHostSpec;
  }

  @Override
  public @Nullable HostSpec readerHostSpec() {
    return this.readerHostSpec;
  }

  @Override
  public void bindWriter(final Connection conn, final HostSpec host) {
    this.writerConnection = conn;
    this.writerHostSpec = host;
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.setWriterConnection",
        new Object[] {host.getHostAndPort()}));
  }

  @Override
  public void setWriterHostSpec(final HostSpec host) {
    this.writerHostSpec = host;
  }

  @Override
  public void bindReader(final Connection conn, final HostSpec host) {
    closeReaderConnectionIfIdle();
    this.readerCacheItem = new CacheItem<>(conn,
        this.helpers.cachePolicy.keepAliveDeadlineNanos(this.isReaderConnFromInternalPool));
    this.readerHostSpec = host;
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.setReaderConnection",
        new Object[] {host.getHostAndPort()}));
  }

  @Override
  public void switchCurrentConnectionTo(final Connection newConnection, final HostSpec newConnectionHost)
      throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection == newConnection) {
      return;
    }
    this.pluginService.setCurrentConnection(newConnection, newConnectionHost);
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.settingCurrentConnection",
        new Object[] {newConnectionHost.getHostAndPort()}));
  }

  @Override
  public void enterReadWriteSplit() {
    this.inReadWriteSplit = true;
  }

  @Override
  public boolean isInReadWriteSplit() {
    return this.inReadWriteSplit;
  }

  @Override
  public boolean isConnectionUsable(final @Nullable Connection connection) throws SQLException {
    return connection != null && !connection.isClosed();
  }

  @Override
  public void closeReaderConnectionIfIdle() {
    if (this.readerCacheItem == null) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final Connection readerConnection = this.readerCacheItem.get(true);
    if (readerConnection != null && readerConnection != currentConnection) {
      try {
        if (!readerConnection.isClosed()) {
          readerConnection.close();
        }
      } catch (final SQLException e) {
        // Do nothing.
      }
      this.readerCacheItem = null;
    }
  }

  @Override
  public void closeWriterConnectionIfIdle() {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (this.writerConnection != null && this.writerConnection != currentConnection) {
      try {
        if (!this.writerConnection.isClosed()) {
          this.writerConnection.close();
        }
      } catch (final SQLException e) {
        // Do nothing.
      }
      this.writerConnection = null;
    }
  }

  @Override
  public void markReaderFromPool(final boolean fromPool) {
    this.isReaderConnFromInternalPool = fromPool;
  }

  @Override
  public void markWriterFromPool(final boolean fromPool) {
    this.isWriterConnFromInternalPool = fromPool;
  }

  @Override
  public boolean isReaderFromPool() {
    return this.isReaderConnFromInternalPool;
  }

  @Override
  public boolean isWriterFromPool() {
    return this.isWriterConnFromInternalPool;
  }

  @Override
  public void logAndThrow(final String logMessage) throws SQLException {
    LOGGER.severe(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage);
  }

  @Override
  public void logAndThrow(final String logMessage, final SqlState sqlState) throws SQLException {
    LOGGER.severe(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage, sqlState.getState());
  }

  private void logAndThrowCause(final String logMessage, final Throwable cause) throws SQLException {
    LOGGER.fine(logMessage);
    if (cause instanceof ReadWriteSplittingSQLException) {
      throw (ReadWriteSplittingSQLException) cause;
    }
    throw new ReadWriteSplittingSQLException(
        logMessage, SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), cause);
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    final List<Pair<String, Object>> state = new ArrayList<>();
    PropertyUtils.addSnapshotState(state, "properties", this.properties);
    state.add(Pair.create("inReadWriteSplit", this.inReadWriteSplit));
    state.add(Pair.create("writerConnection", this.writerConnection));
    state.add(Pair.create("readerCacheItem",
        this.readerCacheItem != null ? this.readerCacheItem.toString() : null));
    state.add(Pair.create("writerHostSpec", this.writerHostSpec != null ? this.writerHostSpec.toString() : null));
    state.add(Pair.create("readerHostSpec", this.readerHostSpec != null ? this.readerHostSpec.toString() : null));
    state.add(Pair.create("isReaderConnFromInternalPool", this.isReaderConnFromInternalPool));
    state.add(Pair.create("isWriterConnFromInternalPool", this.isWriterConnFromInternalPool));
    for (final SnapshotContributor contributor : this.helpers.snapshotContributors()) {
      final List<Pair<String, Object>> fragment = contributor.snapshotState();
      if (fragment != null) {
        state.addAll(fragment);
      }
    }
    return state;
  }

  // ---- Testing accessors ----

  public @Nullable Connection getWriterConnection() {
    return this.writerConnection;
  }

  public @Nullable Connection getReaderConnection() {
    return this.readerCacheItem == null ? null : this.readerCacheItem.get();
  }
}
