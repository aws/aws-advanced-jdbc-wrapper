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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.LowestLoadHostSelector;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.Rebindable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
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
 * Unified read/write splitting plugin. Owns the shared connection state and routing orchestration,
 * delegating every variable decision to injected helpers ({@link RwSplitHelpers}). The concrete
 * plugin variants ({@code readWriteSplitting}, {@code autoReadWriteSplitting}, {@code srw}, …) are
 * assembled by their respective factory classes.
 *
 * <h2>Integration guide</h2>
 *
 * <ol>
 *   <li><b>Plugin chain ordering</b> — place this plugin (or any subclass) <em>after</em>
 *       {@code sqlParser} in the plugin chain when using SQL-based routing
 *       ({@code autoReadWriteSplitting}). The {@code sqlParser} plugin populates the
 *       {@link PluginCallContext} with the parsed {@link software.amazon.jdbc.parser.QueryType} and
 *       optional {@link software.amazon.jdbc.parser.RoutingHint} before this plugin reads them.
 *       Incorrect ordering causes all statements to fall back to writer routing.</li>
 *   <li><b>Lag-based routing needs lag reporting</b> — {@code replicaLagThresholdMs} requires the
 *       cluster to publish reader replication lag. Aurora (MySQL and PostgreSQL) reports lag
 *       through the {@link software.amazon.jdbc.hostlistprovider.RdsHostListProvider} topology
 *       monitor, which keeps an in-memory cache the driver reads without blocking. Any other
 *       database or cluster whose published host specs carry non-null {@link HostSpec#getLagMs()}
 *       (e.g. a custom host-list provider) is supported as well. If no lag data is available, the
 *       feature silently no-ops; a one-time WARNING is logged when lag data is unavailable and the
 *       threshold is configured.</li>
 *   <li><b>Connection pool sizing</b> — the plugin caches up to one writer connection and one
 *       reader connection per wrapper connection. When used behind an external connection pool,
 *       the physical connection count may be up to 2× the logical pool size during split
 *       read/write workloads. Size the pool accordingly.</li>
 *   <li><b>Transaction boundaries</b> — open transactions (local and XA) are never moved. Once
 *       a transaction starts on a reader, all statements in that transaction stay on the same
 *       reader regardless of lag or load-balancing policy. The {@code assumeWriteTransaction}
 *       property can route non-read-only transactions to the writer from the start.</li>
 *   <li><b>Statement re-creation</b> — when {@code allowStatementRecreationOnConnectionSwitch}
 *       is {@code true} (default) and a connection switch happens mid-statement, the plugin
 *       re-creates the {@code Statement}/{@code PreparedStatement} on the new connection. This
 *       requires the statement wrapper to implement {@link software.amazon.jdbc.Rebindable}.
 *       Statements with pending batch operations or one-shot stream parameters cannot be rebound
 *       and stay on their original connection instead.</li>
 * </ol>
 *
 * <h2>⚠ Writer-storm risk — read carefully before enabling {@code replicaLagThresholdMs}</h2>
 *
 * <p>When replica lag rises above {@code replicaLagThresholdMs}, <strong>every read that would
 * normally go to a reader is redirected to the writer</strong> for as long as the lag persists.
 * In a cluster where most traffic is reads, this can instantly concentrate the full read load on
 * the writer, potentially overwhelming it:
 *
 * <ul>
 *   <li><b>Connection storm</b> — each wrapper connection that was routing reads to a reader now
 *       opens or reuses a writer connection. If the lag spike affects many connections
 *       simultaneously, the writer may receive a large burst of new connections.</li>
 *   <li><b>CPU / I/O overload</b> — read queries that were horizontally scaled across multiple
 *       readers now all land on the single writer, which also handles all write traffic.</li>
 *   <li><b>Cascading failure</b> — a writer overloaded by redirected reads may become slow,
 *       causing replication lag to increase further, keeping the fallback active indefinitely.</li>
 * </ul>
 *
 * <p><b>Mitigation recommendations:</b>
 * <ul>
 *   <li>Start with a generous threshold (e.g. several seconds) to limit fallback frequency.
 *       Do not set the threshold to {@code 0} in production unless reads on a lagging replica are
 *       truly intolerable — any positive lag, even 1 ms, will then redirect all reads.</li>
 *   <li>Enable circuit-breaker or rate-limiting at the load balancer or application level so that
 *       redirected read traffic does not overwhelm the writer during a lag spike.</li>
 *   <li>Monitor Aurora's {@code AuroraReplicaLag} CloudWatch metric alongside the wrapper's
 *       WARNING log entries ({@code replicaLagRoutedToWriter}) to detect sustained fallback
 *       and investigate the root cause of the lag rather than relying on the fallback alone.</li>
 *   <li>In multi-reader clusters, consider whether a lagging replica can simply be removed from
 *       the read pool (via host availability / health check) instead of routing all reads to the
 *       writer. With non-lag-minimizing selectors the pessimistic {@code Math.max} lag strategy
 *       means <em>one</em> lagging replica causes all reads to fall back to the writer, even if
 *       other readers are healthy. (Lag-minimizing selectors such as {@code lowestLoad} and
 *       {@code lowestLoadByLag} aggregate via {@code Math.min} instead.)</li>
 *   <li>Do <em>not</em> use this feature as a substitute for Aurora Auto Scaling or proper
 *       capacity planning. It is a last-resort data-freshness guard, not a performance feature.</li>
 * </ul>
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

  /**
   * Maximum acceptable reader replication lag in milliseconds.
   *
   * <p>A non-negative value enables the replica-lag fallback: when the lag of the reader that
   * would serve an upcoming read exceeds this threshold, the read is redirected to the writer
   * instead. A value of {@code -1} (the default) disables the feature entirely.
   *
   * <p><b>⚠ Writer-storm risk:</b> when lag rises above the threshold, <em>all</em> reads that
   * would normally go to readers are redirected to the writer for the duration of the lag spike.
   * In a read-heavy cluster this can instantly overload the writer. See the class-level Javadoc
   * for a full discussion of the risk and mitigation recommendations before enabling this setting.
   *
   * <p>Setting the threshold to {@code 0} redirects reads on <em>any</em> positive lag (even 1 ms)
   * and is not recommended for production unless stale reads are truly intolerable. Prefer a
   * generous threshold (several seconds) to limit fallback frequency and blast radius.
   *
   * <p>This property needs the cluster to publish reader replication lag. On Aurora clusters the
   * {@link software.amazon.jdbc.hostlistprovider.RdsHostListProvider} background monitor provides
   * it from an in-memory cache. Any other database that reports lag on its published host specs
   * (non-null {@link HostSpec#getLagMs()}) is also supported. Databases that do not report lag
   * leave routing unchanged.
   *
   * <p>The fallback never overrides explicit application intent: it is skipped entirely (no lag
   * measurement, no topology refresh) when the current statement carries an explicit routing hint
   * (e.g. {@code /*@reader* /}), when the session is declared read-only
   * ({@code Connection.setReadOnly(true)}), or when a transaction is in progress.
   */
  public static final AwsWrapperProperty REPLICA_LAG_THRESHOLD_MS =
      new AwsWrapperProperty(
          "replicaLagThresholdMs",
          "-1",
          "Maximum acceptable reader replication lag in milliseconds. A non-negative value "
              + "routes a read to the writer when the reader that would serve it exceeds the "
              + "threshold; -1 disables this fallback. WARNING: a lag spike will redirect ALL "
              + "reads to the writer simultaneously, which can overload it. See the class javadoc "
              + "for writer-storm risks and mitigation guidance before enabling this setting.");

  public static final AwsWrapperProperty LOAD_BALANCING_INCLUDE_WRITER =
      new AwsWrapperProperty(
          "loadBalancingIncludeWriter",
          "false",
          "When query-level load balancing is enabled, include the writer node as an eligible "
              + "target in the reader-balancing pool.");

  public static final AwsWrapperProperty ASSUME_WRITE_TRANSACTION =
      new AwsWrapperProperty(
          "assumeWriteTransaction",
          "false",
          "Treat a transaction that was not explicitly declared read-only (via "
              + "Connection.setReadOnly(true)) as a read-write transaction, routing it to the "
              + "writer even when its first statement is a SELECT. Transaction managers announce "
              + "read-only transactions but say nothing about read-write ones, so without this "
              + "setting a read-write transaction whose first statement is a read starts on a "
              + "reader and a later write in the same transaction cannot be served there. Only the "
              + "SQL-routing (auto*) read/write splitting plugins act on this setting; the "
              + "setReadOnly-driven plugins ignore it. Disabled by default.",
          false,
          new String[] {"true", "false"});

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
  private final int replicaLagThresholdMs;

  /**
   * Whether the configured reader host selector is guaranteed to minimise replication lag, i.e. it
   * always picks the reader with the lowest lag from the eligible set. Currently
   * {@link LowestLoadHostSelector#STRATEGY_LOWEST_LOAD_BY_LAG lowestLoadByLag} and
   * {@link LowestLoadHostSelector#STRATEGY_LOWEST_LOAD lowestLoad} satisfy this guarantee (both
   * weight lag heavily in their load score); every other strategy — {@code random},
   * {@code roundRobin}, {@code lowestLoadByCpu}, or any custom selector — is treated as
   * non-guaranteeing and aggregates pessimistically via {@code Math.max}. Computed once at
   * construction: the strategy is fixed for the lifetime of the connection wrapper, so re-parsing
   * the property on every read-routing decision would be avoidable per-read work.
   */
  private final boolean lagMinimizingSelector;

  // Tracks plain Statement objects seen for execute-with-SQL, to warn once per statement when a
  // bound statement is reused and rerouting cannot be applied.
  private final Map<Object, Boolean> seenBoundStatements =
      Collections.synchronizedMap(new WeakHashMap<>());

  // One-shot warning flags for lag-related log entries that would otherwise fire on every read.
  //
  // replicaLagUnavailableWarningLogged: set the first time lag data is unavailable while the
  //   threshold is configured. Not reset after the first occurrence — this is a known limitation
  //   (Bug B1): if lag later becomes available, the flag stays true and subsequent unavailability
  //   events produce no further log output. Until the flag is reset on availability recovery,
  //   operators must rely on CloudWatch or the topology monitor's own metrics to detect sustained
  //   lag-data loss. A future fix should reset this flag when relevantReaderLagMs() returns a
  //   finite value, and ideally apply a log cooldown rather than a permanent one-shot suppression.
  //
  // replicaLagFallbackNotAppliedWarningLogged: set when the lag fallback selected WRITER but the
  //   physical connection remained on a reader (Global Write Forwarding STAY scenario). Also a
  //   permanent one-shot — once GWF keeps the connection on the reader the first time, this flag
  //   prevents the warning from firing again for the lifetime of this plugin instance.
  private boolean replicaLagUnavailableWarningLogged;
  private boolean replicaLagFallbackNotAppliedWarningLogged;

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
    this.replicaLagThresholdMs = REPLICA_LAG_THRESHOLD_MS.getInteger(properties);
    this.lagMinimizingSelector = isLagMinimizingSelector(properties);
  }

  /**
   * Builds the reader load-balancing policy for an assembly: {@link PerQueryBalancedReaderPolicy}
   * when {@code queryLevelLoadBalancing} is enabled (honoring {@code loadBalancingIncludeWriter}),
   * otherwise a {@link StickyReaderPolicy}.
   *
   * @param props the connection properties the policy is configured from
   * @param strategy the host selector strategy used to pick a reader
   * @return the reader load-balancing policy for the assembly
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
      final @Nullable Object[] args)
      throws E {
    // A call that operates on a connection other than the current one is not routed: it belongs to a
    // previous session. Only a Connection is compared. A Statement or ResultSet is not asked which
    // connection it is bound to, because that answer comes from the target driver, which reports the
    // physical connection even when the wrapper holds a pooled or logical handle for the same session
    // (a pooled XA connection does this), so a perfectly valid statement looked stale and skipped
    // routing. A genuinely stale statement cannot reach this point: every execute method subscribed
    // below is declared with JdbcMethod.checkBoundedConnection, so WrapperUtils rejects it first.
    if (methodInvokeOn instanceof Connection
        && methodInvokeOn != this.pluginService.getCurrentConnection()) {
      LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.executingAgainstOldConnection",
          new Object[] {methodInvokeOn}));
      return jdbcMethodFunc.call();
    }

    if (JdbcMethod.CONNECTION_CLEARWARNINGS.methodName.equals(methodName)) {
      try {
        final Connection writerConn = this.writerConnection;
        if (writerConn != null && !writerConn.isClosed()) {
          writerConn.clearWarnings();
        }
        final CacheItem<Connection> readerItem = this.readerCacheItem;
        final Connection readerConn = readerItem == null ? null : readerItem.get();
        if (readerConn != null && this.isConnectionUsable(readerConn)) {
          readerConn.clearWarnings();
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
    this.throwIfCurrentConnectionClosed();

    this.helpers.topologyRefresher.refresh(this);

    final TargetRole targetRole = this.applyReplicaLagFallback(desired);
    this.performSwitchWithCurrentTopology(methodName, targetRole, targetRole != desired);
    this.warnIfReplicaLagFallbackDidNotApply(desired, targetRole);
  }

  private void throwIfCurrentConnectionClosed() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection != null && currentConnection.isClosed()) {
      this.logAndThrow(Messages.get("ReadWriteSplittingPlugin.setReadOnlyOnClosedConnection"),
          SqlState.CONNECTION_NOT_OPEN);
    }
  }

  /** Switches against topology already refreshed by the caller. */
  private void performSwitchWithCurrentTopology(
      final String methodName, final TargetRole desired, final boolean replicaLagFallback)
      throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
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
        if (!replicaLagFallback
            && JdbcMethod.CONNECTION_SETREADONLY.methodName.equals(methodName)
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
   *
   * <p><b>Topology-refresh invariant (exactly one refresh per routing decision):</b>
   * {@link software.amazon.jdbc.plugin.readwritesplitting.signal.SqlRoutingSignal} deliberately
   * returns {@link TargetRole#NO_DECISION} for plain {@code Statement} execute calls in
   * {@link software.amazon.jdbc.plugin.readwritesplitting.signal.RoutingSignal#resolve}, so
   * {@link #performSwitch} is <em>never</em> called from {@link #execute} for this path. That
   * means no topology refresh has occurred yet when this method runs.
   *
   * <p>To guarantee exactly one refresh for the whole routing decision, this method owns the
   * refresh when the lag fallback might change the destination ({@code refreshedForLagDecision}).
   * It then calls {@link #performSwitchWithCurrentTopology} (no extra refresh) instead of
   * {@link #performSwitch} (which would refresh again). When the lag fallback cannot apply,
   * {@code refreshedForLagDecision} is {@code false} and the {@code else} branch delegates to
   * {@link #performSwitch}, which does the single refresh itself — so the total is still one.
   *
   * <p>The two branches are mutually exclusive by construction:
   * <ul>
   *   <li>{@code refreshedForLagDecision = true} → this method refreshed → calls
   *       {@code performSwitchWithCurrentTopology} (no second refresh).</li>
   *   <li>{@code refreshedForLagDecision = false} → this method did not refresh → calls
   *       {@code performSwitch} (does the one refresh).</li>
   * </ul>
   * There is therefore no code path through this method that calls the topology refresher twice.
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

    final PluginCallContext callContext = this.pluginService.getCallContext();
    final Rebindable rebindHandle = callContext == null ? null : callContext.getRebindHandle();
    final boolean reroutable = this.allowStatementRecreationOnConnectionSwitch && rebindHandle != null;

    // SqlRoutingSignal abstains for plain Statement execute calls, so performSwitch has NOT been
    // called yet and no topology refresh has occurred for this routing decision.
    //
    // When the lag fallback could change the destination (READER → possibly WRITER), refresh now
    // so the lag evaluation and the subsequent host-list lookup use the same topology snapshot.
    // The flag also selects performSwitchWithCurrentTopology below, which skips a second refresh.
    // When the lag fallback cannot apply, skip the refresh here and let performSwitch do it once.
    // Either way, exactly one topology refresh happens per routing decision. See javadoc above.
    final boolean refreshedForLagDecision = reroutable && this.replicaLagFallbackCouldApply(sqlRole);
    if (refreshedForLagDecision) {
      this.throwIfCurrentConnectionClosed();
      this.helpers.topologyRefresher.refresh(this);
    }

    final TargetRole targetRole = this.applyReplicaLagFallback(sqlRole);

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    final boolean alreadyOnTarget =
        (targetRole == TargetRole.READER && this.helpers.roleClassifier.isReader(currentHost))
            || (targetRole == TargetRole.WRITER && this.helpers.roleClassifier.isWriter(currentHost));
    if (alreadyOnTarget) {
      return;
    }

    // A transaction/autocommit/keep pin means the statement legitimately stays put; not a miss.
    if (!this.helpers.switchGate.canSwitch(this, targetRole)) {
      return;
    }

    if (!reroutable) {
      // Rerouting is wanted but cannot be applied to this bound statement.
      warnOnceReusedBoundStatement(methodInvokeOn);
      return;
    }

    // refreshedForLagDecision=true  → topology already fresh above → no second refresh
    // refreshedForLagDecision=false → no refresh done yet       → performSwitch does the one refresh
    if (refreshedForLagDecision) {
      this.performSwitchWithCurrentTopology(methodName, targetRole, targetRole != sqlRole);
    } else {
      this.performSwitch(methodName, targetRole);
    }
    this.warnIfReplicaLagFallbackDidNotApply(sqlRole, targetRole);

    final HostSpec newHost = this.pluginService.getCurrentHostSpec();
    final boolean switched =
        (targetRole == TargetRole.READER && this.helpers.roleClassifier.isReader(newHost))
            || (targetRole == TargetRole.WRITER && this.helpers.roleClassifier.isWriter(newHost));
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
   * Converts a read-routing decision to a writer decision when the relevant replica lag is
   * above the configured limit. Lag is optional telemetry: an unavailable cache leaves routing
   * unchanged rather than failing the application operation.
   *
   * <p><b>Writer-storm note:</b> this method is called on <em>every</em> read-routing decision
   * while {@code replicaLagThresholdMs} is enabled. When the lag is above the threshold, it
   * returns {@link TargetRole#WRITER} unconditionally — no rate-limiting, no gradual ramp,
   * no hysteresis. All wrapper connections that are routing reads will simultaneously redirect to
   * the writer for as long as lag stays above the threshold. Callers should be aware that this
   * creates a potential connection storm and CPU spike on the writer during lag events.
   *
   * <p>The fallback is a no-op when {@link #replicaLagFallbackCouldApply} returns {@code false}
   * (explicit routing hint, read-only session, or open transaction): in those cases {@code desired}
   * is returned unchanged without measuring lag.
   */
  private TargetRole applyReplicaLagFallback(final TargetRole desired) throws SQLException {
    if (!this.replicaLagFallbackCouldApply(desired)) {
      return desired;
    }

    final double lagMs = this.relevantReaderLagMs();
    if (Double.isNaN(lagMs)) {
      if (!this.replicaLagUnavailableWarningLogged) {
        this.replicaLagUnavailableWarningLogged = true;
        LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.replicaLagUnavailable",
            new Object[] {this.replicaLagThresholdMs}));
      }
      return desired;
    }

    // Strict greater-than: lag equal to the threshold is still considered acceptable so that
    // threshold = 0 does not trigger on a perfectly synchronised replica (lag = 0.0 ms).
    if (lagMs > this.replicaLagThresholdMs) {
      LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.replicaLagRoutedToWriter",
          new Object[] {lagMs, this.replicaLagThresholdMs}));
      return TargetRole.WRITER;
    }
    return desired;
  }

  /**
   * Returns {@code true} when the replica-lag fallback is enabled, the routing decision is for a
   * reader (the only role the fallback can redirect away from), and the application has not pinned
   * the read to a specific destination.
   *
   * <p>The threshold check uses {@code >= 0} so that {@code replicaLagThresholdMs = 0} is a valid
   * "reject any positive lag" configuration. Only {@code -1} (or any negative value) disables the
   * feature entirely.
   *
   * <p>The fallback — and with it the lag measurement, the topology refresh that a reroute would
   * trigger, and the "routed to writer" warning — is skipped entirely when application intent is
   * explicit:
   * <ul>
   *   <li><b>an explicit {@link RoutingHint}</b> is attached to the current statement (any hint;
   *       only {@code READER} reaches this method) — the hint is the application overriding
   *       automatic routing for this statement, so the fallback must not override it back;</li>
   *   <li><b>a transaction (local or XA) is in progress</b> — {@link TransactionAwareGate}
   *       pins the connection regardless, so measuring lag here would only add wasted work and a
   *       misleading "routed to writer" warning;</li>
   *   <li><b>the session is read-only</b> (declared via {@code Connection.setReadOnly(true)},
   *       which also covers read-only transactions) — the application asked for reads and nothing
   *       but reads, and redirecting to the writer would violate that.</li>
   * </ul>
   */
  private boolean replicaLagFallbackCouldApply(final TargetRole desired) {
    if (this.replicaLagThresholdMs < 0 || desired != TargetRole.READER) {
      return false;
    }

    final PluginCallContext callContext = this.pluginService.getCallContext();
    if (callContext != null
        && callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class) != null) {
      return false;
    }

    if (this.pluginService.isInTransaction() || this.pluginService.isXaTransactionActive()) {
      return false;
    }

    try {
      final Optional<Boolean> readOnly = this.pluginService.getSessionStateService().getReadOnly();
      return !(readOnly.isPresent() && readOnly.get());
    } catch (final SQLException e) {
      // The read-only state could not be read. Refuse to apply the fallback rather than throw into
      // the routing path: routing intent is unknown, so lag must not redirect the read anywhere.
      return false;
    }
  }

  /**
   * Returns the replication lag (in milliseconds) that is relevant for the upcoming read-routing
   * decision, or {@link Double#NaN} when lag data is unavailable.
   *
   * <h3>Pinned-reader path (sticky or cached reader)</h3>
   * When a specific reader is already known to serve this read (the current host is a reader, or a
   * cached reader connection will be reused), only that reader's lag is measured. This is the
   * exact lag the application will actually experience.
   *
   * <h3>Fresh-selection path — lag aggregation strategy</h3>
   * When a fresh reader will be selected, the lag of all <em>eligible</em> candidates is
   * aggregated. The aggregation rule depends on which host selector is configured:
   *
   * <ul>
   *   <li><b>{@code lowestLoadByLag} and {@code lowestLoad}</b> — both selectors are lag-aware and
   *       are guaranteed to pick the reader with the lowest load/lag, so the <b>minimum</b> (best)
   *       lag among eligible readers is the true representative. A read is only redirected to the
   *       writer when <em>every</em> eligible reader is lagging beyond the threshold, i.e. even the
   *       best available reader is too stale.</li>
   *   <li><b>All other selectors</b> (including {@code random}, {@code roundRobin},
   *       {@code lowestLoadByCpu}, or any custom selector) — the selector is not guaranteed to
   *       avoid the worst reader, so the <b>maximum</b> (worst) lag is used. This includes Aurora
   *       topology path, RDS Proxy opaque-endpoint path, and non-Aurora deployments that do
   *       publish lag in their host specs. One lagging eligible reader therefore causes all reads
   *       to fall back to the writer.</li>
   * </ul>
   *
   * <h3>Lag data sources</h3>
   * <ul>
   *   <li><b>Aurora</b> — lag values come from {@link RdsHostListProvider#getStoredTopology()},
   *       an in-memory cache populated by the Aurora topology background monitor. No database
   *       query is issued.</li>
   *   <li><b>RDS Proxy / opaque endpoint</b> — the endpoint host is opaque; Aurora topology is
   *       used as the source of backend reader lags and {@code Math.max} is applied.</li>
   *   <li><b>Non-Aurora with lag-reporting hosts</b> — when the provider is not an
   *       {@link RdsHostListProvider} but the host specs returned by
   *       {@link PluginService#getHosts()} already carry non-null {@link HostSpec#getLagMs()}
   *       values (e.g. a custom host-list provider), lag is read directly from those specs.
   *       {@code Math.min} is applied when {@code lowestLoadByLag} or {@code lowestLoad} is
   *       configured; {@code Math.max} otherwise.</li>
   *   <li><b>Non-Aurora without lag data</b> — all {@link HostSpec#getLagMs()} values are
   *       {@code null}; this method returns {@link Double#NaN} and routing is left unchanged.</li>
   * </ul>
   */
  private double relevantReaderLagMs() throws SQLException {
    // ---- lag data source ----
    // Primary: Aurora topology cache (contains freshest Aurora lag measurements).
    // Fallback: lag carried directly on the published host specs (non-Aurora providers that do
    //           populate HostSpec.lagMs, or future non-RDS topology providers).
    final List<HostSpec> latestTopology = this.cachedTopologyWithLatestLag();
    final boolean usingTopologyCache = (latestTopology != null);

    // ---- pinned-reader fast path ----
    // When the same reader will definitely serve this read (sticky or cached), measure only it.
    // A small linear scan over the (typically tiny) host list replaces a per-decision HashMap;
    // the map is only built below when a fresh reader actually needs to be selected.
    final HostSpec pinnedReader = this.readerThatWillServeRead();
    if (pinnedReader != null) {
      final List<HostSpec> source =
          usingTopologyCache ? latestTopology : this.pluginService.getHosts();
      return source == null ? Double.NaN : lagOfReader(source, pinnedReader);
    }

    // ---- fresh-selection path: aggregate across eligible candidates ----
    // Build a URL→lag map from whichever source is available. This allocation is only incurred
    // when a fresh reader is actually going to be selected (the pinned path already returned).
    final Map<String, Float> lagByUrl = new HashMap<>();
    if (usingTopologyCache) {
      // Aurora: topology snapshot has the freshest lag reported by the background monitor.
      for (final HostSpec host : latestTopology) {
        if (host.getRole() == HostRole.READER) {
          lagByUrl.put(host.getUrl(), host.getLagMs());
        }
      }
    } else {
      // Non-Aurora (or cache unavailable): read lag directly from published host specs.
      // If no host carries lag data the map stays empty → NaN below → routing unchanged.
      final List<HostSpec> publishedHosts = this.pluginService.getHosts();
      if (publishedHosts != null) {
        for (final HostSpec host : publishedHosts) {
          if (host.getRole() == HostRole.READER && host.getLagMs() != null) {
            lagByUrl.put(host.getUrl(), host.getLagMs());
          }
        }
      }
      // If no host reported any lag, return NaN: lag data genuinely unavailable.
      if (lagByUrl.isEmpty()) {
        return Double.NaN;
      }
    }

    final List<HostSpec> hosts;
    if (this.helpers.readerResolver.routesThroughOpaqueEndpoint()) {
      // RDS Proxy / cluster endpoint: the endpoint host is opaque, so the Aurora topology is the
      // only source of backend reader lags. Always pessimistic (Math.max) regardless of selector,
      // because the load-balancer inside the proxy picks the backend, not our selector.
      hosts = usingTopologyCache ? latestTopology : this.pluginService.getHosts();
    } else {
      try {
        hosts = this.helpers.readerResolver.getReaderCandidatesForLag(this);
      } catch (final SQLException e) {
        // Candidate resolution can fail, e.g. when no GDB home-region reader is left.
        // Lag is optional data; let ordinary reader switching report that condition.
        LOGGER.finest(() -> Messages.get(
            "ReadWriteSplittingPlugin.replicaLagCandidatesUnavailable",
            new Object[] {e.getMessage()}));
        return Double.NaN;
      }
    }

    double lagMs = Double.NaN;
    boolean currentHostFound = false;
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (hosts != null) {
      for (final HostSpec host : hosts) {
        if (host.getRole() != HostRole.READER) {
          continue;
        }

        final boolean isCurrentHost = sameHost(host, currentHost);
        currentHostFound |= isCurrentHost;
        // The active reader can still run the pending operation even when the latest candidate
        // view marks it unavailable (e.g. a transient health-check blip). Including it prevents
        // a false "lag unknown" result that would leave a lagging-but-open connection in place.
        // Other unavailable readers are not selectable, so their lag does not matter here.
        if (host.getAvailability() != HostAvailability.AVAILABLE && !isCurrentHost) {
          continue;
        }

        lagMs = this.lagMinimizingSelector
            ? pickMinLag(lagMs, lagByUrl.get(host.getUrl()))
            : pickLag(lagMs, lagByUrl.get(host.getUrl()));
      }
    }

    // The candidate list may not include the currently-open reader if the topology changed
    // between when the connection was established and now (e.g. a replica was removed then
    // re-added with a new HostSpec object). Including the current reader's lag here prevents
    // a false "lag unknown" that would leave an actively-lagging connection in use.
    if (!currentHostFound && currentHost != null && currentHost.getRole() == HostRole.READER) {
      lagMs = this.lagMinimizingSelector
          ? pickMinLag(lagMs, lagByUrl.get(currentHost.getUrl()))
          : pickLag(lagMs, lagByUrl.get(currentHost.getUrl()));
    }

    return lagMs;
  }

  private static boolean isLagMinimizingSelector(final Properties props) {
    final String strategy = ReadWriteSplittingPlugin.readerHostSelectorStrategy(props);
    return LowestLoadHostSelector.STRATEGY_LOWEST_LOAD_BY_LAG.equalsIgnoreCase(strategy)
        || LowestLoadHostSelector.STRATEGY_LOWEST_LOAD.equalsIgnoreCase(strategy);
  }

  /**
   * Returns the latest Aurora cluster topology from the in-memory cache, or {@code null} when the
   * cache is empty or unavailable. <b>Never issues a database query.</b>
   *
   * <p>Lag evaluation is deliberately pull-based (cache-read only). Issuing a blocking topology
   * query here would add latency to every JDBC call that routes a read, and would create a second
   * I/O path that bypasses the topology monitor's rate-limiting and back-off logic. The topology
   * monitor refreshes the cache on its own schedule; this method simply reads the latest snapshot.
   *
   * <p>Returns {@code null} — and thus disables lag-based routing — when:
   * <ul>
   *   <li>the {@link HostListProvider} does not publish an Aurora topology cache — e.g. a
   *       non-Aurora or custom provider; lag is then read directly from the published host specs
   *       instead ({@link HostSpec#getLagMs()}),</li>
   *   <li>the cache has not been populated yet (first connection, monitor not yet started),</li>
   *   <li>the cache read throws {@link java.sql.SQLException} (monitor initialisation failure).</li>
   * </ul>
   * In all these cases the read is routed normally without lag consideration.
   */
  private @Nullable List<HostSpec> cachedTopologyWithLatestLag() {
    final HostListProviderService providerService = this.hostListProviderService;
    if (providerService == null) {
      return null;
    }

    final HostListProvider hostListProvider = providerService.getHostListProvider();
    if (!(hostListProvider instanceof RdsHostListProvider)) {
      return null;
    }

    try {
      return ((RdsHostListProvider) hostListProvider).getStoredTopology();
    } catch (final SQLException e) {
      LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.replicaLagRefreshFailed",
          new Object[] {e.getMessage()}));
      return null;
    }
  }

  /**
   * Returns the reader known to serve this read, or {@code null} when a fresh backend reader is
   * selected. Endpoint-based assemblies cannot pin a backend because their host is opaque.
   */
  private @Nullable HostSpec readerThatWillServeRead() throws SQLException {
    if (this.queryLevelLoadBalancing || this.helpers.readerResolver.routesThroughOpaqueEndpoint()) {
      return null;
    }

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (currentHost != null && currentHost.getRole() == HostRole.READER) {
      return currentHost;
    }

    final CacheItem<Connection> cachedReaderItem = this.readerCacheItem;
    final Connection cachedReader = cachedReaderItem == null ? null : cachedReaderItem.get();
    final HostSpec cachedReaderHost = this.readerHostSpec;
    if (cachedReader != null && cachedReaderHost != null && this.isConnectionUsable(cachedReader)) {
      return cachedReaderHost;
    }

    return null;
  }

  /**
   * Logs a one-time warning when the replica-lag fallback selected the writer but the physical
   * connection is still on a reader after the switch attempt. This happens with Global Write
   * Forwarding: {@link software.amazon.jdbc.plugin.readwritesplitting.resolver.GdbWriterResolver}
   * returns {@link software.amazon.jdbc.plugin.readwritesplitting.resolver.WriterResolution#stay()}
   * so no new connection is opened and the current reader remains active.
   *
   * <p>The check is purely positional — "fallback said writer, but we are still on a reader" —
   * so it must not call {@code switchGate.canSwitch()} here. The gate was already consulted
   * during the switch attempt; re-querying it post-attempt is both redundant and unsafe because
   * {@code canSwitch} is declared to throw {@code SQLException}, which would abort the caller's
   * JDBC call rather than simply skipping the log entry.
   */
  private void warnIfReplicaLagFallbackDidNotApply(
      final TargetRole requestedRole, final TargetRole targetRole) {
    if (requestedRole != TargetRole.READER
        || targetRole != TargetRole.WRITER
        || this.replicaLagFallbackNotAppliedWarningLogged
        || !this.helpers.roleClassifier.isReader(this.pluginService.getCurrentHostSpec())) {
      return;
    }

    this.replicaLagFallbackNotAppliedWarningLogged = true;
    LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.replicaLagFallbackNotApplied",
        new Object[] {this.replicaLagThresholdMs}));
  }

  private static double toLagMs(final @Nullable Float lagMs) {
    return lagMs == null || Float.isNaN(lagMs) ? Double.NaN : lagMs;
  }

  /**
   * Returns the lag (in ms) reported by an already-identified reader, or {@link Double#NaN} when
   * it carries no lag value. Used by the pinned-reader fast path, where no candidate aggregation
   * (and no per-decision map) is needed; the host list is small enough for a linear scan.
   */
  private static double lagOfReader(final List<HostSpec> hosts, final HostSpec target) {
    for (final HostSpec host : hosts) {
      if (host.getRole() == HostRole.READER && host.getUrl().equals(target.getUrl())) {
        return toLagMs(host.getLagMs());
      }
    }
    return Double.NaN;
  }

  /**
   * Returns the worse (higher) of two lag values for pessimistic candidate evaluation.
   *
   * <p>Using {@code Math.max} rather than {@code Math.min} is a deliberate design choice: the goal
   * is to detect whether <em>any</em> eligible reader that might serve this read is lagging beyond
   * the threshold, not whether the best one would be acceptable. If the selector happens to avoid
   * the lagging reader this time, that is not guaranteed on the next call — so the conservative
   * approach is to treat the worst eligible lag as the representative value.
   *
   * <p>Consequence: one lagging replica in the eligible set causes fallback for all reads, even if
   * other replicas are healthy. See {@link #relevantReaderLagMs()} for discussion.
   */
  private static double pickLag(final double current, final @Nullable Float candidate) {
    if (candidate == null || Float.isNaN(candidate)) {
      return current;
    }
    return Double.isNaN(current) ? candidate : Math.max(current, candidate);
  }

  /**
   * Returns the better (lower) of two lag values for lag-minimizing selectors
   * ({@code lowestLoadByLag}, {@code lowestLoad}).
   *
   * <p>Using {@code Math.min} rather than {@code Math.max} is correct for these selectors because
   * they are guaranteed to pick the reader with the lowest lag. The best eligible lag is therefore
   * the true representative: a read only falls back to the writer when <em>every</em> eligible
   * reader is lagging beyond the threshold.
   */
  private static double pickMinLag(final double current, final @Nullable Float candidate) {
    if (candidate == null || Float.isNaN(candidate)) {
      return current;
    }
    return Double.isNaN(current) ? candidate : Math.min(current, candidate);
  }

  private static boolean sameHost(final HostSpec first, final @Nullable HostSpec second) {
    return second != null
        && first.getPort() == second.getPort()
        && first.getHost().equalsIgnoreCase(second.getHost());
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
    final Connection cachedWriter = this.writerConnection;
    // The cached writer host is always recorded together with the cached writer connection (see
    // bindWriter), so a usable cached writer connection implies a non-null cached writer host.
    final HostSpec cachedWriterHost = this.writerHostSpec;
    if (cachedWriter == null || cachedWriterHost == null || !this.isConnectionUsable(cachedWriter)) {
      final WriterResolution wr = this.helpers.writerResolver.resolveWriter(this);
      final Connection resolvedWriter = wr.getConnection();
      final HostSpec resolvedWriterHost = wr.getHostSpec();
      if (wr.isConnected() && resolvedWriter != null && resolvedWriterHost != null) {
        this.markWriterFromPool(Boolean.TRUE.equals(this.pluginService.isPooledConnection()));
        this.bindWriter(resolvedWriter, resolvedWriterHost);
        this.switchCurrentConnectionTo(resolvedWriter, resolvedWriterHost);
      }
      // WriterResolution.STAY (e.g. Global Write Forwarding): remain on the current connection.
    } else {
      this.switchCurrentConnectionTo(cachedWriter, cachedWriterHost);
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
    final CacheItem<Connection> cachedReaderItem = this.readerCacheItem;
    final Connection cachedReader = cachedReaderItem == null ? null : cachedReaderItem.get();
    // The cached reader host is always recorded together with the cached reader connection (see
    // bindReader), so a usable cached reader connection implies a non-null cached reader host.
    final HostSpec cachedReaderHost = this.readerHostSpec;
    if (cachedReader == null || cachedReaderHost == null || !this.isConnectionUsable(cachedReader)) {
      this.helpers.readerResolver.switchToReader(this);
    } else {
      try {
        this.switchCurrentConnectionTo(cachedReader, cachedReaderHost);
        LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
            new Object[] {cachedReaderHost.getHostAndPort()}));
      } catch (final SQLException e) {
        if (e.getMessage() != null) {
          LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReaderWithCause",
              new Object[] {cachedReaderHost.getHostAndPort(), e.getMessage()}));
        } else {
          LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReader",
              new Object[] {cachedReaderHost.getHostAndPort()}));
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
    final CacheItem<Connection> readerItem = this.readerCacheItem;
    if (readerItem == null) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final Connection readerConnection = readerItem.get(true);
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
    final Connection writerConn = this.writerConnection;
    if (writerConn != null && writerConn != currentConnection) {
      try {
        if (!writerConn.isClosed()) {
          writerConn.close();
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

  // Checker Framework: snapshot values are intentionally nullable, but the
  // StateSnapshotProvider contract types them as Pair<String, Object> (non-null Object).
  // Fixing this properly means widening that interface to Pair<String, @Nullable Object>
  // across all ~25 implementers - out of scope for this change. Suppress locally.
  @Override
  @SuppressWarnings("type.arguments.not.inferred")
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
