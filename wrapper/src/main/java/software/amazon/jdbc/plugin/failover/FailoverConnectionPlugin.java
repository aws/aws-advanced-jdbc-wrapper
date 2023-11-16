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

package software.amazon.jdbc.plugin.failover;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.staledns.AuroraStaleDnsHelper;
import software.amazon.jdbc.states.RestoreSessionStateCallable;
import software.amazon.jdbc.states.SessionDirtyFlag;
import software.amazon.jdbc.states.SessionStateHelper;
import software.amazon.jdbc.states.SessionStateTransferCallable;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

/**
 * This plugin provides cluster-aware failover features. The plugin switches connections upon
 * detecting communication related exceptions and/or cluster topology changes.
 */
public class FailoverConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(FailoverConnectionPlugin.class.getName());
  private static final String TELEMETRY_WRITER_FAILOVER = "failover to writer node";
  private static final String TELEMETRY_READER_FAILOVER = "failover to replica";

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          addAll(SubscribedMethodHelper.NETWORK_BOUND_METHODS);
          add("initHostProvider");
          add("connect");
          add("forceConnect");
          add("notifyConnectionChanged");
          add("notifyNodeListChanged");
        }
      });

  private static final String METHOD_SET_READ_ONLY = "Connection.setReadOnly";
  private static final String METHOD_SET_AUTO_COMMIT = "Connection.setAutoCommit";
  private static final String METHOD_GET_AUTO_COMMIT = "Connection.getAutoCommit";
  private static final String METHOD_GET_CATALOG = "Connection.getCatalog";
  private static final String METHOD_GET_SCHEMA = "Connection.getSchema";
  private static final String METHOD_GET_TRANSACTION_ISOLATION = "Connection.getTransactionIsolation";
  static final String METHOD_ABORT = "Connection.abort";
  static final String METHOD_CLOSE = "Connection.close";
  static final String METHOD_IS_CLOSED = "Connection.isClosed";

  protected static SessionStateTransferCallable sessionStateTransferCallable;
  protected static RestoreSessionStateCallable restoreSessionStateCallable;

  private final PluginService pluginService;
  protected final Properties properties;
  protected boolean enableFailoverSetting;
  protected int failoverTimeoutMsSetting;
  protected int failoverClusterTopologyRefreshRateMsSetting;
  protected int failoverWriterReconnectIntervalMsSetting;
  protected int failoverReaderConnectTimeoutMsSetting;
  protected boolean keepSessionStateOnFailover;
  protected FailoverMode failoverMode;
  private boolean telemetryFailoverAdditionalTopTraceSetting;

  private boolean closedExplicitly = false;
  protected boolean isClosed = false;
  protected String closedReason = null;
  private final RdsUtils rdsHelper;
  protected WriterFailoverHandler writerFailoverHandler = null;
  protected ReaderFailoverHandler readerFailoverHandler = null;
  private Throwable lastExceptionDealtWith = null;
  private PluginManagerService pluginManagerService;
  private boolean isInTransaction = false;
  private RdsUrlType rdsUrlType;
  private HostListProviderService hostListProviderService;
  private final AuroraStaleDnsHelper staleDnsHelper;
  private Boolean savedReadOnlyStatus;
  private Boolean savedAutoCommitStatus;

  public static final AwsWrapperProperty FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS =
      new AwsWrapperProperty(
          "failoverClusterTopologyRefreshRateMs",
          "2000",
          "Cluster topology refresh rate in millis during a writer failover process. "
              + "During the writer failover process, "
              + "cluster topology may be refreshed at a faster pace than normal to speed up "
              + "discovery of the newly promoted writer.");

  public static final AwsWrapperProperty FAILOVER_TIMEOUT_MS =
      new AwsWrapperProperty(
          "failoverTimeoutMs",
          "300000",
          "Maximum allowed time for the failover process.");

  public static final AwsWrapperProperty FAILOVER_WRITER_RECONNECT_INTERVAL_MS =
      new AwsWrapperProperty(
          "failoverWriterReconnectIntervalMs",
          "2000",
          "Interval of time to wait between attempts to reconnect to a failed writer during a "
              + "writer failover process.");

  public static final AwsWrapperProperty FAILOVER_READER_CONNECT_TIMEOUT_MS =
      new AwsWrapperProperty(
          "failoverReaderConnectTimeoutMs",
          "30000",
          "Reader connection attempt timeout during a reader failover process.");

  public static final AwsWrapperProperty ENABLE_CLUSTER_AWARE_FAILOVER =
      new AwsWrapperProperty(
          "enableClusterAwareFailover", "true",
          "Enable/disable cluster-aware failover logic");

  public static final AwsWrapperProperty FAILOVER_MODE =
      new AwsWrapperProperty(
          "failoverMode", null,
          "Set node role to follow during failover.");

  public static final AwsWrapperProperty KEEP_SESSION_STATE_ON_FAILOVER =
      new AwsWrapperProperty(
          "keepSessionStateOnFailover", "false",
          "Allow connections to retain a partial previous session state after failover occurs.");

  public static final AwsWrapperProperty TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE =
      new AwsWrapperProperty(
          "telemetryFailoverAdditionalTopTrace", "false",
          "Post an additional top-level trace for failover process.");

  private final TelemetryCounter failoverWriterTriggeredCounter;
  private final TelemetryCounter failoverWriterSuccessCounter;
  private final TelemetryCounter failoverWriterFailedCounter;
  private final TelemetryCounter failoverReaderTriggeredCounter;
  private final TelemetryCounter failoverReaderSuccessCounter;
  private final TelemetryCounter failoverReaderFailedCounter;

  static {
    PropertyDefinition.registerPluginProperties(FailoverConnectionPlugin.class);
  }

  public FailoverConnectionPlugin(final PluginService pluginService, final Properties properties) {
    this(pluginService, properties, new RdsUtils());
  }

  FailoverConnectionPlugin(
      final PluginService pluginService,
      final Properties properties,
      final RdsUtils rdsHelper) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.rdsHelper = rdsHelper;

    if (pluginService instanceof PluginManagerService) {
      this.pluginManagerService = (PluginManagerService) pluginService;
    }

    initSettings();

    this.staleDnsHelper = new AuroraStaleDnsHelper(this.pluginService);

    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    this.failoverWriterTriggeredCounter = telemetryFactory.createCounter("writerFailover.triggered.count");
    this.failoverWriterSuccessCounter = telemetryFactory.createCounter("writerFailover.completed.success.count");
    this.failoverWriterFailedCounter = telemetryFactory.createCounter("writerFailover.completed.failed.count");
    this.failoverReaderTriggeredCounter = telemetryFactory.createCounter("readerFailover.triggered.count");
    this.failoverReaderSuccessCounter = telemetryFactory.createCounter("readerFailover.completed.success.count");
    this.failoverReaderFailedCounter = telemetryFactory.createCounter("readerFailover.completed.failed.count");
  }

  public static void setSessionStateTransferFunc(SessionStateTransferCallable callable) {
    sessionStateTransferCallable = callable;
  }

  public static void resetSessionStateTransferFunc() {
    sessionStateTransferCallable = null;
  }

  public static void setRestoreSessionStateFunc(RestoreSessionStateCallable callable) {
    restoreSessionStateCallable = callable;
  }

  public static void resetRestoreSessionStateFunc() {
    restoreSessionStateCallable = null;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {
    if (!this.enableFailoverSetting || canDirectExecute(methodName)) {
      return jdbcMethodFunc.call();
    }

    if (this.isClosed && !allowedOnClosedConnection(methodName)) {
      try {
        invalidInvocationOnClosedConnection();
      } catch (final SQLException ex) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
      }
    }

    if (methodName.equals(METHOD_SET_READ_ONLY) && jdbcMethodArgs != null && jdbcMethodArgs.length > 0) {
      this.savedReadOnlyStatus = (Boolean) jdbcMethodArgs[0];
    }

    if (methodName.equals(METHOD_SET_AUTO_COMMIT) && jdbcMethodArgs != null && jdbcMethodArgs.length > 0) {
      this.savedAutoCommitStatus = (Boolean) jdbcMethodArgs[0];
    }

    T result = null;

    try {
      if (canUpdateTopology(methodName)) {
        updateTopology(false);
      }
      result = jdbcMethodFunc.call();
    } catch (final IllegalStateException e) {
      dealWithIllegalStateException(e, exceptionClass);
    } catch (final Exception e) {
      this.dealWithOriginalException(e, null, exceptionClass);
    }

    return result;
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties properties,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {
    initHostProvider(
        initialUrl,
        hostListProviderService,
        initHostProviderFunc,
        () ->
            new ClusterAwareReaderFailoverHandler(
                this.pluginService,
                this.properties,
                this.failoverTimeoutMsSetting,
                this.failoverReaderConnectTimeoutMsSetting,
                this.failoverMode == FailoverMode.STRICT_READER),
        () ->
            new ClusterAwareWriterFailoverHandler(
                this.pluginService,
                this.readerFailoverHandler,
                this.properties,
                this.failoverTimeoutMsSetting,
                this.failoverClusterTopologyRefreshRateMsSetting,
                this.failoverWriterReconnectIntervalMsSetting));
  }

  void initHostProvider(
      final String initialUrl,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc,
      final Supplier<ClusterAwareReaderFailoverHandler> readerFailoverHandlerSupplier,
      final Supplier<ClusterAwareWriterFailoverHandler> writerFailoverHandlerSupplier)
      throws SQLException {
    this.hostListProviderService = hostListProviderService;
    if (!this.enableFailoverSetting) {
      return;
    }

    this.readerFailoverHandler = readerFailoverHandlerSupplier.get();
    this.writerFailoverHandler = writerFailoverHandlerSupplier.get();

    initHostProviderFunc.call();

    this.failoverMode = FailoverMode.fromValue(FAILOVER_MODE.getString(this.properties));
    this.rdsUrlType = this.rdsHelper.identifyRdsType(initialUrl);

    if (this.failoverMode == null) {
      if (this.rdsUrlType.isRdsCluster()) {
        this.failoverMode = (this.rdsUrlType == RdsUrlType.RDS_READER_CLUSTER)
            ? FailoverMode.READER_OR_WRITER
            : FailoverMode.STRICT_WRITER;
      } else {
        this.failoverMode = FailoverMode.STRICT_WRITER;
      }
    }

    LOGGER.finer(
        () -> Messages.get(
            "Failover.parameterValue",
            new Object[]{"failoverMode", this.failoverMode}));
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(final Map<String, EnumSet<NodeChangeOptions>> changes) {

    if (!this.enableFailoverSetting) {
      return;
    }

    if (LOGGER.isLoggable(Level.FINEST)) {
      final StringBuilder sb = new StringBuilder("Changes:");
      for (final Map.Entry<String, EnumSet<NodeChangeOptions>> change : changes.entrySet()) {
        if (sb.length() > 0) {
          sb.append("\n");
        }
        sb.append(String.format("\tHost '%s': %s", change.getKey(), change.getValue()));
      }
      LOGGER.finest(sb.toString());
    }

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    final String url = currentHost.getUrl();
    if (isNodeStillValid(url, changes)) {
      return;
    }

    for (final String alias : currentHost.getAliases()) {
      if (isNodeStillValid(alias + "/", changes)) {
        return;
      }
    }

    LOGGER.fine(() -> Messages.get("Failover.invalidNode", new Object[]{currentHost}));
  }

  private boolean isNodeStillValid(final String node, final Map<String, EnumSet<NodeChangeOptions>> changes) {
    if (changes.containsKey(node)) {
      final EnumSet<NodeChangeOptions> options = changes.get(node);
      return !options.contains(NodeChangeOptions.NODE_DELETED) && !options.contains(NodeChangeOptions.WENT_DOWN);
    }
    return true;
  }

  // For testing purposes
  void setRdsUrlType(final RdsUrlType rdsUrlType) {
    this.rdsUrlType = rdsUrlType;
  }

  public boolean isFailoverEnabled() {
    return this.enableFailoverSetting
        && !RdsUrlType.RDS_PROXY.equals(this.rdsUrlType)
        && !Utils.isNullOrEmpty(this.pluginService.getHosts());
  }

  private void initSettings() {
    this.enableFailoverSetting = ENABLE_CLUSTER_AWARE_FAILOVER.getBoolean(this.properties);
    this.failoverTimeoutMsSetting = FAILOVER_TIMEOUT_MS.getInteger(this.properties);
    this.failoverClusterTopologyRefreshRateMsSetting =
        FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInteger(this.properties);
    this.failoverWriterReconnectIntervalMsSetting = FAILOVER_WRITER_RECONNECT_INTERVAL_MS.getInteger(this.properties);
    this.failoverReaderConnectTimeoutMsSetting = FAILOVER_READER_CONNECT_TIMEOUT_MS.getInteger(this.properties);
    this.keepSessionStateOnFailover = KEEP_SESSION_STATE_ON_FAILOVER.getBoolean(this.properties);
    this.telemetryFailoverAdditionalTopTraceSetting =
        TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.getBoolean(this.properties);
  }

  private void invalidInvocationOnClosedConnection() throws SQLException {
    if (!this.closedExplicitly) {
      this.isClosed = false;
      this.closedReason = null;
      pickNewConnection();

      // "The active SQL connection has changed. Please re-configure session state if required."
      LOGGER.info(Messages.get("Failover.connectionChangedError"));
      throw new FailoverSuccessSQLException();
    } else {
      String reason = Messages.get("Failover.noOperationsAfterConnectionClosed");
      if (this.closedReason != null) {
        reason += (" " + this.closedReason);
      }

      throw new SQLException(reason, SqlState.CONNECTION_NOT_OPEN.getState());
    }
  }

  private HostSpec getCurrentWriter() throws SQLException {
    final List<HostSpec> topology = this.pluginService.getHosts();
    if (topology == null) {
      return null;
    }
    return getWriter(topology);
  }

  private HostSpec getWriter(final @NonNull List<HostSpec> hosts) {
    for (final HostSpec hostSpec : hosts) {
      if (hostSpec.getRole() == HostRole.WRITER) {
        return hostSpec;
      }
    }
    return null;
  }

  protected void updateTopology(final boolean forceUpdate) throws SQLException {
    final Connection connection = this.pluginService.getCurrentConnection();
    if (!isFailoverEnabled() || connection == null || connection.isClosed()) {
      return;
    }
    if (forceUpdate) {
      this.pluginService.forceRefreshHostList();
    } else {
      this.pluginService.refreshHostList();
    }
  }

  /**
   * Checks if the given method is allowed on closed connections.
   *
   * @return true if the given method is allowed on closed connections
   */
  private boolean allowedOnClosedConnection(final String methodName) {
    return methodName.equals(METHOD_GET_AUTO_COMMIT)
        || methodName.equals(METHOD_GET_CATALOG)
        || methodName.equals(METHOD_GET_SCHEMA)
        || methodName.equals(METHOD_GET_TRANSACTION_ISOLATION);
  }

  /**
   * Updating topology requires creating and executing a new statement.
   * This may cause interruptions during certain workflows. For instance,
   * the driver should not be updating topology while the connection is fetching a large streaming result set.
   *
   * @param methodName the method to check.
   * @return true if the driver should update topology before executing the method; false otherwise.
   */
  private boolean canUpdateTopology(final String methodName) {
    return SubscribedMethodHelper.METHODS_REQUIRING_UPDATED_TOPOLOGY.contains(methodName);
  }

  /**
   * Connects this dynamic failover connection proxy to the host pointed out by the given host
   * index.
   *
   * @param host The host.
   * @throws SQLException if an error occurs
   */
  private void connectTo(final HostSpec host) throws SQLException {
    try {
      switchCurrentConnectionTo(host, createConnectionForHost(host));
      LOGGER.fine(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[]{host}));
    } catch (final SQLException e) {
      if (this.pluginService.getCurrentConnection() != null) {
        final String msg = "Connection to "
            + (isWriter(host) ? "writer" : "reader")
            + " host '"
            + host.getUrl()
            + "' failed";
        LOGGER.warning(() -> String.format("%s: %s", msg, e.getMessage()));
      }
      throw e;
    }
  }

  /**
   * Checks if the given host index points to the primary host.
   *
   * @param hostSpec The current host.
   * @return true if so
   */
  private boolean isWriter(final HostSpec hostSpec) {
    return hostSpec.getRole() == HostRole.WRITER;
  }

  private void processFailoverFailure(final String message) throws SQLException {
    LOGGER.severe(message);
    throw new FailoverFailedSQLException(message);
  }

  private boolean shouldAttemptReaderConnection() {
    final List<HostSpec> topology = this.pluginService.getHosts();
    if (topology == null || this.failoverMode == FailoverMode.STRICT_WRITER) {
      return false;
    }

    for (final HostSpec hostSpec : topology) {
      if (hostSpec.getRole() == HostRole.READER) {
        return true;
      }
    }
    return false;
  }

  /**
   * Replaces the previous underlying connection by the connection given. State from previous
   * connection, if any, is synchronized with the new one.
   *
   * @param host       The host that matches the given connection.
   * @param connection The connection instance to switch to.
   * @throws SQLException if an error occurs
   */
  private void switchCurrentConnectionTo(final HostSpec host, final Connection connection) throws SQLException {
    Connection currentConnection = this.pluginService.getCurrentConnection();
    HostSpec currentHostSpec = this.pluginService.getCurrentHostSpec();

    if (currentConnection != connection) {
      transferSessionState(currentConnection, currentHostSpec, connection, host);
      invalidateCurrentConnection();
    }

    this.pluginService.setCurrentConnection(connection, host);

    if (this.pluginManagerService != null) {
      this.pluginManagerService.setInTransaction(false);
    }
  }

  /**
   * Transfers session state from one connection to another.
   *
   * @param src The connection to transfer state from
   * @param srcHostSpec The connection {@link HostSpec} to transfer state from
   * @param dest   The connection to transfer state to
   * @param destHostSpec The connection {@link HostSpec} to transfer state to
   * @throws SQLException if a database access error occurs, this method is called on a closed connection, this
   *                      method is called during a distributed transaction, or this method is called during a
   *                      transaction
   */
  protected void transferSessionState(
      final Connection src,
      final HostSpec srcHostSpec,
      final Connection dest,
      final HostSpec destHostSpec) throws SQLException {

    if (src == null || dest == null) {
      return;
    }

    EnumSet<SessionDirtyFlag> sessionState = this.pluginService.getCurrentConnectionState();

    SessionStateTransferCallable callableCopy = sessionStateTransferCallable;
    if (callableCopy != null) {
      final boolean isHandled = callableCopy.transferSessionState(sessionState, src, srcHostSpec, dest, destHostSpec);
      if (isHandled) {
        // Custom function has handled session transfer
        return;
      }
    }

    // Otherwise, lets run default logic.
    sessionState = this.pluginService.getCurrentConnectionState();
    final SessionStateHelper helper = new SessionStateHelper();
    helper.transferSessionState(sessionState, src, dest);
  }

  /**
   * Restores partial session state from saved values to a connection.
   *
   * @param dest   The connection to transfer state to
   * @throws SQLException if a database access error occurs, this method is called on a closed connection, this
   *                      method is called during a distributed transaction, or this method is called during a
   *                      transaction
   */
  protected void restoreSessionState(final Connection dest) throws SQLException {
    if (dest == null) {
      return;
    }

    final RestoreSessionStateCallable callableCopy = restoreSessionStateCallable;
    if (callableCopy != null) {
      final boolean isHandled = callableCopy.restoreSessionState(
          this.pluginService.getCurrentConnectionState(),
          dest,
          this.savedReadOnlyStatus,
          this.savedAutoCommitStatus
      );
      if (isHandled) {
        // Custom function has handled everything.
        return;
      }
    }

    // Otherwise, lets run default logic.
    final SessionStateHelper helper = new SessionStateHelper();
    helper.restoreSessionState(dest, this.savedReadOnlyStatus, this.savedAutoCommitStatus);
  }

  private <E extends Exception> void dealWithOriginalException(
      final Throwable originalException,
      final Throwable wrapperException,
      final Class<E> exceptionClass) throws E {
    Throwable exceptionToThrow = wrapperException;
    if (originalException != null) {
      LOGGER.finer(() -> Messages.get("Failover.detectedException", new Object[]{originalException.getMessage()}));
      if (this.lastExceptionDealtWith != originalException
          && shouldExceptionTriggerConnectionSwitch(originalException)) {
        invalidateCurrentConnection();
        this.pluginService.setAvailability(
            this.pluginService.getCurrentHostSpec().getAliases(), HostAvailability.NOT_AVAILABLE);
        try {
          pickNewConnection();
        } catch (final SQLException e) {
          throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
        }
        this.lastExceptionDealtWith = originalException;
      }

      if (originalException instanceof Error) {
        throw (Error) originalException;
      }
      exceptionToThrow = originalException;
    }

    if (exceptionToThrow instanceof Error) {
      throw (Error) exceptionToThrow;
    }

    throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, exceptionToThrow);
  }

  /**
   * Creates a new physical connection for the given {@link HostSpec}.
   *
   * @param baseHostSpec The host info instance to base the connection off of.
   * @return The new Connection instance.
   * @throws SQLException if an error occurs
   */
  protected Connection createConnectionForHost(final HostSpec baseHostSpec) throws SQLException {
    return this.pluginService.connect(baseHostSpec, properties);
  }

  protected <E extends Exception> void dealWithIllegalStateException(
      final IllegalStateException e,
      final Class<E> exceptionClass) throws E {
    dealWithOriginalException(e.getCause(), e, exceptionClass);
  }

  /**
   * Initiates the failover procedure. This process tries to establish a new connection to an
   * instance in the topology.
   *
   * @param failedHost The host with network errors.
   * @throws SQLException if an error occurs
   */
  protected synchronized void failover(final HostSpec failedHost) throws SQLException {
    if (this.failoverMode == FailoverMode.STRICT_WRITER) {
      failoverWriter();
    } else {
      failoverReader(failedHost);
    }

    if (isInTransaction || this.pluginService.isInTransaction()) {
      if (this.pluginManagerService != null) {
        this.pluginManagerService.setInTransaction(false);
      }
      // "Transaction resolution unknown. Please re-configure session state if required and try
      // restarting transaction."
      final String errorMessage = Messages.get("Failover.transactionResolutionUnknownError");
      LOGGER.info(errorMessage);
      throw new TransactionStateUnknownSQLException();
    } else {
      // "The active SQL connection has changed due to a connection failure. Please re-configure
      // session state if required. "
      LOGGER.severe(() -> Messages.get("Failover.connectionChangedError"));
      throw new FailoverSuccessSQLException();
    }
  }

  protected void failoverReader(final HostSpec failedHostSpec) throws SQLException {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_READER_FAILOVER, TelemetryTraceLevel.NESTED);
    this.failoverReaderTriggeredCounter.inc();

    try {
      LOGGER.fine(() -> Messages.get("Failover.startReaderFailover"));

      HostSpec failedHost = null;
      final Set<String> oldAliases = this.pluginService.getCurrentHostSpec().getAliases();
      if (failedHostSpec != null && failedHostSpec.getRawAvailability() == HostAvailability.AVAILABLE) {
        failedHost = failedHostSpec;
      }
      final ReaderFailoverResult result = readerFailoverHandler.failover(this.pluginService.getHosts(), failedHost);

      if (result != null) {
        final SQLException exception = result.getException();
        if (exception != null) {
          throw exception;
        }
      }

      if (result == null || !result.isConnected()) {
        // "Unable to establish SQL connection to reader instance"
        processFailoverFailure(Messages.get("Failover.unableToConnectToReader"));
        this.failoverReaderFailedCounter.inc();
        return;
      }

      if (keepSessionStateOnFailover) {
        restoreSessionState(result.getConnection());
      }
      this.pluginService.setCurrentConnection(result.getConnection(), result.getHost());

      this.pluginService.getCurrentHostSpec().removeAlias(oldAliases.toArray(new String[]{}));
      updateTopology(true);

      LOGGER.fine(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[]{this.pluginService.getCurrentHostSpec()}));

      this.failoverReaderSuccessCounter.inc();

    } catch (FailoverSuccessSQLException ex) {
      telemetryContext.setSuccess(true);
      telemetryContext.setException(ex);
      this.failoverReaderSuccessCounter.inc();
      throw ex;
    } catch (Exception ex) {
      telemetryContext.setSuccess(false);
      telemetryContext.setException(ex);
      this.failoverReaderFailedCounter.inc();
      throw ex;
    } finally {
      telemetryContext.closeContext();
      if (this.telemetryFailoverAdditionalTopTraceSetting) {
        telemetryFactory.postCopy(telemetryContext, TelemetryTraceLevel.FORCE_TOP_LEVEL);
      }
    }
  }

  protected void failoverWriter() throws SQLException {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_WRITER_FAILOVER, TelemetryTraceLevel.NESTED);
    this.failoverWriterTriggeredCounter.inc();

    try {
      LOGGER.fine(() -> Messages.get("Failover.startWriterFailover"));
      final WriterFailoverResult failoverResult = this.writerFailoverHandler.failover(this.pluginService.getHosts());
      if (failoverResult != null) {
        final SQLException exception = failoverResult.getException();
        if (exception != null) {
          throw exception;
        }
      }
      if (failoverResult == null || !failoverResult.isConnected()) {
        // "Unable to establish SQL connection to writer node"
        processFailoverFailure(Messages.get("Failover.unableToConnectToWriter"));
        this.failoverWriterFailedCounter.inc();
        return;
      }

      // successfully re-connected to a writer node
      final HostSpec writerHostSpec = getWriter(failoverResult.getTopology());
      if (keepSessionStateOnFailover) {
        restoreSessionState(failoverResult.getNewConnection());
      }
      this.pluginService.setCurrentConnection(failoverResult.getNewConnection(), writerHostSpec);

      LOGGER.fine(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[]{this.pluginService.getCurrentHostSpec()}));

      this.pluginService.refreshHostList();

      this.failoverWriterSuccessCounter.inc();
    } catch (FailoverSuccessSQLException ex) {
      telemetryContext.setSuccess(true);
      telemetryContext.setException(ex);
      this.failoverWriterSuccessCounter.inc();
      throw ex;
    } catch (Exception ex) {
      telemetryContext.setSuccess(false);
      telemetryContext.setException(ex);
      this.failoverWriterFailedCounter.inc();
      throw ex;
    } finally {
      telemetryContext.closeContext();
      if (this.telemetryFailoverAdditionalTopTraceSetting) {
        telemetryFactory.postCopy(telemetryContext, TelemetryTraceLevel.FORCE_TOP_LEVEL);
      }
    }
  }

  protected void invalidateCurrentConnection() {
    final Connection conn = this.pluginService.getCurrentConnection();
    if (conn == null) {
      return;
    }

    if (this.pluginService.isInTransaction()) {
      isInTransaction = this.pluginService.isInTransaction();
      try {
        conn.rollback();
      } catch (final SQLException e) {
        // swallow this exception
      }
    }

    try {
      if (!conn.isClosed()) {
        conn.close();
      }
    } catch (final SQLException e) {
      // swallow this exception, current connection should be useless anyway.
    }
  }

  protected synchronized void pickNewConnection() throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      LOGGER.fine(() -> Messages.get("Failover.transactionResolutionUnknownError"));
      return;
    }

    if (this.pluginService.getCurrentConnection() == null && !shouldAttemptReaderConnection()) {
      try {
        connectTo(getCurrentWriter());
      } catch (final SQLException e) {
        failover(getCurrentWriter());
      }
    } else {
      failover(this.pluginService.getCurrentHostSpec());
    }
  }

  protected boolean shouldExceptionTriggerConnectionSwitch(final Throwable t) {

    if (!isFailoverEnabled()) {
      LOGGER.fine(() -> Messages.get("Failover.failoverDisabled"));
      return false;
    }

    String sqlState = null;
    if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    }

    if (sqlState == null) {
      return false;
    }

    return this.pluginService.isNetworkException(t);
  }

  /**
   * Check whether the method provided can be executed directly without the failover functionality.
   *
   * @param methodName The name of the method that is being called
   * @return true if the method can be executed directly; false otherwise.
   */
  private boolean canDirectExecute(final String methodName) {
    return (methodName.equals(METHOD_CLOSE)
        || methodName.equals(METHOD_IS_CLOSED)
        || methodName.equals(METHOD_ABORT));
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, isInitialConnection, connectFunc);
  }

  private Connection connectInternal(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    final Connection conn =
        this.staleDnsHelper.getVerifiedConnection(isInitialConnection, this.hostListProviderService,
            driverProtocol, hostSpec, props, connectFunc);

    if (this.keepSessionStateOnFailover) {
      this.savedReadOnlyStatus = this.savedReadOnlyStatus == null ? conn.isReadOnly() : this.savedReadOnlyStatus;
      this.savedAutoCommitStatus =
          this.savedAutoCommitStatus == null ? conn.getAutoCommit() : this.savedAutoCommitStatus;
    }

    if (isInitialConnection) {
      this.pluginService.refreshHostList(conn);
    }

    return conn;
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, isInitialConnection, forceConnectFunc);
  }
}
