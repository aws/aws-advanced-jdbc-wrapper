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

package com.amazon.awslabs.jdbc.plugin.failover;

import com.amazon.awslabs.jdbc.HostAvailability;
import com.amazon.awslabs.jdbc.HostListProvider;
import com.amazon.awslabs.jdbc.HostListProviderService;
import com.amazon.awslabs.jdbc.HostRole;
import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.JdbcCallable;
import com.amazon.awslabs.jdbc.NodeChangeOptions;
import com.amazon.awslabs.jdbc.OldConnectionSuggestedAction;
import com.amazon.awslabs.jdbc.PluginManagerService;
import com.amazon.awslabs.jdbc.PluginService;
import com.amazon.awslabs.jdbc.PropertyDefinition;
import com.amazon.awslabs.jdbc.ProxyDriverProperty;
import com.amazon.awslabs.jdbc.hostlistprovider.AuroraHostListProvider;
import com.amazon.awslabs.jdbc.plugin.AbstractConnectionPlugin;
import com.amazon.awslabs.jdbc.util.ConnectionUrlParser;
import com.amazon.awslabs.jdbc.util.Messages;
import com.amazon.awslabs.jdbc.util.RdsUrlType;
import com.amazon.awslabs.jdbc.util.RdsUtils;
import com.amazon.awslabs.jdbc.util.SqlState;
import com.amazon.awslabs.jdbc.util.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * This plugin provides cluster-aware failover features. The plugin switches connections upon
 * detecting communication related exceptions and/or cluster topology changes.
 */
public class FailoverConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(FailoverConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("*")));

  static final String METHOD_SET_READ_ONLY = "setReadOnly";
  static final String METHOD_SET_AUTO_COMMIT = "setAutoCommit";
  static final String METHOD_COMMIT = "commit";
  static final String METHOD_ROLLBACK = "rollback";
  private static final String METHOD_GET_AUTO_COMMIT = "getAutoCommit";
  private static final String METHOD_GET_CATALOG = "getCatalog";
  private static final String METHOD_GET_SCHEMA = "getSchema";
  private static final String METHOD_GET_DATABASE = "getDatabase";
  private static final String METHOD_GET_TRANSACTION_ISOLATION = "getTransactionIsolation";
  private static final String METHOD_GET_SESSION_MAX_ROWS = "getSessionMaxRows";
  static final String METHOD_ABORT = "abort";
  static final String METHOD_CLOSE = "close";
  static final String METHOD_IS_CLOSED = "isClosed";
  static final int NO_CONNECTION_INDEX = -1;
  static final int WRITER_CONNECTION_INDEX = 0;
  private final PluginService pluginService;
  protected final Properties properties;
  protected boolean enableFailoverSetting;
  protected int failoverTimeoutMsSetting;
  protected int failoverClusterTopologyRefreshRateMsSetting;
  protected int failoverWriterReconnectIntervalMsSetting;
  protected int failoverReaderConnectTimeoutMsSetting;
  protected boolean autoReconnectSetting;
  protected boolean explicitlyAutoCommit = true;
  Boolean explicitlyReadOnly = false;
  private boolean closedExplicitly = false;
  protected boolean isClosed = false;
  protected String closedReason = null;
  protected boolean isMultiWriterCluster = false;
  private final RdsUtils rdsHelper;
  private final ConnectionUrlParser connectionUrlParser;
  protected WriterFailoverHandler writerFailoverHandler = null;
  protected ReaderFailoverHandler readerFailoverHandler = null;
  private Throwable lastExceptionDealtWith = null;
  private PluginManagerService pluginManagerService;
  private boolean isInTransaction = false;

  public static final ProxyDriverProperty FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS =
      new ProxyDriverProperty(
          "failoverClusterTopologyRefreshRateMs",
          "2000",
          "Cluster topology refresh rate in millis during a writer failover process. "
              + "During the writer failover process, "
              + "cluster topology may be refreshed at a faster pace than normal to speed up "
              + "discovery of the newly promoted writer.");

  public static final ProxyDriverProperty FAILOVER_TIMEOUT_MS =
      new ProxyDriverProperty(
          "failoverTimeoutMs",
          "300000",
          "Cluster topology refresh rate in millis. "
              + "The cached topology for the cluster will be invalidated after the specified time, "
              + "after which it will be updated during the next interaction with the connection.");

  public static final ProxyDriverProperty FAILOVER_WRITER_RECONNECT_INTERVAL_MS =
      new ProxyDriverProperty(
          "failoverWriterReconnectIntervalMs",
          "2000",
          "Interval of time to wait between attempts to reconnect to a failed writer during a "
              + "writer failover process.");

  public static final ProxyDriverProperty FAILOVER_READER_CONNECT_TIMEOUT_MS =
      new ProxyDriverProperty(
          "failoverReaderConnectTimeoutMs",
          "30000",
          "Reader connection attempt timeout during a reader failover process.");

  public static final ProxyDriverProperty AUTO_RECONNECT =
      new ProxyDriverProperty(
          "wrapperAutoReconnect",
          "false",
          "Should the driver try to re-establish stale and/or dead connections? "
              + "If enabled the driver will throw an exception for a queries issued on a stale or dead connection, "
              + "which belong to the current transaction, but will attempt reconnect before the next query issued "
              + "on the connection in a new transaction. "
              + "The use of this feature is not recommended, because it has side effects related to session state "
              + "and data consistency when applications don''t handle SQLExceptions properly, "
              + "and is only designed to be used when you are unable to configure your application to handle "
              + "SQLExceptions resulting from dead and stale connections properly. ");

  public static final ProxyDriverProperty AUTO_RECONNECT_FOR_POOLS =
      new ProxyDriverProperty(
          "wrapperAutoReconnectForPools",
          "false",
          "Use a reconnection strategy appropriate for connection pools (defaults to ''false'')");

  public FailoverConnectionPlugin(PluginService pluginService, Properties properties) {
    this(pluginService, properties, new RdsUtils(), new ConnectionUrlParser());
  }

  FailoverConnectionPlugin(
      PluginService pluginService,
      Properties properties,
      RdsUtils rdsHelper,
      ConnectionUrlParser parser) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.rdsHelper = rdsHelper;
    this.connectionUrlParser = parser;

    if (pluginService instanceof PluginManagerService) {
      this.pluginManagerService = (PluginManagerService) pluginService;
    }

    initSettings();
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      Class<T> resultClass,
      Class<E> exceptionClass,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs)
      throws E {
    if (!this.enableFailoverSetting || canDirectExecute(methodName)) {
      return jdbcMethodFunc.call();
    }

    try {
      final RdsUrlType type = this.getHostListProvider().getRdsUrlType();
      if (type.isRdsCluster()) {
        this.explicitlyReadOnly = (type == RdsUrlType.RDS_READER_CLUSTER);
        LOGGER.finer(
            Messages.get(
                "Failover.parameterValue",
                new Object[] {"explicitlyReadOnly", this.explicitlyReadOnly}));
      }
    } catch (SQLException ex) {
      throw wrapExceptionIfNeeded(exceptionClass, ex);
    }

    if (this.isClosed && !allowedOnClosedConnection(methodName)) {
      try {
        invalidInvocationOnClosedConnection();
      } catch (SQLException ex) {
        throw wrapExceptionIfNeeded(exceptionClass, ex);
      }
    }

    T result = null;

    try {
      updateTopology(false);
      result = jdbcMethodFunc.call();
    } catch (IllegalStateException e) {
      dealWithIllegalStateException(e, exceptionClass);
    } catch (Exception e) {
      this.dealWithOriginalException(e, null, exceptionClass);
    }

    try {
      performSpecialMethodHandlingIfRequired(jdbcMethodArgs, methodName);
    } catch (SQLException e) {
      throw wrapExceptionIfNeeded(exceptionClass, e);
    }

    return result;
  }

  @Override
  public void initHostProvider(
      String driverProtocol,
      String initialUrl,
      Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {
    initHostProvider(
        driverProtocol,
        initialUrl,
        props,
        hostListProviderService,
        initHostProviderFunc,
        () -> new AuroraHostListProvider(driverProtocol, pluginService, props, initialUrl),
        (hostListProvider) ->
            new ClusterAwareReaderFailoverHandler(
                hostListProvider,
                this.pluginService,
                this.properties,
                this.failoverTimeoutMsSetting,
                this.failoverReaderConnectTimeoutMsSetting),
        (hostListProvider) ->
            new ClusterAwareWriterFailoverHandler(
                hostListProvider,
                this.pluginService,
                this.readerFailoverHandler,
                this.properties,
                this.failoverTimeoutMsSetting,
                this.failoverClusterTopologyRefreshRateMsSetting,
                this.failoverWriterReconnectIntervalMsSetting));
  }

  void initHostProvider(
      String driverProtocol,
      String initialUrl,
      Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc,
      Supplier<HostListProvider> hostListProviderSupplier,
      Function<AuroraHostListProvider, ClusterAwareReaderFailoverHandler> readerFailoverHandlerSupplier,
      Function<AuroraHostListProvider, ClusterAwareWriterFailoverHandler> writerFailoverHandlerSupplier)
      throws SQLException {
    if (!this.enableFailoverSetting) {
      return;
    }

    if (hostListProviderService.isStaticHostListProvider()) {
      hostListProviderService.setHostListProvider(hostListProviderSupplier.get());
    }

    final AuroraHostListProvider hostListProvider = this.getHostListProvider();
    this.readerFailoverHandler = readerFailoverHandlerSupplier.apply(hostListProvider);
    this.writerFailoverHandler = writerFailoverHandlerSupplier.apply(hostListProvider);

    initHostProviderFunc.call();
  }

  private AuroraHostListProvider getHostListProvider() throws SQLException {
    final HostListProvider provider = this.pluginService.getHostListProvider();
    if (!(provider instanceof AuroraHostListProvider)) {
      throw new SQLException(Messages.get("Failover.invalidHostListProvider", new Object[] {provider}));
    }

    return (AuroraHostListProvider) provider;
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    if (!this.enableFailoverSetting) {
      return;
    }

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    final String url = currentHost.getUrl();
    if (isNodeStillValid(url, changes)) {
      return;
    }

    for (String alias : currentHost.getAliases()) {
      if (isNodeStillValid(alias, changes)) {
        return;
      }
    }

    LOGGER.fine(Messages.get("Failover.invalidNode", new Object[] {currentHost}));
  }

  private boolean isNodeStillValid(final String node, Map<String, EnumSet<NodeChangeOptions>> changes) {
    if (changes.containsKey(node)) {
      EnumSet<NodeChangeOptions> options = changes.get(node);
      return !options.contains(NodeChangeOptions.NODE_DELETED) && !options.contains(NodeChangeOptions.WENT_DOWN);
    }
    return false;
  }

  public boolean isFailoverEnabled() {
    AuroraHostListProvider hostListProvider = null;
    try {
      hostListProvider = this.getHostListProvider();
    } catch (SQLException e) {
      // ignore
    }

    return this.enableFailoverSetting
        && hostListProvider != null
        && !RdsUrlType.RDS_PROXY.equals(hostListProvider.getRdsUrlType())
        && !Utils.isNullOrEmpty(this.pluginService.getHosts())
        && !this.isMultiWriterCluster;
  }

  private void initSettings() {
    this.enableFailoverSetting = PropertyDefinition.ENABLE_CLUSTER_AWARE_FAILOVER.getBoolean(this.properties);
    this.failoverTimeoutMsSetting = FAILOVER_TIMEOUT_MS.getInteger(this.properties);
    this.failoverClusterTopologyRefreshRateMsSetting =
        FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInteger(this.properties);
    this.failoverWriterReconnectIntervalMsSetting =
        FAILOVER_WRITER_RECONNECT_INTERVAL_MS.getInteger(this.properties);
    this.failoverReaderConnectTimeoutMsSetting =
        FAILOVER_READER_CONNECT_TIMEOUT_MS.getInteger(this.properties);

    this.autoReconnectSetting = AUTO_RECONNECT.getBoolean(this.properties)
        || AUTO_RECONNECT_FOR_POOLS.getBoolean(this.properties);
  }

  private void invalidInvocationOnClosedConnection() throws SQLException {
    if (this.autoReconnectSetting && !this.closedExplicitly) {
      this.isClosed = false;
      this.closedReason = null;
      pickNewConnection();

      // "The active SQL connection has changed. Please re-configure session state if required."
      LOGGER.severe(Messages.get("Failover.connectionChangedError"));
      throw new SQLException(
          Messages.get("Failover.connectionChangedError"),
          SqlState.COMMUNICATION_LINK_CHANGED.getState());
    } else {
      String reason = "No operations allowed after connection closed.";
      if (this.closedReason != null) {
        reason += (" " + this.closedReason);
      }

      throw new SQLException(reason, SqlState.CONNECTION_NOT_OPEN.getState());
    }
  }

  private boolean isExplicitlyReadOnly() {
    return this.explicitlyReadOnly != null && this.explicitlyReadOnly;
  }

  /**
   * Checks if there is an underlying connection for this proxy.
   *
   * @return true if there is a connection
   */
  boolean isConnected() {
    return this.pluginService.getCurrentHostSpec().getAvailability() == HostAvailability.AVAILABLE;
  }

  private HostSpec getCurrentWriter() throws SQLException {
    final List<HostSpec> topology = this.pluginService.getHosts();
    if (topology == null) {
      return null;
    }

    for (HostSpec hostSpec : topology) {
      if (hostSpec.getRole() == HostRole.WRITER) {
        return hostSpec;
      }
    }
    return null;
  }

  protected void updateTopology(boolean forceUpdate) throws SQLException {
    Connection connection = this.pluginService.getCurrentConnection();
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
  private boolean allowedOnClosedConnection(String methodName) {
    return methodName.contains(METHOD_GET_AUTO_COMMIT)
        || methodName.contains(METHOD_GET_CATALOG)
        || methodName.contains(METHOD_GET_SCHEMA)
        || methodName.contains(METHOD_GET_DATABASE)
        || methodName.contains(METHOD_GET_TRANSACTION_ISOLATION)
        || methodName.contains(METHOD_GET_SESSION_MAX_ROWS);
  }

  /**
   * Connects this dynamic failover connection proxy to the host pointed out by the given host
   * index.
   *
   * @param hostIndex The host index in the global hosts list.
   * @throws SQLException if an error occurs
   */
  private void connectTo(int hostIndex) throws SQLException {
    try {
      switchCurrentConnectionTo(hostIndex, createConnectionForHostIndex(hostIndex));
      LOGGER.fine(
          Messages.get(
              "Failover.establishedConnection",
              new Object[] {this.pluginService.getHosts().get(hostIndex)}));
    } catch (SQLException e) {
      final HostSpec host = this.pluginService.getCurrentHostSpec();
      if (this.pluginService.getCurrentConnection() != null) {
        StringBuilder msg =
            new StringBuilder("Connection to ")
                .append(isWriter(host) ? "writer" : "reader")
                .append(" host '")
                .append(host.getUrl())
                .append("' failed");
        LOGGER.warning(String.format("%s: %s", msg, e.getMessage()));
      }
      throw e;
    }
  }

  private void connectToWriterIfRequired(Boolean readOnly) throws SQLException {
    if (shouldReconnectToWriter(readOnly) && !Utils.isNullOrEmpty(this.pluginService.getHosts())) {
      try {
        connectTo(WRITER_CONNECTION_INDEX);
      } catch (SQLException e) {
        failover(getCurrentWriter());
      }
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

  private void performSpecialMethodHandlingIfRequired(Object[] args, String methodName)
      throws SQLException {
    if (methodName.contains(METHOD_SET_AUTO_COMMIT)) {
      this.explicitlyAutoCommit = (Boolean) args[0];
      if (this.pluginManagerService != null) {
        this.pluginManagerService.setInTransaction(!this.explicitlyAutoCommit);
      }
    }

    if (methodName.contains(METHOD_COMMIT) || methodName.contains(METHOD_ROLLBACK)) {
      if (this.pluginManagerService != null) {
        this.pluginManagerService.setInTransaction(false);
      }
    }

    if (methodName.contains(METHOD_SET_READ_ONLY)) {
      this.explicitlyReadOnly = (Boolean) args[0];
      LOGGER.finer(
          Messages.get(
              "Failover.parameterValue",
              new Object[] {"explicitlyReadOnly", this.explicitlyReadOnly}));
      connectToWriterIfRequired(this.explicitlyReadOnly);
    }
  }

  private void processFailoverFailure(String message) throws SQLException {
    LOGGER.severe(message);
    throw new SQLException(message, SqlState.CONNECTION_UNABLE_TO_CONNECT.getState());
  }

  private boolean shouldAttemptReaderConnection() {
    return isExplicitlyReadOnly()
        && this.pluginService.getHosts() != null
        && this.pluginService.getHosts().size() > 1;
  }

  boolean shouldPerformWriterFailover() {
    return this.explicitlyReadOnly == null || !this.explicitlyReadOnly;
  }

  private boolean shouldReconnectToWriter(Boolean readOnly) {
    return readOnly != null && !readOnly && !isWriter(this.pluginService.getCurrentHostSpec());
  }

  /**
   * Replaces the previous underlying connection by the connection given. State from previous
   * connection, if any, is synchronized with the new one.
   *
   * @param hostIndex The host index in the global hosts list that matches the given connection.
   * @param connection The connection instance to switch to.
   * @throws SQLException if an error occurs
   */
  private void switchCurrentConnectionTo(int hostIndex, Connection connection) throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection != connection) {
      invalidateCurrentConnection();
    }

    boolean readOnly;
    if (isWriter(this.pluginService.getCurrentHostSpec())) {
      readOnly = isExplicitlyReadOnly();
    } else if (this.explicitlyReadOnly != null) {
      readOnly = this.explicitlyReadOnly;
    } else if (currentConnection != null) {
      readOnly = currentConnection.isReadOnly();
    } else {
      readOnly = false;
    }
    syncSessionState(currentConnection, connection, readOnly);
    updateCurrentConnection(connection, hostIndex);
    if (this.pluginManagerService != null) {
      this.pluginManagerService.setInTransaction(false);
    }
  }

  /**
   * Synchronizes session state between two connections, allowing to override the read-only status.
   *
   * @param source The connection where to get state from.
   * @param target The connection where to set state.
   * @param readOnly The new read-only status.
   * @throws SQLException if an error occurs
   */
  protected void syncSessionState(
      Connection source,
      Connection target,
      boolean readOnly) throws SQLException {
    if (target != null) {
      target.setReadOnly(readOnly);
    }

    if (source == null || target == null) {
      return;
    }

    // TODO: verify if there are other states to sync

    target.setAutoCommit(source.getAutoCommit());
    target.setTransactionIsolation(source.getTransactionIsolation());
  }

  private void updateCurrentConnection(Connection connection, int hostIndex) throws SQLException {
    updateCurrentConnection(connection, this.pluginService.getHosts().get(hostIndex));
  }

  private void updateCurrentConnection(Connection connection, HostSpec hostSpec)
      throws SQLException {
    this.pluginService.setCurrentConnection(connection, hostSpec);
  }

  /**
   * Creates a new connection instance for host pointed out by the given host index.
   *
   * @param hostIndex The host index in the global hosts list.
   * @return The new connection instance.
   * @throws SQLException if an error occurs
   */
  private Connection createConnectionForHostIndex(int hostIndex) throws SQLException {
    return createConnectionForHost(this.pluginService.getHosts().get(hostIndex));
  }

  private <E extends Exception> void dealWithOriginalException(
      Throwable originalException,
      Throwable wrapperException,
      Class<E> exceptionClass) throws E {
    Throwable exceptionToThrow = wrapperException;
    if (originalException != null) {
      LOGGER.finer(
          String.format("%s:%s", Messages.get("Failover.failoverEnabled"), originalException.getMessage()));
      if (this.lastExceptionDealtWith != originalException
          && shouldExceptionTriggerConnectionSwitch(originalException)) {
        invalidateCurrentConnection();
        try {
          pickNewConnection();
        } catch (SQLException e) {
          throw wrapExceptionIfNeeded(exceptionClass, e);
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

    throw wrapExceptionIfNeeded(exceptionClass, exceptionToThrow);
  }

  /**
   * Creates a new physical connection for the given {@link HostSpec}.
   *
   * @param baseHostSpec The host info instance to base the connection off of.
   * @return The new Connection instance.
   * @throws SQLException if an error occurs
   */
  protected Connection createConnectionForHost(HostSpec baseHostSpec) throws SQLException {
    return this.pluginService.connect(baseHostSpec, properties);
  }

  protected <E extends Exception> void dealWithIllegalStateException(
      IllegalStateException e,
      Class<E> exceptionClass) throws E {
    dealWithOriginalException(e.getCause(), e, exceptionClass);
  }

  /**
   * Initiates the failover procedure. This process tries to establish a new connection to an
   * instance in the topology.
   *
   * @throws SQLException if an error occurs
   */
  protected synchronized void failover(final HostSpec failedHost) throws SQLException {
    if (shouldPerformWriterFailover()) {
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
      LOGGER.severe(errorMessage);
      throw new SQLException(errorMessage, SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState());
    } else {
      // "The active SQL connection has changed due to a connection failure. Please re-configure
      // session state if required."
      final String errorMessage = Messages.get("Failover.connectionChangedError");
      LOGGER.severe(errorMessage);
      throw new SQLException(errorMessage, SqlState.COMMUNICATION_LINK_CHANGED.getState());
    }
  }

  protected void failoverReader(final HostSpec failedHostSpec) throws SQLException {
    LOGGER.fine(Messages.get("Failover.startReaderFailover"));

    HostSpec failedHost = null;
    final Set<String> oldAliases = this.pluginService.getCurrentHostSpec().getAliases();
    if (failedHostSpec != null && failedHostSpec.getAvailability() == HostAvailability.AVAILABLE) {
      failedHost = failedHostSpec;
    }
    ReaderFailoverResult result = readerFailoverHandler.failover(this.pluginService.getHosts(), failedHost);

    if (result != null) {
      final SQLException exception = result.getException();
      if (exception != null) {
        throw exception;
      }
    }

    if (result == null || !result.isConnected()) {
      // "Unable to establish SQL connection to reader instance"
      processFailoverFailure(Messages.get("Failover.unableToConnectToReader"));
      return;
    }

    updateCurrentConnection(result.getConnection(), result.getConnectionIndex());
    this.pluginService.getCurrentHostSpec().removeAlias(oldAliases.toArray(new String[] {}));
    updateTopology(true);

    LOGGER.fine(
        Messages.get(
            "Failover.establishedConnection",
            new Object[] {this.pluginService.getCurrentHostSpec()}));
  }

  protected void failoverWriter() throws SQLException {
    LOGGER.fine(Messages.get("Failover.startWriterFailover"));
    final Set<String> oldAliases = this.pluginService.getCurrentHostSpec().getAliases();
    WriterFailoverResult failoverResult = this.writerFailoverHandler.failover(this.pluginService.getHosts());
    if (failoverResult != null) {
      final SQLException exception = failoverResult.getException();
      if (exception != null) {
        throw exception;
      }
    }
    if (failoverResult == null || !failoverResult.isConnected()) {
      // "Unable to establish SQL connection to writer node"
      processFailoverFailure(Messages.get("Failover.unableToConnectToWriter"));
      return;
    }

    // successfully re-connected to the same writer node
    updateCurrentConnection(
        failoverResult.getNewConnection(),
        failoverResult.getTopology().get(WRITER_CONNECTION_INDEX));
    this.pluginService.getCurrentHostSpec().removeAlias(oldAliases.toArray(new String[] {}));

    LOGGER.fine(
        Messages.get(
            "Failover.establishedConnection",
            new Object[] {this.pluginService.getCurrentHostSpec()}));

    this.pluginService.refreshHostList();
  }

  protected void invalidateCurrentConnection() {
    final Connection conn = this.pluginService.getCurrentConnection();
    if (conn == null) {
      return;
    }

    final HostSpec originalHost = this.pluginService.getCurrentHostSpec();
    if (this.pluginService.isInTransaction()) {
      isInTransaction = this.pluginService.isInTransaction();
      try {
        conn.rollback();
      } catch (SQLException e) {
        // swallow this exception
      }
    }

    try {
      if (!conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException e) {
      // swallow this exception, current connection should be useless anyway.
    }

    try {
      this.pluginService.setCurrentConnection(
          conn,
          new HostSpec(
              originalHost.getHost(),
              originalHost.getPort(),
              originalHost.getRole(),
              HostAvailability.NOT_AVAILABLE));
      this.pluginService.setAvailability(originalHost.getAliases(), HostAvailability.NOT_AVAILABLE);
    } catch (SQLException e) {
      LOGGER.fine("Failed to update current hostspec availability");
    }
  }

  protected synchronized void pickNewConnection() throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      LOGGER.fine(Messages.get("Failover.transactionResolutionUnknownError"));
      return;
    }

    if (isConnected() || Utils.isNullOrEmpty(this.pluginService.getHosts())) {
      failover(this.pluginService.getCurrentHostSpec());
      return;
    }

    if (shouldAttemptReaderConnection()) {
      failover(null);
      return;
    }

    try {
      connectTo(WRITER_CONNECTION_INDEX);
    } catch (SQLException e) {
      failover(getCurrentWriter());
    }
  }

  protected boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
    // TODO: support other drivers

    if (!isFailoverEnabled()) {
      LOGGER.fine(Messages.get("Failover.failoverDisabled"));
      return false;
    }

    String sqlState = null;
    if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    }

    if (sqlState == null) {
      return false;
    }

    return SqlState.isConnectionError(((SQLException) t).getSQLState());
  }

  /**
   * Check whether the method provided can be executed directly without the failover functionality.
   *
   * @param methodName The name of the method that is being called
   * @return true if the method can be executed directly; false otherwise.
   */
  private boolean canDirectExecute(String methodName) {
    return (methodName.contains(METHOD_CLOSE)
        || methodName.contains(METHOD_IS_CLOSED)
        || methodName.contains(METHOD_ABORT));
  }

  /**
   * Check if the throwable is an instance of the given exception and throw it as the required
   * exception class, otherwise throw it as a runtime exception.
   *
   * @param exceptionClass The exception class the exception is exepected to be
   * @param exception The exception that occurred while invoking the given method
   * @return an exception indicating the failure that occurred while invoking the given method
   */
  private <E extends Exception> E wrapExceptionIfNeeded(Class<E> exceptionClass, Throwable exception) {
    if (exceptionClass.isAssignableFrom(exception.getClass())) {
      return exceptionClass.cast(exception);
    }
    return exceptionClass.cast(new RuntimeException(exception));
  }
}
