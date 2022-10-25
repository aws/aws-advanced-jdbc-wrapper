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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.util.ConnectionMethodAnalyzer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.WrapperUtils;

public class ReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {
  private final Map<String, Connection> liveConnections = new HashMap<>();

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("initHostProvider");
          add("connect");
          add("notifyConnectionChanged");
          add("Connection.commit");
          add("Connection.rollback");
          add("Connection.createStatement");
          add("Connection.setReadOnly");
          add("Statement.execute");
          add("Statement.executeQuery");
          add("Statement.executeWithFlags");
          add("PreparedStatement.execute");
          add("PreparedStatement.executeQuery");
          add("PreparedStatement.executeWithFlags");
          add("CallableStatement.execute");
          add("CallableStatement.executeQuery");
          add("CallableStatement.executeWithFlags");
          // executeUpdate and executeBatch do not need to be added since they cannot be executed in read-only mode
        }
      });
  static final String METHOD_SET_READ_ONLY = "setReadOnly";
  static final String PG_DRIVER_PROTOCOL = "jdbc:postgresql:";
  static final String PG_GET_INSTANCE_NAME_SQL = "SELECT aurora_db_instance_identifier()";
  static final String PG_INSTANCE_NAME_COL = "aurora_db_instance_identifier";
  static final String MYSQL_DRIVER_PROTOCOL = "jdbc:mysql:";
  static final String MYSQL_GET_INSTANCE_NAME_SQL = "SELECT @@aurora_server_id";
  static final String MYSQL_INSTANCE_NAME_COL = "@@aurora_server_id";

  private final ConnectionMethodAnalyzer connectionMethodAnalyzer = new ConnectionMethodAnalyzer();
  private final PluginService pluginService;
  private final Properties properties;
  private final RdsUtils rdsUtils = new RdsUtils();
  private final AtomicBoolean inReadWriteSplit = new AtomicBoolean(false);
  private final boolean loadBalanceReadOnlyTraffic;
  private HostListProviderService hostListProviderService;
  private Connection writerConnection;
  private Connection readerConnection;
  private HostSpec readerHostSpec;
  private boolean isTransactionBoundary = false;
  private boolean explicitlyReadOnly = false;

  public static final AwsWrapperProperty LOAD_BALANCE_READ_ONLY_TRAFFIC =
      new AwsWrapperProperty(
          "loadBalanceReadOnlyTraffic",
          "false",
          "Set to true to automatically load-balance read-only transactions when setReadOnly is set to true");

  ReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.properties = properties;

    this.loadBalanceReadOnlyTraffic = LOAD_BALANCE_READ_ONLY_TRAFFIC.getBoolean(this.properties);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
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
    final Connection currentConnection = connectFunc.call();
    if (this.pluginService.getCurrentConnection() != null) {
      return currentConnection;
    }

    final RdsUrlType urlType = rdsUtils.identifyRdsType(hostSpec.getHost());
    if (RdsUrlType.RDS_WRITER_CLUSTER.equals(urlType) || RdsUrlType.RDS_READER_CLUSTER.equals(urlType)) {
      return currentConnection;
    }

    // The current HostSpec role might not be accurate. The rest of the logic in this method updates it if necessary.
    this.pluginService.refreshHostList(currentConnection);
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    final HostSpec updatedCurrentHost;
    if (RdsUrlType.RDS_INSTANCE.equals(urlType)) {
      updatedCurrentHost = getHostSpecFromUrl(currentHost.getUrl());
    } else {
      updatedCurrentHost = getHostSpecFromInstanceId(getCurrentInstanceId(currentConnection, driverProtocol));
    }

    if (updatedCurrentHost == null) {
      logAndThrowException("ReadWriteSplittingPlugin.errorUpdatingHostSpecRole");
      return null;
    }

    final HostSpec updatedRoleHostSpec =
        new HostSpec(currentHost.getHost(), currentHost.getPort(), updatedCurrentHost.getRole(),
            currentHost.getAvailability());

    this.hostListProviderService.setInitialConnectionHostSpec(updatedRoleHostSpec);
    return currentConnection;
  }

  private HostSpec getHostSpecFromUrl(final String url) {
    if (url == null) {
      return null;
    }

    final List<HostSpec> hosts = this.pluginService.getHosts();
    for (final HostSpec host : hosts) {
      if (host.getUrl().equals(url)) {
        return host;
      }
    }

    return null;
  }

  private HostSpec getHostSpecFromInstanceId(final String instanceId) {
    if (instanceId == null) {
      return null;
    }

    final List<HostSpec> hosts = this.pluginService.getHosts();
    for (final HostSpec host : hosts) {
      if (host.getUrl().startsWith(instanceId)) {
        return host;
      }
    }

    return null;
  }

  private String getCurrentInstanceId(final Connection conn, final String driverProtocol) {
    final String retrieveInstanceQuery;
    final String instanceNameCol;
    if (driverProtocol.startsWith(PG_DRIVER_PROTOCOL)) {
      retrieveInstanceQuery = PG_GET_INSTANCE_NAME_SQL;
      instanceNameCol = PG_INSTANCE_NAME_COL;
    } else if (driverProtocol.startsWith(MYSQL_DRIVER_PROTOCOL)) {
      retrieveInstanceQuery = MYSQL_GET_INSTANCE_NAME_SQL;
      instanceNameCol = MYSQL_INSTANCE_NAME_COL;
    } else {
      throw new UnsupportedOperationException(
          Messages.get(
              "ReadWriteSplittingPlugin.unsupportedDriverProtocol",
              new Object[] {driverProtocol}));
    }

    String instanceName = null;
    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(retrieveInstanceQuery)) {
      if (resultSet.next()) {
        instanceName = resultSet.getString(instanceNameCol);
      }
    } catch (final SQLException e) {
      return null;
    }

    return instanceName;
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    try {
      updateInternalConnectionInfo();
    } catch (final SQLException e) {
      // ignore
    }

    if (this.inReadWriteSplit.get()) {
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
    if (methodName.contains(METHOD_SET_READ_ONLY) && args != null && args.length > 0) {
      try {
        final boolean readOnly = (Boolean) args[0];
        switchConnectionIfRequired(readOnly);
        this.explicitlyReadOnly = readOnly;
      } catch (final SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    } else if (this.explicitlyReadOnly && this.loadBalanceReadOnlyTraffic && this.isTransactionBoundary) {
      LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.transactionBoundaryDetectedSwitchingToNewReader"));
      pickNewReaderConnection();
    }

    this.isTransactionBoundary = isTransactionBoundary(methodName, args);

    try {
      return jdbcMethodFunc.call();
    } catch (Exception e) {
      if (e instanceof FailoverSQLException) {
        LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.failoverExceptionWhileExecutingCommand"));
        closeAllConnections();
      } else {
        LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.exceptionWhileExecutingCommand"));
      }
      throw e;
    }
  }

  private boolean isTransactionBoundary(final String methodName, final Object[] args) {
    if (this.connectionMethodAnalyzer.doesCloseTransaction(methodName, args)) {
      return true;
    }

    if (this.pluginService.isInTransaction()) {
      return false;
    }

    final boolean autocommit;
    try {
      final Connection currentConnection = this.pluginService.getCurrentConnection();
      autocommit = currentConnection.getAutoCommit();
    } catch (final SQLException e) {
      return false;
    }

    return autocommit && this.connectionMethodAnalyzer.isExecuteDml(methodName, args);
  }

  private void updateInternalConnectionInfo() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (currentConnection == null || currentHost == null) {
      return;
    }

    if (isWriter(currentHost)) {
      setWriterConnection(currentConnection, currentHost);
    } else {
      setReaderConnection(currentConnection, currentHost);
    }

    if (!currentConnection.isClosed()) {
      liveConnections.put(currentHost.getUrl(), currentConnection);
    }
  }

  private boolean isWriter(final @NonNull HostSpec hostSpec) {
    return HostRole.WRITER.equals(hostSpec.getRole());
  }

  private boolean isReader(final @NonNull HostSpec hostSpec) {
    return HostRole.READER.equals(hostSpec.getRole());
  }

  void pickNewReaderConnection() {
    final List<HostSpec> hosts = this.pluginService.getHosts();
    if (hosts.size() <= 2) {
      LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.driverWillNotSwitchToNewReader"));
      return;
    }

    final ArrayDeque<HostSpec> readerHosts = getRandomReaderHosts();
    while (!readerHosts.isEmpty()) {
      final HostSpec host = readerHosts.poll();
      try {
        getNewReaderConnection(host);
        LOGGER.finest(
            () -> Messages.get(
                "ReadWriteSplittingPlugin.successfullyConnectedToReader",
                new Object[] {
                    host.getUrl()}));
        return;
      } catch (final SQLException e) {
        LOGGER.config(
            () -> Messages.get(
                "ReadWriteSplittingPlugin.failedToConnectToReader",
                new Object[] {
                    host.getUrl()}));
      }
    }
    // If we get here we failed to connect to a new reader. In this case we will stick with the current one
  }

  private ArrayDeque<HostSpec> getRandomReaderHosts() {
    final List<HostSpec> hosts = this.pluginService.getHosts();
    final List<HostSpec> readerHosts = new ArrayList<>();
    for (final HostSpec host : hosts) {
      if (HostRole.READER.equals(host.getRole())
          && !this.pluginService.getCurrentHostSpec().getUrl().equals(host.getUrl())) {
        readerHosts.add(host);
      }
    }
    Collections.shuffle(readerHosts);
    return new ArrayDeque<>(readerHosts);
  }

  private void getNewWriterConnection(final HostSpec writerHostSpec) throws SQLException {
    final Connection conn = getConnectionToHost(writerHostSpec);
    setWriterConnection(conn, writerHostSpec);
    switchCurrentConnectionTo(this.writerConnection, writerHostSpec);
  }

  private void setWriterConnection(final Connection writerConnection, final HostSpec writerHostSpec) {
    this.writerConnection = writerConnection;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setWriterConnection",
            new Object[] {
                writerHostSpec.getUrl()}));
  }

  private void setReaderConnection(final Connection conn, final HostSpec host) {
    this.readerConnection = conn;
    this.readerHostSpec = host;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setReaderConnection",
            new Object[] {
                host.getUrl()}));
  }

  void switchConnectionIfRequired(final boolean readOnly) throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();

    if (isConnectionUsable(currentConnection)) {
      try {
        this.pluginService.refreshHostList();
      } catch (final SQLException e) {
        // ignore
      }
    }

    final List<HostSpec> hosts = this.pluginService.getHosts();
    if (hosts == null || hosts.isEmpty()) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.emptyHostList"));
    }

    if (readOnly) {
      if (!pluginService.isInTransaction() && (!isReader(currentHost) || currentConnection.isClosed())) {
        try {
          switchToReaderConnection(hosts);
        } catch (final SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            // "Unable to establish SQL connection to reader instance"
            logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader"),
                SqlState.CONNECTION_UNABLE_TO_CONNECT);
            return;
          }

          // Failed to switch to a reader; use current connection as a fallback
          LOGGER.warning(() -> Messages.get(
              "ReadWriteSplittingPlugin.fallbackToWriter",
              new Object[] {
                  this.pluginService.getCurrentHostSpec().getUrl()}));
          setReaderConnection(currentConnection, currentHost);
        }
      }
    } else {
      if (!isWriter(currentHost) && pluginService.isInTransaction()) {
        logAndThrowException(
            Messages.get("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"),
            SqlState.ACTIVE_SQL_TRANSACTION);
      }

      if (!isWriter(currentHost) || currentConnection.isClosed()) {
        try {
          switchToWriterConnection(hosts);
        } catch (final SQLException e) {
          // "Unable to establish SQL connection to writer node"
          logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToWriter"),
              SqlState.CONNECTION_UNABLE_TO_CONNECT);
        }
      }
    }
  }

  private void logAndThrowException(final String logMessage) throws SQLException {
    LOGGER.severe(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage);
  }

  private void logAndThrowException(final String logMessage, final SqlState sqlState) throws SQLException {
    LOGGER.severe(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage, sqlState.getState());
  }

  private synchronized void switchToWriterConnection(
      final List<HostSpec> hosts)
      throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isWriter(currentHost) && isConnectionUsable(currentConnection)) {
      return;
    }

    this.inReadWriteSplit.set(true);
    final HostSpec writerHost = getWriter(hosts);
    if (!isConnectionUsable(this.writerConnection)) {
      getNewWriterConnection(writerHost);
    } else {
      switchCurrentConnectionTo(this.writerConnection, writerHost);
    }

    LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromReaderToWriter",
        new Object[] {writerHost.getUrl()}));
  }

  private void switchCurrentConnectionTo(
      final Connection newConnection,
      final HostSpec newConnectionHost)
      throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection == newConnection) {
      return;
    }

    transferSessionStateOnReadWriteSplit(newConnection);
    this.pluginService.setCurrentConnection(newConnection, newConnectionHost);
    LOGGER.finest(() -> Messages.get(
        "ReadWriteSplittingPlugin.settingCurrentConnection",
        new Object[] {
            newConnectionHost.getUrl()}));
  }

  /**
   * Transfers basic session state from one connection to another, except for the read-only status. This method is only
   * called when setReadOnly is being called; the read-only status will be updated when the setReadOnly call continues
   * down the plugin chain
   *
   * @param to The connection to transfer state to
   * @throws SQLException if a database access error occurs, this method is called on a closed connection, or this
   *                      method is called during a distributed transaction
   */
  protected void transferSessionStateOnReadWriteSplit(
      final Connection to) throws SQLException {
    final Connection from = this.pluginService.getCurrentConnection();
    if (from == null || to == null) {
      return;
    }

    // TODO: verify if there are other states to sync

    to.setAutoCommit(from.getAutoCommit());
    to.setTransactionIsolation(from.getTransactionIsolation());
  }

  private synchronized void switchToReaderConnection(final List<HostSpec> hosts) throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isReader(currentHost)
        && isConnectionUsable(currentConnection)) {
      return;
    }

    this.inReadWriteSplit.set(true);
    if (!isConnectionUsable(this.readerConnection)) {
      initializeReaderConnection(hosts);
    } else {
      switchCurrentConnectionTo(this.readerConnection, this.readerHostSpec);
    }

    LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
        new Object[] {this.readerHostSpec.getUrl()}));
  }

  private void initializeReaderConnection(final @NonNull List<HostSpec> hosts) throws SQLException {
    if (hosts.size() == 1) {
      final HostSpec writerHost = getWriter(hosts);
      if (!isConnectionUsable(this.writerConnection)) {
        getNewWriterConnection(writerHost);
      }
      setReaderConnection(this.writerConnection, writerHost);
    } else {
      getNewReaderConnection(getRandomReaderHost(hosts));
    }
  }

  private HostSpec getWriter(final @NonNull List<HostSpec> hosts) throws SQLException {
    HostSpec writerHost = null;
    for (final HostSpec hostSpec : hosts) {
      if (HostRole.WRITER.equals(hostSpec.getRole())) {
        writerHost = hostSpec;
        break;
      }
    }

    if (writerHost == null) {
      logAndThrowException("ReadWriteSplittingPlugin.noWriterFound");
    }

    return writerHost;
  }

  private void getNewReaderConnection(final HostSpec readerHostSpec) throws SQLException {
    final Connection conn = getConnectionToHost(readerHostSpec);
    setReaderConnection(conn, readerHostSpec);
    switchCurrentConnectionTo(this.readerConnection, this.readerHostSpec);
  }

  private HostSpec getRandomReaderHost(final List<HostSpec> hosts) throws SQLException {
    final List<HostSpec> readerHosts = new ArrayList<>();
    for (final HostSpec hostSpec : hosts) {
      if (HostRole.READER.equals(hostSpec.getRole())) {
        readerHosts.add(hostSpec);
      }
    }

    if (readerHosts.isEmpty()) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.noReadersFound"));
    }

    Collections.shuffle(readerHosts);
    return readerHosts.get(0);
  }

  private boolean isConnectionUsable(final Connection connection) throws SQLException {
    return connection != null && !connection.isClosed();
  }

  private Connection getConnectionToHost(final HostSpec host) throws SQLException {
    Connection conn = liveConnections.get(host.getUrl());
    if (conn != null && !conn.isClosed()) {
      return conn;
    }

    conn = this.pluginService.connect(host, this.properties);
    liveConnections.put(host.getUrl(), conn);
    return conn;
  }

  @Override
  public void releaseResources() {
    closeAllConnections();
  }

  private void closeAllConnections() {
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.closingInternalConnections"));
    closeInternalConnection(this.readerConnection);
    closeInternalConnection(this.writerConnection);

    for (final Connection connection : liveConnections.values()) {
      closeInternalConnection(connection);
    }
    liveConnections.clear();
  }

  private void closeInternalConnection(final Connection internalConnection) {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    try {
      if (internalConnection != null && internalConnection != currentConnection && !internalConnection.isClosed()) {
        internalConnection.close();
        if (internalConnection == writerConnection) {
          writerConnection = null;
        }

        if (internalConnection == readerConnection) {
          readerConnection = null;
        }
      }
    } catch (final SQLException e) {
      // ignore
    }
  }
}
