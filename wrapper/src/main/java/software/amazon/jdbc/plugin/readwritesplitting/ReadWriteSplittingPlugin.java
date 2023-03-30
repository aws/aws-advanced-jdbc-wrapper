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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
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
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.WrapperUtils;

public class ReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("initHostProvider");
          add("connect");
          add("notifyConnectionChanged");
          add("Connection.setReadOnly");
        }
      });
  static final String METHOD_SET_READ_ONLY = "Connection.setReadOnly";
  static final String PG_DRIVER_PROTOCOL = "jdbc:postgresql:";
  static final String PG_GET_INSTANCE_NAME_SQL = "SELECT aurora_db_instance_identifier()";
  static final String PG_INSTANCE_NAME_COL = "aurora_db_instance_identifier";
  static final String MYSQL_DRIVER_PROTOCOL = "jdbc:mysql:";
  static final String MYSQL_GET_INSTANCE_NAME_SQL = "SELECT @@aurora_server_id";
  static final String MYSQL_INSTANCE_NAME_COL = "@@aurora_server_id";

  private final PluginService pluginService;
  private final Properties properties;
  private final RdsUtils rdsUtils = new RdsUtils();
  private final AtomicBoolean inReadWriteSplit = new AtomicBoolean(false);
  private final String readerSelectorStrategy;
  private HostListProviderService hostListProviderService;
  private Connection writerConnection;
  private Connection readerConnection;
  private HostSpec readerHostSpec;

  public static final AwsWrapperProperty READER_HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "readerHostSelectorStrategy",
          "random",
          "The strategy that should be used to select a new reader host.");

  ReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.readerSelectorStrategy = READER_HOST_SELECTOR_STRATEGY.getString(properties);
  }

  /**
   * For testing purposes only.
   */
  ReadWriteSplittingPlugin(
      final PluginService pluginService,
      final Properties properties,
      final HostListProviderService hostListProviderService,
      final Connection writerConnection,
      final Connection readerConnection) {
    this(pluginService, properties);
    this.hostListProviderService = hostListProviderService;
    this.writerConnection = writerConnection;
    this.readerConnection = readerConnection;
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

    if (!pluginService.acceptsStrategy(hostSpec.getRole(), this.readerSelectorStrategy)) {
      throw new UnsupportedOperationException(
          Messages.get("ReadWriteSplittingPlugin.unsupportedHostSpecSelectorStrategy",
              new Object[] { this.readerSelectorStrategy }));
    }
    return connectInternal(driverProtocol, hostSpec, isInitialConnection, connectFunc);
  }

  private Connection connectInternal(String driverProtocol, HostSpec hostSpec,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    final Connection currentConnection = connectFunc.call();
    if (!isInitialConnection || this.hostListProviderService.isStaticHostListProvider()) {
      return currentConnection;
    }

    final RdsUrlType urlType = rdsUtils.identifyRdsType(hostSpec.getHost());
    if (RdsUrlType.RDS_WRITER_CLUSTER.equals(urlType)
        || RdsUrlType.RDS_READER_CLUSTER.equals(urlType)) {
      return currentConnection;
    }

    // By default, the initial HostSpec role is assumed to be a writer, which may not be true.
    // The rest of the logic in this method updates it if necessary.
    this.pluginService.refreshHostList(currentConnection);
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    final HostSpec updatedCurrentHost;
    if (RdsUrlType.RDS_INSTANCE.equals(urlType)) {
      updatedCurrentHost = getHostSpecFromUrl(currentHost.getUrl());
    } else {
      updatedCurrentHost =
          getHostSpecFromInstanceId(getCurrentInstanceId(currentConnection, driverProtocol));
    }

    if (updatedCurrentHost == null) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorUpdatingHostSpecRole"));
      return null;
    }

    final HostSpec updatedRoleHostSpec =
        new HostSpec(currentHost.getHost(), currentHost.getPort(), updatedCurrentHost.getRole(),
            currentHost.getAvailability());

    this.hostListProviderService.setInitialConnectionHostSpec(updatedRoleHostSpec);
    return currentConnection;
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, isInitialConnection, forceConnectFunc);
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
  public OldConnectionSuggestedAction notifyConnectionChanged(
      final EnumSet<NodeChangeOptions> changes) {
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
    final Connection conn = WrapperUtils.getConnectionFromSqlObject(methodInvokeOn);
    if (conn != null && conn != this.pluginService.getCurrentConnection()) {
      LOGGER.fine(
          () -> Messages.get("ReadWriteSplittingPlugin.executingAgainstOldConnection",
              new Object[] {methodInvokeOn}));
      return jdbcMethodFunc.call();
    }

    if (methodName.equals(METHOD_SET_READ_ONLY) && args != null && args.length > 0) {
      try {
        switchConnectionIfRequired((Boolean) args[0]);
      } catch (final SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    try {
      return jdbcMethodFunc.call();
    } catch (final Exception e) {
      if (e instanceof FailoverSQLException) {
        LOGGER.finer(
            () -> Messages.get("ReadWriteSplittingPlugin.failoverExceptionWhileExecutingCommand",
                new Object[] {methodName}));
        closeIdleConnections();
      } else {
        LOGGER.finest(
            () -> Messages.get("ReadWriteSplittingPlugin.exceptionWhileExecutingCommand",
                new Object[] {methodName}));
      }
      throw e;
    }
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
  }

  private boolean isWriter(final @NonNull HostSpec hostSpec) {
    return HostRole.WRITER.equals(hostSpec.getRole());
  }

  private boolean isReader(final @NonNull HostSpec hostSpec) {
    return HostRole.READER.equals(hostSpec.getRole());
  }

  private void getNewWriterConnection(final HostSpec writerHostSpec) throws SQLException {
    final Connection conn = this.pluginService.connect(writerHostSpec, this.properties);
    setWriterConnection(conn, writerHostSpec);
    switchCurrentConnectionTo(this.writerConnection, writerHostSpec);
  }

  private void setWriterConnection(final Connection writerConnection,
      final HostSpec writerHostSpec) {
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
    if (currentConnection != null && currentConnection.isClosed()) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.setReadOnlyOnClosedConnection"),
          SqlState.CONNECTION_NOT_OPEN);
    }

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

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (readOnly) {
      if (!pluginService.isInTransaction() && !isReader(currentHost)) {
        try {
          switchToReaderConnection(hosts);
        } catch (final SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader"),
                SqlState.CONNECTION_UNABLE_TO_CONNECT);
            return;
          }

          // Failed to switch to a reader; use writer as a fallback
          LOGGER.warning(() -> Messages.get(
              "ReadWriteSplittingPlugin.fallbackToWriter",
              new Object[] {
                  this.pluginService.getCurrentHostSpec().getUrl()}));
        }
      }
    } else {
      if (!isWriter(currentHost) && pluginService.isInTransaction()) {
        logAndThrowException(
            Messages.get("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"),
            SqlState.ACTIVE_SQL_TRANSACTION);
      }

      if (!isWriter(currentHost)) {
        try {
          switchToWriterConnection(hosts);
        } catch (final SQLException e) {
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

  private void logAndThrowException(final String logMessage, final SqlState sqlState)
      throws SQLException {
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
   * Transfers basic session state from one connection to another, except for the read-only
   * status. This method is only called when setReadOnly is being called; the read-only status
   * will be updated when the setReadOnly call continues down the plugin chain
   *
   * @param to The connection to transfer state to
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *                      connection, or this method is called during a distributed transaction
   */
  protected void transferSessionStateOnReadWriteSplit(
      final Connection to) throws SQLException {
    final Connection from = this.pluginService.getCurrentConnection();
    if (from == null || to == null) {
      return;
    }

    to.setAutoCommit(from.getAutoCommit());
    to.setTransactionIsolation(from.getTransactionIsolation());
  }

  private synchronized void switchToReaderConnection(final List<HostSpec> hosts)
      throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isReader(currentHost) && isConnectionUsable(currentConnection)) {
      return;
    }

    this.inReadWriteSplit.set(true);
    if (!isConnectionUsable(this.readerConnection)) {
      initializeReaderConnection(hosts);
    } else {
      switchCurrentConnectionTo(this.readerConnection, this.readerHostSpec);
      LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
          new Object[] {this.readerHostSpec.getUrl()}));
    }
  }

  private void initializeReaderConnection(final @NonNull List<HostSpec> hosts) throws SQLException {
    if (hosts.size() == 1) {
      final HostSpec writerHost = getWriter(hosts);
      if (!isConnectionUsable(this.writerConnection)) {
        getNewWriterConnection(writerHost);
      }
      LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.noReadersFound",
          new Object[] {writerHost.getUrl()}));
    } else {
      getNewReaderConnection();
      LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
          new Object[] {this.readerHostSpec.getUrl()}));
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
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.noWriterFound"));
    }

    return writerHost;
  }

  private void getNewReaderConnection() throws SQLException {
    Connection conn = null;
    HostSpec readerHost = null;

    int connAttempts = this.pluginService.getHosts().size() * 2;
    for (int i = 0; i < connAttempts; i++) {
      HostSpec hostSpec = this.pluginService.getHostSpecByStrategy(HostRole.READER, this.readerSelectorStrategy);
      try {
        conn = this.pluginService.connect(hostSpec, this.properties);
        readerHost = hostSpec;
        break;
      } catch (final SQLException e) {
        LOGGER.config(
            () -> Messages.get(
                "ReadWriteSplittingPlugin.failedToConnectToReader",
                new Object[] {
                    hostSpec.getUrl()}));
      }
    }

    if (conn == null || readerHost == null) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.noReadersAvailable"),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
      return;
    }

    HostSpec finalReaderHost = readerHost;
    LOGGER.finest(
        () -> Messages.get("ReadWriteSplittingPlugin.successfullyConnectedToReader",
            new Object[] {finalReaderHost.getUrl()}));
    setReaderConnection(conn, readerHost);
    switchCurrentConnectionTo(this.readerConnection, this.readerHostSpec);
  }

  private boolean isConnectionUsable(final Connection connection) throws SQLException {
    return connection != null && !connection.isClosed();
  }

  @Override
  public void releaseResources() {
    closeIdleConnections();
  }

  private void closeIdleConnections() {
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.closingInternalConnections"));
    closeConnectionIfIdle(this.readerConnection);
    closeConnectionIfIdle(this.writerConnection);
  }

  private void closeConnectionIfIdle(final Connection internalConnection) {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    try {
      if (internalConnection != null
          && internalConnection != currentConnection
          && !internalConnection.isClosed()) {
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

  /**
   * Methods for testing purposes only.
   */
  Connection getWriterConnection() {
    return this.writerConnection;
  }

  Connection getReaderConnection() {
    return this.readerConnection;
  }
}
