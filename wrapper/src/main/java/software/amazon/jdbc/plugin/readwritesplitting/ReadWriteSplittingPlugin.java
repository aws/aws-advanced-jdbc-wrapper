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
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
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
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.SubscribedMethodHelper;

public class ReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {
  private final Map<String, Connection> liveConnections = new HashMap<>();

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          addAll(SubscribedMethodHelper.NETWORK_BOUND_METHODS);
          add("connect");
          add("Connection.setReadOnly");
        }
      });
  static final String METHOD_SET_READ_ONLY = "setReadOnly";
  private final PluginService pluginService;
  private final Properties properties;
  private Connection writerConnection;
  private Connection readerConnection;
  private HostSpec readerHostSpec;
  private boolean explicitlyReadOnly = false;
  private final boolean loadBalanceReadOnlyTraffic;

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
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] args)
      throws E {
    if (methodName.contains(METHOD_SET_READ_ONLY) && args != null && args.length > 0) {
      this.explicitlyReadOnly = (Boolean) args[0];
      try {
        switchConnectionIfRequired();
      } catch (final FailoverSQLException failoverException) {
        LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.failoverExceptionWhileExecutingCommand"));
        closeAllConnections();
        throw wrapExceptionIfNeeded(exceptionClass, failoverException);
      } catch (final SQLException e) {
        LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.exceptionWhileExecutingCommand"));
        throw wrapExceptionIfNeeded(exceptionClass, e);
      }
    } else if (this.explicitlyReadOnly && this.loadBalanceReadOnlyTraffic) {
      LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.transactionBoundaryDetectedSwitchingToNewReader"));
      pickNewReaderConnection();
    }

    return jdbcMethodFunc.call();
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    try {
      updateInternalConnectionInfo();
    } catch (SQLException e) {
      // ignore
    }
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  private <E extends Exception> E wrapExceptionIfNeeded(final Class<E> exceptionClass, final Throwable exception) {
    if (exceptionClass.isAssignableFrom(exception.getClass())) {
      return exceptionClass.cast(exception);
    }

    return exceptionClass.cast(new RuntimeException(exception));
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
    updateInternalConnectionInfo();

    return currentConnection;
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
        final Connection currentConnection = this.pluginService.getCurrentConnection();
        switchCurrentConnectionTo(currentConnection, this.readerHostSpec);

        LOGGER.finest(
            () -> Messages.get(
                "ReadWriteSplittingPlugin.successfullyConnectedToReader",
                new Object[] {
                    host.getUrl()}));
        return;
      } catch (SQLException e) {
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
    for (HostSpec host : hosts) {
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

  void switchConnectionIfRequired() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();

    if (isConnectionUsable(currentConnection)) {
      this.pluginService.refreshHostList();
    }

    final List<HostSpec> hosts = this.pluginService.getHosts();
    if (hosts == null || hosts.isEmpty()) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.emptyHostList"));
    }

    if (this.explicitlyReadOnly) {
      if (!pluginService.isInTransaction() && (!isReader(currentHost) || currentConnection.isClosed())) {
        try {
          switchToReaderConnection(hosts);
        } catch (final SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            // "Unable to establish SQL connection to reader instance"
            logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader"));
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
      if (pluginService.isInTransaction()) {
        logAndThrowException(
            Messages.get("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"),
            SqlState.ACTIVE_SQL_TRANSACTION);
      }

      if (!isWriter(currentHost) || currentConnection.isClosed()) {
        try {
          switchToWriterConnection(hosts);
        } catch (final SQLException e) {
          // "Unable to establish SQL connection to writer node"
          logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToWriter"));
        }
      }
    }
  }

  private void logAndThrowException(String logMessage) throws SQLException {
    LOGGER.severe(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage);
  }

  private void logAndThrowException(String logMessage, SqlState sqlState) throws SQLException {
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
    if (currentConnection.equals(newConnection)) {
      return;
    }
    syncSessionStateOnReadWriteSplit(newConnection);
    this.pluginService.setCurrentConnection(newConnection, newConnectionHost);
    LOGGER.finest(() -> Messages.get(
        "ReadWriteSplittingPlugin.settingCurrentConnection",
        new Object[] {
            newConnectionHost.getUrl()}));
  }

  /**
   * Synchronizes session state between two connections, allowing to override the read-only status.
   *
   * @param target The connection where to set state.
   * @throws SQLException if an error occurs
   */
  protected void syncSessionStateOnReadWriteSplit(
      final Connection target) throws SQLException {
    final Connection source = this.pluginService.getCurrentConnection();
    if (source == null || target == null) {
      return;
    }

    // TODO: verify if there are other states to sync

    target.setAutoCommit(source.getAutoCommit());
    target.setTransactionIsolation(source.getTransactionIsolation());
  }

  private synchronized void switchToReaderConnection(final List<HostSpec> hosts) throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isReader(currentHost)
        && isConnectionUsable(currentConnection)) {
      return;
    }

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

  private Connection getConnectionToHost(HostSpec host) throws SQLException {
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
  }

  private void closeInternalConnection(final Connection internalConnection) {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    try {
      if (internalConnection != null && internalConnection != currentConnection && !internalConnection.isClosed()) {
        internalConnection.close();
        if (writerConnection.equals(internalConnection)) {
          writerConnection = null;
        }

        if (readerConnection.equals(internalConnection)) {
          readerConnection = null;
        }

        for (Connection connection : liveConnections.values()) {
          closeInternalConnection(connection);
        }
        liveConnections.clear();
      }
    } catch (final SQLException e) {
      // ignore
    }
  }
}
