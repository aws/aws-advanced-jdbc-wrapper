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

package software.amazon.jdbc.plugin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.Utils;

public class ReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {

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
  public static final int NO_CONNECTION_INDEX = -1;
  private final PluginService pluginService;
  private final Properties properties;
  private Connection writerConnection;
  private Connection readerConnection;
  private HostSpec readerHostSpec;
  private boolean explicitlyReadOnly = false;

  ReadWriteSplittingPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
    this.properties = properties;
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
      Object[] args)
      throws E {
    if (methodName.contains(METHOD_SET_READ_ONLY) && args != null && args.length > 0) {
      this.explicitlyReadOnly = (Boolean) args[0];
      try {
        switchConnectionIfRequired((Boolean) args[0]);
      } catch (SQLException e) {
        throw wrapExceptionIfNeeded(exceptionClass, e);
      }
    }
    return jdbcMethodFunc.call();
  }

  private <E extends Exception> E wrapExceptionIfNeeded(Class<E> exceptionClass, Throwable exception) {
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
    updateInternalConnectionInfo(currentConnection, this.pluginService.getCurrentHostSpec());

    return currentConnection;
  }

  private void updateInternalConnectionInfo(Connection currentConnection, HostSpec currentHost) {
    if (currentConnection == null || currentHost == null) {
      return;
    }
    if (isWriter(currentHost)) {
      setWriterConnection(currentConnection);
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

  private void getNewWriterConnection(HostSpec hostSpec) throws SQLException {
    final Connection conn = this.pluginService.connect(hostSpec, this.properties);
    setWriterConnection(conn);
    switchCurrentConnectionTo(conn, this.writerConnection, hostSpec);
  }

  private void setWriterConnection(Connection conn) {
    this.writerConnection = conn;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setWriterConnection",
            new Object[] {
                this.pluginService.getCurrentHostSpec().getUrl()}));
  }

  private void setReaderConnection(Connection conn, HostSpec host) {
    this.readerConnection = conn;
    this.readerHostSpec = host;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setReaderConnection",
            new Object[] {
                host.getUrl()}));
  }

  void switchConnectionIfRequired(Boolean readOnly) throws SQLException {
    if (readOnly == null) {
      LOGGER.severe(() -> Messages.get("ReadWriteSplittingPlugin.setReadOnlyNullArgument"));
      throw new SQLException(Messages.get("ReadWriteSplittingPlugin.setReadOnlyNullArgument"));
    }

    if (!readOnly && pluginService.isInTransaction()) {
      LOGGER.severe(() -> Messages.get("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"));
      throw new SQLException(Messages.get("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"),
          SqlState.ACTIVE_SQL_TRANSACTION.getState());
    }

    this.explicitlyReadOnly = readOnly;
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();

    if (isConnectionUsable(currentConnection)) {
      this.pluginService.refreshHostList();
    }
    List<HostSpec> hosts = this.pluginService.getHosts();

    if (readOnly) {
      if (!pluginService.isInTransaction() && (!isReader(currentHost) || currentConnection.isClosed())) {
        try {
          switchToReaderConnection(hosts);
        } catch (SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            LOGGER.severe(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader"));
            throw new SQLException(
                Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader"),
                SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e);
          }

          // Failed to switch to a reader; use current connection as a fallback
          LOGGER.warning(() -> Messages.get(
              "ReadWriteSplittingPlugin.fallbackToWriter",
              new Object[]{
                  this.pluginService.getCurrentHostSpec().getUrl()}));
          setReaderConnection(currentConnection, currentHost);
        }
      }
    } else {
      if (!isWriter(currentHost) || currentConnection.isClosed()) {
        try {
          switchToWriterConnection(currentConnection, currentHost, hosts);
        } catch (SQLException e) {
          LOGGER.severe(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToWriter"));
          throw new SQLException(
              Messages.get("ReadWriteSplittingPlugin.errorSwitchingToWriter"),
              SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e);
        }
      }
    }
  }

  private synchronized void switchToWriterConnection(
      Connection currentConnection,
      HostSpec currentHost,
      List<HostSpec> hosts)
      throws SQLException {
    if (isWriter(currentHost) && isConnectionUsable(this.writerConnection)) {
      return;
    }
    if (!isConnectionUsable(this.writerConnection)) {
      getNewWriterConnection(getWriter(hosts));
    }
    switchCurrentConnectionTo(currentConnection, this.writerConnection, getWriter(hosts));
    LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromReaderToWriter",
        new Object[]{getWriter(hosts).getUrl()}));
  }

  private void switchCurrentConnectionTo(
      Connection oldConn,
      Connection newConn,
      HostSpec hostSpec)
      throws SQLException {
    if (oldConn == newConn) {
      return;
    }
    syncSessionStateOnReadWriteSplit(oldConn, newConn);
    this.pluginService.setCurrentConnection(newConn, hostSpec);
    LOGGER.finest(() -> Messages.get(
        "ReadWriteSplittingPlugin.settingCurrentConnection",
        new Object[]{
            this.pluginService.getCurrentHostSpec().getUrl()}));
  }

  /**
   * Synchronizes session state between two connections, allowing to override the read-only status.
   *
   * @param source The connection where to get state from.
   * @param target The connection where to set state.
   * @throws SQLException if an error occurs
   */
  protected void syncSessionStateOnReadWriteSplit(
      Connection source,
      Connection target) throws SQLException {

    if (source == null || target == null) {
      return;
    }

    // TODO: verify if there are other states to sync

    target.setAutoCommit(source.getAutoCommit());
    target.setTransactionIsolation(source.getTransactionIsolation());
  }

  private synchronized void switchToReaderConnection(List<HostSpec> hosts) throws SQLException {
    Connection currentConnection = this.pluginService.getCurrentConnection();
    HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isReader(currentHost) && isConnectionUsable(this.readerConnection)) {
      return;
    }
    if (!isConnectionUsable(this.readerConnection)) {
      initializeReaderConnection(currentConnection, hosts);
      // The current connection may have changed; update it here in case it did.
      currentConnection = this.pluginService.getCurrentConnection();
    }
    switchCurrentConnectionTo(currentConnection, this.readerConnection, this.readerHostSpec);
    LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
        new Object[]{this.readerHostSpec.getUrl()}));
  }

  private void initializeReaderConnection(Connection currentConnection, List<HostSpec> hosts) throws SQLException {
    if (hosts.size() == 1) {
      HostSpec writerHost = getWriter(hosts);
      if (!isConnectionUsable(this.writerConnection)) {
        getNewWriterConnection(writerHost);
        switchCurrentConnectionTo(currentConnection, this.writerConnection, writerHost);
      }
      setReaderConnection(this.writerConnection, writerHost);
    } else if (hosts.size() == 2) {
      getNewReaderConnection(hosts.get(1));
    } else {
      final int randomReaderIndex = getRandomReaderIndex(hosts);
      getNewReaderConnection(hosts.get(randomReaderIndex));
    }
  }

  private HostSpec getWriter(final @NonNull List<HostSpec> hosts) {
    for (HostSpec hostSpec : hosts) {
      if (hostSpec.getRole() == HostRole.WRITER) {
        return hostSpec;
      }
    }
    return null;
  }

  private void getNewReaderConnection(HostSpec host) throws SQLException {
    final Connection conn = pluginService.connect(host, this.properties);
    setReaderConnection(conn, host);
    switchCurrentConnectionTo(conn, this.readerConnection, this.readerHostSpec);
  }

  private int getRandomReaderIndex(List<HostSpec> hosts) {
    if (hosts.size() <= 1) {
      return NO_CONNECTION_INDEX;
    }

    final int minReaderIndex = 1;
    final int maxReaderIndex = hosts.size() - 1;
    return (int) (Math.random() * ((maxReaderIndex - minReaderIndex) + 1)) + minReaderIndex;
  }

  private boolean isConnectionUsable(Connection connection) throws SQLException {
    return connection != null && !connection.isClosed();
  }

  @Override
  public void releaseResources() {
    closeAllConnections();
  }

  private void closeAllConnections() {
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.closingInternalConnections"));
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    closeInternalConnection(this.readerConnection, currentConnection);
    closeInternalConnection(this.writerConnection, currentConnection);
  }

  private void closeInternalConnection(Connection internalConnection, Connection currentConnection) {
    try {
      if (internalConnection != null && internalConnection != currentConnection && !internalConnection.isClosed()) {
        internalConnection.close();
        if (writerConnection == internalConnection) {
          writerConnection = null;
        }

        if (readerConnection == internalConnection) {
          readerConnection = null;
        }
      }
    } catch (SQLException e) {
      // ignore
    }
  }
}
