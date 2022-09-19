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
import java.util.Collections;
import java.util.HashSet;
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

  private final PluginService pluginService;
  private final Properties properties;
  private Connection writerConnection;
  private Connection readerConnection;
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
    if ("setReadOnly".equals(methodName) && args != null && args.length > 0) {
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
    if (isWriter(this.pluginService.getCurrentHostSpec())) {
      setWriterConnection(currentConnection);
    } else {
      setReaderConnection(currentConnection);
    }
    return currentConnection;
  }

  /**
   * Checks if the given host index points to the primary host.
   *
   * @param hostSpec The current host.
   * @return true if so
   */
  private boolean isWriter(final @NonNull HostSpec hostSpec) {
    return HostRole.WRITER.equals(hostSpec.getRole());
  }

  private boolean isReader(final @NonNull HostSpec hostSpec) {
    return HostRole.READER.equals(hostSpec.getRole());
  }

  private void setWriterConnection(Connection conn) {
    this.writerConnection = conn;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setWriterConnection",
            new Object[] {
                this.pluginService.getCurrentHostSpec().getUrl()}));
  }

  private void setReaderConnection(Connection conn) {
    this.readerConnection = conn;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setReaderConnection",
            new Object[] {
                this.pluginService.getCurrentHostSpec().getUrl()}));
  }

  void switchConnectionIfRequired(Boolean readOnly) throws SQLException {
    if (readOnly == null) {
      LOGGER.warning(Messages.get("ReadWriteSplittingPlugin.setReadOnlyNullArgument"));
      throw new SQLException(Messages.get("ReadWriteSplittingPlugin.setReadOnlyNullArgument"));
    }

    if (!readOnly && isInTransaction()) {
      LOGGER.warning(Messages.get("ReadWriteSplittingPlugin.setReadOnlyCalledInTransaction"));
      throw new SQLException(Messages.get("ReadWriteSplittingPlugin.setReadOnlyCalledInTransaction"),
          SqlState.SQL_STATE_ACTIVE_SQL_TRANSACTION.getState());
    }

    this.explicitlyReadOnly = readOnly;
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();

    if (readOnly) {
      if (!isInTransaction() && (!isReader(currentHost) || currentConnection.isClosed())) {
        try {
          switchToReaderConnection(currentHost);
        } catch (SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            LOGGER.warning(Messages.get("ReadWriteSplittingPlugin.errorSwitchingReaderConnection"));
            throw new SQLException(
                Messages.get("ReadWriteSplittingPlugin..errorSwitchingReaderConnection"),
                SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e);
          }

          // Failed to switch to a reader; use current connection as a fallback
          LOGGER.info(Messages.get(
              "ReadWriteSplittingPlugin.failedSwitchingToReader",
              new Object[]{
                  this.pluginService.getCurrentHostSpec().getUrl()}));
          setReaderConnection(currentConnection);
        }
      }
    } else {
      if (!isWriter(currentHost) || currentConnection.isClosed()) {
        try {
          switchToWriterConnection(currentHost);
        } catch (SQLException e) {
          LOGGER.warning(Messages.get("ReadWriteSplittingPlugin.errorSwitchingWriterConnection"));
          throw new SQLException(
              Messages.get("ReadWriteSplittingPlugin.errorSwitchingWriterConnection"),
              SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), e);
        }
      }
    }
  }

  private synchronized void switchToReaderConnection(HostSpec currentHost) throws SQLException {
    if (isReader(currentHost) && isConnectionUsable(this.readerConnection)) {
      return;
    }
  }

  private synchronized void switchToWriterConnection(HostSpec currentHost) throws SQLException {
    if (isWriter(currentHost) && isConnectionUsable(this.writerConnection)) {
      return;
    }
  }

  private boolean isConnectionUsable(Connection connection) throws SQLException {
    return connection != null && !connection.isClosed();
  }

  private boolean isInTransaction() {
    return pluginService.isInTransaction();
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
