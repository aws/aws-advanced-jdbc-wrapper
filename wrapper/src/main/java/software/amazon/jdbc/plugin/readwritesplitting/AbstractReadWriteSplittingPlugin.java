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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.util.CacheItem;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.WrapperUtils;

public abstract class AbstractReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(AbstractReadWriteSplittingPlugin.class.getName());
  private static final Set<String> subscribedMethods =
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
  protected volatile boolean inReadWriteSplit = false;
  protected HostListProviderService hostListProviderService;
  protected Connection writerConnection;
  protected CacheItem<Connection> readerCacheItem;
  protected HostSpec writerHostSpec;
  protected HostSpec readerHostSpec;
  protected boolean isReaderConnFromInternalPool;
  protected boolean isWriterConnFromInternalPool;

  public static final AwsWrapperProperty CACHED_READER_KEEP_ALIVE_TIMEOUT =
      new AwsWrapperProperty(
          "cachedReaderKeepAliveTimeoutMs",
          "0",
          "The time in milliseconds to keep a reader connection alive in the cache. "
              + "Default value 0 means the Wrapper will keep reusing the same cached reader connection.");

  static {
    PropertyDefinition.registerPluginProperties(ReadWriteSplittingPlugin.class);
  }

  public AbstractReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.properties = properties;
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
  public OldConnectionSuggestedAction notifyConnectionChanged(
      final EnumSet<NodeChangeOptions> changes) {
    try {
      updateInternalConnectionInfo();
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
      LOGGER.fine(
          () -> Messages.get("ReadWriteSplittingPlugin.executingAgainstOldConnection",
              new Object[] {methodInvokeOn}));
      return jdbcMethodFunc.call();
    }

    if (JdbcMethod.CONNECTION_CLEARWARNINGS.methodName.equals(methodName)) {
      try {
        if (this.writerConnection != null && !this.writerConnection.isClosed()) {
          this.writerConnection.clearWarnings();
        }
        if (this.readerCacheItem != null && isConnectionUsable(this.readerCacheItem.get())) {
          this.readerCacheItem.get().clearWarnings();
        }
      } catch (final SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    if (JdbcMethod.CONNECTION_SETREADONLY.methodName.equals(methodName)
        && args != null
        && args.length > 0) {
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

    if (shouldUpdateWriterConnection(currentConnection, currentHost)) {
      setWriterConnection(currentConnection, currentHost);
    } else if (shouldUpdateReaderConnection(currentConnection, currentHost)) {
      setReaderConnection(currentConnection, currentHost);
    }
  }

  protected void setWriterConnection(final Connection conn, final HostSpec host) {
    this.writerConnection = conn;
    this.writerHostSpec = host;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setWriterConnection",
            new Object[] {
                host.getHostAndPort()}));
  }

  protected void setReaderConnection(final Connection conn, final HostSpec host) {
    closeReaderConnectionIfIdle();
    this.readerCacheItem = new CacheItem<>(conn, this.getKeepAliveTimeout(this.isReaderConnFromInternalPool));
    this.readerHostSpec = host;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setReaderConnection",
            new Object[] {
                host.getHostAndPort()}));
  }

  public void switchConnectionIfRequired(final boolean readOnly) throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection != null && currentConnection.isClosed()) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.setReadOnlyOnClosedConnection"),
          SqlState.CONNECTION_NOT_OPEN);
    }

    this.refreshAndStoreTopology(currentConnection);

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (readOnly) {
      if (!pluginService.isInTransaction() && !isReader(currentHost)) {
        try {
          switchToReaderConnection();
        } catch (final SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            logAndThrowException(
                Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader", new Object[] {e.getMessage()}),
                e);
            return;
          }

          // Failed to switch to a reader. The current connection will be used as a fallback.
          LOGGER.fine(() -> Messages.get(
              "ReadWriteSplittingPlugin.fallbackToCurrentConnection",
              new Object[] {
                  this.pluginService.getCurrentHostSpec().getHostAndPort(),
                  e.getMessage()}));
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
          switchToWriterConnection();
        } catch (final SQLException e) {
          logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToWriter"),
              e);
        }
      }
    }
  }

  protected void logAndThrowException(final String logMessage) throws SQLException {
    LOGGER.severe(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage);
  }

  protected void logAndThrowException(final String logMessage, final SqlState sqlState)
      throws SQLException {
    LOGGER.severe(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage, sqlState.getState());
  }

  private void logAndThrowException(
      final String logMessage, final Throwable cause)
      throws SQLException {
    LOGGER.fine(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage, SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), cause);
  }

  private void switchToWriterConnection()
      throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isWriter(currentHost) && isConnectionUsable(currentConnection)) {
      // Already connected to the writer.
      return;
    }

    this.inReadWriteSplit = true;
    if (!isConnectionUsable(this.writerConnection)) {
      initializeWriterConnection();
    } else {
      switchCurrentConnectionTo(this.writerConnection, this.writerHostSpec);
    }

    if (this.isReaderConnFromInternalPool) {
      this.closeReaderConnectionIfIdle();
    }

    LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromReaderToWriter",
        new Object[] {this.writerHostSpec.getHostAndPort()}));
  }

  protected void switchCurrentConnectionTo(
      final Connection newConnection,
      final HostSpec newConnectionHost)
      throws SQLException {

    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection == newConnection) {
      return;
    }
    this.pluginService.setCurrentConnection(newConnection, newConnectionHost);
    LOGGER.finest(() -> Messages.get(
        "ReadWriteSplittingPlugin.settingCurrentConnection",
        new Object[] {
            newConnectionHost.getHostAndPort()}));
  }

  private void switchToReaderConnection()
      throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isReader(currentHost) && isConnectionUsable(currentConnection)) {
      // Already connected to reader.
      return;
    }

    this.closeReaderIfNecessary();

    this.inReadWriteSplit = true;
    if (this.readerCacheItem == null || !isConnectionUsable(this.readerCacheItem.get())) {
      initializeReaderConnection();
    } else {
      try {
        switchCurrentConnectionTo(this.readerCacheItem.get(), this.readerHostSpec);
        LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
            new Object[] {this.readerHostSpec.getHostAndPort()}));
      } catch (SQLException e) {
        if (e.getMessage() != null) {
          LOGGER.warning(
              () -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReaderWithCause",
                  new Object[] {this.readerHostSpec.getHostAndPort(), e.getMessage()}));
        } else {
          LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReader",
              new Object[] {this.readerHostSpec.getHostAndPort()}));
        }

        closeReaderConnectionIfIdle();
        initializeReaderConnection();
      }
    }

    if (this.isWriterConnFromInternalPool) {
      this.closeWriterConnectionIfIdle();
    }
  }

  protected boolean isConnectionUsable(final Connection connection) throws SQLException {
    return connection != null && !connection.isClosed();
  }

  protected long getKeepAliveTimeout(boolean isPooledConnection) {
    if (isPooledConnection) {
      // Let the connection pool handle the lifetime of the reader connection.
      return 0;
    }
    final long keepAliveMs = CACHED_READER_KEEP_ALIVE_TIMEOUT.getLong(properties);
    return keepAliveMs > 0 ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(keepAliveMs) : 0;
  }

  @Override
  public void releaseResources() {
    closeIdleConnections();
  }

  private void closeIdleConnections() {
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.closingInternalConnections"));
    closeReaderConnectionIfIdle();
    closeWriterConnectionIfIdle();
  }

  public void closeReaderConnectionIfIdle() {
    if (this.readerCacheItem == null) {
      return;
    }

    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final Connection readerConnection = this.readerCacheItem.get(true);

    if (readerConnection != null && readerConnection != currentConnection) {
      try {
        if (!readerConnection.isClosed()) {
          // readerConnection is open but is not currently in use, so we close it.
          readerConnection.close();
        }
      } catch (SQLException e) {
        // Do nothing.
      }
      this.readerCacheItem = null;
    }
  }

  public void closeWriterConnectionIfIdle() {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (this.writerConnection != null && this.writerConnection != currentConnection) {
      try {
        if (!this.writerConnection.isClosed()) {
          // writerConnection is open but is not currently in use, so we close it.
          this.writerConnection.close();
        }
      } catch (SQLException e) {
        // Do nothing.
      }
      this.writerConnection = null;
    }
  }

  protected abstract boolean shouldUpdateReaderConnection(
      Connection currentConnection, HostSpec currentHost) throws SQLException;

  protected abstract boolean shouldUpdateWriterConnection(
      Connection currentConnection, HostSpec currentHost) throws SQLException;

  protected abstract boolean isWriter(HostSpec currentHost);

  protected abstract boolean isReader(HostSpec currentHost);

  protected abstract void refreshAndStoreTopology(Connection currentConnection) throws SQLException;

  protected abstract void initializeWriterConnection() throws SQLException;

  protected abstract void initializeReaderConnection() throws SQLException;

  protected abstract void closeReaderIfNecessary();

  /**
   * Methods for testing purposes only.
   */
  public Connection getWriterConnection() {
    return this.writerConnection;
  }

  public Connection getReaderConnection() {
    return this.readerCacheItem == null ? null : this.readerCacheItem.get();
  }

  public HostSpec getReaderHostSpec() {
    return this.readerHostSpec;
  }

  public HostSpec getWriterHostSpec() {
    return this.writerHostSpec;
  }
}
