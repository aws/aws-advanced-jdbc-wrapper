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

package software.amazon.jdbc.plugin.srw;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingSQLException;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

public class SimpleReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(SimpleReadWriteSplittingPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.INITHOSTPROVIDER.methodName);
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

  private final PluginService pluginService;
  private final Properties properties;
  private final boolean verifyNewConnections;
  private volatile boolean inReadWriteSplit = false;
  private HostListProviderService hostListProviderService;
  private Connection writerConnection;
  private Connection readerConnection;
  private final String writeEndpoint;
  private final String readEndpoint;
  private HostSpec readEndpointHostSpec;
  private HostSpec writeEndpointHostSpec;

  public static final AwsWrapperProperty SRW_READ_ENDPOINT =
      new AwsWrapperProperty(
          "srwReadEndpoint",
          null,
          "The read-only endpoint that should be used to connect to a new reader host.");
  public static final AwsWrapperProperty SRW_WRITE_ENDPOINT =
      new AwsWrapperProperty(
          "srwWriteEndpoint",
          null,
          "The read-write/cluster endpoint that should be used to connect to the writer.");
  public static final AwsWrapperProperty VERIFY_NEW_SRW_CONNECTIONS =
      new AwsWrapperProperty(
          "verifyNewSrwConnections",
          "true",
          "Enables role-verification for new connections made by the Simple Read Write Splitting Plugin.",
          false,
          new String[] {
              "true", "false"
          });

  static {
    PropertyDefinition.registerPluginProperties(SimpleReadWriteSplittingPlugin.class);
  }

  SimpleReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    String writeEndpoint = SRW_WRITE_ENDPOINT.getString(properties);
    if (StringUtils.isNullOrEmpty(writeEndpoint)) {
      throw new
          RuntimeException(
          Messages.get(
              "SimpleReadWriteSplittingPlugin.missingRequiredConfigParameter",
              new Object[] {SRW_WRITE_ENDPOINT.name}));
    }
    this.pluginService = pluginService;
    this.properties = properties;
    this.writeEndpoint = writeEndpoint;
    this.readEndpoint = SRW_READ_ENDPOINT.getString(properties);
    this.verifyNewConnections = VERIFY_NEW_SRW_CONNECTIONS.getBoolean(properties);
  }

  /**
   * For testing purposes only.
   */
  SimpleReadWriteSplittingPlugin(
      final PluginService pluginService,
      final Properties properties,
      final HostListProviderService hostListProviderService,
      final Connection writerConnection,
      final Connection readerConnection,
      final HostSpec writeEndpointHostSpec,
      final HostSpec readEndpointHostSpec) {
    this(pluginService, properties);
    this.hostListProviderService = hostListProviderService;
    this.writerConnection = writerConnection;
    this.readerConnection = readerConnection;
    this.writeEndpointHostSpec = writeEndpointHostSpec;
    this.readEndpointHostSpec = readEndpointHostSpec;
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
        if (this.readerConnection != null && !this.readerConnection.isClosed()) {
          this.readerConnection.clearWarnings();
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

    // Only update internal connection info if connection is to the endpoint and different from internal connection.
    if (isWriteEndpoint(currentHost) && !currentConnection.equals(this.writerConnection)
        && (!this.verifyNewConnections || this.pluginService.getHostRole(currentConnection) == HostRole.WRITER)) {
      setWriterConnection(currentConnection, currentHost);
    } else if (isReadEndpoint(currentHost) && !currentConnection.equals(this.readerConnection)
        && (!this.verifyNewConnections || this.pluginService.getHostRole(currentConnection) == HostRole.READER)) {
      setReaderConnection(currentConnection, currentHost);
    }
  }

  private boolean isWriteEndpoint(final @NonNull HostSpec hostSpec) {
    return Objects.equals(hostSpec.getHost(), this.writeEndpoint);
  }

  private boolean isReadEndpoint(final @NonNull HostSpec hostSpec) {
    return Objects.equals(hostSpec.getHost(), this.readEndpoint);
  }

  private void setWriterConnection(final Connection conn, final HostSpec host) {
    this.writerConnection = conn;
    this.writeEndpointHostSpec = host;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setWriterConnection",
            new Object[] {
                host.getUrl()}));
  }

  private void setReaderConnection(final Connection conn, final HostSpec host) {
    this.readerConnection = conn;
    this.readEndpointHostSpec = host;
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

    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (readOnly) {
      if (!pluginService.isInTransaction() && !isReadEndpoint(currentHost)) {
        try {
          switchToReaderConnection();
        } catch (final SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            logAndThrowException(
                Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader", new Object[]{e.getMessage()}),
                e);
          }
          // Failed to switch to the reader endpoint. The current connection will be used as a fallback.
          LOGGER.fine(() -> Messages.get(
              "SimpleReadWriteSplittingPlugin.fallbackToCurrentConnection",
              new Object[] {
                  this.pluginService.getCurrentHostSpec().getUrl(),
                  e.getMessage()}));
        }
      }
    } else {
      if (!isWriteEndpoint(currentHost) && pluginService.isInTransaction()) {
        logAndThrowException(
            Messages.get("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"),
            SqlState.ACTIVE_SQL_TRANSACTION);
      }

      if (!isWriteEndpoint(currentHost)) {
        try {
          switchToWriterConnection();
          LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromReaderToWriter",
              new Object[] {writeEndpointHostSpec.getUrl()}));
        } catch (final SQLException e) {
          logAndThrowException(Messages.get("ReadWriteSplittingPlugin.errorSwitchingToWriter"),
              e);
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

  private void logAndThrowException(
      final String logMessage, final Throwable cause)
      throws SQLException {
    LOGGER.fine(logMessage);
    throw new ReadWriteSplittingSQLException(logMessage, SqlState.CONNECTION_UNABLE_TO_CONNECT.getState(), cause);
  }

  private void switchToReaderConnection() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isReadEndpoint(currentHost) && isConnectionUsable(currentConnection)) {
      // Already connected to the read-only endpoint.
      return;
    }

    this.inReadWriteSplit = true;
    if (!isConnectionUsable(this.readerConnection)) {
      initializeReaderConnection();
    } else {
      try {
        switchCurrentConnectionTo(this.readerConnection, this.readEndpointHostSpec);
        LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
            new Object[] {this.readEndpointHostSpec.getUrl()}));
      } catch (SQLException e) {
        if (e.getMessage() != null) {
          LOGGER.warning(
              () -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReaderWithCause",
                  new Object[] {this.readEndpointHostSpec.getUrl(), e.getMessage()}));
        } else {
          LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.errorSwitchingToCachedReader",
              new Object[] {this.readEndpointHostSpec.getUrl()}));
        }

        this.readerConnection.close();
        this.readerConnection = null;
        initializeReaderConnection();
      }
    }
  }

  private void switchToWriterConnection() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHost = this.pluginService.getCurrentHostSpec();
    if (isWriteEndpoint(currentHost) && isConnectionUsable(currentConnection)) {
      // Already connected to the cluster/read-write endpoint.
      return;
    }

    this.inReadWriteSplit = true;
    if (!isConnectionUsable(this.writerConnection)) {
      getNewWriterConnection();
    } else {
      switchCurrentConnectionTo(this.writerConnection, this.writeEndpointHostSpec);
    }
  }

  private void initializeReaderConnection() throws SQLException {
    if (readEndpoint == null) {
      LOGGER.warning(() -> Messages.get("SimpleReadWriteSplittingPlugin.noReaderEndpointProvided",
          new Object[] {writeEndpoint}));
      switchToWriterConnection();
    } else {
      getNewReaderConnection();
      LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
          new Object[] {readEndpoint}));
    }
  }

  private void getNewWriterConnection() throws SQLException {
    if (this.writeEndpointHostSpec == null) {
      this.writeEndpointHostSpec = createHostSpec(this.writeEndpoint, HostRole.WRITER);
    }
    final Connection conn = this.pluginService.connect(this.writeEndpointHostSpec, this.properties, this);

    if (this.verifyNewConnections && this.pluginService.getHostRole(conn) != HostRole.WRITER) {
      logAndThrowException(
          Messages.get("SimpleReadWriteSplittingPlugin.writerEndpointConnectedToReader",
              new Object[]{this.writeEndpoint}));
    }

    setWriterConnection(conn, writeEndpointHostSpec);
    switchCurrentConnectionTo(this.writerConnection, writeEndpointHostSpec);
  }

  private void getNewReaderConnection() throws SQLException {
    if (this.readEndpointHostSpec == null) {
      this.readEndpointHostSpec = createHostSpec(this.readEndpoint, HostRole.READER);
    }
    Connection conn = null;
    try {
      conn = this.pluginService.connect(this.readEndpointHostSpec, this.properties, this);
    } catch (final SQLException e) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.failedToConnectToReader",
              new Object[]{this.readEndpoint}),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
    }

    if (conn == null) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.failedToConnectToReader",
              new Object[]{this.readEndpoint}),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
    } else if (this.verifyNewConnections && this.pluginService.getHostRole(conn) != HostRole.READER) {
      // Warn if the reader connection is to a writer. Do not store connection.
      LOGGER.severe(
          Messages.get("SimpleReadWriteSplittingPlugin.readerEndpointConnectedToWriter",
              new Object[]{this.readEndpoint}));
    } else {
      LOGGER.finest(
          () -> Messages.get("ReadWriteSplittingPlugin.successfullyConnectedToReader",
              new Object[]{readEndpointHostSpec.getUrl()}));
      // Stores reader connection for reuse.
      setReaderConnection(conn, readEndpointHostSpec);
    }

    switchCurrentConnectionTo(conn, this.readEndpointHostSpec);
  }

  private void switchCurrentConnectionTo(
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
            newConnectionHost.getUrl()}));
  }

  private HostSpec createHostSpec(String host, HostRole role) {
    HostSpecBuilder hostSpecBuilder = this.hostListProviderService.getHostSpecBuilder();
    return hostSpecBuilder
        .host(host)
        .port(this.hostListProviderService.getCurrentHostSpec().getPort())
        .role(role)
        .availability(HostAvailability.AVAILABLE)
        .build();
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
    this.readerConnection = null;
    this.writerConnection = null;
  }

  void closeConnectionIfIdle(final Connection internalConnection) {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    try {
      if (internalConnection != null
          && internalConnection != currentConnection
          && !internalConnection.isClosed()) {
        internalConnection.close();
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
