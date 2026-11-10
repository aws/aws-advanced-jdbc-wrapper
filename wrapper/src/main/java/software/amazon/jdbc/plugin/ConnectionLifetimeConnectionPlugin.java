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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.WrapperUtils;

public class ConnectionLifetimeConnectionPlugin extends AbstractConnectionPlugin {

  public static final AwsWrapperProperty CONNECTION_LIFETIME_MS =
      new AwsWrapperProperty(
              "connectionLifetimeMs",
              null,
              Messages.get("ConnectionLifetimeConnectionPlugin.connectionLifetimeMsDescription"),
              true)
          .withBounds(1L, null);

  public static final AwsWrapperProperty CONNECTION_LIFETIME_ACTION =
      new AwsWrapperProperty(
          "connectionLifetimeAction",
          "reconnect",
          Messages.get("ConnectionLifetimeConnectionPlugin.connectionLifetimeActionDescription"),
          false,
          new String[] {"close", "reconnect"});

  private static final Logger LOGGER =
      Logger.getLogger(ConnectionLifetimeConnectionPlugin.class.getName());

  static {
    PropertyDefinition.registerPluginProperties(ConnectionLifetimeConnectionPlugin.class);
  }

  private enum LifetimeAction {
    CLOSE,
    RECONNECT
  }

  private final PluginService pluginService;
  private final Properties properties;
  private final LongSupplier nanoTimeSupplier;
  private final long connectionLifetimeNanos;
  private final LifetimeAction action;
  private final Set<String> networkBoundMethods;
  private final Set<String> subscribedMethods;
  private final Map<Connection, Long> connectionCreationTimes = new IdentityHashMap<>();

  private @Nullable HostSpec originalHostSpec;
  private @Nullable Connection timedConnection;
  private @Nullable Long connectionCreationTimeNanos;
  private boolean terminal;

  public ConnectionLifetimeConnectionPlugin(
      final FullServicesContainer servicesContainer, final Properties properties) {
    this(servicesContainer, properties, System::nanoTime);
  }

  ConnectionLifetimeConnectionPlugin(
      final FullServicesContainer servicesContainer,
      final Properties properties,
      final LongSupplier nanoTimeSupplier) {
    this.pluginService = servicesContainer.getPluginService();
    this.properties = properties;
    this.nanoTimeSupplier = nanoTimeSupplier;
    this.connectionLifetimeNanos = TimeUnit.MILLISECONDS.toNanos(getConnectionLifetimeMs(properties));
    this.action = getLifetimeAction(properties);

    this.networkBoundMethods = Collections.unmodifiableSet(
        new HashSet<>(this.pluginService.getTargetDriverDialect().getNetworkBoundMethodNames(properties)));
    final Set<String> methods = new HashSet<>(this.networkBoundMethods);
    methods.add(JdbcMethod.CONNECT.methodName);
    methods.add(JdbcMethod.INITHOSTPROVIDER.methodName);
    methods.add(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName);
    this.subscribedMethods = Collections.unmodifiableSet(methods);
  }

  private static long getConnectionLifetimeMs(final Properties properties) {
    try {
      return CONNECTION_LIFETIME_MS.getLong(properties);
    } catch (final RuntimeException exception) {
      throw new IllegalArgumentException(
          Messages.get(
              "ConnectionLifetimeConnectionPlugin.invalidConnectionLifetime",
              new Object[] {properties.get(CONNECTION_LIFETIME_MS.name)}),
          exception);
    }
  }

  private static LifetimeAction getLifetimeAction(final Properties properties) {
    final String configuredAction = CONNECTION_LIFETIME_ACTION.getString(properties);
    if (configuredAction != null) {
      try {
        return LifetimeAction.valueOf(configuredAction.trim().toUpperCase(Locale.ROOT));
      } catch (final IllegalArgumentException exception) {
        throw new IllegalArgumentException(
            Messages.get(
                "ConnectionLifetimeConnectionPlugin.invalidConnectionLifetimeAction",
                new Object[] {configuredAction}),
            exception);
      }
    }

    throw new IllegalArgumentException(
        Messages.get(
            "ConnectionLifetimeConnectionPlugin.invalidConnectionLifetimeAction",
            new Object[] {null}));
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
    initHostProviderFunc.call();
    this.originalHostSpec = this.pluginService.getInitialConnectionHostSpec();
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    final Connection connection = connectFunc.call();
    recordConnectionCreationTime(connection);
    return connection;
  }

  private void recordConnectionCreationTime(final @Nullable Connection connection) {
    if (connection != null) {
      this.connectionCreationTimes.put(connection, this.nanoTimeSupplier.getAsLong());
    }
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(
      final EnumSet<NodeChangeOptions> changes) {
    if (changes.contains(NodeChangeOptions.INITIAL_CONNECTION)
        || changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED)) {
      selectCurrentConnectionTimestamp();
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
      final @Nullable Object[] jdbcMethodArgs)
      throws E {
    if (!this.networkBoundMethods.contains(methodName)) {
      return jdbcMethodFunc.call();
    }

    if (this.terminal) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, newConnectionClosedException(null));
    }

    if (!isCurrentConnectionExpired()) {
      return jdbcMethodFunc.call();
    }

    if (this.pluginService.isInTransaction() || this.pluginService.isXaTransactionActive()) {
      LOGGER.fine(
          () -> Messages.get(
              "ConnectionLifetimeConnectionPlugin.actionDeferred",
              new Object[] {methodName}));
      return jdbcMethodFunc.call();
    }

    if (this.action == LifetimeAction.CLOSE) {
      throw WrapperUtils.wrapExceptionIfNeeded(
          exceptionClass, closeCurrentConnection());
    }

    throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, reconnect());
  }

  private void selectCurrentConnectionTimestamp() {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    Long creationTime = this.connectionCreationTimes.get(currentConnection);
    if (creationTime == null) {
      creationTime = this.nanoTimeSupplier.getAsLong();
      this.connectionCreationTimes.put(currentConnection, creationTime);
    }

    this.timedConnection = currentConnection;
    this.connectionCreationTimeNanos = creationTime;
  }

  private boolean isCurrentConnectionExpired() {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    if (currentConnection != this.timedConnection || this.connectionCreationTimeNanos == null) {
      selectCurrentConnectionTimestamp();
    }

    final Long creationTime = this.connectionCreationTimeNanos;
    return creationTime != null
        && this.nanoTimeSupplier.getAsLong() - creationTime >= this.connectionLifetimeNanos;
  }

  private ConnectionLifetimeSQLException closeCurrentConnection() {
    this.terminal = true;
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    SQLException closeFailure = null;

    try {
      currentConnection.close();
    } catch (final SQLException exception) {
      closeFailure = exception;
      LOGGER.warning(
          () -> Messages.get(
              "ConnectionLifetimeConnectionPlugin.closeConnectionFailed",
              new Object[] {exception.getMessage()}));
    }

    return newConnectionClosedException(closeFailure);
  }

  private ConnectionLifetimeSQLException newConnectionClosedException(
      final @Nullable Throwable cause) {
    final String message = Messages.get("ConnectionLifetimeConnectionPlugin.connectionClosed");
    return cause == null
        ? new ConnectionLifetimeSQLException(message, SqlState.CONNECTION_NOT_OPEN.getState())
        : new ConnectionLifetimeSQLException(
            message, SqlState.CONNECTION_NOT_OPEN.getState(), cause);
  }

  private ConnectionLifetimeSQLException reconnect() {
    final Connection previousConnection = this.pluginService.getCurrentConnection();
    Connection replacementConnection = null;
    try {
      final HostSpec reconnectHostSpec = this.originalHostSpec;
      if (reconnectHostSpec == null) {
        throw new SQLException(
            Messages.get("ConnectionLifetimeConnectionPlugin.originalHostUnavailable"));
      }

      LOGGER.fine(
          () -> Messages.get(
              "ConnectionLifetimeConnectionPlugin.reconnecting",
              new Object[] {reconnectHostSpec.getHostAndPort()}));
      replacementConnection = this.pluginService.connect(reconnectHostSpec, this.properties, this);
      if (replacementConnection == null) {
        throw new SQLException(
            Messages.get("ConnectionLifetimeConnectionPlugin.reconnectReturnedNull"));
      }
      final long replacementCreationTime = this.nanoTimeSupplier.getAsLong();

      this.pluginService.setCurrentConnection(replacementConnection, reconnectHostSpec, this);
      removeClosedConnectionCreationTime(previousConnection);
      this.connectionCreationTimes.put(replacementConnection, replacementCreationTime);
      this.timedConnection = replacementConnection;
      this.connectionCreationTimeNanos = replacementCreationTime;
    } catch (final Exception exception) {
      cleanupFailedReplacement(replacementConnection, previousConnection, exception);
      return new ConnectionLifetimeFailedSQLException(exception);
    }

    return new ConnectionLifetimeSuccessSQLException();
  }

  private void removeClosedConnectionCreationTime(final @Nullable Connection connection) {
    if (connection == null) {
      return;
    }

    boolean closed;
    try {
      closed = connection.isClosed();
    } catch (final SQLException exception) {
      // A connection that cannot report its state is unusable; do not retain it.
      closed = true;
    }

    if (closed) {
      this.connectionCreationTimes.remove(connection);
    }
  }

  private void cleanupFailedReplacement(
      final @Nullable Connection replacementConnection,
      final @Nullable Connection previousConnection,
      final Exception reconnectFailure) {
    if (replacementConnection != null
        && replacementConnection == this.pluginService.getCurrentConnection()
        && replacementConnection != previousConnection) {
      this.terminal = true;
      closeConnectionAfterFailedInstallation(replacementConnection, reconnectFailure);
      closeConnectionAfterFailedInstallation(previousConnection, reconnectFailure);
      return;
    }

    closeUnattachedReplacement(replacementConnection, reconnectFailure);
  }

  private void closeConnectionAfterFailedInstallation(
      final @Nullable Connection connection, final Exception reconnectFailure) {
    if (connection == null) {
      return;
    }

    this.connectionCreationTimes.remove(connection);
    try {
      connection.close();
    } catch (final SQLException closeException) {
      reconnectFailure.addSuppressed(closeException);
      LOGGER.warning(
          () -> Messages.get(
              "ConnectionLifetimeConnectionPlugin.failedInstallationConnectionCloseFailed",
              new Object[] {closeException.getMessage()}));
    }
  }

  private void closeUnattachedReplacement(
      final @Nullable Connection replacementConnection, final Exception reconnectFailure) {
    if (replacementConnection == null
        || replacementConnection == this.pluginService.getCurrentConnection()) {
      return;
    }

    this.connectionCreationTimes.remove(replacementConnection);
    try {
      replacementConnection.close();
    } catch (final SQLException closeException) {
      reconnectFailure.addSuppressed(closeException);
      LOGGER.warning(
          () -> Messages.get(
              "ConnectionLifetimeConnectionPlugin.replacementCloseFailed",
              new Object[] {closeException.getMessage()}));
    }
  }
}
