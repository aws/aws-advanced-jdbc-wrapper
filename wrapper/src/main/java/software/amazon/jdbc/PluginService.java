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

package software.amazon.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Interface for retrieving the current active {@link Connection} and its {@link HostSpec}.
 */
public interface PluginService extends ExceptionHandler {

  Connection getCurrentConnection();

  HostSpec getCurrentHostSpec();

  void setCurrentConnection(final @NonNull Connection connection, final @NonNull HostSpec hostSpec)
      throws SQLException;

  /**
   * Set a new internal connection. While setting a new connection, a notification may be sent to all plugins.
   * See {@link ConnectionPlugin#notifyConnectionChanged(EnumSet)} for more details. A plugin mentioned
   * in parameter skipNotificationForThisPlugin won't be receiving such notification.
   *
   * @param connection the new internal connection.
   * @param hostSpec  the host details for a new internal connection.
   * @param skipNotificationForThisPlugin A reference to a plugin that doesn't need to receive notification
   *                                      about connection change. Usually, a plugin that initiates connection change
   *                                      doesn't need to receive such notification and uses a pointer to
   *                                      itself as a call parameter.
   * @return a set of notification options about this connection switch.
   */
  EnumSet<NodeChangeOptions> setCurrentConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec hostSpec,
      @Nullable ConnectionPlugin skipNotificationForThisPlugin)
      throws SQLException;

  /**
   * Get host information for all hosts in the cluster.
   *
   * @return host information for all hosts in the cluster.
   */
  List<HostSpec> getAllHosts();

  /**
   * Get host information for allowed hosts in the cluster. Certain hosts in the cluster may be disallowed, and these
   * hosts will not be returned by this function. For example, if a custom endpoint is being used, hosts outside the
   * custom endpoint will not be returned.
   *
   * @return host information for allowed hosts in the cluster.
   */
  List<HostSpec> getHosts();

  HostSpec getInitialConnectionHostSpec();

  String getOriginalUrl();

  /**
   * Set the collection of hosts that should be allowed and/or blocked for connections.
   *
   * @param allowedAndBlockedHosts An object defining the allowed and blocked sets of hosts.
   */
  void setAllowedAndBlockedHosts(AllowedAndBlockedHosts allowedAndBlockedHosts);

  /**
   * Returns a boolean indicating if the available {@link ConnectionProvider} or
   * {@link ConnectionPlugin} instances support the selection of a host with the requested role and
   * strategy via {@link #getHostSpecByStrategy}.
   *
   * @param role     the desired host role
   * @param strategy the strategy that should be used to pick a host (eg "random")
   * @return true if the available {@link ConnectionProvider} or {@link ConnectionPlugin} instances
   *     support the selection of a host with the requested role and strategy via
   *     {@link #getHostSpecByStrategy}. Otherwise, return false.
   */
  boolean acceptsStrategy(HostRole role, String strategy) throws SQLException;

  /**
   * Selects a {@link HostSpec} with the requested role from available hosts using the requested
   * strategy. {@link #acceptsStrategy} should be called first to evaluate if the available
   * {@link ConnectionProvider} or {@link ConnectionPlugin} instances support the selection of a
   * host with the requested role and strategy.
   *
   * @param role     the desired role of the host - either a writer or a reader
   * @param strategy the strategy that should be used to select a {@link HostSpec} from the
   *                 available hosts (eg "random")
   * @return a {@link HostSpec} with the requested role
   * @throws SQLException                  if the available {@link ConnectionProvider} or
   *                                       {@link ConnectionPlugin} instances do not cannot find a
   *                                       host matching the requested role or an error occurs while
   *                                       selecting a host
   * @throws UnsupportedOperationException if the available {@link ConnectionProvider} or
   *                                       {@link ConnectionPlugin} instances do not support the
   *                                       requested strategy
   */
  HostSpec getHostSpecByStrategy(HostRole role, String strategy)
      throws SQLException, UnsupportedOperationException;

  /**
   * Selects a {@link HostSpec} with the requested role from available hosts using the requested
   * strategy. {@link #acceptsStrategy} should be called first to evaluate if the available
   * {@link ConnectionProvider} or {@link ConnectionPlugin} instances support the selection of a
   * host with the requested role and strategy.
   *
   * @param hosts    the list of {@link HostSpec} from which a {@link HostSpec} will be selected from
   * @param role     the desired role of the host - either a writer or a reader
   * @param strategy the strategy that should be used to select a {@link HostSpec} from the
   *                 available hosts (eg "random")
   * @return a {@link HostSpec} with the requested role
   * @throws SQLException                  if the available {@link ConnectionProvider} or
   *                                       {@link ConnectionPlugin} instances do not cannot find a
   *                                       host matching the requested role or an error occurs while
   *                                       selecting a host
   * @throws UnsupportedOperationException if the available {@link ConnectionProvider} or
   *                                       {@link ConnectionPlugin} instances do not support the
   *                                       requested strategy
   */
  HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy)
      throws SQLException, UnsupportedOperationException;

  /**
   * Evaluates the host role of the given connection - either a writer or a reader.
   *
   * @param conn a connection to the database instance whose role should be determined
   * @return the role of the given connection - either a writer or a reader
   * @throws SQLException if there is a problem executing or processing the SQL query used to
   *                      determine the host role
   */
  HostRole getHostRole(Connection conn) throws SQLException;

  void setAvailability(Set<String> hostAliases, HostAvailability availability);

  boolean isInTransaction();

  HostListProvider getHostListProvider();

  void refreshHostList() throws SQLException;

  void refreshHostList(Connection connection) throws SQLException;

  void forceRefreshHostList() throws SQLException;

  void forceRefreshHostList(Connection connection) throws SQLException;

  /**
   * Initiates a topology update.
   *
   * @param shouldVerifyWriter true, if a caller expects to get topology with the latest confirmed writer
   * @param timeoutMs timeout in msec to wait until topology gets refreshed and a new (or existing) writer is
   *                  confirmed (if shouldVerifyWriter has a value of <code>true</code>).
   * @return true, if successful. False, if operation is unsuccessful or timeout is reached
   * @throws SQLException if there was an error establishing a connection or fetching a topology
   */
  boolean forceRefreshHostList(final boolean shouldVerifyWriter, final long timeoutMs) throws SQLException;

  Connection connect(HostSpec hostSpec, Properties props, final @Nullable ConnectionPlugin pluginToSkip)
      throws SQLException;

  /**
   * Establishes a connection to the given host using the given properties. If a non-default
   * {@link ConnectionProvider} has been set with
   * {@link Driver#setCustomConnectionProvider(ConnectionProvider)} and
   * {@link ConnectionProvider#acceptsUrl(String, HostSpec, Properties)} returns true for the
   * desired protocol, host, and properties, the connection will be created by the non-default
   * ConnectionProvider. Otherwise, the connection will be created by the default
   * ConnectionProvider. The default ConnectionProvider will be {@link DriverConnectionProvider} for
   * connections requested via the {@link java.sql.DriverManager} and
   * {@link DataSourceConnectionProvider} for connections requested via an
   * {@link software.amazon.jdbc.ds.AwsWrapperDataSource}.
   *
   * @param hostSpec the host details for the desired connection
   * @param props    the connection properties
   * @return a {@link Connection} to the requested host
   * @throws SQLException if there was an error establishing a {@link Connection} to the requested
   *                      host
   */
  Connection connect(HostSpec hostSpec, Properties props) throws SQLException;

  /**
   * Establishes a connection to the given host using the given properties. This call differs from
   * {@link ConnectionPlugin#connect} in that the default {@link ConnectionProvider} will be used to
   * establish the connection even if a non-default ConnectionProvider has been set via
   * {@link Driver#setCustomConnectionProvider(ConnectionProvider)}. The default ConnectionProvider will be
   * {@link DriverConnectionProvider} for connections requested via the
   * {@link java.sql.DriverManager} and {@link DataSourceConnectionProvider} for connections
   * requested via an {@link software.amazon.jdbc.ds.AwsWrapperDataSource}.
   *
   * @param hostSpec the host details for the desired connection
   * @param props    the connection properties
   * @return a {@link Connection} to the requested host
   * @throws SQLException if there was an error establishing a {@link Connection} to the requested
   *                      host
   */
  Connection forceConnect(HostSpec hostSpec, Properties props) throws SQLException;

  Connection forceConnect(
      HostSpec hostSpec, Properties props, final @Nullable ConnectionPlugin pluginToSkip) throws SQLException;

  Dialect getDialect();

  TargetDriverDialect getTargetDriverDialect();

  void updateDialect(final @NonNull Connection connection) throws SQLException;

  @Nullable HostSpec identifyConnection(final Connection connection) throws SQLException;

  void fillAliases(final Connection connection, final HostSpec hostSpec) throws SQLException;

  HostSpecBuilder getHostSpecBuilder();

  @Deprecated
  ConnectionProvider getConnectionProvider();

  boolean isPooledConnectionProvider(HostSpec host, Properties props);

  String getDriverProtocol();

  Properties getProperties();

  TelemetryFactory getTelemetryFactory();

  String getTargetName();

  @NonNull SessionStateService getSessionStateService();

  <T> T getPlugin(final Class<T> pluginClazz);

  <T> void setStatus(final Class<T> clazz, final @Nullable T status, final boolean clusterBound);

  <T> void setStatus(final Class<T> clazz, final @Nullable T status, final String key);

  <T> T getStatus(final @NonNull Class<T> clazz, final boolean clusterBound);

  <T> T getStatus(final @NonNull Class<T> clazz, final String key);

  boolean isPluginInUse(final Class<? extends ConnectionPlugin> pluginClazz);
}
