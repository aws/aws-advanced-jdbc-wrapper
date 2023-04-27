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
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Interface for connection plugins. This class implements ways to execute a JDBC method and to clean up resources used
 * before closing the plugin.
 */
public interface ConnectionPlugin {

  Set<String> getSubscribedMethods();

  <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E;

  /**
   * Establishes a connection to the given host using the given driver protocol and properties. If a
   * non-default {@link ConnectionProvider} has been set with
   * {@link ConnectionProviderManager#setConnectionProvider} and
   * {@link ConnectionProvider#acceptsUrl(String, HostSpec, Properties)} returns true for the given
   * protocol, host, and properties, the connection will be created by the non-default
   * ConnectionProvider. Otherwise, the connection will be created by the default
   * ConnectionProvider. The default ConnectionProvider will be {@link DriverConnectionProvider} for
   * connections requested via the {@link java.sql.DriverManager} and
   * {@link DataSourceConnectionProvider} for connections requested via an
   * {@link software.amazon.jdbc.ds.AwsWrapperDataSource}.
   *
   * @param driverProtocol      the driver protocol that should be used to establish the connection
   * @param hostSpec            the host details for the desired connection
   * @param props               the connection properties
   * @param isInitialConnection a boolean indicating whether the current {@link Connection} is
   *                            establishing an initial physical connection to the database or has
   *                            already established a physical connection in the past
   * @param connectFunc         the function to call to continue the connect request down the
   *                            connect pipeline
   * @return a {@link Connection} to the requested host
   * @throws SQLException if there was an error establishing a {@link Connection} to the requested
   *                      host
   */
  Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException;

  /**
   * Establishes a connection to the given host using the given driver protocol and properties. This
   * call differs from {@link ConnectionPlugin#connect} in that the default
   * {@link ConnectionProvider} will be used to establish the connection even if a non-default
   * ConnectionProvider has been set via {@link ConnectionProviderManager#setConnectionProvider}.
   * The default ConnectionProvider will be {@link DriverConnectionProvider} for connections
   * requested via the {@link java.sql.DriverManager} and {@link DataSourceConnectionProvider} for
   * connections requested via an {@link software.amazon.jdbc.ds.AwsWrapperDataSource}.
   *
   * @param driverProtocol      the driver protocol that should be used to establish the connection
   * @param hostSpec            the host details for the desired connection
   * @param props               the connection properties
   * @param isInitialConnection a boolean indicating whether the current {@link Connection} is
   *                            establishing an initial physical connection to the database or has
   *                            already established a physical connection in the past
   * @param forceConnectFunc    the function to call to continue the forceConnect request down the
   *                            forceConnect pipeline
   * @return a {@link Connection} to the requested host
   * @throws SQLException if there was an error establishing a {@link Connection} to the requested
   *                      host
   */
  Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException;

  /**
   * Returns a boolean indicating if this {@link ConnectionPlugin} implements the specified host
   * selection strategy for the given role in {@link #getHostSpecByStrategy}.
   *
   * @param role     the desired host role
   * @param strategy the strategy that should be used to pick a host (eg "random")
   * @return true if this {@link ConnectionPlugin} supports the selection of a host with the requested role and strategy
   *     via {@link #getHostSpecByStrategy}. Otherwise, return false.
   */
  boolean acceptsStrategy(final HostRole role, final String strategy);

  /**
   * Selects a {@link HostSpec} with the requested role from available hosts using the requested
   * strategy. {@link #acceptsStrategy} should be called first to evaluate if this
   * {@link ConnectionPlugin} supports the selection of a host with the requested role and
   * strategy.
   *
   * @param role     the desired role of the host - either a writer or a reader
   * @param strategy the strategy that should be used to select a {@link HostSpec} from the
   *                 available hosts (eg "random")
   * @return a {@link HostSpec} with the requested role
   * @throws SQLException                  if the available hosts do not contain any hosts matching
   *                                       the requested role or an error occurs while selecting a
   *                                       host
   * @throws UnsupportedOperationException if this {@link ConnectionPlugin} does not support the
   *                                       requested strategy
   */
  HostSpec getHostSpecByStrategy(final HostRole role, final String strategy)
      throws SQLException, UnsupportedOperationException;

  void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException;

  OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes);

  void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes);
}
