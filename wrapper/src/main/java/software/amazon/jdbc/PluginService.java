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
import software.amazon.jdbc.exceptions.ExceptionHandler;

/**
 * Interface for retrieving the current active {@link Connection} and its {@link HostSpec}.
 */
public interface PluginService extends ExceptionHandler {

  Connection getCurrentConnection();

  HostSpec getCurrentHostSpec();

  void setCurrentConnection(final @NonNull Connection connection, final @NonNull HostSpec hostSpec)
      throws SQLException;

  EnumSet<NodeChangeOptions> setCurrentConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec hostSpec,
      @Nullable ConnectionPlugin skipNotificationForThisPlugin)
      throws SQLException;

  List<HostSpec> getHosts();

  HostSpec getInitialConnectionHostSpec();

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
   * Evaluates the host role of the given connection - either a writer or a reader.
   *
   * @param conn a connection to the database instance whose role should be determined
   * @return the role of the given connection - either a writer or a reader
   * @throws SQLException if there is a problem executing or processing the SQL query used to
   *                      determine the host role
   */
  HostRole getHostRole(Connection conn) throws SQLException;

  void setAvailability(Set<String> hostAliases, HostAvailability availability);

  boolean isExplicitReadOnly();

  boolean isReadOnly();

  boolean isInTransaction();

  HostListProvider getHostListProvider();

  void refreshHostList() throws SQLException;

  void refreshHostList(Connection connection) throws SQLException;

  void forceRefreshHostList() throws SQLException;

  void forceRefreshHostList(Connection connection) throws SQLException;

  /**
   * Establishes a connection to the given host using the given properties. If a non-default
   * {@link ConnectionProvider} has been set with
   * {@link ConnectionProviderManager#setConnectionProvider} and
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
   * {@link ConnectionProviderManager#setConnectionProvider}. The default ConnectionProvider will be
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
}
