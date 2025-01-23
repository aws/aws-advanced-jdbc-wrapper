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
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;

public class ConnectionProviderManager {

  private final ConnectionProvider defaultProvider;
  private final @Nullable ConnectionProvider effectiveConnProvider;

  /**
   * {@link ConnectionProviderManager} constructor.
   *
   * @param defaultProvider the default {@link ConnectionProvider} to use if a non-default
   *                        ConnectionProvider has not been set or the non-default
   *                        ConnectionProvider has been set but does not accept a requested URL
   * @param effectiveConnProvider the non-default {@link ConnectionProvider} to use
   *
   */
  public ConnectionProviderManager(
      final ConnectionProvider defaultProvider,
      final @Nullable ConnectionProvider effectiveConnProvider) {
    this.defaultProvider = defaultProvider;
    this.effectiveConnProvider = effectiveConnProvider;
  }

  /**
   * Setter that can optionally be called to request a non-default {@link ConnectionProvider}. The
   * requested ConnectionProvider will be used to establish future connections unless it does not
   * support a requested URL, in which case the default ConnectionProvider will be used. See
   * {@link ConnectionProvider#acceptsUrl} for more info.
   *
   * @param connProvider the {@link ConnectionProvider} to use to establish new connections
   * @deprecated Use {@link Driver#setCustomConnectionProvider(ConnectionProvider)} instead.
   */
  @Deprecated
  public static void setConnectionProvider(ConnectionProvider connProvider) {
    Driver.setCustomConnectionProvider(connProvider);
  }

  /**
   * Get the {@link ConnectionProvider} to use to establish a connection using the given driver
   * protocol, host details, and properties. If a non-default ConnectionProvider has been set using
   * {@link #setConnectionProvider} and {@link ConnectionProvider#acceptsUrl} returns true, the
   * non-default ConnectionProvider will be returned. Otherwise, the default ConnectionProvider will
   * be returned. See {@link ConnectionProvider#acceptsUrl} for more info.
   *
   * @param driverProtocol the driver protocol that will be used to establish the connection
   * @param host           the host info for the connection that will be established
   * @param props          the connection properties for the connection that will be established
   * @return the {@link ConnectionProvider} to use to establish a connection using the given driver
   *     protocol, host details, and properties
   */
  public ConnectionProvider getConnectionProvider(
      String driverProtocol, HostSpec host, Properties props) {

    final ConnectionProvider customConnectionProvider = Driver.getCustomConnectionProvider();
    if (customConnectionProvider != null && customConnectionProvider.acceptsUrl(driverProtocol, host, props)) {
      return customConnectionProvider;
    }

    if (this.effectiveConnProvider != null && this.effectiveConnProvider.acceptsUrl(driverProtocol, host, props)) {
      return this.effectiveConnProvider;
    }

    return defaultProvider;
  }

  /**
   * Get the default {@link ConnectionProvider}.
   *
   * @return the default {@link ConnectionProvider}.
   */
  public ConnectionProvider getDefaultProvider() {
    return defaultProvider;
  }

  /**
   * Returns a boolean indicating if the available {@link ConnectionProvider} instances support the
   * selection of a host with the requested role and strategy via {@link #getHostSpecByStrategy}.
   *
   * @param role     the desired host role
   * @param strategy the strategy that should be used to pick a host (eg "random")
   * @return true if the available {@link ConnectionProvider} instances support the selection of a
   *     host with the requested role and strategy via {@link #getHostSpecByStrategy}. Otherwise,
   *     return false
   */
  public boolean acceptsStrategy(HostRole role, String strategy) {
    final ConnectionProvider customConnectionProvider = Driver.getCustomConnectionProvider();
    if (customConnectionProvider != null && customConnectionProvider.acceptsStrategy(role, strategy)) {
      return true;
    }

    if (this.effectiveConnProvider != null && this.effectiveConnProvider.acceptsStrategy(role, strategy)) {
      return true;
    }

    return this.defaultProvider.acceptsStrategy(role, strategy);
  }

  /**
   * Select a {@link HostSpec} with the desired role from the given hosts using the requested
   * strategy. {@link #acceptsStrategy} should be called first to evaluate if the available
   * {@link ConnectionProvider} instances support the selection of a host with the requested role
   * and strategy.
   *
   * @param hosts    the list of hosts to select from
   * @param role     the desired role of the host - either a writer or a reader
   * @param strategy the strategy that should be used to select a {@link HostSpec} from the host
   *                 list (eg "random")
   * @param props    any properties that are required by the provided strategy to select a host
   * @return a {@link HostSpec} with the requested role
   * @throws SQLException                  if the available {@link ConnectionProvider} instances
   *                                       cannot find a host in the host list matching the
   *                                       requested role or an error occurs while selecting a host
   * @throws UnsupportedOperationException if the available {@link ConnectionProvider} instances do
   *                                       not support the requested strategy
   */
  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy, Properties props)
      throws SQLException, UnsupportedOperationException {
    HostSpec host = null;
    final ConnectionProvider customConnectionProvider = Driver.getCustomConnectionProvider();
    try {
      if (customConnectionProvider != null && customConnectionProvider.acceptsStrategy(role, strategy)) {
        host = customConnectionProvider.getHostSpecByStrategy(hosts, role, strategy, props);
      }
    } catch (UnsupportedOperationException e) {
      // The custom provider does not support the provided strategy, ignore it and try with the other providers.
    }

    if (host != null) {
      return host;
    }

    if (this.effectiveConnProvider != null && this.effectiveConnProvider.acceptsStrategy(role, strategy)) {
      host = this.effectiveConnProvider.getHostSpecByStrategy(hosts, role, strategy, props);
      if (host != null) {
        return host;
      }
    }

    return this.defaultProvider.getHostSpecByStrategy(hosts, role, strategy, props);
  }

  /**
   * Clears the non-default {@link ConnectionProvider} if it has been set. The default
   * ConnectionProvider will be used if the non-default ConnectionProvider has not been set or has
   * been cleared.
   *
   * @deprecated Use {@link Driver#resetCustomConnectionProvider()} instead
   */
  @Deprecated
  public static void resetProvider() {
    Driver.resetCustomConnectionProvider();
  }

  /**
   * Releases any resources held by the available {@link ConnectionProvider} instances.
   */
  public static void releaseResources() {
    final ConnectionProvider customConnectionProvider = Driver.getCustomConnectionProvider();
    if (customConnectionProvider instanceof CanReleaseResources) {
      ((CanReleaseResources) customConnectionProvider).releaseResources();
    }
  }

  /**
   * Sets a custom connection initialization function. It'll be used
   * for every brand-new database connection.
   *
   * @deprecated Use {@link Driver#setConnectionInitFunc(ConnectionInitFunc)} instead
   */
  @Deprecated
  public static void setConnectionInitFunc(final @NonNull ConnectionInitFunc func) {
    Driver.setConnectionInitFunc(func);
  }

  /**
   * Resets a custom connection initialization function.
   *
   * @deprecated Use {@link Driver#resetConnectionInitFunc()} instead
   */
  @Deprecated
  public static void resetConnectionInitFunc() {
    Driver.resetConnectionInitFunc();
  }

  public void initConnection(
      final @Nullable Connection connection,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    final ConnectionInitFunc connectionInitFunc = Driver.getConnectionInitFunc();
    if (connectionInitFunc == null) {
      return;
    }

    connectionInitFunc.initConnection(connection, protocol, hostSpec, props);
  }

  public interface ConnectionInitFunc {
    void initConnection(
        final @Nullable Connection connection,
        final @NonNull String protocol,
        final @NonNull HostSpec hostSpec,
        final @NonNull Properties props) throws SQLException;
  }
}
