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

import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import software.amazon.jdbc.cleanup.CanReleaseResources;

public class ConnectionProviderManager {

  private static final ReentrantReadWriteLock connProviderLock = new ReentrantReadWriteLock();
  private static ConnectionProvider connProvider = null;
  private final ConnectionProvider defaultProvider;

  /**
   * {@link ConnectionProviderManager} constructor.
   *
   * @param defaultProvider the default {@link ConnectionProvider} to use if a non-default
   *                        ConnectionProvider has not been set or the non-default
   *                        ConnectionProvider has been set but does not accept a requested URL
   */
  public ConnectionProviderManager(ConnectionProvider defaultProvider) {
    this.defaultProvider = defaultProvider;
  }

  /**
   * Setter that can optionally be called to request a non-default {@link ConnectionProvider}. The
   * requested ConnectionProvider will be used to establish future connections unless it does not
   * support a requested URL, in which case the default ConnectionProvider will be used. See
   * {@link ConnectionProvider#acceptsUrl} for more info.
   *
   * @param connProvider the {@link ConnectionProvider} to use to establish new connections
   */
  public static void setConnectionProvider(ConnectionProvider connProvider) {
    connProviderLock.writeLock().lock();
    try {
      ConnectionProviderManager.connProvider = connProvider;
    } finally {
      connProviderLock.writeLock().unlock();
    }
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
    if (connProvider != null) {
      connProviderLock.readLock().lock();
      try {
        if (connProvider != null && connProvider.acceptsUrl(driverProtocol, host, props)) {
          return connProvider;
        }
      } finally {
        connProviderLock.readLock().unlock();
      }
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
    boolean acceptsStrategy = false;
    if (connProvider != null) {
      connProviderLock.readLock().lock();
      try {
        if (connProvider != null) {
          acceptsStrategy = connProvider.acceptsStrategy(role, strategy);
        }
      } finally {
        connProviderLock.readLock().unlock();
      }
    }

    if (!acceptsStrategy) {
      acceptsStrategy = defaultProvider.acceptsStrategy(role, strategy);
    }

    return acceptsStrategy;
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
   * @return a {@link HostSpec} with the requested role
   * @throws SQLException                  if the available {@link ConnectionProvider} instances
   *                                       cannot find a host in the host list matching the
   *                                       requested role or an error occurs while selecting a host
   * @throws UnsupportedOperationException if the available {@link ConnectionProvider} instances do
   *                                       not support the requested strategy
   */
  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy)
      throws SQLException, UnsupportedOperationException {
    HostSpec host = null;
    if (connProvider != null) {
      connProviderLock.readLock().lock();
      try {
        if (connProvider != null && connProvider.acceptsStrategy(role, strategy)) {
          host = connProvider.getHostSpecByStrategy(hosts, role, strategy);
        }
      } catch (UnsupportedOperationException e) {
        // The custom provider does not support the provided strategy, ignore it and try with the default provider.
      } finally {
        connProviderLock.readLock().unlock();
      }
    }

    if (host == null) {
      host = defaultProvider.getHostSpecByStrategy(hosts, role, strategy);
    }

    return host;
  }

  /**
   * Clears the non-default {@link ConnectionProvider} if it has been set. The default
   * ConnectionProvider will be used if the non-default ConnectionProvider has not been set or has
   * been cleared.
   */
  public static void resetProvider() {
    if (connProvider != null) {
      connProviderLock.writeLock().lock();
      connProvider = null;
      connProviderLock.writeLock().unlock();
    }
  }

  /**
   * Releases any resources held by the available {@link ConnectionProvider} instances.
   */
  public static void releaseResources() {
    if (connProvider != null) {
      connProviderLock.writeLock().lock();
      try {
        if (connProvider instanceof CanReleaseResources) {
          ((CanReleaseResources) connProvider).releaseResources();
        }
      } finally {
        connProviderLock.writeLock().unlock();
      }
    }
  }
}
