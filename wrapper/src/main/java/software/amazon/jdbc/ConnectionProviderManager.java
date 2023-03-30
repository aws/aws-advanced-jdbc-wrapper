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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import software.amazon.jdbc.cleanup.CanReleaseResources;

public class ConnectionProviderManager {

  private static final ReentrantReadWriteLock connProviderLock = new ReentrantReadWriteLock();
  private static ConnectionProvider connProvider = null;
  private final ConnectionProvider defaultProvider;

  public ConnectionProviderManager(ConnectionProvider defaultProvider) {
    this.defaultProvider = defaultProvider;
  }

  public static void setConnectionProvider(ConnectionProvider connProvider) {
    connProviderLock.writeLock().lock();
    try {
      ConnectionProviderManager.connProvider = connProvider;
    } finally {
      connProviderLock.writeLock().unlock();
    }
  }

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

  public ConnectionProvider getDefaultProvider() {
    return defaultProvider;
  }

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

  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy) throws SQLException {
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

  public static void resetProvider() {
    if (connProvider != null) {
      connProviderLock.writeLock().lock();
      connProvider = null;
      connProviderLock.writeLock().unlock();
    }
  }

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
