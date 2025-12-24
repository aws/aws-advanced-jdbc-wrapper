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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

public class InternalConnectionPoolService {

  private static final Logger LOGGER = Logger.getLogger(InternalConnectionPoolService.class.getName());

  private static final AtomicReference<InternalConnectionPoolService> INSTANCE = new AtomicReference<>(null);

  private final Map<String, PooledConnectionProvider> pooledProviderMap = new ConcurrentHashMap<>();

  private InternalConnectionPoolService() {
  }

  public static InternalConnectionPoolService getInstance() {
    InternalConnectionPoolService instance = INSTANCE.get();
    if (instance == null) {
      InternalConnectionPoolService newInstance = new InternalConnectionPoolService();
      if (INSTANCE.compareAndSet(null, newInstance)) {
        instance = newInstance;
      } else {
        instance = INSTANCE.get();
      }
    }
    return instance;
  }

  public static void releaseResources() {
    InternalConnectionPoolService instance = INSTANCE.getAndSet(null);
    if (instance != null) {
      instance.pooledProviderMap.values().forEach((x) -> {
        if (x instanceof CanReleaseResources) {
          ((CanReleaseResources) x).releaseResources();
        }
      });
      instance.pooledProviderMap.clear();
    }
  }

  public ConnectionProvider getEffectiveConnectionProvider(final Properties props) {

    final String clusterId = RdsHostListProvider.CLUSTER_ID.getString(props);
    return this.pooledProviderMap.computeIfAbsent(clusterId, (key) -> {
      final String connectionPoolType = PropertyDefinition.CONNECTION_POOL_TYPE.getString(props);
      if (StringUtils.isNullOrEmpty(connectionPoolType)) {
        return null;
      }
      PooledConnectionProvider provider = null;
      switch (connectionPoolType) {
        case "c3p0":
          try {
            provider = WrapperUtils.createInstance(
                "software.amazon.jdbc.C3P0PooledConnectionProvider", PooledConnectionProvider.class);
          } catch (InstantiationException e) {
            throw new RuntimeException("Can't create connection pool provider.", e);
          }
          break;
        case "hikari":
          try {
            provider = WrapperUtils.createInstance(
                "software.amazon.jdbc.HikariPooledConnectionProvider", PooledConnectionProvider.class);
          } catch (InstantiationException e) {
            throw new RuntimeException("Can't create connection pool provider.", e);
          }
          break;
        default:
          throw new RuntimeException("Unknown connection pool type: " + connectionPoolType);
      }

      LOGGER.finest(() -> String.format(
          "[clusterId: %s] Created a new connection pool (%s).", clusterId, connectionPoolType));

      return provider;
    });
  }
}
