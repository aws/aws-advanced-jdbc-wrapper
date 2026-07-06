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

import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

public class InternalConnectionPoolService {

  private static final Logger LOGGER = Logger.getLogger(InternalConnectionPoolService.class.getName());

  private static final AtomicReference<@Nullable InternalConnectionPoolService> INSTANCE =
      new AtomicReference<>(null);

  private final Map<String, PooledConnectionProvider> pooledProviderMap = new ConcurrentHashMap<>();

  private InternalConnectionPoolService() {
  }

  public static InternalConnectionPoolService getInstance() {
    final InternalConnectionPoolService instance = INSTANCE.get();
    if (instance != null) {
      return instance;
    }
    final InternalConnectionPoolService newInstance = new InternalConnectionPoolService();
    if (INSTANCE.compareAndSet(null, newInstance)) {
      return newInstance;
    }
    // Another thread won the race and stored its instance, which is therefore non-null.
    final InternalConnectionPoolService current = INSTANCE.get();
    return current != null ? current : newInstance;
  }

  public static void releaseResources() {
    final @Nullable InternalConnectionPoolService instance = INSTANCE.getAndSet(null);
    if (instance != null) {
      instance.pooledProviderMap.values().forEach((x) -> {
        if (x instanceof CanReleaseResources) {
          ((CanReleaseResources) x).releaseResources();
        }
      });
      instance.pooledProviderMap.clear();
    }
  }

  public @Nullable ConnectionProvider getEffectiveConnectionProvider(final Properties props) {

    final String clusterId = RdsHostListProvider.CLUSTER_ID.getString(props);
    // CLUSTER_ID resolves to a non-null value (it has a default), so this guard never triggers in
    // practice; it mirrors ConcurrentHashMap's rejection of null keys used by the prior
    // computeIfAbsent call.
    if (clusterId == null) {
      return null;
    }

    final PooledConnectionProvider existing = this.pooledProviderMap.get(clusterId);
    if (existing != null) {
      return existing;
    }

    final String connectionPoolType = PropertyDefinition.CONNECTION_POOL_TYPE.getString(props);
    if (StringUtils.isNullOrEmpty(connectionPoolType)) {
      return null;
    }
    final PooledConnectionProvider provider;
    switch (connectionPoolType.toLowerCase()) {
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

    // putIfAbsent keeps a single provider per clusterId even under concurrent creation, matching
    // the prior computeIfAbsent semantics.
    final PooledConnectionProvider previous = this.pooledProviderMap.putIfAbsent(clusterId, provider);
    return previous != null ? previous : provider;
  }
}
