package software.amazon.jdbc;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
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
