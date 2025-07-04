package software.amazon.jdbc.plugin.cache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import software.amazon.jdbc.AwsWrapperProperty;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.StringUtils;

// Abstraction layer on top of a connection to a remote cache server
public class CacheConnection {
  private static final Logger LOGGER = Logger.getLogger(CacheConnection.class.getName());
  // Adding support for read and write connection pools to the remote cache server
  private static volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readConnectionPool;
  private static volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writeConnectionPool;
  private static final GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig = createPoolConfig();
  private final String cacheRwServerAddr; // read-write cache server
  private final String cacheRoServerAddr; // read-only cache server
  private MessageDigest msgHashDigest = null;

  private static final int DEFAULT_POOL_SIZE  = 10;
  private static final int DEFAULT_POOL_MAX_IDLE = 10;
  private static final int DEFAULT_POOL_MIN_IDLE = 0;
  private static final long DEFAULT_MAX_BORROW_WAIT_MS = 50;

  private static final ReentrantLock READ_LOCK = new ReentrantLock();
  private static final ReentrantLock WRITE_LOCK = new ReentrantLock();

  protected static final AwsWrapperProperty CACHE_RW_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRw",
          null,
          "The cache read-write server endpoint address.");

  private static final AwsWrapperProperty CACHE_RO_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRo",
          null,
          "The cache read-only server endpoint address.");

  protected static final AwsWrapperProperty CACHE_USE_SSL =
      new AwsWrapperProperty(
          "cacheUseSSL",
          "true",
          "Whether to use SSL for cache connections.");

  private final boolean useSSL;

  static {
    PropertyDefinition.registerPluginProperties(CacheConnection.class);
  }

  public CacheConnection(final Properties properties) {
    this.cacheRwServerAddr = CACHE_RW_ENDPOINT_ADDR.getString(properties);
    this.cacheRoServerAddr = CACHE_RO_ENDPOINT_ADDR.getString(properties);
    this.useSSL = Boolean.parseBoolean(CACHE_USE_SSL.getString(properties));
  }

  /* Here we check if we need to initialise connection pool for read or write to cache.
  With isRead we check if we need to initialise connection pool for read or write to cache.
  If isRead is true, we initialise connection pool for read.
  If isRead is false, we initialise connection pool for write.
   */
  private void initializeCacheConnectionIfNeeded(boolean isRead) {
    if (StringUtils.isNullOrEmpty(cacheRwServerAddr)) return;
    // Initialize the message digest
    if (msgHashDigest == null) {
      try {
        msgHashDigest = MessageDigest.getInstance("SHA-384");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("SHA-384 not supported", e);
      }
    }

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> cacheConnectionPool =
        isRead ? readConnectionPool : writeConnectionPool;
    if (cacheConnectionPool == null) {
      ReentrantLock connectionPoolLock = isRead ? READ_LOCK : WRITE_LOCK;
      connectionPoolLock.lock();
      try {
        if ((isRead && readConnectionPool == null) || (!isRead && writeConnectionPool == null)) {
          createConnectionPool(isRead);
        }
      } finally {
        connectionPoolLock.unlock();
      }
    }
  }

  private void createConnectionPool(boolean isRead) {
    ClientResources resources = ClientResources.builder().build();
    try {
      // cache server addr string is in the format "<server hostname>:<port>"
      String serverAddr = cacheRwServerAddr;
      // If read-only server is specified, use it for the read-only connections
      if (isRead && !StringUtils.isNullOrEmpty(cacheRoServerAddr)) {
        serverAddr = cacheRoServerAddr;
      }
      String[] hostnameAndPort = serverAddr.split(":");
      RedisURI redisUriCluster = RedisURI.Builder.redis(hostnameAndPort[0])
          .withPort(Integer.parseInt(hostnameAndPort[1]))
          .withSsl(useSSL).withVerifyPeer(false).withLibraryName("aws-jdbc-lettuce").build();

      RedisClient client = RedisClient.create(resources, redisUriCluster);
      GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool =
          ConnectionPoolSupport.createGenericObjectPool(
              () -> {
                StatefulRedisConnection<byte[], byte[]> connection = client.connect(new ByteArrayCodec());
                // In cluster mode, we need to send READONLY command to the server for reading from replica.
                // Note: we gracefully ignore ERR reply to support non cluster mode.
                if (isRead) {
                  try {
                    connection.sync().readOnly();
                  } catch (RedisCommandExecutionException e) {
                    if (e.getMessage().contains("ERR This instance has cluster support disabled")) {
                      LOGGER.fine("------ Note: this cache cluster has cluster support disabled ------");
                    } else {
                      throw e;
                    }
                  }
                }
                return connection;
              },
              poolConfig
          );

      if (isRead) {
        readConnectionPool = pool;
      } else {
        writeConnectionPool = pool;
      }

    } catch (Exception e) {
      String poolType = isRead ? "read" : "write";
      String errorMsg = String.format("Failed to create Cache %s connection pool", poolType);
      LOGGER.warning(errorMsg + ": " + e.getMessage());
      throw new RuntimeException(errorMsg, e);
    }
  }

  private static GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> createPoolConfig() {
    GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(DEFAULT_POOL_SIZE);
    poolConfig.setMaxIdle(DEFAULT_POOL_MAX_IDLE);
    poolConfig.setMinIdle(DEFAULT_POOL_MIN_IDLE);
    poolConfig.setMaxWait(Duration.ofMillis(DEFAULT_MAX_BORROW_WAIT_MS));
    return poolConfig;
  }

  // Get the hash digest of the given key.
  private byte[] computeHashDigest(byte[] key) {
    msgHashDigest.update(key);
    return msgHashDigest.digest();
  }

  public byte[] readFromCache(String key) {
    boolean isBroken = false;
    StatefulRedisConnection<byte[], byte[]> conn = null;
    // get a connection from the read connection pool
    try {
      initializeCacheConnectionIfNeeded(true);
      conn = readConnectionPool.borrowObject();
      return conn.sync().get(computeHashDigest(key.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      if (conn != null) {
        isBroken = true;
      }
      LOGGER.warning("Failed to read result from cache. Treating it as a cache miss: " + e.getMessage());
      return null;
    } finally {
      if (conn != null && readConnectionPool != null) {
        try {
          this.returnConnectionBackToPool(conn, isBroken, true);
        } catch (Exception ex) {
          LOGGER.warning("Error closing read connection: " + ex.getMessage());
        }
      }
    }
  }

  protected void handleCompletedCacheWrite(StatefulRedisConnection<byte[], byte[]> conn, Throwable ex) {
    // Note: this callback upon completion of cache write is on a different thread
    if (ex != null) {
      LOGGER.warning("Failed to write to cache: " + ex.getMessage());
      if (writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, true, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning broken write connection back to pool: " + e.getMessage());
        }
      }
    } else {
      if (writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, false, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning write connection back to pool: " + e.getMessage());
        }
      }
    }
  }

  public void writeToCache(String key, byte[] value, int expiry) {
    StatefulRedisConnection<byte[], byte[]> conn = null;
    try {
      initializeCacheConnectionIfNeeded(false);
      // get a connection from the write connection pool
      conn = writeConnectionPool.borrowObject();
      // Write to the cache is async.
      RedisAsyncCommands<byte[], byte[]> asyncCommands = conn.async();
      byte[] keyHash = computeHashDigest(key.getBytes(StandardCharsets.UTF_8));
      StatefulRedisConnection<byte[], byte[]> finalConn = conn;
      asyncCommands.set(keyHash, value, SetArgs.Builder.ex(expiry))
          .whenComplete((result, exception) -> handleCompletedCacheWrite(finalConn, exception));
    } catch (Exception e) {
      // Failed to trigger the async write to the cache, return the cache connection to the pool as broken
      LOGGER.warning("Failed to write to cache: " + e.getMessage());
      if (conn != null && writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, true, false);
        } catch (Exception ex) {
          LOGGER.warning("Error closing write connection: " + ex.getMessage());
        }
      }
    }
  }

  private void returnConnectionBackToPool(StatefulRedisConnection <byte[], byte[]> connection, boolean isConnectionBroken, boolean isRead) {
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool = isRead ? readConnectionPool : writeConnectionPool;
    if (isConnectionBroken) {
        try {
          pool.invalidateObject(connection);
        } catch (Exception e) {
          throw new RuntimeException("Could not invalidate connection for the pool", e);
        }
    } else {
        pool.returnObject(connection);
    }
  }

  // Used for unit testing only
  protected void setConnectionPools(GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool,
      GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool) {
    readConnectionPool = readPool;
    writeConnectionPool = writePool;
  }
}
