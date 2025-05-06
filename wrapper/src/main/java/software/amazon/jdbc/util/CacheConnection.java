package software.amazon.jdbc.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import software.amazon.jdbc.AwsWrapperProperty;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

// Abstraction layer on top of a connection to a remote cache server
public class CacheConnection {
  // Adding support for read and write connection pools to the remote cache server
  private static volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readConnectionPool;
  private static volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writeConnectionPool;
  private static final GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig = createPoolConfig();
  private final String cacheServerAddr;
  private MessageDigest msgHashDigest = null;

  private static final int DEFAULT_POOL_SIZE  = 10;
  private static final int DEFAULT_POOL_MAX_IDLE = 10;
  private static final int DEFAULT_POOL_MIN_IDLE = 0;
  private static final long DEFAULT_MAX_BORROW_WAIT_MS = 50;

  private static final Object READ_LOCK = new Object();
  private static final Object WRITE_LOCK = new Object();

  private static final AwsWrapperProperty CACHE_RW_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRw",
          null,
          "The cache server endpoint address.");

  public CacheConnection(final Properties properties) {
    this.cacheServerAddr = CACHE_RW_ENDPOINT_ADDR.getString(properties);
  }

  /* Here we check if we need to initialise connection pool for read or write to cache.
  With isRead we check if we need to initialise connection pool for read or write to cache.
  If isRead is true, we initialise connection pool for read.
  If isRead is false, we initialise connection pool for write.
   */
  private void initializeCacheConnectionIfNeeded(boolean isRead) {
    if (StringUtils.isNullOrEmpty(cacheServerAddr)) return;
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
    Object lock = isRead ? READ_LOCK : WRITE_LOCK;

    if (cacheConnectionPool == null) {
      synchronized (lock) {
        if ((isRead && readConnectionPool == null) || (!isRead && writeConnectionPool == null)) {
          createConnectionPool(isRead);
        }
      }
    }
  }

  private void createConnectionPool(boolean isRead) {
    ClientResources resources = ClientResources.builder().build();
    try {
      RedisURI redisUriCluster = RedisURI.Builder.redis(cacheServerAddr).
          withPort(6379).withSsl(true).withVerifyPeer(false).build();

      RedisClient client = RedisClient.create(resources, redisUriCluster);
      GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool =
          ConnectionPoolSupport.createGenericObjectPool(
              () -> client.connect(new ByteArrayCodec()),
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
      System.err.println(errorMsg + ": " + e.getMessage());
      throw new RuntimeException(errorMsg, e);
    }
  }

  private static GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> createPoolConfig() {
    GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(DEFAULT_POOL_SIZE);
    poolConfig.setMaxIdle(DEFAULT_POOL_MAX_IDLE);
    poolConfig.setMinIdle(DEFAULT_POOL_MIN_IDLE);
    poolConfig.setMaxWaitMillis(DEFAULT_MAX_BORROW_WAIT_MS);
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
    initializeCacheConnectionIfNeeded(true);
    // get a connection from the read connection pool
    try {
      conn = readConnectionPool.borrowObject();
      return conn.sync().get(computeHashDigest(key.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      if (conn != null) {
        isBroken = true;
      }
      System.err.println("Failed to read from cache: " + e.getMessage());
      return null;
    } finally {
      if (conn != null && readConnectionPool != null) {
        try {
          this.returnConnectionBackToPool(conn, isBroken, true);
        } catch (Exception ex) {
          System.err.println("Error closing read connection: " + ex.getMessage());
        }
      }
    }
  }

  public void writeToCache(String key, byte[] value) {
    boolean isBroken = false;
    initializeCacheConnectionIfNeeded(false);
    // get a connection from the write connection pool
    StatefulRedisConnection<byte[], byte[]> conn = null;
    try {
      conn = writeConnectionPool.borrowObject();
      conn.sync().setex(computeHashDigest(key.getBytes(StandardCharsets.UTF_8)), 300, value);
    } catch (Exception e) {
      if (conn !=  null){
        isBroken = true;
      }
      System.err.println("Failed to write to cache: " + e.getMessage());
    } finally {
      if (conn != null && writeConnectionPool != null) {
        try {
          this.returnConnectionBackToPool(conn, isBroken, false);
        } catch (Exception ex) {
          System.err.println("Error closing write connection: " + ex.getMessage());
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
}
