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

// Abstraction layer on top of a connection to a remote cache server
public class CacheConnection {
  // TODO: support connection pools to the remote cache server for read and write
  private StatefulRedisConnection<byte[], byte[]> connection = null;
  private final String cacheServerAddr;
  private MessageDigest msgHashDigest = null;

  private static final AwsWrapperProperty CACHE_RW_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRw",
          null,
          "The cache server endpoint address.");

  public CacheConnection(final Properties properties) {
    this.cacheServerAddr = CACHE_RW_ENDPOINT_ADDR.getString(properties);
  }

  private void initializeCacheConnectionIfNeeded() {
    if (StringUtils.isNullOrEmpty(cacheServerAddr)) return;
    // Initialize the message digest
    if (msgHashDigest == null) {
      try {
        msgHashDigest = MessageDigest.getInstance("SHA-384");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("SHA-384 not supported", e);
      }
    }
    // Create a stateful redis connection with TLS enabled
    if (connection == null) {
      System.out.println("Now we are creating a new Redis connection......");
      ClientResources resources = ClientResources.builder().build();
      final RedisURI redisUriCluster = RedisURI.Builder.redis(cacheServerAddr).
          withPort(6379).withSsl(true).withVerifyPeer(false).build();
      RedisClient clusterClient = RedisClient.create(resources, redisUriCluster);
      connection = clusterClient.connect(new ByteArrayCodec());
    }
  }

  // Get the hash digest of the given key.
  private byte[] computeHashDigest(byte[] key) {
    msgHashDigest.update(key);
    return msgHashDigest.digest();
  }

  public byte[] readFromCache(String key) {
    initializeCacheConnectionIfNeeded();
    // TODO: get a connection from the read connection pool
    return connection.sync().get(computeHashDigest(key.getBytes(StandardCharsets.UTF_8)));
  }

  public void writeToCache(String key, byte[] value) {
    initializeCacheConnectionIfNeeded();
    // TODO: get a connection from the write connection pool
    connection.sync().setex(computeHashDigest(key.getBytes(StandardCharsets.UTF_8)), 300, value);
  }
}
