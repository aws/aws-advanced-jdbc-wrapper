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

package software.amazon.jdbc.plugin.cache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.PooledObject;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.plugin.iam.ElastiCacheIamTokenUtility;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Abstraction for a cache connection that can be pinged.
 * Hides cache-client implementation details (Lettuce/Glide) from CacheMonitor.
 */
interface CachePingConnection {
  /**
   * Pings the cache server to check health.
   * @return true if ping successful (PONG received), false otherwise
   */
  boolean ping();

  /**
   * Checks if the connection is open.
   * @return true if connection is open, false otherwise
   */
  boolean isOpen();

  /**
   * Closes the connection.
   */
  void close();
}

// Abstraction layer on top of a connection to a remote cache server
public class CacheConnection {
  private static final Logger LOGGER = Logger.getLogger(CacheConnection.class.getName());

  private static final int DEFAULT_POOL_MIN_IDLE = 0;
  private static final int DEFAULT_MAX_POOL_SIZE = 200;
  private static final long DEFAULT_MAX_BORROW_WAIT_MS = 100;
  private static final long TOKEN_CACHE_DURATION = 15 * 60 - 30;

  private static final ReentrantLock READ_LOCK = new ReentrantLock();
  private static final ReentrantLock WRITE_LOCK = new ReentrantLock();

  private final String cacheRwServerAddr; // read-write cache server
  private final String cacheRoServerAddr; // read-only cache server
  private final String[] defaultCacheServerHostAndPort;
  private MessageDigest msgHashDigest = null;

  protected static final AwsWrapperProperty CACHE_RW_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRw",
          null,
          "The cache read-write server endpoint address.");

  protected static final AwsWrapperProperty CACHE_RO_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRo",
          null,
          "The cache read-only server endpoint address.");

  protected static final AwsWrapperProperty CACHE_USE_SSL =
      new AwsWrapperProperty(
          "cacheUseSSL",
          "true",
          "Whether to use SSL for cache connections.");

  protected static final AwsWrapperProperty CACHE_IAM_REGION =
      new AwsWrapperProperty(
          "cacheIamRegion",
          null,
          "AWS region for ElastiCache IAM authentication.");

  protected static final AwsWrapperProperty CACHE_USERNAME =
      new AwsWrapperProperty(
          "cacheUsername",
          null,
          "Username for ElastiCache regular authentication.");

  protected static final AwsWrapperProperty CACHE_PASSWORD =
      new AwsWrapperProperty(
          "cachePassword",
          null,
          "Password for ElastiCache regular authentication.");

  protected static final AwsWrapperProperty CACHE_NAME =
      new AwsWrapperProperty(
          "cacheName",
          null,
          "Explicit cache name for ElastiCache IAM authentication. ");

  protected static final AwsWrapperProperty CACHE_CONNECTION_TIMEOUT =
      new AwsWrapperProperty(
          "cacheConnectionTimeout",
          "2000",
          "Cache connection request timeout duration in milliseconds.");

  protected static final AwsWrapperProperty CACHE_CONNECTION_POOL_SIZE =
      new AwsWrapperProperty(
          "cacheConnectionPoolSize",
          "20",
          "Cache connection pool size.");

  protected static final AwsWrapperProperty FAIL_WHEN_CACHE_DOWN =
      new AwsWrapperProperty(
          "failWhenCacheDown",
          "false",
          "Whether to throw SQLException on cache failures under Degraded mode.");

  // Adding support for read and write connection pools to the remote cache server
  private volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readConnectionPool;
  private volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writeConnectionPool;
  // Cache endpoint registry to hold connection pools for multi end points
  private static final ConcurrentHashMap<String, GenericObjectPool<StatefulRedisConnection<byte[], byte[]>>> endpointToPoolRegistry = new ConcurrentHashMap<>();

  private final boolean useSSL;
  private final boolean iamAuthEnabled;
  private final String cacheIamRegion;
  private final String cacheUsername;
  private final String cacheName;
  private final String cachePassword;
  private final Duration cacheConnectionTimeout;
  private final int cacheConnectionPoolSize;
  private final Properties awsProfileProperties;
  private final AwsCredentialsProvider credentialsProvider;
  private final boolean failWhenCacheDown;
  private final TelemetryFactory telemetryFactory;
  private final long inFlightWriteSizeLimitBytes;
  private final boolean healthCheckInHealthyState;
  private volatile boolean cacheMonitorRegistered = false;

  static {
    PropertyDefinition.registerPluginProperties(CacheConnection.class);
  }

  /**
   * Wraps a StatefulRedisConnection and exposes only ping functionality.
   */
  private static class PingConnection implements CachePingConnection {
    private final StatefulRedisConnection<byte[], byte[]> connection;

    PingConnection(StatefulRedisConnection<byte[], byte[]> connection) {
      this.connection = connection;
    }

    @Override
    public boolean ping() {
      try {
        if (!connection.isOpen()) {
          return false;
        }
        String result = connection.sync().ping();
        return "PONG".equalsIgnoreCase(result);
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public boolean isOpen() {
      return connection.isOpen();
    }

    @Override
    public void close() {
      try {
        connection.close();
      } catch (Exception e) {
        // Ignore close errors
      }
    }
  }

  public CacheConnection(final Properties properties, TelemetryFactory telemetryFactory) {
    this.telemetryFactory = telemetryFactory;
    this.inFlightWriteSizeLimitBytes = CacheMonitor.CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT.getLong(properties);
    this.healthCheckInHealthyState = CacheMonitor.CACHE_HEALTH_CHECK_IN_HEALTHY_STATE.getBoolean(properties);

    this.cacheRwServerAddr = CACHE_RW_ENDPOINT_ADDR.getString(properties);
    this.cacheRoServerAddr = CACHE_RO_ENDPOINT_ADDR.getString(properties);
    this.useSSL = Boolean.parseBoolean(CACHE_USE_SSL.getString(properties));
    this.cacheName = CACHE_NAME.getString(properties);
    this.cacheIamRegion = CACHE_IAM_REGION.getString(properties);
    this.cacheUsername = CACHE_USERNAME.getString(properties);
    this.cachePassword = CACHE_PASSWORD.getString(properties);
    this.cacheConnectionTimeout = Duration.ofMillis(CACHE_CONNECTION_TIMEOUT.getInteger(properties));
    this.cacheConnectionPoolSize = CACHE_CONNECTION_POOL_SIZE.getInteger(properties);
    if (this.cacheConnectionPoolSize <= 0 || this.cacheConnectionPoolSize > DEFAULT_MAX_POOL_SIZE) {
      throw new IllegalArgumentException(
          "Cache connection pool size must be within valid range: 1-" + DEFAULT_MAX_POOL_SIZE + ", but was: " + this.cacheConnectionPoolSize);
    }
    this.failWhenCacheDown = FAIL_WHEN_CACHE_DOWN.getBoolean(properties);
    this.iamAuthEnabled = !StringUtils.isNullOrEmpty(this.cacheIamRegion);
    boolean hasTraditionalAuth = !StringUtils.isNullOrEmpty(this.cachePassword);
    // Validate authentication configuration
    if (this.iamAuthEnabled && hasTraditionalAuth) {
      throw new IllegalArgumentException(
          "Cannot specify both IAM authentication (cacheIamRegion) and traditional authentication (cachePassword). Choose one authentication method.");
    }
    if (this.cacheRwServerAddr == null) {
      throw new IllegalArgumentException("Cache endpoint address is required");
    }
    this.defaultCacheServerHostAndPort = getHostnameAndPort(this.cacheRwServerAddr);
    if (this.iamAuthEnabled) {
      if (this.cacheUsername == null || this.defaultCacheServerHostAndPort[0] == null || this.cacheName == null) {
        throw new IllegalArgumentException("IAM authentication requires cache name, username, region, and hostname");
      }
    }
    if (PropertyDefinition.AWS_PROFILE.getString(properties) != null) {
      this.awsProfileProperties = new Properties();
      this.awsProfileProperties.setProperty(
          PropertyDefinition.AWS_PROFILE.name,
          PropertyDefinition.AWS_PROFILE.getString(properties)
      );
    } else {
      this.awsProfileProperties = null;
    }
    if (this.iamAuthEnabled) {
      // Handle null case
      Properties propsToPass = (this.awsProfileProperties != null)
          ? this.awsProfileProperties
          : new Properties();
      this.credentialsProvider = AwsCredentialsManager.getProvider(null, propsToPass);
    } else {
      this.credentialsProvider = null;
    }
  }

  // for unit testing only
  public CacheConnection(final Properties properties) {
    this(properties, null);
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

    // Register cluster with CacheMonitor on first cache operation (skip if telemetryFactory is null = test mode)
    if (telemetryFactory != null && !cacheMonitorRegistered) {
      synchronized (this) {
        if (!cacheMonitorRegistered) {
          CacheMonitor.registerCluster(
              inFlightWriteSizeLimitBytes, healthCheckInHealthyState, telemetryFactory,
              this.cacheRwServerAddr, this.cacheRoServerAddr,
              this.useSSL, this.cacheConnectionTimeout, this.iamAuthEnabled, this.credentialsProvider,
              this.cacheIamRegion, this.cacheName, this.cacheUsername, this.cachePassword
          );
          cacheMonitorRegistered = true;
        }
      }
    }

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> cacheConnectionPool =
        isRead ? this.readConnectionPool : this.writeConnectionPool;
    if (cacheConnectionPool == null) {
      ReentrantLock connectionPoolLock = isRead ? READ_LOCK : WRITE_LOCK;
      connectionPoolLock.lock();
      try {
        if ((isRead && this.readConnectionPool == null) || (!isRead && this.writeConnectionPool == null)) {
          createConnectionPool(isRead);
        }
      } finally {
        connectionPoolLock.unlock();
      }
    }
  }

  private void createConnectionPool(boolean isRead) {
    try {
      // cache server addr string is in the format "<server hostname>:<port>"
      String serverAddr = cacheRwServerAddr;
      // If read-only server is specified, use it for the read-only connections
      if (isRead && !StringUtils.isNullOrEmpty(cacheRoServerAddr)) {
        serverAddr = cacheRoServerAddr;
      }
      String[] hostnameAndPort = getHostnameAndPort(serverAddr);
      RedisURI redisUri = buildRedisURI(hostnameAndPort[0], Integer.parseInt(hostnameAndPort[1]));

      // Appending RW and RO tag to the server address to make it unique in case RO and RW has same endpoint
      String poolKey = (isRead ? "RO:" : "RW:") + serverAddr;
      GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool = endpointToPoolRegistry.get(poolKey);

      if (pool == null) {
        GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig = createPoolConfig();
        poolConfig.setMaxTotal(this.cacheConnectionPoolSize);
        poolConfig.setMaxIdle(this.cacheConnectionPoolSize);

        pool = endpointToPoolRegistry.computeIfAbsent(poolKey, k ->
            new GenericObjectPool<>(
                new BasePooledObjectFactory<StatefulRedisConnection<byte[], byte[]>>() {
                  public StatefulRedisConnection<byte[], byte[]> create() {
                    return CacheConnection.createRedisConnection(isRead, redisUri);
                  }
                  public PooledObject<StatefulRedisConnection<byte[], byte[]>> wrap(StatefulRedisConnection<byte[], byte[]> connection) {
                    return new DefaultPooledObject<>(connection);
                  }
                }, poolConfig)
        );
      }

      if (isRead) {
        this.readConnectionPool = pool;
      } else {
        this.writeConnectionPool = pool;
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
    poolConfig.setMinIdle(DEFAULT_POOL_MIN_IDLE);
    poolConfig.setMaxWait(Duration.ofMillis(DEFAULT_MAX_BORROW_WAIT_MS));
    return poolConfig;
  }

  private static StatefulRedisConnection<byte[], byte[]> createRedisConnection(boolean isReadOnly, RedisURI redisUri) {

    ClientResources resources = ClientResources.builder().build();
    RedisClient client = RedisClient.create(resources, redisUri);
    StatefulRedisConnection<byte[], byte[]> conn = client.connect(new ByteArrayCodec());

    // Set READONLY mode for RO endpoint in cluster mode
    if (isReadOnly) {
      try {
        conn.sync().readOnly();
      } catch (RedisCommandExecutionException e) {
        if (e.getMessage().contains("ERR This instance has cluster support disabled")) {
          LOGGER.fine("Note: this cache cluster has cluster support disabled");
        } else {
          throw e;
        }
      }
    }

    return conn;
  }

  /**
   * Static helper to build RedisURI with authentication configuration.
   * Used by both createPingConnection (static) and buildRedisURI (instance).
   */
  private static RedisURI buildRedisURIStatic(String hostname, int port, boolean useSSL, Duration connectionTimeout,
      boolean iamAuthEnabled, AwsCredentialsProvider credentialsProvider, String cacheIamRegion,
      String cacheName, String cacheUsername, String cachePassword) {

    RedisURI.Builder uriBuilder = RedisURI.Builder.redis(hostname)
        .withPort(port)
        .withSsl(useSSL)
        .withVerifyPeer(false)
        .withLibraryName("aws-sql-jdbc-lettuce")
        .withTimeout(connectionTimeout);

    if (iamAuthEnabled) {
      // Create a credentials provider that Lettuce will call whenever authentication is needed
      RedisCredentialsProvider redisCredentialsProvider = () -> {
        // Create a cached token supplier that automatically refreshes tokens every 14.5 minutes
        Supplier<String> tokenSupplier = CachedSupplier.memoizeWithExpiration(
            () -> {
              ElastiCacheIamTokenUtility tokenUtility = new ElastiCacheIamTokenUtility(cacheName);
              return tokenUtility.generateAuthenticationToken(
                  credentialsProvider,
                  Region.of(cacheIamRegion),
                  hostname,
                  port,
                  cacheUsername
              );
            },
            TOKEN_CACHE_DURATION,
            TimeUnit.SECONDS
        );
        return Mono.just(RedisCredentials.just(cacheUsername, tokenSupplier.get()));
      };
      uriBuilder.withAuthentication(redisCredentialsProvider);
    } else if (!StringUtils.isNullOrEmpty(cachePassword)) {
      uriBuilder.withAuthentication(cacheUsername, cachePassword);
    }

    return uriBuilder.build();
  }

  /**
   * Creates a cache ping connection with the specified configuration.
   * This is a static helper that abstracts Lettuce-specific logic for CacheMonitor.
   * Returns an interface to hide implementation details.
   */
  static CachePingConnection createPingConnection(String hostname, int port, boolean isReadOnly, boolean useSSL,
      Duration connectionTimeout, boolean iamAuthEnabled, AwsCredentialsProvider credentialsProvider, String cacheIamRegion,
      String cacheName, String cacheUsername, String cachePassword) {

    try {
      // Use the static helper to build RedisURI
      RedisURI redisUri = buildRedisURIStatic(hostname, port, useSSL, connectionTimeout, iamAuthEnabled,
          credentialsProvider, cacheIamRegion, cacheName, cacheUsername, cachePassword
      );

      // Create Lettuce connection using the low-level helper
      StatefulRedisConnection<byte[], byte[]> conn = createRedisConnection(isReadOnly, redisUri);

      // Wrap in abstraction interface
      return new PingConnection(conn);

    } catch (Exception e) {
      LOGGER.fine(String.format("Failed to create ping connection for %s:%d: %s",
          hostname, port, e.getMessage()));
      return null;
    }
  }

  // Get the hash digest of the given key.
  private byte[] computeHashDigest(byte[] key) {
    msgHashDigest.update(key);
    return msgHashDigest.digest();
  }

  public byte[] readFromCache(String key) throws SQLException {
    // Check cluster state before attempting read
    CacheMonitor.HealthState state = getClusterHealthStateFromCacheMonitor();
    if (!shouldProceedWithOperation(state)) {
      if (failWhenCacheDown) {
        throw new SQLException("Cache cluster is in DEGRADED state and failWhenCacheDown is enabled");
      }
      return null; // Treat as cache miss
    }

    boolean isBroken = false;
    StatefulRedisConnection<byte[], byte[]> conn = null;
    // get a connection from the read connection pool
    try {
      initializeCacheConnectionIfNeeded(true);
      conn = this.readConnectionPool.borrowObject();
      return conn.sync().get(computeHashDigest(key.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      if (conn != null) {
        isBroken = true;
      }
      // Report error to CacheMonitor for the read endpoint
      reportErrorToCacheMonitor(false, e, "READ");
      LOGGER.warning("Failed to read result from cache. Treating it as a cache miss: " + e.getMessage());
      return null;
    } finally {
      if (conn != null && this.readConnectionPool != null) {
        try {
          this.returnConnectionBackToPool(conn, isBroken, true);
        } catch (Exception ex) {
          LOGGER.warning("Error closing read connection: " + ex.getMessage());
        }
      }
    }
  }

  protected void handleCompletedCacheWrite(StatefulRedisConnection<byte[], byte[]> conn, long writeSize, Throwable ex) {
    // Note: this callback upon completion of cache write is on a different thread
    // Always decrement in-flight size (write completed, whether success or failure)
    decrementInFlightSize(writeSize);

    if (ex != null) {
      // Report error to CacheMonitor for RW endpoint
      reportErrorToCacheMonitor(true, ex, "WRITE");
      if (this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, true, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning broken write connection back to pool: " + e.getMessage());
        }
      }
    } else {
      if (this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, false, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning write connection back to pool: " + e.getMessage());
        }
      }
    }
  }

  public void writeToCache(String key, byte[] value, int expiry) {
    // Check cluster state before attempting write
    CacheMonitor.HealthState state = getClusterHealthStateFromCacheMonitor();
    if (!shouldProceedWithOperation(state)) {
      LOGGER.finest("Skipping cache write - cluster is DEGRADED");
      return; // Exit without writing
    }

    StatefulRedisConnection<byte[], byte[]> conn = null;
    try {
      initializeCacheConnectionIfNeeded(false);

      // Calculate write size and increment before borrowing connection
      byte[] keyHash = computeHashDigest(key.getBytes(StandardCharsets.UTF_8));
      long writeSize = keyHash.length + value.length;
      incrementInFlightSize(writeSize);

      try {
        conn = this.writeConnectionPool.borrowObject();
      } catch (Exception borrowException) {
        // Connection borrow failed (timeout/pool exhaustion) - decrement immediately
        decrementInFlightSize(writeSize);
        reportErrorToCacheMonitor(true, borrowException, "WRITE");
        return;
      }

      RedisAsyncCommands<byte[], byte[]> asyncCommands = conn.async();
      StatefulRedisConnection<byte[], byte[]> finalConn = conn;

      asyncCommands.set(keyHash, value, SetArgs.Builder.ex(expiry))
          .whenComplete((result, exception) -> handleCompletedCacheWrite(finalConn, writeSize, exception));

    } catch (Exception e) {
      // Connection failed, but we already incremented and will be able to detect shard level failures
      reportErrorToCacheMonitor(true, e, "WRITE");

      if (conn != null && this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, true, false);
        } catch (Exception ex) {
          LOGGER.warning("Error closing write connection: " + ex.getMessage());
        }
      }
    }
  }

  private void returnConnectionBackToPool(StatefulRedisConnection <byte[], byte[]> connection, boolean isConnectionBroken, boolean isRead) {
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool = isRead ? this.readConnectionPool : this.writeConnectionPool;
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


  protected RedisURI buildRedisURI(String hostname, int port) {
    // Delegate to the static helper
    return buildRedisURIStatic(hostname, port, this.useSSL, this.cacheConnectionTimeout, this.iamAuthEnabled,
        this.credentialsProvider, this.cacheIamRegion, this.cacheName, this.cacheUsername, this.cachePassword
    );
  }

  private String[] getHostnameAndPort(String serverAddr) {
    return serverAddr.split(":");
  }

  protected CacheMonitor.HealthState getClusterHealthStateFromCacheMonitor() {
    return CacheMonitor.getClusterState(cacheRwServerAddr, cacheRoServerAddr);
  }

  protected void reportErrorToCacheMonitor(boolean isWrite, Throwable error, String operation) {
    CacheMonitor.reportError(cacheRwServerAddr, cacheRoServerAddr, isWrite, error, operation);
  }

  protected void incrementInFlightSize(long writeSize) {
    CacheMonitor.incrementInFlightSizeStatic(cacheRwServerAddr, cacheRoServerAddr, writeSize);
  }

  protected void decrementInFlightSize(long writeSize) {
    CacheMonitor.decrementInFlightSizeStatic(cacheRwServerAddr, cacheRoServerAddr, writeSize);
  }

  protected boolean shouldProceedWithOperation(CacheMonitor.HealthState state) {
    return state != CacheMonitor.HealthState.DEGRADED;
  }

  // Used for unit testing only
  protected void setConnectionPools(GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool,
      GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool) {
    this.readConnectionPool = readPool;
    this.writeConnectionPool = writePool;
  }

  // Used for unit testing only
  protected void triggerPoolInit(boolean isRead) {
    initializeCacheConnectionIfNeeded(isRead);
  }
}
