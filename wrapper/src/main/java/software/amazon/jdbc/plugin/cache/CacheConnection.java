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
import io.lettuce.core.SocketOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.resource.Delay;
import io.lettuce.core.resource.DirContextDnsResolver;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.logging.Level;
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
import java.util.logging.Logger;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.PooledObject;
import software.amazon.awssdk.utils.cache.RefreshResult;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.plugin.iam.ElastiCacheIamTokenUtility;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.awssdk.utils.cache.CachedSupplier;

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
  private final FullServicesContainer servicesContainer;

  private static final ResourceLock connectionInitializationLock = new ResourceLock();
  // Cache endpoint registry to hold connection pools for multi end points
  private static final ConcurrentHashMap<String, GenericObjectPool<StatefulConnection<byte[], byte[]>>> endpointToPoolRegistry = new ConcurrentHashMap<>();

  public static final AwsWrapperProperty CACHE_RW_ENDPOINT_ADDR =
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
          "cacheConnectionTimeoutMs",
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

  protected static final AwsWrapperProperty CACHE_KEY_PREFIX =
      new AwsWrapperProperty(
          "cacheKeyPrefix",
          null,
          "Optional prefix for cache keys (max 10 characters). Enables multi-tenant cache isolation.");

  private final String cacheRwServerAddr; // read-write cache server
  private final String cacheRoServerAddr; // read-only cache server
  private MessageDigest msgHashDigest = null;
  // Adding support for read and write connection pools to the remote cache server
  private volatile GenericObjectPool<StatefulConnection<byte[], byte[]>> readConnectionPool;
  private volatile GenericObjectPool<StatefulConnection<byte[], byte[]>> writeConnectionPool;

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
  private final String cacheKeyPrefix;
  private volatile boolean cacheMonitorRegistered = false;
  private volatile Boolean isClusterMode = null; // null = not yet detected, true = CME, false = CMD

  static {
    PropertyDefinition.registerPluginProperties(CacheConnection.class);
  }

  public CacheConnection(final Properties properties, TelemetryFactory telemetryFactory, FullServicesContainer servicesContainer) {
    this.telemetryFactory = telemetryFactory;
    this.servicesContainer = servicesContainer;
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
          Messages.get("CacheConnection.invalidPoolSize",
              new Object[] {DEFAULT_MAX_POOL_SIZE, this.cacheConnectionPoolSize}));
    }
    this.failWhenCacheDown = FAIL_WHEN_CACHE_DOWN.getBoolean(properties);
    this.cacheKeyPrefix = CACHE_KEY_PREFIX.getString(properties);
    if (this.cacheKeyPrefix != null) {
      if (this.cacheKeyPrefix.trim().isEmpty()) {
        throw new IllegalArgumentException(Messages.get("CacheConnection.emptyKeyPrefix"));
      }
      if (this.cacheKeyPrefix.length() > 10) {
        throw new IllegalArgumentException(Messages.get("CacheConnection.keyPrefixTooLong"));
      }
    }
    this.iamAuthEnabled = !StringUtils.isNullOrEmpty(this.cacheIamRegion);
    boolean hasTraditionalAuth = !StringUtils.isNullOrEmpty(this.cachePassword);
    // Validate authentication configuration
    if (this.iamAuthEnabled && hasTraditionalAuth) {
      throw new IllegalArgumentException(Messages.get("CacheConnection.bothAuthMethods"));
    }
    // Warn if no authentication is configured
    if (!this.iamAuthEnabled && !hasTraditionalAuth) {
      LOGGER.warning(Messages.get("CacheConnection.noAuthenticationConfigured"));
    }
    if (this.cacheRwServerAddr == null) {
      throw new IllegalArgumentException(Messages.get("CacheConnection.endpointRequired"));
    }
    String[] defaultCacheServerHostAndPort = getHostnameAndPort(this.cacheRwServerAddr);
    if (this.iamAuthEnabled) {
      if (this.cacheUsername == null || defaultCacheServerHostAndPort[0] == null || this.cacheName == null) {
        throw new IllegalArgumentException(Messages.get("CacheConnection.iamAuthMissingParams"));
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
    this(properties, null, null);
  }

  /**
   * Detects whether the Redis endpoint is running in cluster mode by executing INFO command.
   * Caches the result to avoid repeated detection. The caller of this function needs to hold a lock for thread safety.
   */
  private void detectClusterMode() {
    if (this.isClusterMode != null) {
      return;
    }

    String[] hostnameAndPort = getHostnameAndPort(this.cacheRwServerAddr);
    RedisURI redisUri = buildRedisURI(hostnameAndPort[0], Integer.parseInt(hostnameAndPort[1]));

    ClientResources resources = ClientResources.builder().build();

    try (RedisClient client = RedisClient.create(resources, redisUri);
         StatefulRedisConnection<byte[], byte[]> conn = client.connect(new ByteArrayCodec())) {

      String infoOutput = conn.sync().info("cluster");
      boolean clusterEnabled = false;
      if (infoOutput != null) {
        // Parse the INFO output line by line
        for (String line : infoOutput.split("\r?\n")) {
          if (line.startsWith("cluster_enabled:")) {
            String value = line.substring("cluster_enabled:".length()).trim();
            clusterEnabled = "1".equals(value);
            break;
          }
        }
      }
      this.isClusterMode = clusterEnabled;
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, Messages.get("CacheConnection.failedToDetectClusterMode"), e);
      this.isClusterMode = false;
    } finally {
      try {
        resources.shutdown().get(5, TimeUnit.SECONDS);
      } catch (Exception ignored) {}
    }
  }

  /* Here we check if we need to initialise connection pool for read or write to cache.
  With isRead we check if we need to initialise connection pool for read or write to cache.
  If isRead is true, we initialise connection pool for read.
  If isRead is false, we initialise connection pool for write.
   */
  private void initializeCacheConnectionIfNeeded(boolean isRead) {
    if (StringUtils.isNullOrEmpty(this.cacheRwServerAddr)) {
      return;
    }

    // Initialize the message digest
    if (this.msgHashDigest == null) {
      try {
        this.msgHashDigest = MessageDigest.getInstance("SHA-384");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(Messages.get("CacheConnection.sha384NotSupported"), e);
      }
    }

    // return early if connection pool is already initialized before acquiring the lock
    if ((isRead && this.readConnectionPool != null) || (!isRead && this.writeConnectionPool != null)) {
      return;
    }

    try (ResourceLock ignored = connectionInitializationLock.obtain()) {
      // Double check after lock is acquired
      if ((isRead && this.readConnectionPool != null) || (!isRead && this.writeConnectionPool != null)) {
        return;
      }
      // Detect cluster mode first (cached for reuse)
      detectClusterMode();

      // Register cluster with CacheMonitor on first cache operation
      // Skip only in test mode (when servicesContainer is null)
      if (servicesContainer != null && !this.cacheMonitorRegistered) {
        CacheMonitor.registerCluster(
            this.servicesContainer,
            inFlightWriteSizeLimitBytes, healthCheckInHealthyState, telemetryFactory,
            this.cacheRwServerAddr, this.cacheRoServerAddr,
            this.useSSL, this.cacheConnectionTimeout, this.iamAuthEnabled, this.credentialsProvider,
            this.cacheIamRegion, this.cacheName, this.cacheUsername, this.cachePassword
        );
        this.cacheMonitorRegistered = true;
      }

      if ((isRead && this.readConnectionPool == null) || (!isRead && this.writeConnectionPool == null)) {
          createConnectionPool(isRead);
      }
    }
  }

  private void createConnectionPool(boolean isRead) {
    try {
      // cache server addr string is in the format "<server hostname>:<port>"
      String serverAddr = this.cacheRwServerAddr;
      // If read-only server is specified, use it for the read-only connections
      if (isRead && !StringUtils.isNullOrEmpty(this.cacheRoServerAddr)) {
        serverAddr = this.cacheRoServerAddr;
      }
      String[] hostnameAndPort = getHostnameAndPort(serverAddr);
      RedisURI redisUri = buildRedisURI(hostnameAndPort[0], Integer.parseInt(hostnameAndPort[1]));

      // Appending RW and RO tag to the server address to make it unique in case RO and RW has same endpoint
      String poolKey = (isRead ? "RO:" : "RW:") + serverAddr;
      GenericObjectPool<StatefulConnection<byte[], byte[]>> pool = endpointToPoolRegistry.get(poolKey);

      if (pool == null) {
        GenericObjectPoolConfig<StatefulConnection<byte[], byte[]>> poolConfig = createPoolConfig();
        poolConfig.setMaxTotal(this.cacheConnectionPoolSize);
        poolConfig.setMaxIdle(this.cacheConnectionPoolSize);

        pool = endpointToPoolRegistry.computeIfAbsent(poolKey, k ->
            new GenericObjectPool<>(
                new BasePooledObjectFactory<StatefulConnection<byte[], byte[]>>() {
                  public StatefulConnection<byte[], byte[]> create() {
                    return createRedisConnection(isRead, redisUri, isClusterMode);
                  }
                  public PooledObject<StatefulConnection<byte[], byte[]>> wrap(StatefulConnection<byte[], byte[]> connection) {
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
      String errorMsg = Messages.get("CacheConnection.createPoolFailed", new Object[] {poolType});
      LOGGER.log(Level.WARNING, errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  private static GenericObjectPoolConfig<StatefulConnection<byte[], byte[]>> createPoolConfig() {
    GenericObjectPoolConfig<StatefulConnection<byte[], byte[]>> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMinIdle(DEFAULT_POOL_MIN_IDLE);
    poolConfig.setMaxWait(Duration.ofMillis(DEFAULT_MAX_BORROW_WAIT_MS));
    return poolConfig;
  }

  /**
   * Creates a Redis connection for either standalone or cluster mode.
   * Returns StatefulConnection which works for both RedisClient and RedisClusterClient.
   */
  private static StatefulConnection<byte[], byte[]> createRedisConnection(
      boolean isReadOnly, RedisURI redisUri, boolean isClusterMode) {

    ClientResources resources = ClientResources.builder()
      .dnsResolver(new DirContextDnsResolver())
      .reconnectDelay(
        Delay.fullJitter(
          Duration.ofMillis(100),      // minimum 100ms delay
          Duration.ofSeconds(10),       // maximum 10s delay
          100, TimeUnit.MILLISECONDS))  // 100ms base
      .build();

    StatefulConnection<byte[], byte[]> conn;

    if (isClusterMode) {
      // Multi-shard cluster mode: use RedisClusterClient
      RedisClusterClient client = RedisClusterClient.create(resources, redisUri);

      // Configure cluster topology refresh per AWS best practices
      ClusterTopologyRefreshOptions topologyRefreshOptions =
          ClusterTopologyRefreshOptions.builder()
          .enableAllAdaptiveRefreshTriggers()
          .enablePeriodicRefresh()
          .closeStaleConnections(true)
          .build();

      // Configure cluster client options per AWS best practices
      ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
          .topologyRefreshOptions(topologyRefreshOptions)
          .nodeFilter(node ->
            !(node.is(RedisClusterNode.NodeFlag.FAIL)
              || node.is(RedisClusterNode.NodeFlag.EVENTUAL_FAIL)
              || node.is(RedisClusterNode.NodeFlag.HANDSHAKE)
              || node.is(RedisClusterNode.NodeFlag.NOADDR)))
          .validateClusterNodeMembership(false)
          .autoReconnect(true)
          .socketOptions(SocketOptions.builder()
              .keepAlive(true)
              .build())
          .build();

      client.setOptions(clusterClientOptions);

      // Connect and configure ReadFrom for replica reads
      StatefulRedisClusterConnection<byte[], byte[]> clusterConn = client.connect(new ByteArrayCodec());

      // Configure read preference: replicas for RO connections, master for RW
      if (isReadOnly) {
        clusterConn.setReadFrom(ReadFrom.REPLICA_PREFERRED);
      } else {
        clusterConn.setReadFrom(ReadFrom.MASTER);
      }

      conn = clusterConn;
    } else {
      // Single-shard standalone mode: use RedisClient
      RedisClient client = RedisClient.create(resources, redisUri);
      conn = client.connect(new ByteArrayCodec());
    }

    // Set READONLY mode for RO endpoint
    if (isReadOnly) {
      try {
        if (conn instanceof StatefulRedisClusterConnection) {
          ((StatefulRedisClusterConnection<byte[], byte[]>) conn).sync().readOnly();
        } else {
          ((StatefulRedisConnection<byte[], byte[]>) conn).sync().readOnly();
        }
      } catch (RedisCommandExecutionException e) {
        if (e.getMessage().contains("ERR This instance has cluster support disabled")) {
          LOGGER.fine(Messages.get("CacheConnection.clusterSupportDisabled"));
        } else {
          LOGGER.log(Level.FINE, Messages.get("CacheConnection.readonlyCommandFailed"), e);
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
        Supplier<String> tokenSupplier =
            CachedSupplier.builder(() -> {
                  ElastiCacheIamTokenUtility tokenUtility =
                      new ElastiCacheIamTokenUtility(cacheName);
                  String token = tokenUtility.generateAuthenticationToken(
                      credentialsProvider,
                      Region.of(cacheIamRegion),
                      hostname,
                      port,
                      cacheUsername
                  );

                  Instant now = Instant.now();
                  Instant expiresAt = now.plusSeconds(TOKEN_CACHE_DURATION);
                  return RefreshResult.builder(token)
                      .staleTime(expiresAt)
                      .build();
                }).build();

        return Mono.just(
            RedisCredentials.just(cacheUsername, tokenSupplier.get())
        );
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
      // Create Lettuce connection (use standalone mode for ping - works for both)
      StatefulConnection<byte[], byte[]> conn = createRedisConnection(isReadOnly, redisUri, false);

      // Wrap in abstraction interface
      return new PingConnection(conn);

    } catch (Exception e) {
      LOGGER.log(Level.FINE, Messages.get("CacheConnection.failedToCreatePingConnection",
          new Object[] {hostname, port}), e);
      return null;
    }
  }

  // Get the hash digest of the given key.
  private byte[] computeHashDigest(byte[] key) {
    this.msgHashDigest.update(key);
    return this.msgHashDigest.digest();
  }

  // Computes the final cache key by hashing the input key and preprending the configured prefix
  private byte[] computeCacheKey(String key) {
    byte[] keyHash = computeHashDigest(key.getBytes(StandardCharsets.UTF_8));

    if (this.cacheKeyPrefix == null) {
      return keyHash;
    }
    byte[] prefixBytes = this.cacheKeyPrefix.getBytes(StandardCharsets.UTF_8);
    byte[] finalKey = new byte[prefixBytes.length + keyHash.length];
    System.arraycopy(prefixBytes, 0, finalKey, 0, prefixBytes.length);
    System.arraycopy(keyHash, 0, finalKey, prefixBytes.length, keyHash.length);
    return finalKey;
  }

  public byte[] readFromCache(String key) throws SQLException {
    // Check cluster state before attempting read
    CacheMonitor.HealthState state = getClusterHealthStateFromCacheMonitor();
    if (!shouldProceedWithOperation(state)) {
      if (failWhenCacheDown) {
        throw new SQLException(Messages.get("CacheConnection.degradedState"));
      }
      return null; // Treat as cache miss
    }

    boolean isBroken = false;
    StatefulConnection<byte[], byte[]> conn = null;
    // get a connection from the read connection pool
    try {
      initializeCacheConnectionIfNeeded(true);
      conn = this.readConnectionPool.borrowObject();

      // Cast to appropriate type and execute get command
      byte[] cacheKey = computeCacheKey(key);
      if (conn instanceof StatefulRedisClusterConnection) {
        return ((StatefulRedisClusterConnection<byte[], byte[]>) conn).sync().get(cacheKey);
      } else {
        return ((StatefulRedisConnection<byte[], byte[]>) conn).sync().get(cacheKey);
      }
    } catch (Exception e) {
      if (conn != null) {
        isBroken = true;
      }
      // Report error to CacheMonitor for the read endpoint
      reportErrorToCacheMonitor(false, e, "READ");
      LOGGER.log(Level.WARNING, Messages.get("CacheConnection.failedToReadFromCache"), e);
      return null;
    } finally {
      if (conn != null && this.readConnectionPool != null) {
        try {
          this.returnConnectionBackToPool(conn, isBroken, true);
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, Messages.get("CacheConnection.errorClosingReadConnection"), e);
        }
      }
    }
  }

  protected void handleCompletedCacheWrite(StatefulConnection<byte[], byte[]> conn, long writeSize, Throwable ex) {
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
          LOGGER.log(Level.WARNING, Messages.get("CacheConnection.errorReturningBrokenWriteConnection"), e);
        }
      }
    } else {
      if (this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, false, false);
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, Messages.get("CacheConnection.errorReturningWriteConnection"), e);
        }
      }
    }
  }

  public void writeToCache(String key, byte[] value, int expiry) {
    // Check cluster state before attempting write
    CacheMonitor.HealthState state = getClusterHealthStateFromCacheMonitor();
    if (!shouldProceedWithOperation(state)) {
      LOGGER.finest(Messages.get("CacheConnection.skippingCacheWriteDegraded"));
      return; // Exit without writing
    }

    StatefulConnection<byte[], byte[]> conn = null;
    try {
      initializeCacheConnectionIfNeeded(false);

      // Calculate write size and increment before borrowing connection
      byte[] cacheKey = computeCacheKey(key);
      long writeSize = cacheKey.length + value.length;
      incrementInFlightSize(writeSize);

      try {
        conn = this.writeConnectionPool.borrowObject();
      } catch (Exception borrowException) {
        // Connection borrow failed (timeout/pool exhaustion) - decrement immediately
        decrementInFlightSize(writeSize);
        reportErrorToCacheMonitor(true, borrowException, "WRITE");
        return;
      }

      // Get async commands and execute set operation based on connection type
      StatefulConnection<byte[], byte[]> finalConn = conn;

      if (conn instanceof StatefulRedisClusterConnection) {
        RedisAdvancedClusterAsyncCommands<byte[], byte[]> clusterAsyncCommands =
            ((StatefulRedisClusterConnection<byte[], byte[]>) conn).async();
        clusterAsyncCommands.set(cacheKey, value, SetArgs.Builder.ex(expiry))
            .whenComplete((result, exception) -> handleCompletedCacheWrite(finalConn, writeSize, exception));
      } else {
        RedisAsyncCommands<byte[], byte[]> asyncCommands =
            ((StatefulRedisConnection<byte[], byte[]>) conn).async();
        asyncCommands.set(cacheKey, value, SetArgs.Builder.ex(expiry))
            .whenComplete((result, exception) -> handleCompletedCacheWrite(finalConn, writeSize, exception));
      }

    } catch (Exception e) {
      // Connection failed, but we already incremented and will be able to detect shard level failures
      reportErrorToCacheMonitor(true, e, "WRITE");

      if (conn != null && this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, true, false);
        } catch (Exception ex) {
          LOGGER.log(Level.WARNING, Messages.get("CacheConnection.errorClosingWriteConnection"), ex);
        }
      }
    }
  }

  private void returnConnectionBackToPool(StatefulConnection<byte[], byte[]> connection, boolean isConnectionBroken, boolean isRead) {
    GenericObjectPool<StatefulConnection<byte[], byte[]>> pool = isRead ? this.readConnectionPool : this.writeConnectionPool;
    if (isConnectionBroken) {
      if (!connection.isOpen()) {
        try {
          pool.invalidateObject(connection);
          return;
        } catch (Exception e) {
          throw new RuntimeException(Messages.get("CacheConnection.invalidateConnectionFailed"), e);
        }
      } else {
        LOGGER.fine(Messages.get("CacheConnection.connectionErrorButStillOpen"));
      }
    }
    pool.returnObject(connection);
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
    return CacheMonitor.getClusterState(this.cacheRwServerAddr, this.cacheRoServerAddr);
  }

  protected void reportErrorToCacheMonitor(boolean isWrite, Throwable error, String operation) {
    // When using single endpoint (roEndpoint is null), treat all operations as RW
    // This ensures health state tracking matches the actual endpoint being used
    boolean isRwEndpoint = isWrite || (this.cacheRoServerAddr == null);
    CacheMonitor.reportError(this.cacheRwServerAddr, this.cacheRoServerAddr, isRwEndpoint, error, operation);
  }

  protected void incrementInFlightSize(long writeSize) {
    CacheMonitor.incrementInFlightSizeStatic(this.cacheRwServerAddr, this.cacheRoServerAddr, writeSize);
  }

  protected void decrementInFlightSize(long writeSize) {
    CacheMonitor.decrementInFlightSizeStatic(this.cacheRwServerAddr, this.cacheRoServerAddr, writeSize);
  }

  protected boolean shouldProceedWithOperation(CacheMonitor.HealthState state) {
    return state != CacheMonitor.HealthState.DEGRADED;
  }

  @Override
  public String toString() {
    return String.format("%s [rwEndpoint: %s, roEndpoint: %s, SSLConnection: %s, IAMEnabled: %s, failWhenCacheDown: %s]",
        super.toString(),
        this.cacheRwServerAddr, this.cacheRoServerAddr, this.useSSL, this.iamAuthEnabled, this.failWhenCacheDown);
  }

  /**
   * Wraps a StatefulConnection (either StatefulRedisConnection or StatefulRedisClusterConnection)
   * and exposes only ping functionality.
   */
  private static class PingConnection implements CachePingConnection {
    private final StatefulConnection<byte[], byte[]> connection;

    PingConnection(StatefulConnection<byte[], byte[]> connection) {
      this.connection = connection;
    }

    @Override
    public boolean ping() {
      try {
        if (!connection.isOpen()) {
          return false;
        }

        // Cast to appropriate type to access sync() method
        String result;
        if (connection instanceof StatefulRedisClusterConnection) {
          result = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).sync().ping();
        } else {
          result = ((StatefulRedisConnection<byte[], byte[]>) connection).sync().ping();
        }
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

  /* ========== Below methods are used for internal testing purposes only ============ */
  protected void setConnectionPools(GenericObjectPool<StatefulConnection<byte[], byte[]>> readPool,
      GenericObjectPool<StatefulConnection<byte[], byte[]>> writePool) {
    this.readConnectionPool = readPool;
    this.writeConnectionPool = writePool;
  }

  protected void triggerPoolInit(boolean isRead) {
    initializeCacheConnectionIfNeeded(isRead);
  }

  // allows tests to bypass cluster detection
  protected void setClusterMode(boolean clusterMode) {
    this.isClusterMode = clusterMode;
  }

  // Used for integration testing only to avoid cross tests pollution
  public static void clearEndpointPoolRegistry() {
    endpointToPoolRegistry.clear();
  }
}
