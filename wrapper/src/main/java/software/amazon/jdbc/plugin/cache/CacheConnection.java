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

import static java.nio.file.Files.readAllBytes;

import glide.api.BaseClient;
import glide.api.GlideClient;
import glide.api.GlideClusterClient;
import glide.api.models.GlideString;
import glide.api.models.commands.InfoOptions.Section;
import glide.api.models.commands.SetOptions;
import glide.api.models.configuration.AdvancedGlideClientConfiguration;
import glide.api.models.configuration.AdvancedGlideClusterClientConfiguration;
import glide.api.models.configuration.BackoffStrategy;
import glide.api.models.configuration.BaseClientConfiguration;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.IamAuthConfig;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ReadFrom;
import glide.api.models.configuration.ServerCredentials;
import glide.api.models.configuration.ServiceType;
import glide.api.models.configuration.TlsAdvancedConfiguration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

// Abstraction layer on top of a connection to a remote cache server
public class CacheConnection {
  private static final Logger LOGGER = Logger.getLogger(CacheConnection.class.getName());
  private static final int DEFAULT_POOL_MIN_IDLE = 0;
  private static final int DEFAULT_MAX_POOL_SIZE = 200;
  private static final long DEFAULT_MAX_BORROW_WAIT_MS = 100;
  private final FullServicesContainer servicesContainer;

  private static final ResourceLock connectionInitializationLock = new ResourceLock();
  // Cache endpoint registry to hold connection pools for multi end points
  private static final ConcurrentHashMap<String, GenericObjectPool<BaseClient>>
      endpointToPoolRegistry = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, byte[]> endpointCaCertRegistry = new ConcurrentHashMap<>();

  public static final AwsWrapperProperty CACHE_RW_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRw",
          null,
          "The cache read-write server endpoint address.");

  protected static final AwsWrapperProperty CACHE_RO_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRo",
          null,
          "The cache read-only server endpoint address. This is an optional parameter to allow "
              + "performing read operations from read replica cache nodes.");

  protected static final AwsWrapperProperty CACHE_USE_SSL =
      new AwsWrapperProperty(
          "cacheUseSSL",
          "true",
          "Whether to use SSL for cache connections.");

  protected static final AwsWrapperProperty CACHE_TLS_CA_CERT_PATH =
      new AwsWrapperProperty(
          "cacheTlsCaCertPath",
          null,
          "File path to the CA certificate (PEM) for verifying the cache server's TLS certificate.");

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
  private volatile GenericObjectPool<BaseClient> readConnectionPool;
  private volatile GenericObjectPool<BaseClient> writeConnectionPool;

  private final boolean useSSL;
  private final byte[] cacheTlsCaCertBytes;
  private final boolean iamAuthEnabled;
  private final String cacheIamRegion;
  private final String cacheUsername;
  private final String cacheName;
  private final String cachePassword;
  private final Duration cacheConnectionTimeout;
  private final int cacheConnectionPoolSize;
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

  public CacheConnection(final Properties properties, TelemetryFactory telemetryFactory,
      FullServicesContainer servicesContainer) {
    this.telemetryFactory = telemetryFactory;
    this.servicesContainer = servicesContainer;
    this.inFlightWriteSizeLimitBytes = CacheMonitor.CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT.getLong(properties);
    this.healthCheckInHealthyState = CacheMonitor.CACHE_HEALTH_CHECK_IN_HEALTHY_STATE.getBoolean(properties);

    this.cacheRwServerAddr = CACHE_RW_ENDPOINT_ADDR.getString(properties);
    this.cacheRoServerAddr = CACHE_RO_ENDPOINT_ADDR.getString(properties);
    this.useSSL = Boolean.parseBoolean(CACHE_USE_SSL.getString(properties));
    String certPath = CACHE_TLS_CA_CERT_PATH.getString(properties);
    if (!StringUtils.isNullOrEmpty(certPath)) {
      try {
        this.cacheTlsCaCertBytes = readAllBytes(java.nio.file.Paths.get(certPath));
      } catch (IOException e) {
        throw new RuntimeException(
            Messages.get("CacheConnection.failedToReadCaCert", new Object[]{certPath}), e);
      }
    } else {
      this.cacheTlsCaCertBytes = null;
    }
    if (!this.useSSL && this.cacheTlsCaCertBytes != null) {
      throw new IllegalArgumentException(Messages.get("CacheConnection.caCertWithoutTls"));
    }
    this.cacheName = CACHE_NAME.getString(properties);
    this.cacheIamRegion = CACHE_IAM_REGION.getString(properties);
    this.cacheUsername = CACHE_USERNAME.getString(properties);
    this.cachePassword = CACHE_PASSWORD.getString(properties);
    this.cacheConnectionTimeout = Duration.ofMillis(CACHE_CONNECTION_TIMEOUT.getInteger(properties));
    this.cacheConnectionPoolSize = CACHE_CONNECTION_POOL_SIZE.getInteger(properties);
    if (this.cacheConnectionPoolSize <= 0 || this.cacheConnectionPoolSize > DEFAULT_MAX_POOL_SIZE) {
      throw new IllegalArgumentException(
          Messages.get("CacheConnection.invalidPoolSize",
              new Object[]{DEFAULT_MAX_POOL_SIZE, this.cacheConnectionPoolSize}));
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
  }

  // for unit testing only
  CacheConnection(final Properties properties) {
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
    int port = Integer.parseInt(hostnameAndPort[1]);

    BaseClientConfiguration config = buildClientConfigurationStatic(
        hostnameAndPort[0], port, useSSL, cacheConnectionTimeout,
        iamAuthEnabled, cacheIamRegion, cacheName, cacheUsername, cachePassword,
        true, false, this.cacheTlsCaCertBytes);

    try (GlideClient client = GlideClient.createClient((GlideClientConfiguration) config).get()) {

      // customCommand returns Object (usually String for INFO)
      Section[] section = {Section.CLUSTER};
      Object result = client.info(section).get();
      String infoOutput = String.valueOf(result);
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
    }
  }

  /**
   * Initializes the connection pool for reading from or writing to the cache, if not already initialized.
   *
   * @param isRead if {@code true}, initializes the read connection pool.
   *               Otherwise, initializes the write connection pool
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
      if (this.cacheTlsCaCertBytes != null) {
        endpointCaCertRegistry.putIfAbsent(this.cacheRwServerAddr, this.cacheTlsCaCertBytes);
        if (this.cacheRoServerAddr != null) {
          endpointCaCertRegistry.putIfAbsent(this.cacheRoServerAddr, this.cacheTlsCaCertBytes);
        }
      }

      // Register cluster with CacheMonitor on first cache operation
      // Skip only in test mode (when servicesContainer is null)
      if (servicesContainer != null && !this.cacheMonitorRegistered) {
        CacheMonitor.registerCluster(
            this.servicesContainer,
            inFlightWriteSizeLimitBytes, healthCheckInHealthyState, telemetryFactory,
            this.cacheRwServerAddr, this.cacheRoServerAddr,
            this.useSSL, this.cacheConnectionTimeout, this.iamAuthEnabled,
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
      int port = Integer.parseInt(hostnameAndPort[1]);
      String host = hostnameAndPort[0];

      // Appending RW and RO tag to the server address to make it unique in case RO and RW has same endpoint
      String poolKey = (isRead ? "RO:" : "RW:") + serverAddr;
      GenericObjectPool<BaseClient> pool = endpointToPoolRegistry.get(poolKey);

      if (pool == null) {
        GenericObjectPoolConfig<BaseClient> poolConfig = createPoolConfig();
        poolConfig.setMaxTotal(this.cacheConnectionPoolSize);
        poolConfig.setMaxIdle(this.cacheConnectionPoolSize);

        pool = endpointToPoolRegistry.computeIfAbsent(poolKey, k ->
            new GenericObjectPool<>(
                new BasePooledObjectFactory<BaseClient>() {
                  public BaseClient create() throws Exception {
                    BaseClientConfiguration config = buildClientConfiguration(host, port, isRead);
                    if (Boolean.TRUE.equals(isClusterMode)) {
                      return GlideClusterClient.createClient((GlideClusterClientConfiguration) config).get();
                    } else {
                      return GlideClient.createClient((GlideClientConfiguration) config).get();
                    }
                  }

                  public PooledObject<BaseClient> wrap(BaseClient client) {
                    return new DefaultPooledObject<>(client);
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
      String errorMsg = Messages.get("CacheConnection.createPoolFailed", new Object[]{poolType});
      LOGGER.log(Level.WARNING, errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  private static GenericObjectPoolConfig<BaseClient> createPoolConfig() {
    GenericObjectPoolConfig<BaseClient> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMinIdle(DEFAULT_POOL_MIN_IDLE);
    poolConfig.setMaxWait(Duration.ofMillis(DEFAULT_MAX_BORROW_WAIT_MS));
    return poolConfig;
  }

  /**
   * Builds Glide client configuration using instance fields.
   * Delegates to static method for actual configuration building.
   */
  protected BaseClientConfiguration buildClientConfiguration(String hostname, int port, Boolean isRead) {
    return buildClientConfigurationStatic(hostname, port, this.useSSL, this.cacheConnectionTimeout,
        this.iamAuthEnabled, this.cacheIamRegion, this.cacheName,
        this.cacheUsername, this.cachePassword, isRead, this.isClusterMode, this.cacheTlsCaCertBytes);
  }

  /**
   * Builds Glide client configuration for standalone or cluster mode.
   * Returns GlideClientConfiguration for standalone or GlideClusterClientConfiguration for cluster mode.
   *
   * @param isRead        true for read-only replica connections, false for primary connections
   * @param isClusterMode true for cluster mode (CME), false for standalone mode (CMD)
   * @return BaseClientConfiguration configured for the specified mode
   */
  protected static BaseClientConfiguration buildClientConfigurationStatic(
      String hostname, int port, boolean useSSL, Duration connectionTimeout,
      boolean iamAuthEnabled, String cacheIamRegion, String cacheName, String cacheUsername,
      String cachePassword, Boolean isRead, Boolean isClusterMode, byte[] caCert) {

    NodeAddress address = buildNodeAddress(hostname, port);
    ServerCredentials credentials = buildServerCredentials(
        iamAuthEnabled, cacheIamRegion, cacheName,
        cacheUsername, cachePassword);

    // checks for cluster mode and returns appropriate client configuration
    if (Boolean.TRUE.equals(isClusterMode)) {
      GlideClusterClientConfiguration.GlideClusterClientConfigurationBuilder builder =
          GlideClusterClientConfiguration.builder()
              .address(address)
              .useTLS(useSSL)
              .libName("aws-sql-jdbc-glide")
              .requestTimeout((int) connectionTimeout.toMillis())
              .lazyConnect(true)
              .readFrom(Boolean.TRUE.equals(isRead) ? ReadFrom.PREFER_REPLICA : ReadFrom.PRIMARY)
              .reconnectStrategy(reconnectStrategyBuilder());

      if (useSSL) {
        TlsAdvancedConfiguration tlsConfig = (caCert != null)
            ? TlsAdvancedConfiguration.builder().rootCertificates(caCert).build()
            : TlsAdvancedConfiguration.builder().build();
        AdvancedGlideClusterClientConfiguration advancedConfig =
            AdvancedGlideClusterClientConfiguration.builder()
                .connectionTimeout((int) connectionTimeout.toMillis())
                .tlsAdvancedConfiguration(tlsConfig)
                .build();
        builder.advancedConfiguration(advancedConfig);
      }
      if (credentials != null) {
        builder.credentials(credentials);
      }
      return builder.build();
    } else {
      GlideClientConfiguration.GlideClientConfigurationBuilder builder =
          GlideClientConfiguration.builder()
              .address(address)
              .useTLS(useSSL)
              .libName("aws-sql-jdbc-glide")
              .requestTimeout((int) connectionTimeout.toMillis())
              .lazyConnect(true)
              .readFrom(Boolean.TRUE.equals(isRead) ? ReadFrom.PREFER_REPLICA : ReadFrom.PRIMARY)
              .readOnly(Boolean.TRUE.equals(isRead))
              .reconnectStrategy(reconnectStrategyBuilder());

      if (useSSL) {
        TlsAdvancedConfiguration tlsConfig = (caCert != null)
            ? TlsAdvancedConfiguration.builder().rootCertificates(caCert).build()
            : TlsAdvancedConfiguration.builder().build();
        AdvancedGlideClientConfiguration advancedConfig =
            AdvancedGlideClientConfiguration.builder()
                .connectionTimeout((int) connectionTimeout.toMillis())
                .tlsAdvancedConfiguration(tlsConfig)
                .build();
        builder.advancedConfiguration(advancedConfig);
      }
      if (credentials != null) {
        builder.credentials(credentials);
      }
      return builder.build();
    }
  }

  // Builds NodeAddress for Glide clients
  private static NodeAddress buildNodeAddress(String hostname, int port) {
    return NodeAddress.builder()
        .host(hostname)
        .port(port)
        .build();
  }

  // Builds ServerCredentials for IAM or password-based authentication.
  private static ServerCredentials buildServerCredentials(
      boolean iamAuthEnabled, String cacheIamRegion, String cacheName,
      String cacheUsername, String cachePassword) {

    if (iamAuthEnabled) {
      IamAuthConfig iamConfig = IamAuthConfig.builder()
          .clusterName(cacheName)
          .service(ServiceType.ELASTICACHE)
          .region(cacheIamRegion)
          .build();

      return ServerCredentials.builder()
          .username(cacheUsername)
          .iamConfig(iamConfig)
          .build();
    } else if (!StringUtils.isNullOrEmpty(cachePassword)) {
      return ServerCredentials.builder()
          .username(cacheUsername)
          .password(cachePassword)
          .build();
    }
    return null;
  }

  // Builds exponential backoff strategy for connection retries.
  private static BackoffStrategy reconnectStrategyBuilder() {
    return BackoffStrategy.builder()
        .numOfRetries(5)
        .exponentBase(2)
        .factor(100)
        .jitterPercent(20)
        .build();
  }

  /**
   * Creates a cache ping connection with the specified configuration.
   * This is a static helper that abstracts Lettuce-specific logic for CacheMonitor.
   * Returns an interface to hide implementation details.
   */
  static CachePingConnection createPingConnection(String hostname, int port, boolean useSSL,
      Duration connectionTimeout, boolean iamAuthEnabled,
      String cacheIamRegion, String cacheName, String cacheUsername, String cachePassword) {
    try {
      byte[] caCert = endpointCaCertRegistry.get(hostname + ":" + port);
      // Creating GlideClient (use standalone for ping - works for both)
      BaseClientConfiguration config = buildClientConfigurationStatic(
          hostname, port, useSSL, connectionTimeout, iamAuthEnabled,
          cacheIamRegion, cacheName, cacheUsername, cachePassword,
          true, false, caCert);

      BaseClient client = GlideClient.createClient((GlideClientConfiguration) config).get();
      return new PingConnection(client);
    } catch (Exception e) {
      LOGGER.fine(Messages.get("CacheConnection.failedToCreatePingConnection",
          new Object[]{hostname, port}) + " " + e.getMessage());
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
    BaseClient client = null;
    // get a connection from the read connection pool
    try {
      initializeCacheConnectionIfNeeded(true);
      client = this.readConnectionPool.borrowObject();
      byte[] cacheKey = computeCacheKey(key);
      GlideString keyGs = GlideString.of(cacheKey);
      GlideString gs = client.get(keyGs).get();
      return (gs != null) ? gs.getBytes() : null;
    } catch (Exception e) {
      if (client != null) {
        isBroken = true;
      }
      // Report error to CacheMonitor for the read endpoint
      reportErrorToCacheMonitor(false, e, "READ");
      LOGGER.log(Level.WARNING, Messages.get("CacheConnection.failedToReadFromCache"), e);
      return null;
    } finally {
      if (client != null && this.readConnectionPool != null) {
        try {
          this.returnConnectionBackToPool(client, isBroken, true);
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, Messages.get("CacheConnection.errorClosingReadConnection"), e);
        }
      }
    }
  }

  protected void handleCompletedCacheWrite(BaseClient client, long writeSize, Throwable ex) {
    // Note: this callback upon completion of cache write is on a different thread
    // Always decrement in-flight size (write completed, whether success or failure)
    decrementInFlightSize(writeSize);

    if (ex != null) {
      // Report error to CacheMonitor for RW endpoint
      reportErrorToCacheMonitor(true, ex, "WRITE");
      if (this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(client, true, false);
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, Messages.get("CacheConnection.errorReturningBrokenWriteConnection"), e);
        }
      }
    } else {
      if (this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(client, false, false);
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

    BaseClient client = null;
    try {
      initializeCacheConnectionIfNeeded(false);

      // Calculate write size and increment before borrowing connection
      byte[] cacheKey = computeCacheKey(key);
      long writeSize = cacheKey.length + value.length;
      incrementInFlightSize(writeSize);

      try {
        client = this.writeConnectionPool.borrowObject();
      } catch (Exception borrowException) {
        // Connection borrow failed (timeout/pool exhaustion) - decrement immediately
        decrementInFlightSize(writeSize);
        reportErrorToCacheMonitor(true, borrowException, "WRITE");
        return;
      }

      // Get async commands and execute set operation based on connection type
      BaseClient finalClient = client;

      GlideString keyGs = GlideString.of(cacheKey);
      GlideString valueGs = GlideString.of(value);
      SetOptions options = SetOptions.builder().expiry(SetOptions.Expiry.Seconds((long) expiry)).build();

      client.set(keyGs, valueGs, options)
          .whenComplete((result, exception) -> handleCompletedCacheWrite(finalClient, writeSize, exception));
    } catch (Exception e) {
      // Connection failed, but we already incremented and will be able to detect shard level failures
      reportErrorToCacheMonitor(true, e, "WRITE");

      if (client != null && this.writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(client, true, false);
        } catch (Exception ex) {
          LOGGER.log(Level.WARNING, Messages.get("CacheConnection.errorClosingWriteConnection"), ex);
        }
      }
    }
  }

  private void returnConnectionBackToPool(BaseClient connection, boolean isConnectionBroken,
      boolean isRead) {
    GenericObjectPool<BaseClient> pool = isRead ? this.readConnectionPool :
        this.writeConnectionPool;
    if (isConnectionBroken) {
      if (!connection.isConnected()) {
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
    return String.format("%s [rwEndpoint: %s, roEndpoint: %s, SSLConnection: %s, "
            + "IAMEnabled: %s, failWhenCacheDown: %s]",
        super.toString(),
        this.cacheRwServerAddr, this.cacheRoServerAddr, this.useSSL, this.iamAuthEnabled, this.failWhenCacheDown);
  }

  /**
   * Wraps a BaseClient (either GlideClient or GlideClusterClient)
   * and exposes only ping functionality.
   */
  private static class PingConnection implements CachePingConnection {
    private final BaseClient connection;

    PingConnection(BaseClient connection) {
      this.connection = connection;
    }

    @Override
    public boolean ping() {
      try {
        // Cast to appropriate type to access sync() method
        if (!connection.isConnected()) {
          return false;
        }
        String result;
        result = ((GlideClient) connection).ping().get();
        return "PONG".equalsIgnoreCase(result);
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public boolean isConnected() {
      return connection.isConnected();
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
  protected void setConnectionPools(GenericObjectPool<BaseClient> readPool,
      GenericObjectPool<BaseClient> writePool) {
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
    endpointCaCertRegistry.clear();
    CacheMonitor.resetInstance();
  }
}
