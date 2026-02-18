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

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

/**
 * This class uses a background thread to monitor cache cluster health and manage state transitions.
 *
 * <p>Implements a three-state machine (HEALTHY → SUSPECT → DEGRADED) with proactive health checks
 * that only run when clusters are in SUSPECT or DEGRADED states.
 */
public class CacheMonitor extends AbstractMonitor {

  private static final Logger LOGGER = Logger.getLogger(CacheMonitor.class.getName());

  // Singleton instance
  private static volatile CacheMonitor instance;
  private static final Object INSTANCE_LOCK = new Object();

  private static final long THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final int CACHE_HEALTH_CHECK_INTERVAL = 5;
  private static final int CACHE_CONSECUTIVE_SUCCESS_THRESHOLD = 3;
  private static final int CACHE_CONSECUTIVE_FAILURE_THRESHOLD = 3;

  // Configuration properties
  public static final AwsWrapperProperty CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT =
      new AwsWrapperProperty(
          "cacheInFlightWriteSizeLimitBytes",
          String.valueOf(50 * 1024 * 1024), // 50 MB
          "Maximum in-flight write size in bytes before triggering degraded mode.");

  public static final AwsWrapperProperty CACHE_HEALTH_CHECK_IN_HEALTHY_STATE =
      new AwsWrapperProperty(
          "cacheHealthCheckInHealthyState",
          "false",
          "Whether to run health checks (pings) in healthy state.");

  private static final Map<String, ClusterHealthState> clusterStates = new ConcurrentHashMap<>();
  private final long inFlightWriteSizeLimitBytes;
  private final boolean healthCheckInHealthyState;

  // Telemetry
  private final TelemetryFactory telemetryFactory;
  private static TelemetryCounter stateTransitionCounter;
  private static TelemetryCounter healthCheckSuccessCounter;
  private static TelemetryCounter healthCheckFailureCounter;
  private static TelemetryCounter errorCounter;
  private static TelemetryGauge consecutiveSuccessGauge;
  private static TelemetryGauge consecutiveFailureGauge;

  /**
   * Enum representing the health state of a cache cluster or endpoint.
   */
  protected enum HealthState {
    HEALTHY,
    SUSPECT,
    DEGRADED
  }

  /* Categories of cache errors for classification and handling */
  protected enum ErrorCategory {
    CONNECTION,  // Network issues, timeouts, connection refused
    COMMAND,     // Invalid syntax, wrong arguments
    DATA,        // Serialization failures, data corruption
    RESOURCE     // Memory limits, cluster issues
  }

  /**
   * Represents a cache cluster with RW and optional RO endpoints.
   * This is the single source of truth for cluster health state.
   */
  static class ClusterHealthState {
    final String rwEndpoint;
    final String roEndpoint;  // Null if no separate RO endpoint

    // Separate health states for RW and RO endpoints
    volatile HealthState rwHealthState;
    volatile HealthState roHealthState;

    // Consecutive result tracking for RW endpoint (only accessed by CacheMonitor thread)
    int consecutiveRwSuccesses;
    int consecutiveRwFailures;

    // Consecutive result tracking for RO endpoint (only accessed by CacheMonitor thread)
    int consecutiveRoSuccesses;
    int consecutiveRoFailures;

    // Memory pressure tracking (only for RW endpoint, accessed by multiple threads)
    final AtomicLong inFlightWriteSizeBytes;

    // Ping connections - one for RW, one for RO if different
    volatile CachePingConnection rwPingConnection;
    volatile CachePingConnection roPingConnection;  // Null if no separate RO endpoint

    // Configuration for creating ping connections
    final boolean useSSL;
    final Duration cacheConnectionTimeout;
    final boolean iamAuthEnabled;
    final AwsCredentialsProvider credentialsProvider;
    final String cacheIamRegion;
    final String cacheName;
    final String cacheUsername;
    final String cachePassword;

    ClusterHealthState(String rwEndpoint, String roEndpoint, boolean useSSL, Duration cacheConnectionTimeout,
        boolean iamAuthEnabled, AwsCredentialsProvider credentialsProvider, String cacheIamRegion,
        String cacheName, String cacheUsername, String cachePassword) {
      this.rwEndpoint = rwEndpoint;
      // If roEndpoint equals rwEndpoint, treat it as null
      this.roEndpoint = (roEndpoint != null && roEndpoint.equals(rwEndpoint)) ? null : roEndpoint;
      this.useSSL = useSSL;
      this.cacheConnectionTimeout = cacheConnectionTimeout;
      this.iamAuthEnabled = iamAuthEnabled;
      this.credentialsProvider = credentialsProvider;
      this.cacheIamRegion = cacheIamRegion;
      this.cacheName = cacheName;
      this.cacheUsername = cacheUsername;
      this.cachePassword = cachePassword;

      // Initialize health states
      this.rwHealthState = HealthState.HEALTHY;
      this.roHealthState = HealthState.HEALTHY;

      // Initialize counters
      this.consecutiveRwSuccesses = 0;
      this.consecutiveRwFailures = 0;
      this.consecutiveRoSuccesses = 0;
      this.consecutiveRoFailures = 0;
      this.inFlightWriteSizeBytes = new AtomicLong(0);
    }

    String getClusterKey() {
      return generateClusterKey(rwEndpoint, roEndpoint);
    }

    static String generateClusterKey(String rwEndpoint, String roEndpoint) {
      String normalizedRo = (roEndpoint != null && roEndpoint.equals(rwEndpoint)) ? null : roEndpoint;
      return normalizedRo == null ? rwEndpoint + "|" : rwEndpoint + "|" + normalizedRo;
    }

    /**
     * Transition health state for a specific endpoint.
     */
    void transitionToState(HealthState newState, boolean isRw, String triggerReason, TelemetryCounter counter) {
      String endpoint = isRw ? rwEndpoint : roEndpoint;
      HealthState oldState = isRw ? rwHealthState : roHealthState;

      // Update state
      if (isRw) {
        rwHealthState = newState;
        consecutiveRwSuccesses = 0;
        consecutiveRwFailures = 0;
      } else {
        roHealthState = newState;
        consecutiveRoSuccesses = 0;
        consecutiveRoFailures = 0;
      }

      // Emit telemetry
      if (counter != null) {
        counter.inc();
      }

      // Log state transition
      if (!triggerReason.startsWith("recoverable_error_")) {
        LOGGER.fine(() -> Messages.get("CacheMonitor.stateTransition",
            new Object[] {oldState, newState, endpoint, isRw ? "RW" : "RO", triggerReason}));
      }
    }

    /**
     * Get the overall cluster health state based on both endpoints.
     */
    HealthState getClusterHealthState() {
      // If no separate RO endpoint, return RW state
      if (roEndpoint == null) {
        return rwHealthState;
      }

      // Compute cluster state based on both endpoints
      if (rwHealthState == HealthState.HEALTHY && roHealthState == HealthState.HEALTHY) {
        return HealthState.HEALTHY;
      } else if (rwHealthState == HealthState.DEGRADED || roHealthState == HealthState.DEGRADED) {
        return HealthState.DEGRADED;
      } else {
        return HealthState.SUSPECT; // Mixed states
      }
    }
  }

  protected static void registerCluster(FullServicesContainer servicesContainer, long inFlightWriteSizeLimitBytes,
      boolean healthCheckInHealthyState, TelemetryFactory telemetryFactory,
      String rwEndpoint, String roEndpoint, boolean useSSL, Duration cacheConnectionTimeout,
      boolean iamAuthEnabled, AwsCredentialsProvider credentialsProvider, String cacheIamRegion,
      String cacheName, String cacheUsername, String cachePassword, boolean createPingConnection,
      boolean startMonitorThread) {
    if (getCluster(rwEndpoint, roEndpoint) != null) {
      return; // if cluster has already been registered
    }
    if (instance == null) {
      synchronized (INSTANCE_LOCK) {
        if (instance == null) {
          instance = new CacheMonitor(inFlightWriteSizeLimitBytes, healthCheckInHealthyState, telemetryFactory);
          LOGGER.info(Messages.get("CacheMonitor.createdInstance",
              new Object[] {telemetryFactory != null ? "with" : "without"}));
        }
      }
    }
    ClusterHealthState clusterState = new ClusterHealthState(rwEndpoint, roEndpoint, useSSL, cacheConnectionTimeout,
        iamAuthEnabled, credentialsProvider, cacheIamRegion, cacheName, cacheUsername, cachePassword);
    ClusterHealthState existingCluster = clusterStates.putIfAbsent(clusterState.getClusterKey(), clusterState);
    if (existingCluster == null) {
      LOGGER.info(() -> Messages.get("CacheMonitor.registeredCluster",
          new Object[] {clusterState.getClusterKey()}));
      if (createPingConnection) {
        instance.createInitialPingConnections(clusterState);
      }
    }
    // Start monitor thread via MonitorService (replaces startMonitoring())
    if (startMonitorThread && servicesContainer != null) {
      try {
        servicesContainer.getMonitorService().runIfAbsent(
            CacheMonitor.class,
            "VALKEY_CACHE_HEALTH_CHECK_MONITOR_SINGLETON",
            servicesContainer,
            new Properties(),
            (container) -> instance // Return existing instance
        );
      } catch (SQLException e) {
        LOGGER.log(Level.WARNING, Messages.get("CacheMonitor.failedToStartMonitor"), e);
      }
    }
  }

  protected static void registerCluster(FullServicesContainer servicesContainer, long inFlightWriteSizeLimitBytes,
      boolean healthCheckInHealthyState, TelemetryFactory telemetryFactory,
      String rwEndpoint, String roEndpoint, boolean useSSL,
      Duration cacheConnectionTimeout, boolean iamAuthEnabled,
      AwsCredentialsProvider credentialsProvider, String cacheIamRegion,
      String cacheName, String cacheUsername, String cachePassword) {
    registerCluster(servicesContainer, inFlightWriteSizeLimitBytes, healthCheckInHealthyState, telemetryFactory,
        rwEndpoint, roEndpoint, useSSL,
        cacheConnectionTimeout, iamAuthEnabled, credentialsProvider,
        cacheIamRegion, cacheName, cacheUsername, cachePassword, true, true);
  }

  protected CacheMonitor(long inFlightWriteSizeLimitBytes, boolean healthCheckInHealthyState,
      TelemetryFactory telemetryFactory) {
    super(30); // 30 seconds termination timeout
    this.telemetryFactory = telemetryFactory;
    this.inFlightWriteSizeLimitBytes = inFlightWriteSizeLimitBytes;
    this.healthCheckInHealthyState = healthCheckInHealthyState;

    if (telemetryFactory != null && stateTransitionCounter == null) {
      stateTransitionCounter = telemetryFactory.createCounter("dataRemoteCache.cache.stateTransition");
      healthCheckSuccessCounter = telemetryFactory.createCounter("dataRemoteCache.cache.healthCheck.success");
      healthCheckFailureCounter = telemetryFactory.createCounter("dataRemoteCache.cache.healthCheck.failure");
      errorCounter = telemetryFactory.createCounter("dataRemoteCache.cache.error");

      consecutiveSuccessGauge = telemetryFactory.createGauge("dataRemoteCache.cache.healthCheck.consecutiveSuccess",
          () -> clusterStates.values().stream()
              .mapToLong(c -> Math.max(c.consecutiveRwSuccesses, c.consecutiveRoSuccesses))
              .max().orElse(0L));
      consecutiveFailureGauge = telemetryFactory.createGauge("dataRemoteCache.cache.healthCheck.consecutiveFailure",
          () -> clusterStates.values().stream()
              .mapToLong(c -> Math.max(c.consecutiveRwFailures, c.consecutiveRoFailures))
              .max().orElse(0L));
    }
  }

  private void createInitialPingConnections(ClusterHealthState cluster) {
    cluster.rwPingConnection = createPingConnection(cluster, false);
    if (cluster.roEndpoint != null) {
      cluster.roPingConnection = createPingConnection(cluster, true);
    }
  }

  // Used for unit testing only
  protected void setPingConnections(ClusterHealthState cluster,
      CachePingConnection rwConnection,
      CachePingConnection roConnection) {
    cluster.rwPingConnection = rwConnection;
    cluster.roPingConnection = roConnection;
  }

  private CachePingConnection createPingConnection(ClusterHealthState cluster, boolean isReadOnly) {
    String[] hostPort = isReadOnly ? cluster.roEndpoint.split(":") : cluster.rwEndpoint.split(":");
    return CacheConnection.createPingConnection(hostPort[0], Integer.parseInt(hostPort[1]),
        isReadOnly, cluster.useSSL, cluster.cacheConnectionTimeout, cluster.iamAuthEnabled, cluster.credentialsProvider,
        cluster.cacheIamRegion, cluster.cacheName, cluster.cacheUsername, cluster.cachePassword);
  }

  private static ClusterHealthState getCluster(String rwEndpoint, String roEndpoint) {
    return clusterStates.get(ClusterHealthState.generateClusterKey(rwEndpoint, roEndpoint));
  }

  protected static HealthState getClusterState(String rwEndpoint, String roEndpoint) {
    if (instance == null) {
      return HealthState.HEALTHY;
    }
    ClusterHealthState cluster = getCluster(rwEndpoint, roEndpoint);
    return cluster != null ? cluster.getClusterHealthState() : HealthState.HEALTHY;
  }

  private static ErrorCategory classifyError(Throwable error) {
    if (error instanceof RedisConnectionException) {
      return ErrorCategory.CONNECTION;
    }
    if (error instanceof RedisCommandExecutionException) {
      String msg = error.getMessage();
      if (msg == null) {
        return ErrorCategory.RESOURCE;
      }
      if (msg.contains("READONLY") || msg.contains("WRONGTYPE") || msg.contains("MOVED") || msg.contains("ASK")) {
        return ErrorCategory.COMMAND;
      }
      if (msg.contains("OOM") || msg.contains("CLUSTERDOWN") || msg.contains("LOADING")
          || msg.contains("NOAUTH") || msg.contains("WRONGPASS")) {
        return ErrorCategory.RESOURCE;
      }
      return ErrorCategory.COMMAND;
    }
    if (error instanceof RedisException) {
      return ErrorCategory.CONNECTION;
    }
    return ErrorCategory.DATA;
  }

  private static boolean isRecoverableError(ErrorCategory category) {
    return category != ErrorCategory.DATA;
  }

  protected static void reportError(String rwEndpoint, String roEndpoint, boolean isRw,
      Throwable error, String operation) {
    ClusterHealthState cluster = getCluster(rwEndpoint, roEndpoint);
    if (cluster == null) {
      LOGGER.warning(Messages.get("CacheMonitor.errorReportUnregisteredCluster",
          new Object[] {rwEndpoint, roEndpoint}));
      return;
    }
    ErrorCategory category = classifyError(error);
    if (errorCounter != null) {
      errorCounter.inc();
    }
    if (!isRecoverableError(category)) {
      LOGGER.log(Level.SEVERE, () -> Messages.get("CacheMonitor.nonRecoverableError",
          new Object[] {category, isRw ? rwEndpoint : roEndpoint, error.getMessage()}));
      return;
    }
    synchronized (cluster) {
      HealthState currentState = isRw ? cluster.rwHealthState : cluster.roHealthState;
      if (currentState == HealthState.HEALTHY) {
        LOGGER.log(Level.WARNING, Messages.get("CacheMonitor.healthyToSuspect",
            new Object[] {isRw ? rwEndpoint : roEndpoint, operation, category, error.getMessage()}));
        cluster.transitionToState(HealthState.SUSPECT, isRw,
            "recoverable_error_" + category, stateTransitionCounter);
      }
    }
  }

  private void incrementInFlightSize(String rwEndpoint, String roEndpoint, long bytes) {
    ClusterHealthState cluster = getCluster(rwEndpoint, roEndpoint);
    if (cluster != null) {
      long newSize = cluster.inFlightWriteSizeBytes.addAndGet(bytes);
      synchronized (cluster) {
        if (newSize > inFlightWriteSizeLimitBytes && cluster.rwHealthState != HealthState.DEGRADED) {
          LOGGER.warning(Messages.get("CacheMonitor.inFlightSizeLimitExceeded",
              new Object[] {newSize}));
          cluster.transitionToState(HealthState.DEGRADED, true, "memory_pressure", stateTransitionCounter);
        }
      }
    }
  }

  private void decrementInFlightSize(String rwEndpoint, String roEndpoint, long bytes) {
    ClusterHealthState cluster = getCluster(rwEndpoint, roEndpoint);
    if (cluster != null) {
      cluster.inFlightWriteSizeBytes.updateAndGet(x -> Math.max(0, x - bytes));
    }
  }

  protected static void incrementInFlightSizeStatic(String rwEndpoint, String roEndpoint, long bytes) {
    if (instance != null) {
      instance.incrementInFlightSize(rwEndpoint, roEndpoint, bytes);
    }
  }

  protected static void decrementInFlightSizeStatic(String rwEndpoint, String roEndpoint, long bytes) {
    if (instance != null) {
      instance.decrementInFlightSize(rwEndpoint, roEndpoint, bytes);
    }
  }

  @Override
  public void monitor() throws Exception {
    LOGGER.info(Messages.get("CacheMonitor.monitorThreadStarted"));
    while (!this.stop.get()) {
      try {
        this.lastActivityTimestampNanos.set(System.nanoTime());
        long start = System.currentTimeMillis();
        boolean hasActiveMonitoring = false;
        for (ClusterHealthState cluster : clusterStates.values()) {
          if (cluster.getClusterHealthState() == HealthState.HEALTHY && !healthCheckInHealthyState) {
            continue;
          }
          hasActiveMonitoring = true;
          executePing(cluster, true);
          if (cluster.roEndpoint != null) {
            executePing(cluster, false);
          }
        }
        long duration = System.currentTimeMillis() - start;
        long target = hasActiveMonitoring
            ? TimeUnit.SECONDS.toMillis(CACHE_HEALTH_CHECK_INTERVAL)
            : THREAD_SLEEP_WHEN_INACTIVE_MILLIS;
        sleep(Math.max(0, target - duration));
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, Messages.get("CacheMonitor.monitoringException"), e);
      }
    }
  }

  private void executePing(ClusterHealthState cluster, boolean isRw) {
    String endpoint = isRw ? cluster.rwEndpoint : cluster.roEndpoint;
    boolean success = ping(cluster, isRw);

    HealthState currentState = isRw ? cluster.rwHealthState : cluster.roHealthState;
    if (success) {
      if (healthCheckSuccessCounter != null) {
        healthCheckSuccessCounter.inc();
      }
      if (isRw) {
        cluster.consecutiveRwSuccesses++;
        cluster.consecutiveRwFailures = 0;
      } else {
        cluster.consecutiveRoSuccesses++;
        cluster.consecutiveRoFailures = 0;
      }
      int consecutiveSuccess = isRw ? cluster.consecutiveRwSuccesses : cluster.consecutiveRoSuccesses;
      if (consecutiveSuccess >= CACHE_CONSECUTIVE_SUCCESS_THRESHOLD) {
        synchronized (cluster) {
          if (currentState == HealthState.SUSPECT) {
            cluster.transitionToState(HealthState.HEALTHY, isRw, "consecutive_successes", stateTransitionCounter);
          } else if (currentState == HealthState.DEGRADED
              && cluster.inFlightWriteSizeBytes.get() < inFlightWriteSizeLimitBytes) {
            cluster.transitionToState(HealthState.HEALTHY, isRw,
                "consecutive_successes_and_memory_recovered", stateTransitionCounter);
          }
        }
      }
    } else {
      LOGGER.warning(() -> Messages.get("CacheMonitor.pingFailed",
          new Object[] {endpoint, isRw ? "RW" : "RO"}));
      if (healthCheckFailureCounter != null) {
        healthCheckFailureCounter.inc();
      }
      if (isRw) {
        cluster.consecutiveRwFailures++;
        cluster.consecutiveRwSuccesses = 0;
      } else {
        cluster.consecutiveRoFailures++;
        cluster.consecutiveRoSuccesses = 0;
      }
      int consecutiveFailure = isRw ? cluster.consecutiveRwFailures : cluster.consecutiveRoFailures;
      synchronized (cluster) {
        if (currentState == HealthState.HEALTHY && consecutiveFailure >= 1) {
          cluster.transitionToState(HealthState.SUSPECT, isRw, "first_failure", stateTransitionCounter);
        } else if (currentState == HealthState.SUSPECT && consecutiveFailure >= CACHE_CONSECUTIVE_FAILURE_THRESHOLD) {
          cluster.transitionToState(HealthState.DEGRADED, isRw, "consecutive_failures", stateTransitionCounter);
        }
      }
    }
  }

  protected void sleep(long duration) throws InterruptedException {
    TimeUnit.MILLISECONDS.sleep(duration);
  }

  private boolean ping(ClusterHealthState cluster, boolean isRw) {
    CachePingConnection conn = isRw ? cluster.rwPingConnection : cluster.roPingConnection;
    if (conn == null || !conn.isOpen()) {
      return false;
    }
    try {
      return conn.ping();
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, Messages.get("CacheMonitor.pingFailed",
          new Object[] {isRw ? cluster.rwEndpoint : cluster.roEndpoint, isRw ? "RW" : "RO"}), e);
      return false;
    }
  }
}
