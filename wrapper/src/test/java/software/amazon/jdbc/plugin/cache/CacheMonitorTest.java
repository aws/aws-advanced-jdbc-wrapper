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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class CacheMonitorTest {
  private Properties props;
  private AutoCloseable closeable;

  @Mock
  private TelemetryFactory mockTelemetryFactory;
  @Mock
  private TelemetryCounter mockStateTransitionCounter;
  @Mock
  private TelemetryCounter mockHealthCheckSuccessCounter;
  @Mock
  private TelemetryCounter mockHealthCheckFailureCounter;
  @Mock
  private TelemetryCounter mockErrorCounter;
  @Mock
  private TelemetryGauge mockConsecutiveSuccessGauge;
  @Mock
  private TelemetryGauge mockConsecutiveFailureGauge;

  @Mock
  private CachePingConnection mockRwPingConnection;
  @Mock
  private CachePingConnection mockRoPingConnection;

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    props.setProperty("wrapperPlugins", "dataRemoteCache");
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheEndpointAddrRo", "localhost:6380");

    // Setup telemetry mocks
    when(mockTelemetryFactory.createCounter("dataRemoteCache.cache.stateTransition"))
        .thenReturn(mockStateTransitionCounter);
    when(mockTelemetryFactory.createCounter("dataRemoteCache.cache.healthCheck.success"))
        .thenReturn(mockHealthCheckSuccessCounter);
    when(mockTelemetryFactory.createCounter("dataRemoteCache.cache.healthCheck.failure"))
        .thenReturn(mockHealthCheckFailureCounter);
    when(mockTelemetryFactory.createCounter("dataRemoteCache.cache.error"))
        .thenReturn(mockErrorCounter);
    when(mockTelemetryFactory.createGauge(eq("dataRemoteCache.cache.healthCheck.consecutiveSuccess"), any()))
        .thenReturn(mockConsecutiveSuccessGauge);
    when(mockTelemetryFactory.createGauge(eq("dataRemoteCache.cache.healthCheck.consecutiveFailure"), any()))
        .thenReturn(mockConsecutiveFailureGauge);

    // Reset singleton state between tests
    resetCacheMonitorState();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  private void registerCluster(String rwEndpoint, String roEndpoint) throws Exception {
    long inFlightLimit = CacheMonitor.CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT.getLong(props);
    boolean healthCheckInHealthy = CacheMonitor.CACHE_HEALTH_CHECK_IN_HEALTHY_STATE.getBoolean(props);

    CacheMonitor.registerCluster(null, inFlightLimit, healthCheckInHealthy, null, rwEndpoint, roEndpoint,
        false, Duration.ofSeconds(5), false, null, null, null, null, null, false, false);

    CacheMonitor.ClusterHealthState cluster = getCluster(rwEndpoint, roEndpoint);
    CacheMonitor instance = (CacheMonitor) getStaticField("instance");
    if (instance != null) {
      instance.setPingConnections(cluster, mockRwPingConnection,
          roEndpoint != null ? mockRoPingConnection : null);
    }
  }

  private void registerClusterWithTelemetry(String rwEndpoint, String roEndpoint) throws Exception {
    long inFlightLimit = CacheMonitor.CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT.getLong(props);
    boolean healthCheckInHealthy = CacheMonitor.CACHE_HEALTH_CHECK_IN_HEALTHY_STATE.getBoolean(props);

    CacheMonitor.registerCluster(null, inFlightLimit, healthCheckInHealthy, mockTelemetryFactory, rwEndpoint,
        roEndpoint,
        false, Duration.ofSeconds(5), false, null, null, null, null, null, false, false);

    CacheMonitor.ClusterHealthState cluster = getCluster(rwEndpoint, roEndpoint);
    CacheMonitor instance = (CacheMonitor) getStaticField("instance");
    if (instance != null) {
      instance.setPingConnections(cluster, mockRwPingConnection,
          roEndpoint != null ? mockRoPingConnection : null);
    }
  }

  /**
   * Reset CacheMonitor singleton state between tests using reflection
   * to prevent test pollution from static fields.
   */
  private void resetCacheMonitorState() throws Exception {
    setStaticField("instance", null);

    @SuppressWarnings("unchecked")
    Map<String, CacheMonitor.ClusterHealthState> clusterStates =
        (Map<String, CacheMonitor.ClusterHealthState>) getStaticField("clusterStates");
    if (clusterStates != null) {
      clusterStates.clear();
    }

    setStaticField("stateTransitionCounter", null);
    setStaticField("healthCheckSuccessCounter", null);
    setStaticField("healthCheckFailureCounter", null);
    setStaticField("errorCounter", null);
    setStaticField("consecutiveSuccessGauge", null);
    setStaticField("consecutiveFailureGauge", null);
  }

  private static Object getStaticField(String fieldName) throws Exception {
    Field field = CacheMonitor.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(null);
  }

  private static void setStaticField(String fieldName, Object value) throws Exception {
    Field field = CacheMonitor.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(null, value);
  }

  private void setInstanceField(Object instance, String fieldName, Object value) throws Exception {
    Field field = instance.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(instance, value);
  }

  @SuppressWarnings("unchecked")
  private Map<String, CacheMonitor.ClusterHealthState> getClusterStates() throws Exception {
    return (Map<String, CacheMonitor.ClusterHealthState>) getStaticField("clusterStates");
  }

  private CacheMonitor.ClusterHealthState getCluster(String rwEndpoint, String roEndpoint) throws Exception {
    Map<String, CacheMonitor.ClusterHealthState> clusterStates = getClusterStates();
    String key = CacheMonitor.ClusterHealthState.generateClusterKey(rwEndpoint, roEndpoint);
    return clusterStates.get(key);
  }

  @Test
  void testRegisterCluster() throws Exception {
    // Test 1: RW-only endpoint
    registerCluster("localhost:6379", null);
    assertEquals(1, getClusterStates().size());
    assertEquals(CacheMonitor.HealthState.HEALTHY,
        CacheMonitor.getClusterState("localhost:6379", null));

    CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", null);
    assertNotNull(cluster);
    assertEquals("localhost:6379", cluster.rwEndpoint);
    assertNull(cluster.roEndpoint);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.rwHealthState);
    assertEquals(0, cluster.consecutiveRwSuccesses);
    assertEquals(0, cluster.consecutiveRwFailures);
    assertEquals(0L, cluster.inFlightWriteSizeBytes.get());

    Object instance = getStaticField("instance");
    assertNotNull(instance);

    // Test 2: Dual endpoints
    registerCluster("localhost:6380", "localhost:6381");
    assertEquals(2, getClusterStates().size());
    cluster = getCluster("localhost:6380", "localhost:6381");
    assertNotNull(cluster);
    assertEquals("localhost:6380", cluster.rwEndpoint);
    assertEquals("localhost:6381", cluster.roEndpoint);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.rwHealthState);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.roHealthState);

    // Test 3: Duplicate registration should not create duplicate
    registerCluster("localhost:6380", "localhost:6381");
    assertEquals(2, getClusterStates().size());

    // Test 4: Same RW/RO endpoint should normalize RO to null
    registerCluster("localhost:6382", "localhost:6382");
    cluster = getCluster("localhost:6382", "localhost:6382");
    assertNotNull(cluster);
    assertNull(cluster.roEndpoint);
    assertEquals(3, getClusterStates().size());
  }

  @Test
  void testReportError() throws Exception {
    registerClusterWithTelemetry("localhost:6379", "localhost:6380");
    CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", "localhost:6380");

    // Test 1: Recoverable error transitions to SUSPECT
    CacheMonitor.reportError("localhost:6379", "localhost:6380", true,
        new RedisConnectionException("Connection refused"), "SET");
    assertEquals(CacheMonitor.HealthState.SUSPECT, cluster.rwHealthState);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.roHealthState);
    verify(mockErrorCounter, times(1)).inc();
    verify(mockStateTransitionCounter, times(1)).inc();

    // Test 2: Non-recoverable error doesn't change state
    CacheMonitor.reportError("localhost:6379", "localhost:6380", true,
        new RuntimeException("Serialization failed"), "SET");
    assertEquals(CacheMonitor.HealthState.SUSPECT, cluster.rwHealthState);
    verify(mockErrorCounter, times(2)).inc();
    verify(mockStateTransitionCounter, times(1)).inc();

    // Test 3: RO endpoint error transitions independently
    CacheMonitor.reportError("localhost:6379", "localhost:6380", false,
        new RedisCommandExecutionException("READONLY"), "SET");
    assertEquals(CacheMonitor.HealthState.SUSPECT, cluster.rwHealthState);
    assertEquals(CacheMonitor.HealthState.SUSPECT, cluster.roHealthState);
    verify(mockErrorCounter, times(3)).inc();
    verify(mockStateTransitionCounter, times(2)).inc();

    // Test 4: Unregistered cluster logs warning without metrics
    CacheMonitor.reportError("localhost:9999", null, true,
        new RedisConnectionException("Connection refused"), "SET");
    verify(mockErrorCounter, times(3)).inc();
    verify(mockStateTransitionCounter, times(2)).inc();
  }

  @Test
  void testInFlightSize_MemoryPressureScenarios() throws Exception {
    props.setProperty(CacheMonitor.CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT.name, "1000");
    registerClusterWithTelemetry("localhost:6379", null);
    CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", null);

    // Test 1: Below limit remains healthy
    CacheMonitor.incrementInFlightSizeStatic("localhost:6379", null, 500);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.rwHealthState);
    assertEquals(500L, cluster.inFlightWriteSizeBytes.get());

    // Test 2: Exceeds limit transitions to degraded
    CacheMonitor.incrementInFlightSizeStatic("localhost:6379", null, 1000);
    assertEquals(CacheMonitor.HealthState.DEGRADED, cluster.rwHealthState);
    assertEquals(1500L, cluster.inFlightWriteSizeBytes.get());
    verify(mockStateTransitionCounter, times(1)).inc();

    // Test 3: Decrement reduces size
    CacheMonitor.decrementInFlightSizeStatic("localhost:6379", null, 900);
    assertEquals(600L, cluster.inFlightWriteSizeBytes.get());

    // Test 4: Decrement never goes negative
    CacheMonitor.decrementInFlightSizeStatic("localhost:6379", null, 1000);
    assertEquals(0L, cluster.inFlightWriteSizeBytes.get());
  }

  @Test
  void testClusterStateAggregation() throws Exception {
    // Test 1: Single endpoint state transitions
    registerCluster("localhost:6379", null);
    CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", null);
    assertEquals(CacheMonitor.HealthState.HEALTHY,
        CacheMonitor.getClusterState("localhost:6379", null));

    cluster.transitionToState(CacheMonitor.HealthState.SUSPECT, true, "test_setup", null);
    assertEquals(CacheMonitor.HealthState.SUSPECT,
        CacheMonitor.getClusterState("localhost:6379", null));

    cluster.transitionToState(CacheMonitor.HealthState.DEGRADED, true, "test_setup", null);
    assertEquals(CacheMonitor.HealthState.DEGRADED,
        CacheMonitor.getClusterState("localhost:6379", null));

    // Test 2: Dual endpoints
    registerCluster("localhost:6379", "localhost:6380");
    cluster = getCluster("localhost:6379", "localhost:6380");
    assertEquals(CacheMonitor.HealthState.HEALTHY,
        CacheMonitor.getClusterState("localhost:6379", "localhost:6380"));

    cluster.transitionToState(CacheMonitor.HealthState.DEGRADED, true, "test_setup", null);
    assertEquals(CacheMonitor.HealthState.DEGRADED,
        CacheMonitor.getClusterState("localhost:6379", "localhost:6380"));

    cluster.transitionToState(CacheMonitor.HealthState.HEALTHY, true, "test_setup", null);
    cluster.transitionToState(CacheMonitor.HealthState.DEGRADED, false, "test_setup", null);
    assertEquals(CacheMonitor.HealthState.DEGRADED,
        CacheMonitor.getClusterState("localhost:6379", "localhost:6380"));

    cluster.transitionToState(CacheMonitor.HealthState.SUSPECT, true, "test_setup", null);
    cluster.transitionToState(CacheMonitor.HealthState.HEALTHY, false, "test_setup", null);
    assertEquals(CacheMonitor.HealthState.SUSPECT,
        CacheMonitor.getClusterState("localhost:6379", "localhost:6380"));

    // Test 3: Unregistered cluster defaults to healthy
    assertEquals(CacheMonitor.HealthState.HEALTHY,
        CacheMonitor.getClusterState("nonexistent:9999", null));
  }

  @Test
  void testExecutePing_StateTransitions() throws Exception {
    registerClusterWithTelemetry("localhost:6379", null);
    CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", null);
    CacheMonitor instance = (CacheMonitor) getStaticField("instance");

    // Success: maintains healthy state
    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping()).thenReturn(true);
    invokeExecutePing(instance, cluster, true);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.rwHealthState);
    assertEquals(1, cluster.consecutiveRwSuccesses);
    verify(mockHealthCheckSuccessCounter, times(1)).inc();

    // Failure: HEALTHY → SUSPECT
    when(mockRwPingConnection.ping()).thenReturn(false);
    invokeExecutePing(instance, cluster, true);
    assertEquals(CacheMonitor.HealthState.SUSPECT, cluster.rwHealthState);
    verify(mockHealthCheckFailureCounter, times(1)).inc();
    verify(mockStateTransitionCounter, times(1)).inc();

    // Three consecutive failures: SUSPECT → DEGRADED
    invokeExecutePing(instance, cluster, true);
    invokeExecutePing(instance, cluster, true);
    invokeExecutePing(instance, cluster, true);
    assertEquals(CacheMonitor.HealthState.DEGRADED, cluster.rwHealthState);
    verify(mockStateTransitionCounter, times(2)).inc();

    // Recovery: Three successes from SUSPECT → HEALTHY
    cluster.transitionToState(CacheMonitor.HealthState.SUSPECT, true, "test_setup", null);
    when(mockRwPingConnection.ping()).thenReturn(true);
    invokeExecutePing(instance, cluster, true);
    invokeExecutePing(instance, cluster, true);
    invokeExecutePing(instance, cluster, true);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.rwHealthState);
    verify(mockStateTransitionCounter, times(3)).inc();
  }

  @Test
  void testExecutePing_EdgeCases() throws Exception {
    // Dual endpoints track independently
    registerClusterWithTelemetry("localhost:6379", "localhost:6380");
    final CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", "localhost:6380");
    final CacheMonitor instance = (CacheMonitor) getStaticField("instance");

    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping()).thenReturn(false);
    when(mockRoPingConnection.isOpen()).thenReturn(true);
    when(mockRoPingConnection.ping()).thenReturn(true);

    invokeExecutePing(instance, cluster, true);
    invokeExecutePing(instance, cluster, false);
    assertEquals(CacheMonitor.HealthState.SUSPECT, cluster.rwHealthState);
    assertEquals(CacheMonitor.HealthState.HEALTHY, cluster.roHealthState);

    // Connection closed = failure (no ping call)
    reset(mockRwPingConnection);
    when(mockRwPingConnection.isOpen()).thenReturn(false);
    invokeExecutePing(instance, cluster, true);
    verify(mockRwPingConnection, never()).ping();

    // Exception during ping = failure
    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping()).thenThrow(new RuntimeException("timeout"));
    invokeExecutePing(instance, cluster, true);
    assertEquals(CacheMonitor.HealthState.SUSPECT, cluster.rwHealthState);

    // Recovery from DEGRADED when memory clears
    resetCacheMonitorState();
    props.setProperty(CacheMonitor.CACHE_IN_FLIGHT_WRITE_SIZE_LIMIT.name, "1000");
    registerClusterWithTelemetry("localhost:6379", null);
    CacheMonitor.ClusterHealthState singleCluster = getCluster("localhost:6379", null);
    final CacheMonitor singleInstance = (CacheMonitor) getStaticField("instance");
    singleCluster.transitionToState(CacheMonitor.HealthState.DEGRADED, true, "test_setup", null);
    singleCluster.inFlightWriteSizeBytes.set(500);

    reset(mockRwPingConnection);
    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping()).thenReturn(true);
    invokeExecutePing(singleInstance, singleCluster, true);
    invokeExecutePing(singleInstance, singleCluster, true);
    invokeExecutePing(singleInstance, singleCluster, true);
    assertEquals(CacheMonitor.HealthState.HEALTHY, singleCluster.rwHealthState);
  }

  private void invokeExecutePing(
      CacheMonitor instance,
      CacheMonitor.ClusterHealthState cluster,
      boolean isRw
  ) throws Exception {
    Method method = CacheMonitor.class.getDeclaredMethod("executePing", CacheMonitor.ClusterHealthState.class,
        boolean.class);
    method.setAccessible(true);
    method.invoke(instance, cluster, isRw);
  }

  @Test
  void testMonitor_MonitoringBehavior() throws Exception {
    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping()).thenReturn(true);

    // HEALTHY: skips ping by default
    registerClusterWithTelemetry("localhost:6379", null);
    CacheMonitor instance = (CacheMonitor) getStaticField("instance");
    CacheMonitor spy = spy(instance);
    AtomicInteger sleepCount = new AtomicInteger(0);
    CacheMonitor finalSpy = spy;
    doAnswer(inv -> {
      if (sleepCount.incrementAndGet() >= 2) {  // ← ADD counter check back
        Field stopField = AbstractMonitor.class.getDeclaredField("stop");
        stopField.setAccessible(true);
        ((AtomicBoolean) stopField.get(finalSpy)).set(true);
      }
      return null;
    }).when(spy).sleep(anyLong());
    try {
      spy.monitor();
    } catch (Exception e) {
      // Do nothing
    }
    verify(mockRwPingConnection, never()).ping();

    // HEALTHY with health check enabled: executes ping
    resetCacheMonitorState();
    props.setProperty(CacheMonitor.CACHE_HEALTH_CHECK_IN_HEALTHY_STATE.name, "true");
    registerClusterWithTelemetry("localhost:6379", null);
    instance = (CacheMonitor) getStaticField("instance");
    spy = spy(instance);
    sleepCount.set(0);
    CacheMonitor finalSpy1 = spy;
    doAnswer(inv -> {
      if (sleepCount.incrementAndGet() >= 2) {  // ← ADD counter check back
        Field stopField = AbstractMonitor.class.getDeclaredField("stop");
        stopField.setAccessible(true);
        ((AtomicBoolean) stopField.get(finalSpy1)).set(true);
      }
      return null;
    }).when(spy).sleep(anyLong());
    try {
      spy.monitor();
    } catch (Exception e) {
      // Do nothing
    }
    verify(mockRwPingConnection, times(2)).ping();

    // SUSPECT/DEGRADED: always pings
    resetCacheMonitorState();
    props.remove("cacheHealthCheckInHealthyState");
    registerClusterWithTelemetry("localhost:6379", null);
    CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", null);
    cluster.transitionToState(CacheMonitor.HealthState.SUSPECT, true, "test_setup", null);
    instance = (CacheMonitor) getStaticField("instance");
    spy = spy(instance);
    sleepCount.set(0);
    CacheMonitor finalSpy2 = spy;
    doAnswer(inv -> {
      if (sleepCount.incrementAndGet() >= 2) {
        Field stopField = AbstractMonitor.class.getDeclaredField("stop");
        stopField.setAccessible(true);
        ((AtomicBoolean) stopField.get(finalSpy2)).set(true);
      }
      return null;
    }).when(spy).sleep(anyLong());
    try {
      spy.monitor();
    } catch (Exception e) {
      // Do nothing
    }
    verify(mockRwPingConnection, times(4)).ping();

    // Dual endpoints: pings both
    resetCacheMonitorState();
    registerClusterWithTelemetry("localhost:6379", "localhost:6380");
    cluster = getCluster("localhost:6379", "localhost:6380");
    cluster.transitionToState(CacheMonitor.HealthState.SUSPECT, true, "test_setup", null);
    instance = (CacheMonitor) getStaticField("instance");
    reset(mockRwPingConnection, mockRoPingConnection);
    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping()).thenReturn(true);
    when(mockRoPingConnection.isOpen()).thenReturn(true);
    when(mockRoPingConnection.ping()).thenReturn(true);
    spy = spy(instance);
    sleepCount.set(0);
    CacheMonitor finalSpy3 = spy;
    doAnswer(inv -> {
      if (sleepCount.incrementAndGet() >= 2) {
        Field stopField = AbstractMonitor.class.getDeclaredField("stop");
        stopField.setAccessible(true);
        ((AtomicBoolean) stopField.get(finalSpy3)).set(true);
      }
      return null;
    }).when(spy).sleep(anyLong());
    try {
      spy.monitor();
    } catch (Exception e) {
      // Do nothing
    }
    verify(mockRwPingConnection, times(2)).ping();
    verify(mockRoPingConnection, times(2)).ping();

    // Exception during ping: continues monitoring
    resetCacheMonitorState();
    registerClusterWithTelemetry("localhost:6379", null);
    cluster = getCluster("localhost:6379", null);
    cluster.transitionToState(CacheMonitor.HealthState.SUSPECT, true, "test_setup", null);
    instance = (CacheMonitor) getStaticField("instance");
    reset(mockRwPingConnection);
    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping())
        .thenThrow(new RuntimeException("Test exception"))
        .thenReturn(true);
    spy = spy(instance);
    sleepCount.set(0);
    CacheMonitor finalSpy4 = spy;
    doAnswer(inv -> {
      if (sleepCount.incrementAndGet() >= 2) {
        Field stopField = AbstractMonitor.class.getDeclaredField("stop");
        stopField.setAccessible(true);
        ((AtomicBoolean) stopField.get(finalSpy4)).set(true);
      }
      return null;
    }).when(spy).sleep(anyLong());
    try {
      spy.monitor();
    } catch (Exception e) {
      // Do nothing
    }
    verify(mockRwPingConnection, times(2)).ping();
  }

  @Test
  void testPrivateHelperMethods() throws Exception {
    // Test 1: classifyError categorizes exceptions correctly
    Method classifyMethod = CacheMonitor.class.getDeclaredMethod("classifyError", Throwable.class);
    classifyMethod.setAccessible(true);

    assertEquals(CacheMonitor.ErrorCategory.CONNECTION,
        classifyMethod.invoke(null, new RedisConnectionException("Connection refused")));
    assertEquals(CacheMonitor.ErrorCategory.COMMAND,
        classifyMethod.invoke(null, new RedisCommandExecutionException("READONLY")));
    assertEquals(CacheMonitor.ErrorCategory.COMMAND,
        classifyMethod.invoke(null, new RedisCommandExecutionException("WRONGTYPE")));
    assertEquals(CacheMonitor.ErrorCategory.RESOURCE,
        classifyMethod.invoke(null, new RedisCommandExecutionException("OOM")));
    assertEquals(CacheMonitor.ErrorCategory.RESOURCE,
        classifyMethod.invoke(null, new RedisCommandExecutionException("CLUSTERDOWN")));
    assertEquals(CacheMonitor.ErrorCategory.RESOURCE,
        classifyMethod.invoke(null, new RedisCommandExecutionException((String) null)));
    assertEquals(CacheMonitor.ErrorCategory.CONNECTION,
        classifyMethod.invoke(null, new io.lettuce.core.RedisException("Generic error")));
    assertEquals(CacheMonitor.ErrorCategory.DATA,
        classifyMethod.invoke(null, new RuntimeException("Serialization failed")));

    // Test 2: isRecoverableError determines recoverability
    Method recoverableMethod = CacheMonitor.class.getDeclaredMethod("isRecoverableError",
        CacheMonitor.ErrorCategory.class);
    recoverableMethod.setAccessible(true);

    assertTrue((Boolean) recoverableMethod.invoke(null, CacheMonitor.ErrorCategory.CONNECTION));
    assertTrue((Boolean) recoverableMethod.invoke(null, CacheMonitor.ErrorCategory.COMMAND));
    assertTrue((Boolean) recoverableMethod.invoke(null, CacheMonitor.ErrorCategory.RESOURCE));
    assertFalse((Boolean) recoverableMethod.invoke(null, CacheMonitor.ErrorCategory.DATA));

    // Test 3: ping method handles various connection states
    registerCluster("localhost:6379", null);
    CacheMonitor.ClusterHealthState cluster = getCluster("localhost:6379", null);
    CacheMonitor instance = (CacheMonitor) getStaticField("instance");

    Method pingMethod = CacheMonitor.class.getDeclaredMethod("ping", CacheMonitor.ClusterHealthState.class,
        boolean.class);
    pingMethod.setAccessible(true);

    cluster.rwPingConnection = null;
    assertFalse((Boolean) pingMethod.invoke(instance, cluster, true));

    cluster.rwPingConnection = mockRwPingConnection;
    when(mockRwPingConnection.isOpen()).thenReturn(false);
    assertFalse((Boolean) pingMethod.invoke(instance, cluster, true));

    when(mockRwPingConnection.isOpen()).thenReturn(true);
    when(mockRwPingConnection.ping()).thenReturn(true);
    assertTrue((Boolean) pingMethod.invoke(instance, cluster, true));

    when(mockRwPingConnection.ping()).thenThrow(new RuntimeException("Ping failed"));
    assertFalse((Boolean) pingMethod.invoke(instance, cluster, true));
  }
}
