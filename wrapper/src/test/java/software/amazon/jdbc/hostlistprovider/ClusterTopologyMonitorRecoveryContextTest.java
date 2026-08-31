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

package software.amazon.jdbc.hostlistprovider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.monitoring.MonitorInitializer;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.storage.TestStorageServiceImpl;

/**
 * Tests for the recovery-context rebinding mechanism in {@link ClusterTopologyMonitorImpl}.
 * Validates that:
 * <ul>
 *   <li>A monitor in panic mode accepts a recovery context.</li>
 *   <li>A healthy monitor rejects a recovery context (no-op).</li>
 *   <li>Topology sharing by clusterId is preserved — only one monitor per clusterId.</li>
 *   <li>Concurrent offers do not corrupt monitor state.</li>
 *   <li>Monitoring properties are correctly rebuilt and sanitized from the new context.</li>
 *   <li>{@link RdsHostListProvider} offers its context when obtaining a shared monitor.</li>
 * </ul>
 */
class ClusterTopologyMonitorRecoveryContextTest {

  private static final String CLUSTER_ID = "test-cluster";
  private static final String MONITORING_PREFIX = "topology-monitoring-";

  @Mock private PluginService mockPluginService;
  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private FullServicesContainer mockServicesContainer2;
  @Mock private FullServicesContainer mockMinimalContainer;
  @Mock private TopologyUtils mockTopologyUtils;
  @Mock private EventPublisher mockEventPublisher;
  @Mock private MonitorService mockMonitorService;
  @Mock private HostListProviderService mockHostListProviderService;
  @Mock private TargetDriverDialect mockTargetDriverDialect;
  @Mock private Connection mockConnection;

  private StorageService storageService;
  private AutoCloseable closeable;

  private final HostSpec hostA = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("host-a.cluster.us-east-1.rds.amazonaws.com").port(5432).build();
  private final HostSpec hostB = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("host-b.cluster.us-east-1.rds.amazonaws.com").port(5432).build();
  private final HostSpec instanceTemplate = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("?.us-east-1.rds.amazonaws.com").port(5432).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    storageService = new TestStorageServiceImpl(mockEventPublisher);

    when(mockServicesContainer.getStorageService()).thenReturn(storageService);
    when(mockServicesContainer.getEventPublisher()).thenReturn(mockEventPublisher);
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getMonitorService()).thenReturn(mockMonitorService);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.removeHostSelectionProperties(any(Properties.class)))
        .thenReturn(Collections.emptySet());

    when(mockServicesContainer2.getStorageService()).thenReturn(storageService);
    when(mockServicesContainer2.getEventPublisher()).thenReturn(mockEventPublisher);
    when(mockServicesContainer2.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer2.getMonitorService()).thenReturn(mockMonitorService);
    when(mockMinimalContainer.getStorageService()).thenReturn(storageService);
    when(mockMinimalContainer.getEventPublisher()).thenReturn(mockEventPublisher);
    when(mockMinimalContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockMinimalContainer.getMonitorService()).thenReturn(mockMonitorService);
    when(mockHostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
  }

  @AfterEach
  void tearDown() throws Exception {
    storageService.clearAll();
    closeable.close();
  }

  /**
   * Creates a spied monitor with {@code createRecoveryServiceContainer} stubbed to return
   * {@link #mockMinimalContainer}, avoiding the need for deep integration infrastructure.
   */
  private ClusterTopologyMonitorImpl createMonitor(
      HostSpec initialHost, Properties props, FullServicesContainer container) {
    ClusterTopologyMonitorImpl monitor = spy(new ClusterTopologyMonitorImpl(
        container,
        mockTopologyUtils,
        CLUSTER_ID,
        initialHost,
        props,
        instanceTemplate,
        TimeUnit.SECONDS.toNanos(30),
        TimeUnit.MILLISECONDS.toNanos(100)));
    try {
      doReturn(mockMinimalContainer).when(monitor)
          .createRecoveryServiceContainer(any(FullServicesContainer.class), any(Properties.class));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return monitor;
  }

  // ---- offerRecoveryContext tests ----

  @Test
  void testOfferRecoveryContext_acceptedWhenInPanicMode() {
    Properties propsA = new Properties();
    propsA.setProperty("user", "userA");
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // Monitor starts in panic mode (no monitoring connection)
    assertTrue(monitor.isInPanicMode(), "Monitor should start in panic mode");

    Properties propsB = new Properties();
    propsB.setProperty("user", "userB");
    boolean accepted = monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);
    assertTrue(accepted, "Recovery context should be accepted when in panic mode");

    // Verify the pending context is set
    ClusterTopologyMonitorImpl.RecoveryContext pending = monitor.pendingRecoveryContext.get();
    assertNotNull(pending, "Pending recovery context should be non-null");
    assertSame(hostB, pending.initialHostSpec);
    assertSame(propsB, pending.properties);
    assertSame(mockServicesContainer2, pending.servicesContainer);
  }

  @Test
  void testOfferRecoveryContext_rejectedWhenHealthy() {
    Properties props = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, props, mockServicesContainer);

    // Simulate healthy state by setting a monitoring connection
    monitor.monitoringConnection.set(mockConnection);
    assertFalse(monitor.isInPanicMode(), "Monitor should not be in panic mode");

    boolean accepted = monitor.offerRecoveryContext(hostB, new Properties(), mockServicesContainer2);
    assertFalse(accepted, "Recovery context should be rejected when monitor is healthy");
    assertNull(monitor.pendingRecoveryContext.get(), "No pending context should be stored");
  }

  // ---- applyPendingRecoveryContext tests ----

  @Test
  void testApplyPendingRecoveryContext_replacesFields() {
    Properties propsA = new Properties();
    propsA.setProperty("user", "userA");
    propsA.setProperty("password", "passA");
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // Verify initial state
    assertSame(hostA, monitor.initialHostSpec);
    assertSame(propsA, monitor.properties);
    assertSame(mockServicesContainer, monitor.servicesContainer);

    // Offer new context
    Properties propsB = new Properties();
    propsB.setProperty("user", "userB");
    propsB.setProperty("password", "passB");
    monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);

    // Apply it (simulating the monitoring thread)
    monitor.applyPendingRecoveryContext();

    // Verify fields were replaced
    assertSame(hostB, monitor.initialHostSpec);
    assertSame(propsB, monitor.properties);
    // The container should be the minimal container, not the provider's full container.
    assertSame(mockMinimalContainer, monitor.servicesContainer,
        "servicesContainer should be the minimal container created from the provider's context");

    // Verify pending context was consumed
    assertNull(monitor.pendingRecoveryContext.get());
  }

  @Test
  void testApplyPendingRecoveryContext_rebuildsMonitoringProperties() {
    // Prepare new context with monitoring prefix overrides
    Properties propsB = new Properties();
    propsB.setProperty("user", "userB");
    propsB.setProperty(MONITORING_PREFIX + "socketTimeout", "9999");
    propsB.setProperty(MONITORING_PREFIX + "connectTimeout", "8888");

    Properties propsA = new Properties();
    propsA.setProperty("user", "userA");
    final ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);
    monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);
    monitor.applyPendingRecoveryContext();

    Properties monProps = monitor.monitoringProperties;
    assertEquals("9999", monProps.getProperty("socketTimeout"),
        "Monitoring prefix override should be applied");
    assertEquals("8888", monProps.getProperty("connectTimeout"),
        "Monitoring prefix override should be applied");
    // The prefixed keys themselves should be removed
    assertNull(monProps.getProperty(MONITORING_PREFIX + "socketTimeout"),
        "Prefixed key should be removed from monitoring properties");
  }

  @Test
  void testApplyPendingRecoveryContext_setsDefaultTimeouts() {
    Properties propsA = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // Offer context with no timeout overrides
    Properties propsB = new Properties();
    propsB.setProperty("user", "userB");
    monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);
    monitor.applyPendingRecoveryContext();

    Properties monProps = monitor.monitoringProperties;
    assertEquals(String.valueOf(ClusterTopologyMonitorImpl.defaultSocketTimeoutMs),
        PropertyDefinition.SOCKET_TIMEOUT.getString(monProps),
        "Default socket timeout should be set");
    assertEquals(String.valueOf(ClusterTopologyMonitorImpl.defaultConnectionTimeoutMs),
        PropertyDefinition.CONNECT_TIMEOUT.getString(monProps),
        "Default connect timeout should be set");
  }

  @Test
  void testApplyPendingRecoveryContext_noOpWhenNoPendingContext() {
    Properties propsA = new Properties();
    propsA.setProperty("user", "userA");
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // No offer made — apply should be a no-op
    monitor.applyPendingRecoveryContext();
    assertSame(hostA, monitor.initialHostSpec, "Fields should remain unchanged");
    assertSame(propsA, monitor.properties, "Properties should remain unchanged");
  }

  @Test
  void testApplyPendingRecoveryContext_noOpIfMonitorBecomesHealthy() {
    Properties propsA = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // Offer context while in panic mode
    monitor.offerRecoveryContext(hostB, new Properties(), mockServicesContainer2);

    // Simulate the monitor becoming healthy before apply
    monitor.monitoringConnection.set(mockConnection);

    monitor.applyPendingRecoveryContext();
    // Fields should not be replaced because monitor is no longer in panic mode
    assertSame(hostA, monitor.initialHostSpec, "Fields should remain unchanged when healthy");
  }

  @Test
  void testApplyPendingRecoveryContext_closesOldConnectionHandler() {
    Properties propsA = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // Force handler creation
    MonitoringConnectionHandler handler = monitor.getConnectionHandler();
    assertNotNull(handler);

    // Offer and apply
    monitor.offerRecoveryContext(hostB, new Properties(), mockServicesContainer2);
    monitor.applyPendingRecoveryContext();

    // The old handler should be nulled (will be lazily recreated)
    // Access it again to get a new one
    MonitoringConnectionHandler newHandler = monitor.getConnectionHandler();
    assertNotNull(newHandler);
    // They should be different instances
    assertTrue(handler != newHandler, "Connection handler should be recreated after context replacement");
  }

  // ---- Concurrent offer safety ----

  @Test
  void testConcurrentOffers_doNotCorruptState() throws Exception {
    Properties propsA = new Properties();
    propsA.setProperty("user", "userA");
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    final int threadCount = 8;
    CyclicBarrier barrier = new CyclicBarrier(threadCount);
    CountDownLatch done = new CountDownLatch(threadCount);
    AtomicInteger acceptedCount = new AtomicInteger(0);
    AtomicReference<Throwable> error = new AtomicReference<>(null);

    for (int i = 0; i < threadCount; i++) {
      final int idx = i;
      final HostSpec host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("host-" + idx + ".cluster.us-east-1.rds.amazonaws.com").port(5432).build();
      final Properties props = new Properties();
      props.setProperty("user", "user" + idx);

      new Thread(() -> {
        try {
          barrier.await(5, TimeUnit.SECONDS);
          boolean accepted = monitor.offerRecoveryContext(host, props, mockServicesContainer2);
          if (accepted) {
            acceptedCount.incrementAndGet();
          }
        } catch (Throwable t) {
          error.compareAndSet(null, t);
        } finally {
          done.countDown();
        }
      }).start();
    }

    assertTrue(done.await(10, TimeUnit.SECONDS), "All threads should complete");
    assertNull(error.get(), "No errors should occur during concurrent offers");

    // All should have been accepted since monitor stays in panic mode
    assertEquals(threadCount, acceptedCount.get(), "All offers should be accepted in panic mode");

    // The pending context should be one of the offered contexts (last-writer-wins)
    ClusterTopologyMonitorImpl.RecoveryContext pending = monitor.pendingRecoveryContext.get();
    assertNotNull(pending, "A pending context should exist");
    assertNotNull(pending.initialHostSpec);
    assertNotNull(pending.properties);

    // Apply and verify no corruption
    monitor.applyPendingRecoveryContext();
    assertNotNull(monitor.initialHostSpec, "initialHostSpec should not be null after apply");
    assertNotNull(monitor.properties, "properties should not be null after apply");
    assertNotNull(monitor.monitoringProperties, "monitoringProperties should not be null after apply");
  }

  // ---- RdsHostListProvider integration ----

  @Test
  void testRdsHostListProvider_sharesMonitorByClusterId() throws SQLException {
    // Two providers with same clusterId should get the same monitor
    ClusterTopologyMonitorImpl sharedMonitor =
        createMonitor(hostA, new Properties(), mockServicesContainer);

    when(mockMonitorService.runIfAbsent(
        eq(ClusterTopologyMonitorImpl.class),
        anyString(),
        any(FullServicesContainer.class),
        any(Properties.class),
        any(MonitorInitializer.class)))
        .thenReturn(sharedMonitor);
    when(mockServicesContainer.getHostListProviderService()).thenReturn(mockHostListProviderService);
    when(mockPluginService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    RdsHostListProvider provider1 = new RdsHostListProvider(
        mockTopologyUtils, new Properties(), "jdbc:someprotocol://url1/", mockServicesContainer);
    provider1.init();
    ClusterTopologyMonitor m1 = provider1.getOrCreateMonitor();

    RdsHostListProvider provider2 = new RdsHostListProvider(
        mockTopologyUtils, new Properties(), "jdbc:someprotocol://url2/", mockServicesContainer);
    provider2.init();
    ClusterTopologyMonitor m2 = provider2.getOrCreateMonitor();

    assertSame(m1, m2, "Both providers should get the same monitor instance");
    verify(mockMonitorService, times(2)).runIfAbsent(
        eq(ClusterTopologyMonitorImpl.class),
        anyString(),
        any(FullServicesContainer.class),
        any(Properties.class),
        any(MonitorInitializer.class));
  }

  @Test
  void testRdsHostListProvider_offersContextToExistingMonitor() throws SQLException {
    ClusterTopologyMonitorImpl sharedMonitor =
        createMonitor(hostA, new Properties(), mockServicesContainer);
    // Monitor is in panic mode by default (no monitoring connection)
    assertTrue(sharedMonitor.isInPanicMode());

    when(mockMonitorService.runIfAbsent(
        eq(ClusterTopologyMonitorImpl.class),
        anyString(),
        any(FullServicesContainer.class),
        any(Properties.class),
        any(MonitorInitializer.class)))
        .thenReturn(sharedMonitor);
    when(mockServicesContainer.getHostListProviderService()).thenReturn(mockHostListProviderService);
    when(mockPluginService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    Properties providerProps = new Properties();
    providerProps.setProperty("user", "provider2-user");

    RdsHostListProvider provider = new RdsHostListProvider(
        mockTopologyUtils, providerProps, "jdbc:someprotocol://url/", mockServicesContainer);
    provider.init();
    provider.getOrCreateMonitor();

    // The provider should have offered its context
    ClusterTopologyMonitorImpl.RecoveryContext pending = sharedMonitor.pendingRecoveryContext.get();
    assertNotNull(pending, "Provider should offer recovery context to a monitor in panic mode");
  }

  @Test
  void testRdsHostListProvider_doesNotOfferContextToHealthyMonitor() throws SQLException {
    ClusterTopologyMonitorImpl sharedMonitor =
        createMonitor(hostA, new Properties(), mockServicesContainer);
    // Make monitor healthy
    sharedMonitor.monitoringConnection.set(mockConnection);
    assertFalse(sharedMonitor.isInPanicMode());

    when(mockMonitorService.runIfAbsent(
        eq(ClusterTopologyMonitorImpl.class),
        anyString(),
        any(FullServicesContainer.class),
        any(Properties.class),
        any(MonitorInitializer.class)))
        .thenReturn(sharedMonitor);
    when(mockServicesContainer.getHostListProviderService()).thenReturn(mockHostListProviderService);
    when(mockPluginService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    RdsHostListProvider provider = new RdsHostListProvider(
        mockTopologyUtils, new Properties(), "jdbc:someprotocol://url/", mockServicesContainer);
    provider.init();
    provider.getOrCreateMonitor();

    assertNull(sharedMonitor.pendingRecoveryContext.get(),
        "No context should be offered to a healthy monitor");
  }

  // ---- buildMonitoringProperties tests ----

  @Test
  void testBuildMonitoringProperties_appliesPrefixOverrides() {
    Properties source = new Properties();
    source.setProperty("user", "testUser");
    source.setProperty(MONITORING_PREFIX + "socketTimeout", "12345");
    source.setProperty(MONITORING_PREFIX + "connectTimeout", "67890");

    Properties result = ClusterTopologyMonitorImpl.buildMonitoringProperties(source, mockServicesContainer);

    assertEquals("12345", result.getProperty("socketTimeout"));
    assertEquals("67890", result.getProperty("connectTimeout"));
    assertNull(result.getProperty(MONITORING_PREFIX + "socketTimeout"),
        "Prefixed key should be removed");
  }

  @Test
  void testBuildMonitoringProperties_stripsHostSelectionProperties() {
    Properties source = new Properties();
    source.setProperty("user", "testUser");

    ClusterTopologyMonitorImpl.buildMonitoringProperties(source, mockServicesContainer);

    verify(mockTargetDriverDialect).removeHostSelectionProperties(any(Properties.class));
  }

  @Test
  void testBuildMonitoringProperties_setsDefaultTimeoutsWhenMissing() {
    Properties source = new Properties();

    Properties result = ClusterTopologyMonitorImpl.buildMonitoringProperties(source, mockServicesContainer);

    assertEquals(String.valueOf(ClusterTopologyMonitorImpl.defaultSocketTimeoutMs),
        PropertyDefinition.SOCKET_TIMEOUT.getString(result));
    assertEquals(String.valueOf(ClusterTopologyMonitorImpl.defaultConnectionTimeoutMs),
        PropertyDefinition.CONNECT_TIMEOUT.getString(result));
  }

  @Test
  void testBuildMonitoringProperties_preservesExplicitTimeouts() {
    Properties source = new Properties();
    PropertyDefinition.SOCKET_TIMEOUT.set(source, "99999");
    PropertyDefinition.CONNECT_TIMEOUT.set(source, "88888");

    Properties result = ClusterTopologyMonitorImpl.buildMonitoringProperties(source, mockServicesContainer);

    assertEquals("99999", PropertyDefinition.SOCKET_TIMEOUT.getString(result));
    assertEquals("88888", PropertyDefinition.CONNECT_TIMEOUT.getString(result));
  }

  @Test
  void testApplyPendingRecoveryContext_updatesLogUnclosedConnections() {
    Properties propsA = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);
    assertFalse(monitor.logUnclosedConnections);

    Properties propsB = new Properties();
    PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.set(propsB, "true");
    monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);
    monitor.applyPendingRecoveryContext();

    assertTrue(monitor.logUnclosedConnections,
        "logUnclosedConnections should be updated from new properties");
  }

  @Test
  void testApplyPendingRecoveryContext_resetsNodeMonitorState() {
    Properties propsA = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // Simulate some node monitor state
    monitor.submittedNodes.put("host-1", true);
    monitor.submittedNodes.put("host-2", true);
    HostSpec reader = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader").hostId("reader-1").build();
    monitor.readerTopologiesById.put("reader-1",
        Collections.singletonList(reader));
    monitor.completedOneCycle.put("reader-1", true);
    monitor.stableTopologiesStartNano = 12345L;

    monitor.offerRecoveryContext(hostB, new Properties(), mockServicesContainer2);
    monitor.applyPendingRecoveryContext();

    assertTrue(monitor.submittedNodes.isEmpty(), "submittedNodes should be cleared");
    assertTrue(monitor.readerTopologiesById.isEmpty(), "readerTopologiesById should be cleared");
    assertTrue(monitor.completedOneCycle.isEmpty(), "completedOneCycle should be cleared");
    assertEquals(0, monitor.stableTopologiesStartNano, "stableTopologiesStartNano should be reset");
  }

  // ---- Stale context race regression tests ----

  /**
   * Regression test: a recovery context offered during the panic→recovery transition
   * must be discarded once the monitor becomes healthy, not carried forward to a future
   * panic episode where it would apply stale credentials. The test calls the production
   * {@link ClusterTopologyMonitorImpl#discardPendingRecoveryContext()} method rather than
   * manipulating the {@code AtomicReference} directly so it will break if that method is
   * removed or changed.
   */
  @Test
  void testPendingContextDiscardedAfterRecovery() {
    Properties propsA = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);
    assertTrue(monitor.isInPanicMode(), "Monitor should start in panic mode");

    // 1. Apply a valid recovery context (simulating first panic-mode iteration).
    Properties propsB = new Properties();
    propsB.setProperty("user", "userB");
    monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);
    monitor.applyPendingRecoveryContext();
    assertNull(monitor.pendingRecoveryContext.get(), "Pending context should be consumed");

    // 2. While still in panic (no connection yet), a third provider offers context.
    //    This simulates the race: offer arrives after apply but before recovery completes.
    HostSpec hostC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host-c.cluster.us-east-1.rds.amazonaws.com").port(5432).build();
    Properties propsC = new Properties();
    propsC.setProperty("user", "userC-stale");
    monitor.offerRecoveryContext(hostC, propsC, mockServicesContainer2);
    assertNotNull(monitor.pendingRecoveryContext.get(), "Late offer should be pending");

    // 3. Monitor recovers (gets a connection).
    monitor.monitoringConnection.set(mockConnection);
    assertFalse(monitor.isInPanicMode(), "Monitor should be healthy now");

    // 4. The production regular-mode branch calls discardPendingRecoveryContext().
    monitor.discardPendingRecoveryContext();

    // 5. Verify the stale context was discarded.
    assertNull(monitor.pendingRecoveryContext.get(),
        "Stale context must be discarded after recovery");

    // 6. Simulate a new panic episode — fields should still reflect the last successful apply,
    //    not the discarded stale context.
    monitor.monitoringConnection.set(null);
    assertTrue(monitor.isInPanicMode());
    monitor.applyPendingRecoveryContext(); // no-op: nothing pending
    assertSame(hostB, monitor.initialHostSpec,
        "initialHostSpec should remain from last successful recovery, not stale offer");
  }

  // ---- Container lifecycle safety tests ----

  @Test
  void testApplyPendingRecoveryContext_createsMinimalContainer() throws SQLException {
    Properties propsA = new Properties();
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    Properties propsB = new Properties();
    propsB.setProperty("user", "userB");
    monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);
    monitor.applyPendingRecoveryContext();

    // Verify createRecoveryServiceContainer was called with the provider's container (not the monitor's)
    verify(monitor).createRecoveryServiceContainer(eq(mockServicesContainer2), eq(propsB));
    // The monitor's container should now be the minimal one, not the provider's full container
    assertSame(mockMinimalContainer, monitor.servicesContainer,
        "Monitor should use the minimal container, not the provider's full container");
  }

  @Test
  void testApplyPendingRecoveryContext_abortsOnContainerCreationFailure() throws SQLException {
    Properties propsA = new Properties();
    propsA.setProperty("user", "userA");
    ClusterTopologyMonitorImpl monitor = createMonitor(hostA, propsA, mockServicesContainer);

    // Simulate existing panic-mode node worker state that should survive the failed apply.
    monitor.submittedNodes.put("node-1", true);
    monitor.submittedNodes.put("node-2", true);
    monitor.completedOneCycle.put("node-1", true);
    monitor.stableTopologiesStartNano = 99999L;

    // Override the spy to throw on container creation
    doThrow(new SQLException("simulated container failure")).when(monitor)
        .createRecoveryServiceContainer(any(FullServicesContainer.class), any(Properties.class));

    Properties propsB = new Properties();
    propsB.setProperty("user", "userB");
    monitor.offerRecoveryContext(hostB, propsB, mockServicesContainer2);
    monitor.applyPendingRecoveryContext();

    // Context fields should remain unchanged because container creation failed.
    assertSame(hostA, monitor.initialHostSpec,
        "initialHostSpec should remain unchanged after container creation failure");
    assertSame(propsA, monitor.properties,
        "properties should remain unchanged after container creation failure");
    assertSame(mockServicesContainer, monitor.servicesContainer,
        "servicesContainer should remain unchanged after container creation failure");

    // Node worker state should be untouched — the destructive reset must not have run.
    assertEquals(2, monitor.submittedNodes.size(),
        "submittedNodes should be untouched after container creation failure");
    assertTrue(monitor.completedOneCycle.containsKey("node-1"),
        "completedOneCycle should be untouched after container creation failure");
    assertEquals(99999L, monitor.stableTopologiesStartNano,
        "stableTopologiesStartNano should be untouched after container creation failure");

    // Pending context should be consumed (not retried with potentially the same broken context)
    assertNull(monitor.pendingRecoveryContext.get(),
        "Pending context should be consumed even on failure");
  }
}
