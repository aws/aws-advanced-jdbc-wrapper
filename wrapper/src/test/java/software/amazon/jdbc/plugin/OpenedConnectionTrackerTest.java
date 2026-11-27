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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class OpenedConnectionTrackerTest {

  private static final Logger TRACKER_LOGGER = Logger.getLogger(OpenedConnectionTracker.class.getName());

  private AutoCloseable closeable;

  @Mock private PluginService mockPluginService;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock private TelemetryContext mockTelemetryContext;
  @Mock private Connection mockConnection;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(any(), any())).thenReturn(mockTelemetryContext);
  }

  @AfterEach
  void tearDown() throws Exception {
    OpenedConnectionTracker.clearCache();
    closeable.close();
  }

  @Test
  void testReleaseResources_shutsDownExecutors() {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance1.cluster-xyz.us-east-1.rds.amazonaws.com")
        .build();

    final OpenedConnectionTracker tracker = new OpenedConnectionTracker(mockPluginService);

    // Track a connection to trigger executor creation
    tracker.populateOpenedConnectionQueue(hostSpec, mockConnection);
    assertFalse(OpenedConnectionTracker.openedConnections.isEmpty());

    // Release resources should clear connections
    OpenedConnectionTracker.releaseResources();
    assertTrue(OpenedConnectionTracker.openedConnections.isEmpty());
  }

  @Test
  void testExecutorsRecreatedAfterReleaseResources() {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance1.cluster-xyz.us-east-1.rds.amazonaws.com")
        .build();

    final OpenedConnectionTracker tracker = new OpenedConnectionTracker(mockPluginService);

    // Track a connection to trigger executor creation
    tracker.populateOpenedConnectionQueue(hostSpec, mockConnection);
    assertFalse(OpenedConnectionTracker.openedConnections.isEmpty());

    // Release resources - shuts down executors
    OpenedConnectionTracker.releaseResources();
    assertTrue(OpenedConnectionTracker.openedConnections.isEmpty());

    // Track again after release - executors should be recreated
    tracker.populateOpenedConnectionQueue(hostSpec, mockConnection);
    assertFalse(OpenedConnectionTracker.openedConnections.isEmpty());
  }

  @Test
  void testInvalidateConnectionsAfterReleaseResources() throws SQLException, InterruptedException {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance1.cluster-xyz.us-east-1.rds.amazonaws.com")
        .build();

    final AtomicBoolean aborted = new AtomicBoolean(false);
    final Connection abortableConnection = mock(Connection.class);
    doAnswer(invocation -> {
      aborted.set(true);
      return null;
    }).when(abortableConnection).abort(any());

    final OpenedConnectionTracker tracker = new OpenedConnectionTracker(mockPluginService);

    // Track a connection
    tracker.populateOpenedConnectionQueue(hostSpec, abortableConnection);

    // Release resources - shuts down executors
    OpenedConnectionTracker.releaseResources();

    // Track a new connection after release
    tracker.populateOpenedConnectionQueue(hostSpec, abortableConnection);

    // Invalidate connections - this should recreate the invalidate executor
    tracker.invalidateAllConnections(hostSpec);

    // Wait for async invalidation to complete
    TimeUnit.MILLISECONDS.sleep(500);

    // Connection should have been aborted
    assertTrue(aborted.get());
  }

  @Test
  void gracePeriodAbortsOnlyCapturedConnectionsStillTracked() throws SQLException {
    final OpenedConnectionTracker tracker = spy(new OpenedConnectionTracker(mockPluginService));
    final HostSpec host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("blue.example.com").hostId("blue-instance").build();
    final HostSpec otherHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("blue-reader.example.com").hostId("blue-reader").build();
    final Connection failing = mock(Connection.class);
    final Connection closed = mock(Connection.class);
    final Connection green = mock(Connection.class);
    tracker.populateOpenedConnectionQueue(host, mockConnection);
    tracker.populateOpenedConnectionQueue(otherHost, mockConnection);
    tracker.populateOpenedConnectionQueue(host, failing);
    final TrackedConnectionList.Node closingNode = tracker.populateOpenedConnectionQueue(otherHost, closed);
    doThrow(new SQLException("abort failed")).when(failing).abort(any());

    final ArgumentCaptor<Runnable> timer = ArgumentCaptor.forClass(Runnable.class);
    doNothing().when(tracker).scheduleInvalidation(timer.capture(), eq(5000L));
    tracker.scheduleInvalidation(Arrays.asList(host, otherHost), 5000);
    verify(mockConnection, never()).abort(any());

    closed.close();
    tracker.removeConnectionTracking(closingNode);
    tracker.populateOpenedConnectionQueue(host, green);

    final Level previousLevel = TRACKER_LOGGER.getLevel();
    final Handler handler = mock(Handler.class);
    TRACKER_LOGGER.addHandler(handler);
    try {
      TRACKER_LOGGER.setLevel(Level.FINEST);
      timer.getValue().run();
    } finally {
      TRACKER_LOGGER.removeHandler(handler);
      TRACKER_LOGGER.setLevel(previousLevel);
    }

    final String expectedMessage = Messages.get("OpenedConnectionTracker.invalidatingSnapshot", new Object[] {2});
    final InOrder events = inOrder(handler, mockConnection);
    events.verify(handler).publish(argThat(record ->
        record.getLevel() == Level.FINEST && expectedMessage.equals(record.getMessage())));
    events.verify(mockConnection).abort(any());
    verify(failing).abort(any());
    verify(closed, never()).abort(any());
    verify(green, never()).abort(any());
    assertEquals(1, OpenedConnectionTracker.openedConnections.get(host.getHostAndPort()).size());
  }

  @Test
  void clearingCacheDuringGracePeriodDiscardsCapturedConnections() throws SQLException {
    final OpenedConnectionTracker tracker = spy(new OpenedConnectionTracker(mockPluginService));
    final HostSpec host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("blue.example.com").build();
    tracker.populateOpenedConnectionQueue(host, mockConnection);
    final ArgumentCaptor<Runnable> timer = ArgumentCaptor.forClass(Runnable.class);
    doNothing().when(tracker).scheduleInvalidation(timer.capture(), eq(200L));

    tracker.scheduleInvalidation(Collections.singletonList(host), 200);
    OpenedConnectionTracker.clearCache();
    tracker.populateOpenedConnectionQueue(host, mockConnection);
    timer.getValue().run();

    verify(mockConnection, never()).abort(any());
    assertEquals(1, OpenedConnectionTracker.openedConnections.get(host.getHostAndPort()).size());
  }

  @Test
  void scheduledInvalidationWaitsForGracePeriod() throws SQLException, InterruptedException {
    final HostSpec host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("blue.example.com").build();
    final OpenedConnectionTracker tracker = new OpenedConnectionTracker(mockPluginService);
    tracker.populateOpenedConnectionQueue(host, mockConnection);
    final CountDownLatch aborted = new CountDownLatch(1);
    final AtomicLong abortTime = new AtomicLong();
    doAnswer(invocation -> {
      abortTime.set(System.nanoTime());
      aborted.countDown();
      return null;
    }).when(mockConnection).abort(any());

    final long start = System.nanoTime();
    tracker.scheduleInvalidation(Collections.singletonList(host), 50);
    assertTrue(aborted.await(5, TimeUnit.SECONDS));
    assertTrue(abortTime.get() - start >= TimeUnit.MILLISECONDS.toNanos(50));
  }

  @Test
  void testMultipleReleaseResourcesCalls() {
    // Calling releaseResources multiple times should not throw
    OpenedConnectionTracker.releaseResources();
    OpenedConnectionTracker.releaseResources();
    OpenedConnectionTracker.releaseResources();

    // Should still be able to track connections after multiple releases
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance1.cluster-xyz.us-east-1.rds.amazonaws.com")
        .build();

    final OpenedConnectionTracker tracker = new OpenedConnectionTracker(mockPluginService);
    TrackedConnectionList.Node node = tracker.populateOpenedConnectionQueue(hostSpec, mockConnection);
    assertNotNull(node);
    assertFalse(OpenedConnectionTracker.openedConnections.isEmpty());
  }
}
