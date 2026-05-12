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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class OpenedConnectionTrackerTest {

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
