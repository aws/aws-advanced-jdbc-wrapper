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

package software.amazon.jdbc.util.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointMonitorImpl;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class MonitorServiceImplTest {
  @Mock StorageService mockStorageService;
  @Mock FullServicesContainer mockContainer;
  @Mock ConnectionProvider mockConnectionProvider;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock Dialect mockDbDialect;
  @Mock EventPublisher mockPublisher;
  MonitorServiceImpl spyMonitorService;
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    spyMonitorService = spy(new MonitorServiceImpl(mockPublisher));
    doNothing().when(spyMonitorService).initCleanupThread(anyInt());

    try {
      doReturn(mockContainer).when(spyMonitorService)
          .getNewServicesContainer(any(), any(), any(), any(), any(), any(), any(), any());
    } catch (SQLException e) {
      Assertions.fail(
          "Encountered exception while stubbing MonitorServiceImpl#getConnectionService: " + e.getMessage());
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
    spyMonitorService.releaseResources();
  }

  @Test
  public void testMonitorError_monitorReCreated() throws SQLException, InterruptedException {
    spyMonitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(1),
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );
    String key = "testMonitor";
    NoOpMonitor monitor = spyMonitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        mockStorageService,
        mockTelemetryFactory,
        mockConnectionProvider,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        mockTargetDriverDialect,
        mockDbDialect,
        new Properties(),
        (serviceContainer) -> new NoOpMonitor(30)
    );

    Monitor storedMonitor = spyMonitorService.get(NoOpMonitor.class, key);
    assertNotNull(storedMonitor);
    assertEquals(monitor, storedMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, monitor.getState());

    monitor.state.set(MonitorState.ERROR);
    spyMonitorService.checkMonitors();

    assertEquals(MonitorState.STOPPED, monitor.getState());

    Monitor newMonitor = spyMonitorService.get(NoOpMonitor.class, key);
    assertNotNull(newMonitor);
    assertNotEquals(monitor, newMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, newMonitor.getState());
  }

  @Test
  public void testMonitorStuck_monitorReCreated() throws SQLException, InterruptedException {
    spyMonitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        1, // heartbeat times out immediately
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );
    String key = "testMonitor";
    NoOpMonitor monitor = spyMonitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        mockStorageService,
        mockTelemetryFactory,
        mockConnectionProvider,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        mockTargetDriverDialect,
        mockDbDialect,
        new Properties(),
        (serviceContainer) -> new NoOpMonitor(30)
    );

    Monitor storedMonitor = spyMonitorService.get(NoOpMonitor.class, key);
    assertNotNull(storedMonitor);
    assertEquals(monitor, storedMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, monitor.getState());

    // checkMonitors() should detect the heartbeat/inactivity timeout, stop the monitor, and re-create a new one.
    spyMonitorService.checkMonitors();

    assertEquals(MonitorState.STOPPED, monitor.getState());

    Monitor newMonitor = spyMonitorService.get(NoOpMonitor.class, key);
    assertNotNull(newMonitor);
    assertNotEquals(monitor, newMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, newMonitor.getState());
  }

  @Test
  public void testMonitorExpired() throws SQLException, InterruptedException {
    spyMonitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MILLISECONDS.toNanos(200), // monitor expires after 200ms
        TimeUnit.MINUTES.toNanos(1),
        // even though we pass a re-create policy, we should not re-create it if the monitor is expired since this
        // indicates it is not being used.
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );
    String key = "testMonitor";
    NoOpMonitor monitor = spyMonitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        mockStorageService,
        mockTelemetryFactory,
        mockConnectionProvider,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        mockTargetDriverDialect,
        mockDbDialect,
        new Properties(),
        (serviceContainer) -> new NoOpMonitor(30)
    );

    Monitor storedMonitor = spyMonitorService.get(NoOpMonitor.class, key);
    assertNotNull(storedMonitor);
    assertEquals(monitor, storedMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, monitor.getState());

    // checkMonitors() should detect the expiration timeout and stop/remove the monitor.
    spyMonitorService.checkMonitors();

    assertEquals(MonitorState.STOPPED, monitor.getState());

    Monitor newMonitor = spyMonitorService.get(NoOpMonitor.class, key);
    // monitor should have been removed when checkMonitors() was called.
    assertNull(newMonitor);
  }

  @Test
  public void testMonitorMismatch() {
    assertThrows(IllegalStateException.class, () -> spyMonitorService.runIfAbsent(
        CustomEndpointMonitorImpl.class,
        "testMonitor",
        mockStorageService,
        mockTelemetryFactory,
        mockConnectionProvider,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        mockTargetDriverDialect,
        mockDbDialect,
        new Properties(),
        // indicated monitor class is CustomEndpointMonitorImpl, but actual monitor is NoOpMonitor. The monitor
        // service should detect this and throw an exception.
        (serviceContainer) -> new NoOpMonitor(30)
    ));
  }

  @Test
  public void testRemove() throws SQLException, InterruptedException {
    spyMonitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(1),
        // even though we pass a re-create policy, we should not re-create it if the monitor is expired since this
        // indicates it is not being used.
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );

    String key = "testMonitor";
    NoOpMonitor monitor = spyMonitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        mockStorageService,
        mockTelemetryFactory,
        mockConnectionProvider,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        mockTargetDriverDialect,
        mockDbDialect,
        new Properties(),
        (serviceContainer) -> new NoOpMonitor(30)
    );
    assertNotNull(monitor);

    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    Monitor removedMonitor = spyMonitorService.remove(NoOpMonitor.class, key);
    assertEquals(monitor, removedMonitor);
    assertEquals(MonitorState.RUNNING, monitor.getState());
  }

  @Test
  public void testStopAndRemove() throws SQLException, InterruptedException {
    spyMonitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(1),
        // even though we pass a re-create policy, we should not re-create it if the monitor is expired since this
        // indicates it is not being used.
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );

    String key = "testMonitor";
    NoOpMonitor monitor = spyMonitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        mockStorageService,
        mockTelemetryFactory,
        mockConnectionProvider,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        mockTargetDriverDialect,
        mockDbDialect,
        new Properties(),
        (serviceContainer) -> new NoOpMonitor(30)
    );
    assertNotNull(monitor);

    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    spyMonitorService.stopAndRemove(NoOpMonitor.class, key);
    assertNull(spyMonitorService.get(NoOpMonitor.class, key));
    assertEquals(MonitorState.STOPPED, monitor.getState());
  }

  static class NoOpMonitor extends AbstractMonitor {
    protected NoOpMonitor(
        long terminationTimeoutSec) {
      super(terminationTimeoutSec);
    }

    @Override
    public void monitor() {
      // do nothing.
    }
  }
}
