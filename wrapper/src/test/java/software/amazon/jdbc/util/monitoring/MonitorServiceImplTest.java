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

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointMonitorImpl;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class MonitorServiceImplTest {
  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImplTest.class.getName());

  @Mock StorageService storageService;
  @Mock TelemetryFactory telemetryFactory;
  @Mock TargetDriverDialect targetDriverDialect;
  @Mock Dialect dbDialect;
  @Mock EventPublisher publisher;
  MonitorServiceImpl monitorService;
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    monitorService = new MonitorServiceImpl(publisher) {
      @Override
      protected void initCleanupThread(long cleanupIntervalNanos) {
        // Do nothing
      }
    };
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
    monitorService.releaseResources();
  }

  @Test
  public void testMonitorError_monitorReCreated() throws SQLException, InterruptedException {
    monitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(1),
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
        );
    String key = "testMonitor";
    NoOpMonitor monitor = monitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        storageService,
        telemetryFactory,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        targetDriverDialect,
        dbDialect,
        new Properties(),
        (connectionService, pluginService) -> new NoOpMonitor(monitorService, 30)
    );

    Monitor storedMonitor = monitorService.get(NoOpMonitor.class, key);
    assertNotNull(storedMonitor);
    assertEquals(monitor, storedMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, monitor.getState());

    monitor.state = MonitorState.ERROR;
    monitorService.checkMonitors();

    assertEquals(MonitorState.STOPPED, monitor.getState());

    Monitor newMonitor = monitorService.get(NoOpMonitor.class, key);
    assertNotNull(newMonitor);
    assertNotEquals(monitor, newMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, newMonitor.getState());
  }

  @Test
  public void testMonitorStuck_monitorReCreated() throws SQLException, InterruptedException {
    monitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        1, // heartbeat times out immediately
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );
    String key = "testMonitor";
    NoOpMonitor monitor = monitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        storageService,
        telemetryFactory,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        targetDriverDialect,
        dbDialect,
        new Properties(),
        (connectionService, pluginService) -> new NoOpMonitor(monitorService, 30)
    );

    Monitor storedMonitor = monitorService.get(NoOpMonitor.class, key);
    assertNotNull(storedMonitor);
    assertEquals(monitor, storedMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, monitor.getState());

    // checkMonitors() should detect the heartbeat/inactivity timeout, stop the monitor, and re-create a new one.
    monitorService.checkMonitors();

    assertEquals(MonitorState.STOPPED, monitor.getState());

    Monitor newMonitor = monitorService.get(NoOpMonitor.class, key);
    assertNotNull(newMonitor);
    assertNotEquals(monitor, newMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, newMonitor.getState());
  }

  @Test
  public void testMonitorExpired() throws SQLException, InterruptedException {
    monitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MILLISECONDS.toNanos(200), // monitor expires after 200ms
        TimeUnit.MINUTES.toNanos(1),
        // even though we pass a re-create policy, we should not re-create it if the monitor is expired since this
        // indicates it is not being used.
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );
    String key = "testMonitor";
    NoOpMonitor monitor = monitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        storageService,
        telemetryFactory,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        targetDriverDialect,
        dbDialect,
        new Properties(),
        (connectionService, pluginService) -> new NoOpMonitor(monitorService, 30)
    );

    Monitor storedMonitor = monitorService.get(NoOpMonitor.class, key);
    assertNotNull(storedMonitor);
    assertEquals(monitor, storedMonitor);
    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    assertEquals(MonitorState.RUNNING, monitor.getState());

    // checkMonitors() should detect the expiration timeout and stop/remove the monitor.
    monitorService.checkMonitors();

    assertEquals(MonitorState.STOPPED, monitor.getState());

    Monitor newMonitor = monitorService.get(NoOpMonitor.class, key);
    // monitor should have been removed when checkMonitors() was called.
    assertNull(newMonitor);
  }

  @Test
  public void testMonitorMismatch() {
    assertThrows(IllegalStateException.class, () -> {
      monitorService.runIfAbsent(
          CustomEndpointMonitorImpl.class,
          "testMonitor",
          storageService,
          telemetryFactory,
          "jdbc:postgresql://somehost/somedb",
          "someProtocol",
          targetDriverDialect,
          dbDialect,
          new Properties(),
          // indicated monitor class is CustomEndpointMonitorImpl, but actual monitor is NoOpMonitor. The monitor
          // service should detect this and throw an exception.
          (connectionService, pluginService) -> new NoOpMonitor(monitorService, 30)
      );
    });
  }

  @Test
  public void testRemove() throws SQLException, InterruptedException {
    monitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(1),
        // even though we pass a re-create policy, we should not re-create it if the monitor is expired since this
        // indicates it is not being used.
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );

    String key = "testMonitor";
    NoOpMonitor monitor = monitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        storageService,
        telemetryFactory,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        targetDriverDialect,
        dbDialect,
        new Properties(),
        (connectionService, pluginService) -> new NoOpMonitor(monitorService, 30)
    );
    assertNotNull(monitor);

    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    Monitor removedMonitor = monitorService.remove(NoOpMonitor.class, key);
    assertEquals(monitor, removedMonitor);
    assertEquals(MonitorState.RUNNING, monitor.getState());
  }

  @Test
  public void testStopAndRemove() throws SQLException, InterruptedException {
    monitorService.registerMonitorTypeIfAbsent(
        NoOpMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(1),
        // even though we pass a re-create policy, we should not re-create it if the monitor is expired since this
        // indicates it is not being used.
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
    );

    String key = "testMonitor";
    NoOpMonitor monitor = monitorService.runIfAbsent(
        NoOpMonitor.class,
        key,
        storageService,
        telemetryFactory,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        targetDriverDialect,
        dbDialect,
        new Properties(),
        (connectionService, pluginService) -> new NoOpMonitor(monitorService, 30)
    );
    assertNotNull(monitor);

    // need to wait to give time for the monitor executor to start the monitor thread.
    TimeUnit.MILLISECONDS.sleep(250);
    monitorService.stopAndRemove(NoOpMonitor.class, key);
    assertNull(monitorService.get(NoOpMonitor.class, key));
    assertEquals(MonitorState.STOPPED, monitor.getState());
  }

  static class NoOpMonitor extends AbstractMonitor {
    protected NoOpMonitor(
        MonitorService monitorService,
        long terminationTimeoutSec) {
      super(monitorService, terminationTimeoutSec);
    }

    @Override
    public void monitor() {
      // do nothing.
    }

    @Override
    public void close() {
      // do nothing.
    }
  }
}
