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
import static org.junit.jupiter.api.Assertions.fail;

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
  final static long CLEANUP_INTERVAL_MS = 1000;
  MonitorServiceImpl monitorService;
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    monitorService = new MonitorServiceImpl(TimeUnit.MILLISECONDS.toNanos(CLEANUP_INTERVAL_MS), publisher);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
    monitorService.releaseResources();
  }

  // @RepeatedTest(100)
  @Test
  public void testMonitorRecreation() throws SQLException, InterruptedException {
    monitorService.registerMonitorTypeIfAbsent(
        ExceptionThrowingMonitor.class,
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(1),
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE)),
        null
        );
    String key = "testMonitor";
    Monitor monitor = monitorService.runIfAbsent(
        ExceptionThrowingMonitor.class,
        "testMonitor",
        storageService,
        telemetryFactory,
        "jdbc:postgresql://somehost/somedb",
        "someProtocol",
        targetDriverDialect,
        dbDialect,
        new Properties(),
        (connectionService, pluginService) -> new ExceptionThrowingMonitor(
            // We want to throw the test exception shortly before cleanup
            monitorService, 30, CLEANUP_INTERVAL_MS  - 300)
    );

    MonitorServiceImpl.MonitorItem monitorItem =
        monitorService.monitorCaches.get(ExceptionThrowingMonitor.class).getCache().get(key);
    assertNotNull(monitorItem);
    assertEquals(monitor, monitorItem.getMonitor());

    // Wait for monitor service cleanup thread to execute
    TimeUnit.MILLISECONDS.sleep(CLEANUP_INTERVAL_MS + 200);
    LOGGER.finest("Done sleeping, testing monitor state...");
    assertEquals(MonitorState.STOPPED, monitor.getState());

    MonitorServiceImpl.MonitorItem newMonitorItem =
        monitorService.monitorCaches.get(ExceptionThrowingMonitor.class).getCache().get(key);
    assertNotNull(newMonitorItem);
    assertNotEquals(monitor, newMonitorItem.getMonitor());
    assertEquals(MonitorState.RUNNING, newMonitorItem.getMonitor().getState());
  }

  static class MonitorTestException extends RuntimeException {

  }

  static class ExceptionThrowingMonitor extends AbstractMonitor {
    final long exceptionDelayMs;

    protected ExceptionThrowingMonitor(
        MonitorService monitorService,
        long terminationTimeoutSec,
        long exceptionDelayMs) {
      super(monitorService, terminationTimeoutSec);
      this.exceptionDelayMs = exceptionDelayMs;
    }

    @Override
    public void monitor() {
      try {
        TimeUnit.MILLISECONDS.sleep(this.exceptionDelayMs);
        throw new MonitorTestException();
      } catch (InterruptedException e) {
        fail("Unexpected InterruptedException in test monitor");
      }
    }

    @Override
    public void close() {
      // do nothing.
    }
  }
}
