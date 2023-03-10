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

package software.amazon.jdbc.plugin.efm;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.BooleanProperty;
import com.mysql.cj.conf.LongProperty;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;

class MonitorImplTest {

  @Mock PluginService pluginService;
  @Mock Connection connection;
  @Mock HostSpec hostSpec;
  @Mock Properties properties;
  @Mock MonitorConnectionContext contextWithShortInterval;
  @Mock MonitorConnectionContext contextWithLongInterval;
  @Mock BooleanProperty booleanProperty;
  @Mock LongProperty longProperty;
  @Mock ExecutorServiceInitializer executorServiceInitializer;
  @Mock ExecutorService executorService;
  @Mock Future<?> futureResult;
  @Mock MonitorServiceImpl monitorService;

  private static final long SHORT_INTERVAL_MILLIS = 30;
  private static final long SHORT_INTERVAL_SECONDS = TimeUnit.MILLISECONDS.toSeconds(SHORT_INTERVAL_MILLIS);
  private static final long LONG_INTERVAL_MILLIS = 300;

  private AutoCloseable closeable;
  private MonitorImpl monitor;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(contextWithShortInterval.getFailureDetectionIntervalMillis())
        .thenReturn(SHORT_INTERVAL_MILLIS);
    when(contextWithLongInterval.getFailureDetectionIntervalMillis())
        .thenReturn(LONG_INTERVAL_MILLIS);
    when(booleanProperty.getStringValue()).thenReturn(Boolean.TRUE.toString());
    when(longProperty.getValue()).thenReturn(SHORT_INTERVAL_MILLIS);
    when(pluginService.connect(any(HostSpec.class), any(Properties.class))).thenReturn(connection);
    when(executorServiceInitializer.createExecutorService()).thenReturn(executorService);
    MonitorThreadContainer.getInstance(executorServiceInitializer);

    monitor = spy(new MonitorImpl(pluginService, hostSpec, properties, 0L, monitorService));
  }

  @AfterEach
  void cleanUp() throws Exception {
    monitorService.releaseResources();
    MonitorThreadContainer.releaseInstance();
    closeable.close();
  }

  @Test
  void test_5_isConnectionHealthyWithNoExistingConnection() throws SQLException {
    final MonitorImpl.ConnectionStatus status =
        monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    verify(pluginService).connect(any(HostSpec.class), any(Properties.class));
    assertTrue(status.isValid);
    assertTrue(status.elapsedTimeNano >= 0);
  }

  @Test
  void test_6_isConnectionHealthyWithExistingConnection() throws SQLException {
    when(connection.isValid(eq((int) SHORT_INTERVAL_SECONDS))).thenReturn(Boolean.TRUE, Boolean.FALSE);
    when(connection.isClosed()).thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    final MonitorImpl.ConnectionStatus status1 =
        monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    assertTrue(status1.isValid);

    final MonitorImpl.ConnectionStatus status2 =
        monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    assertFalse(status2.isValid);

    verify(connection, times(2)).isValid(anyInt());
  }

  @Test
  void test_7_isConnectionHealthyWithSQLException() throws SQLException {
    when(connection.isValid(anyInt())).thenThrow(new SQLException());
    when(connection.isClosed()).thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    assertDoesNotThrow(
        () -> {
          final MonitorImpl.ConnectionStatus status =
              monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
          assertFalse(status.isValid);
          assertTrue(status.elapsedTimeNano >= 0);
        });
  }

  @Test
  void test_8_runWithoutContext() {
    final MonitorThreadContainer container =
        MonitorThreadContainer.getInstance(executorServiceInitializer);
    final Map<String, Monitor> monitorMap = container.getMonitorMap();
    final Map<Monitor, Future<?>> taskMap = container.getTasksMap();

    doAnswer(
        invocation -> {
          container.releaseResource(invocation.getArgument(0));
          return null;
        })
        .when(monitorService)
        .notifyUnused(any(Monitor.class));

    // Put monitor into container map
    final String nodeKey = "monitorA";
    monitorMap.put(nodeKey, monitor);
    taskMap.put(monitor, futureResult);

    // Run monitor without contexts
    // Should end by itself
    monitor.run();

    // After running with empty context, monitor should be out of the map
    assertNull(monitorMap.get(nodeKey));
    assertNull(taskMap.get(monitor));

    // Clean-up
    MonitorThreadContainer.releaseInstance();
  }

  @RepeatedTest(1000)
  void test_9_runWithContext() {
    final MonitorThreadContainer container =
        MonitorThreadContainer.getInstance(executorServiceInitializer);
    final Map<String, Monitor> monitorMap = container.getMonitorMap();
    final Map<Monitor, Future<?>> taskMap = container.getTasksMap();

    doAnswer(
        invocation -> {
          container.releaseResource(invocation.getArgument(0));
          return null;
        })
        .when(monitorService)
        .notifyUnused(any(Monitor.class));

    // Put monitor into container map
    final String nodeKey = "monitorA";
    monitorMap.put(nodeKey, monitor);
    taskMap.put(monitor, futureResult);

    // Put context
    monitor.startMonitoring(contextWithShortInterval);
    // Set and start thread to remove context from monitor
    final Thread thread =
        new Thread(
            () -> {
              try {
                Thread.sleep(SHORT_INTERVAL_MILLIS);
              } catch (InterruptedException e) {
                fail("Thread to stop monitoring context was interrupted.", e);
              } finally {
                monitor.stopMonitoring(contextWithShortInterval);
              }
            });
    thread.start();

    // Run monitor
    // Should end by itself once thread above stops monitoring 'contextWithShortInterval'
    monitor.run();

    // After running monitor should be out of the map
    assertNull(monitorMap.get(nodeKey));
    assertNull(taskMap.get(monitor));

    // Clean-up
    MonitorThreadContainer.releaseInstance();
  }
}
