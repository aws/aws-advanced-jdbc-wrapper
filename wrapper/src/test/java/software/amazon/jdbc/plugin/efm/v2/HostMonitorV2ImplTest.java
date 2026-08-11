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

package software.amazon.jdbc.plugin.efm.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class HostMonitorV2ImplTest {

  @Mock PluginService pluginService;
  @Mock FullServicesContainer servicesContainer;
  @Mock Connection connection;
  @Mock HostSpec hostSpec;
  @Mock HostMonitorConnectionContextV2 context;
  @Mock TelemetryFactory telemetryFactory;
  @Mock TelemetryContext telemetryContext;
  @Mock TelemetryCounter telemetryCounter;
  @Mock EventPublisher eventPublisher;

  private static final int FAILURE_DETECTION_TIME_MILLIS = 30000;
  private static final int FAILURE_DETECTION_INTERVAL_MILLIS = 5000;
  private static final int FAILURE_DETECTION_COUNT = 3;

  private AutoCloseable closeable;
  private HostMonitorV2Impl monitor;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    final Properties properties = new Properties();

    when(pluginService.forceConnect(any(HostSpec.class), any(Properties.class))).thenReturn(connection);
    when(pluginService.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(telemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(telemetryContext);
    when(telemetryFactory.createCounter(anyString())).thenReturn(telemetryCounter);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(servicesContainer.getEventPublisher()).thenReturn(eventPublisher);
    when(hostSpec.getHost()).thenReturn("test-host");
    when(hostSpec.getUrl()).thenReturn("test-url");

    monitor = spy(new HostMonitorV2Impl(
        servicesContainer,
        hostSpec,
        properties,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT,
        telemetryCounter));
  }

  @AfterEach
  void cleanUp() throws Exception {
    if (monitor != null) {
      monitor.stop();
    }
    closeable.close();
  }

  @Test
  @Timeout(30)
  void newContextRun_survivesTransientException_andKeepsDraining() throws Exception {
    // Queue one healthy context into newContexts (uses the real clock).
    when(context.isActive()).thenReturn(true);
    monitor.startMonitoring(context);
    assertEquals(1, newContextsSize());

    // Make the first loop iteration throw; every later call returns a far-future time so the
    // queued context is past-due and must be drained into activeContexts.
    final long future = System.nanoTime() + TimeUnit.HOURS.toNanos(1);
    final AtomicBoolean thrownOnce = new AtomicBoolean(false);
    doAnswer(inv -> {
      if (thrownOnce.compareAndSet(false, true)) {
        throw new RuntimeException("simulated transient monitoring error");
      }
      return future;
    }).when(monitor).getCurrentTimeNano();

    final Thread drainer = new Thread(monitor::newContextRun, "test-newContextRun");
    drainer.setDaemon(true);
    drainer.start();

    boolean drained = false;
    for (int i = 0; i < 100 && !drained; i++) {
      drained = newContextsSize() == 0 && activeContextsSize() == 1;
      TimeUnit.MILLISECONDS.sleep(100);
    }
    monitor.stop();
    drainer.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(drained,
        "newContextRun() must keep draining newContexts after a transient exception. Observed newContexts="
            + newContextsSize() + ", activeContexts=" + activeContextsSize());
  }

  @Test
  @Timeout(30)
  void newContextRun_stopsTheMonitorWhenTheThreadExits() throws Exception {
    // Whenever the drainer thread is gone, nothing drains newContexts anymore. The monitor must be
    // marked as stopped so that the monitor service replaces it, rather than leaving a monitor that
    // keeps accepting contexts it will never process.
    final Thread drainer = new Thread(monitor::newContextRun, "test-newContextRun");
    drainer.setDaemon(true);
    drainer.start();

    TimeUnit.MILLISECONDS.sleep(200);
    drainer.interrupt();
    drainer.join(TimeUnit.SECONDS.toMillis(5));

    assertFalse(drainer.isAlive(), "the drainer thread should have exited");
    assertTrue(isStopped(), "newContextRun() must stop the monitor when its thread exits");
  }

  @Test
  void startMonitoring_doesNotQueueContextsWhenStopped() throws Exception {
    monitor.stop();

    monitor.startMonitoring(context);

    assertEquals(0, newContextsSize(),
        "a stopped monitor never drains newContexts, so it must not queue new contexts");
  }

  private boolean isStopped() throws Exception {
    final Field field = getInheritedField("stop");
    return ((AtomicBoolean) field.get(monitor)).get();
  }

  @SuppressWarnings("unchecked")
  private int newContextsSize() throws Exception {
    final Field field = HostMonitorV2Impl.class.getDeclaredField("newContexts");
    field.setAccessible(true);
    final Map<Long, Queue<WeakReference<HostMonitorConnectionContextV2>>> newContexts =
        (Map<Long, Queue<WeakReference<HostMonitorConnectionContextV2>>>) field.get(monitor);
    return newContexts.values().stream().mapToInt(Queue::size).sum();
  }

  @SuppressWarnings("unchecked")
  private int activeContextsSize() throws Exception {
    final Field field = HostMonitorV2Impl.class.getDeclaredField("activeContexts");
    field.setAccessible(true);
    final Queue<WeakReference<HostMonitorConnectionContextV2>> activeContexts =
        (Queue<WeakReference<HostMonitorConnectionContextV2>>) field.get(monitor);
    return activeContexts.size();
  }

  private Field getInheritedField(final String name) throws Exception {
    Class<?> clazz = HostMonitorV2Impl.class;
    while (clazz != null) {
      try {
        final Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (final NoSuchFieldException ex) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }
}
