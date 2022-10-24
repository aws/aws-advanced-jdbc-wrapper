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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.jdbc.JdbcConnection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;

class MonitorServiceImplTest {

  private static final Set<String> NODE_KEYS =
      new HashSet<>(Collections.singletonList("any.node.domain"));
  private static final int FAILURE_DETECTION_TIME_MILLIS = 10;
  private static final int FAILURE_DETECTION_INTERVAL_MILLIS = 100;
  private static final int FAILURE_DETECTION_COUNT = 3;

  @Mock private MonitorInitializer monitorInitializer;
  @Mock private ExecutorServiceInitializer executorServiceInitializer;
  @Mock private Monitor monitorA;
  @Mock private Monitor monitorB;
  @Mock private ExecutorService executorService;
  @Mock private Future<?> task;
  @Mock private HostSpec hostSpec;
  @Mock private JdbcConnection connection;

  private Properties properties;
  private AutoCloseable closeable;
  private MonitorServiceImpl monitorService;
  private ArgumentCaptor<MonitorConnectionContext> contextCaptor;

  @BeforeEach
  void init() {
    properties = new Properties();
    closeable = MockitoAnnotations.openMocks(this);
    contextCaptor = ArgumentCaptor.forClass(MonitorConnectionContext.class);

    when(monitorInitializer.createMonitor(
            any(HostSpec.class), any(Properties.class), any(MonitorService.class)))
        .thenReturn(monitorA, monitorB);

    when(executorServiceInitializer.createExecutorService()).thenReturn(executorService);

    doReturn(task).when(executorService).submit(any(Monitor.class));

    monitorService = new MonitorServiceImpl(monitorInitializer, executorServiceInitializer);
  }

  @AfterEach
  void cleanUp() throws Exception {
    monitorService.releaseResources();
    closeable.close();
  }

  @Test
  void test_startMonitoringWithNoExecutor() {
    doNothing().when(monitorA).startMonitoring(contextCaptor.capture());

    monitorService.startMonitoring(
        connection,
        NODE_KEYS,
        hostSpec,
        properties,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    assertNotNull(contextCaptor.getValue());
    verify(executorService).submit(eq(monitorA));
  }

  @Test
  void test_startMonitoringCalledMultipleTimes() {
    doNothing().when(monitorA).startMonitoring(contextCaptor.capture());

    final int runs = 5;

    for (int i = 0; i < runs; i++) {
      monitorService.startMonitoring(
          connection,
          NODE_KEYS,
          hostSpec,
          properties,
          FAILURE_DETECTION_TIME_MILLIS,
          FAILURE_DETECTION_INTERVAL_MILLIS,
          FAILURE_DETECTION_COUNT);
    }

    assertNotNull(contextCaptor.getValue());

    // executorService should only be called once.
    verify(executorService).submit(eq(monitorA));
  }

  @Test
  void test_stopMonitoringWithInterruptedThread() {
    doNothing().when(monitorA).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context =
        monitorService.startMonitoring(
            connection,
            NODE_KEYS,
            hostSpec,
            properties,
            FAILURE_DETECTION_TIME_MILLIS,
            FAILURE_DETECTION_INTERVAL_MILLIS,
            FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(context);

    assertEquals(context, contextCaptor.getValue());
    verify(monitorA).stopMonitoring(any());
  }

  @Test
  void test_stopMonitoringCalledTwice() {
    doNothing().when(monitorA).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context =
        monitorService.startMonitoring(
            connection,
            NODE_KEYS,
            hostSpec,
            properties,
            FAILURE_DETECTION_TIME_MILLIS,
            FAILURE_DETECTION_INTERVAL_MILLIS,
            FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(context);

    assertEquals(context, contextCaptor.getValue());

    monitorService.stopMonitoring(context);
    verify(monitorA, times(2)).stopMonitoring(any());
  }

  @Test
  void test_stopMonitoringForAllConnections_withInvalidNodeKeys() {
    monitorService.stopMonitoringForAllConnections(Collections.emptySet());
    monitorService.stopMonitoringForAllConnections(new HashSet<>(Collections.singletonList("foo")));
  }

  @Test
  void test_stopMonitoringForAllConnections() {
    final Set<String> keysA = new HashSet<>(Collections.singletonList("monitorA"));
    final Set<String> keysB = new HashSet<>(Collections.singletonList("monitorB"));

    // Populate threadContainer with MonitorA and MonitorB
    monitorService.getMonitor(keysA, new HostSpec("test"), new Properties());
    monitorService.getMonitor(keysB, new HostSpec("test"), new Properties());

    monitorService.stopMonitoringForAllConnections(keysA);
    verify(monitorA).clearContexts();

    monitorService.stopMonitoringForAllConnections(keysB);
    verify(monitorB).clearContexts();
  }

  @Test
  void test_getMonitorCalledWithMultipleNodesInKeys() {
    final Set<String> nodeKeys = new HashSet<>();
    nodeKeys.add("nodeOne.domain");
    nodeKeys.add("nodeTwo.domain");

    final Set<String> nodeKeysTwo = new HashSet<>();
    nodeKeysTwo.add("nodeTwo.domain");

    final Monitor monitorOne = monitorService.getMonitor(nodeKeys, hostSpec, properties);
    assertNotNull(monitorOne);

    // Should get the same monitor as before as contain the same key "nodeTwo.domain"
    final Monitor monitorOneSame = monitorService.getMonitor(nodeKeysTwo, hostSpec, properties);
    assertNotNull(monitorOneSame);
    assertEquals(monitorOne, monitorOneSame);

    // Make sure createMonitor was called once
    verify(monitorInitializer).createMonitor(eq(hostSpec), eq(properties), eq(monitorService));
  }

  @Test
  void test_getMonitorCalledWithDifferentNodeKeys() {
    final Set<String> nodeKeys = new HashSet<>();
    nodeKeys.add("nodeNEW.domain");

    final Monitor monitorOne = monitorService.getMonitor(nodeKeys, hostSpec, properties);
    assertNotNull(monitorOne);

    // Ensuring monitor is the same one and not creating a new one
    final Monitor monitorOneDupe = monitorService.getMonitor(nodeKeys, hostSpec, properties);
    assertEquals(monitorOne, monitorOneDupe);

    // Ensuring monitors are not the same as they have different keys
    // "any.node.domain" compared to "nodeNEW.domain"
    final Monitor monitorTwo = monitorService.getMonitor(NODE_KEYS, hostSpec, properties);
    assertNotNull(monitorTwo);
    assertNotEquals(monitorOne, monitorTwo);
  }

  @Test
  void test_getMonitorCalledWithSameKeysInDifferentNodeKeys() {
    final Set<String> nodeKeys = new HashSet<>();
    nodeKeys.add("nodeA");

    final Set<String> nodeKeysTwo = new HashSet<>();
    nodeKeysTwo.add("nodeA");
    nodeKeysTwo.add("nodeB");

    final Set<String> nodeKeysThree = new HashSet<>();
    nodeKeysThree.add("nodeB");

    final Monitor monitorOne = monitorService.getMonitor(nodeKeys, hostSpec, properties);
    assertNotNull(monitorOne);

    // Add a new key using the same monitor
    // Adding "nodeB" as a new key using the same monitor as "nodeA"
    final Monitor monitorOneDupe = monitorService.getMonitor(nodeKeysTwo, hostSpec, properties);
    assertEquals(monitorOne, monitorOneDupe);

    // Using new keyset but same node, "nodeB" should return same monitor
    final Monitor monitorOneDupeAgain =
        monitorService.getMonitor(nodeKeysThree, hostSpec, properties);
    assertEquals(monitorOne, monitorOneDupeAgain);

    // Make sure createMonitor was called once
    verify(monitorInitializer).createMonitor(eq(hostSpec), eq(properties), eq(monitorService));
  }

  @Test
  void test_startMonitoringNoNodeKeys() {
    final Set<String> nodeKeysEmpty = new HashSet<>();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            monitorService.startMonitoring(
                connection,
                nodeKeysEmpty,
                hostSpec,
                properties,
                FAILURE_DETECTION_TIME_MILLIS,
                FAILURE_DETECTION_INTERVAL_MILLIS,
                FAILURE_DETECTION_COUNT));
  }

  @Test
  void test_releaseResourceTwice() {
    // Ensure no NullPointerException.
    monitorService.releaseResources();
    monitorService.releaseResources();
  }
}
