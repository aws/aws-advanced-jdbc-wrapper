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

package software.amazon.jdbc.plugin.bluegreen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.BlueGreenDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.FullServicesContainer;

/**
 * Tests how a monitor chooses the cluster it watches from the rows of the deployment status table.
 */
class BlueGreenStatusMonitorTest {

  private static final String PRODUCTION_CLUSTER = "app-db.cluster-abc123.us-east-2.rds.amazonaws.com";
  private static final String GREEN_CLUSTER = "app-db-green-x1y2z3.cluster-abc123.us-east-2.rds.amazonaws.com";
  private static final String PRODUCTION_IP = "10.0.0.1";
  private static final String GREEN_IP = "10.0.1.1";

  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private PluginService mockPluginService;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockStatement;

  private AutoCloseable closeable;
  private Dialect mockDialect;
  private final HostSpec productionHostSpec =
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(PRODUCTION_CLUSTER).port(3306).build();

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    mockDialect = mock(Dialect.class, withSettings().extraInterfaces(BlueGreenDialect.class));
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(((BlueGreenDialect) mockDialect).isBlueGreenStatusAvailable(any())).thenReturn(true);
    when(((BlueGreenDialect) mockDialect).getBlueGreenStatusQuery()).thenReturn("SELECT 1");
    when(mockConnection.isClosed()).thenReturn(false);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  /** A monitor connected to the application's endpoint, which has not chosen its cluster yet. */
  private BlueGreenStatusMonitor newMonitor() {
    final Map<BlueGreenIntervalRate, Long> intervals = new EnumMap<>(BlueGreenIntervalRate.class);
    final BlueGreenStatusMonitor monitor = new BlueGreenStatusMonitor(
        BlueGreenRole.TARGET, "bgd-1", productionHostSpec, mockServicesContainer, new Properties(), intervals, null) {
      @Override
      protected Optional<String> getIpAddress(final String host) {
        return Optional.of(PRODUCTION_CLUSTER.equals(host) ? PRODUCTION_IP : GREEN_IP);
      }

      @Override
      protected void initHostListProvider() {
        // Not needed here: the tests read whether the cluster was chosen.
      }
    };
    monitor.connection.set(mockConnection);
    monitor.connectionHostSpec.set(productionHostSpec);
    monitor.connectedIpAddress.set(PRODUCTION_IP);
    return monitor;
  }

  /** Makes the status table answer one TARGET row. */
  private void statusRow(final String status, final String endpoint) throws SQLException {
    final ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("version")).thenReturn("1.0");
    when(resultSet.getString("endpoint")).thenReturn(endpoint);
    when(resultSet.getInt("port")).thenReturn(3306);
    when(resultSet.getString("role")).thenReturn("BLUE_GREEN_DEPLOYMENT_TARGET");
    when(resultSet.getString("status")).thenReturn(status);
    when(mockStatement.executeQuery()).thenReturn(resultSet);
  }

  @ParameterizedTest
  @ValueSource(strings = {"SWITCHOVER_COMPLETED", "SWITCHOVER_IN_POST_PROCESSING"})
  void finishedRowAtTheConnectedEndpointDoesNotChooseTheCluster(final String status) throws SQLException {
    final BlueGreenStatusMonitor monitor = newMonitor();
    statusRow(status, PRODUCTION_CLUSTER);

    monitor.collectStatus();

    assertFalse(monitor.connectionHostSpecCorrect.get());
    assertTrue(monitor.panicMode.get());
    assertEquals(productionHostSpec, monitor.connectionHostSpec.get());
  }

  @Test
  void finishedRowAtAnotherEndpointDoesNotReconnectTheMonitor() throws SQLException {
    final BlueGreenStatusMonitor monitor = newMonitor();
    statusRow("SWITCHOVER_COMPLETED", GREEN_CLUSTER);

    monitor.collectStatus();

    assertFalse(monitor.connectionHostSpecCorrect.get());
    assertEquals(productionHostSpec, monitor.connectionHostSpec.get());
  }

  @Test
  void createdRowAtTheConnectedEndpointChoosesTheCluster() throws SQLException {
    final BlueGreenStatusMonitor monitor = newMonitor();
    statusRow("AVAILABLE", PRODUCTION_CLUSTER);

    monitor.collectStatus();

    assertTrue(monitor.connectionHostSpecCorrect.get());
    assertFalse(monitor.panicMode.get());
    assertEquals(productionHostSpec, monitor.connectionHostSpec.get());
  }

  @Test
  void createdRowAtAnotherEndpointReconnectsTheMonitor() throws SQLException {
    final BlueGreenStatusMonitor monitor = newMonitor();
    statusRow("AVAILABLE", GREEN_CLUSTER);

    monitor.collectStatus();

    assertTrue(monitor.connectionHostSpecCorrect.get());
    assertEquals(GREEN_CLUSTER, monitor.connectionHostSpec.get().getHost());
  }

  @Test
  void newDeploymentAfterAFinishedRowIsStillTargeted() throws SQLException {
    final BlueGreenStatusMonitor monitor = newMonitor();
    statusRow("SWITCHOVER_COMPLETED", PRODUCTION_CLUSTER);
    monitor.collectStatus();
    assertFalse(monitor.connectionHostSpecCorrect.get());

    // The next deployment is created: its green cluster is another endpoint, so the monitor moves to it
    monitor.connection.set(mockConnection);
    statusRow("AVAILABLE", GREEN_CLUSTER);
    monitor.collectStatus();

    assertTrue(monitor.connectionHostSpecCorrect.get());
    assertEquals(GREEN_CLUSTER, monitor.connectionHostSpec.get().getHost());
  }

  /**
   * A monitor loop that is still running when the application's main thread returns must not keep
   * the JVM alive, so its threads have to be daemon threads like every other wrapper thread.
   */
  @Test
  void monitorThreadsAreDaemonThreads() throws Exception {
    final BlueGreenStatusMonitor monitor = newMonitor();
    try {
      final Future<Boolean> loopThreadIsDaemon =
          monitor.executorService.submit(() -> Thread.currentThread().isDaemon());
      final Future<Boolean> openConnectionThreadIsDaemon =
          monitor.openConnectionExecutorService.submit(() -> Thread.currentThread().isDaemon());
      assertTrue(loopThreadIsDaemon.get(5, TimeUnit.SECONDS));
      assertTrue(openConnectionThreadIsDaemon.get(5, TimeUnit.SECONDS));
    } finally {
      monitor.setStop(true);
    }
    assertTrue(monitor.executorService.isShutdown());
    assertTrue(monitor.openConnectionExecutorService.isShutdown());
  }
}
