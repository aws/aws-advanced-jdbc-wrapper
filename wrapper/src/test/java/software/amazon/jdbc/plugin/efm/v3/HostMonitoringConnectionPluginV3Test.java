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

package software.amazon.jdbc.plugin.efm.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

class HostMonitoringConnectionPluginV3Test {

  private static final String MONITORED_METHOD = JdbcMethod.STATEMENT_EXECUTEQUERY.methodName;
  private static final String UNMONITORED_METHOD = JdbcMethod.CONNECTION_ABORT.methodName;
  private static final Object[] EMPTY_ARGS = {};

  @Mock PluginService pluginService;
  @Mock FullServicesContainer servicesContainer;
  @Mock TargetDriverDialect targetDriverDialect;
  @Mock Connection connection;
  @Mock Connection otherConnection;
  @Mock HostSpec hostSpec;
  @Mock HostSpec instanceHostSpec;
  @Mock RdsUtils rdsUtils;
  @Mock HostMonitorServiceV3Impl monitorService;
  @Mock HostMonitorV3Impl monitor;
  @Mock JdbcCallable<ResultSet, SQLException> sqlFunction;
  @Mock ResultSet resultSet;

  private final Properties properties = new Properties();
  private AutoCloseable closeable;
  private HostMonitoringConnectionPluginV3 plugin;

  @BeforeEach
  void init() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(pluginService.getTargetDriverDialect()).thenReturn(targetDriverDialect);
    when(pluginService.getCurrentConnection()).thenReturn(connection);
    when(pluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(targetDriverDialect.getNetworkBoundMethodNames(any()))
        .thenReturn(new HashSet<>(Collections.singletonList(MONITORED_METHOD)));
    when(hostSpec.getHost()).thenReturn("instance-1.xyz.us-east-2.rds.amazonaws.com");
    when(rdsUtils.identifyRdsType(any())).thenReturn(RdsUrlType.RDS_INSTANCE);
    when(sqlFunction.call()).thenReturn(resultSet);
    when(monitor.isStopped()).thenReturn(false);
    doAnswer(inv -> {
      final MonitoredConnectionSlot slot = inv.getArgument(0);
      slot.setMonitor(monitor);
      return monitor;
    }).when(monitorService).register(any(MonitoredConnectionSlot.class));
    plugin = newPlugin();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  private HostMonitoringConnectionPluginV3 newPlugin() {
    return new HostMonitoringConnectionPluginV3(servicesContainer, properties, () -> monitorService, rdsUtils);
  }

  private ResultSet execute() throws SQLException {
    return plugin.execute(ResultSet.class, SQLException.class, connection, MONITORED_METHOD, sqlFunction, EMPTY_ARGS);
  }

  @Test
  void subscribedMethods_areNetworkBoundMethodsAndConnectionChanges_notConnect() {
    assertTrue(plugin.getSubscribedMethods().contains(MONITORED_METHOD));
    assertTrue(plugin.getSubscribedMethods().contains(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName));
    assertFalse(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECT.methodName));
    assertFalse(plugin.getSubscribedMethods().contains(JdbcMethod.FORCECONNECT.methodName));
  }

  @Test
  void disabledPlugin_subscribesToNothing() {
    properties.setProperty("failureDetectionEnabled", "false");
    plugin = newPlugin();
    assertTrue(plugin.getSubscribedMethods().isEmpty());
  }

  @Test
  void unmonitoredMethod_passesThroughWithoutTouchingTheMonitor() throws SQLException {
    plugin.execute(ResultSet.class, SQLException.class, connection, UNMONITORED_METHOD, sqlFunction, EMPTY_ARGS);
    verify(sqlFunction).call();
    verify(monitorService, never()).register(any());
    verify(pluginService, never()).getCurrentConnection();
    assertNull(plugin.slot);
  }

  @Test
  void execute_registersTheConnectionOnce_andReusesTheSlot() throws SQLException {
    assertSame(resultSet, execute());
    final MonitoredConnectionSlot first = plugin.slot;
    assertNotNull(first);
    assertSame(connection, first.getConnection());
    assertSame(hostSpec, first.getHostSpec());
    assertEquals(MonitoredConnectionSlot.IDLE, first.getInFlightSinceNanos());

    execute();
    execute();

    assertSame(first, plugin.slot);
    verify(monitorService, times(1)).register(first);
    verify(sqlFunction, times(3)).call();
    verify(pluginService, never()).setAvailability(any(), any());
    verify(pluginService, never()).identifyConnection(any(), any());
  }

  @Test
  void execute_reRegistersWhenTheMonitorHasStopped() throws SQLException {
    execute();
    when(monitor.isStopped()).thenReturn(true);
    execute();
    verify(monitorService, times(2)).register(plugin.slot);
  }

  @Test
  void execute_marksHostUnavailable_whenTheMonitorAbortedTheCall() throws SQLException {
    when(sqlFunction.call()).thenAnswer(inv -> {
      final MonitoredConnectionSlot slot = plugin.slot;
      assertTrue(slot.getInFlightSinceNanos() > MonitoredConnectionSlot.IDLE, "call must be in flight");
      assertTrue(slot.claim(slot.getInFlightSinceNanos()));
      throw new SQLException("connection aborted");
    });

    assertThrows(SQLException.class, this::execute);

    verify(pluginService).setAvailability(hostSpec, HostAvailability.NOT_AVAILABLE);
    assertEquals(MonitoredConnectionSlot.IDLE, plugin.slot.getInFlightSinceNanos());
  }

  @Test
  void execute_marksHostUnavailable_whenTheIdleConnectionWasAborted() throws SQLException {
    execute();
    verify(pluginService, never()).setAvailability(any(), any());

    assertTrue(plugin.slot.claim(MonitoredConnectionSlot.IDLE));
    execute();

    verify(pluginService, times(1)).setAvailability(hostSpec, HostAvailability.NOT_AVAILABLE);
    assertEquals(MonitoredConnectionSlot.IDLE, plugin.slot.getInFlightSinceNanos());
  }

  @Test
  void execute_identifiesClusterEndpointOnce_andMonitorsTheInstance() throws SQLException {
    when(rdsUtils.identifyRdsType(any())).thenReturn(RdsUrlType.RDS_WRITER_CLUSTER);
    when(pluginService.identifyConnection(connection, hostSpec)).thenReturn(instanceHostSpec);

    execute();
    execute();

    verify(pluginService, times(1)).identifyConnection(connection, hostSpec);
    assertSame(instanceHostSpec, plugin.slot.getHostSpec());
    verify(pluginService, never()).setCurrentConnection(any(), any());
    verify(pluginService, never()).setRoutedHostSpec(any());
  }

  @Test
  void execute_fallsBackToTheEndpoint_whenTheConnectionCannotBeIdentified() throws SQLException {
    when(rdsUtils.identifyRdsType(any())).thenReturn(RdsUrlType.RDS_PROXY);
    when(pluginService.identifyConnection(connection, hostSpec)).thenReturn(null);

    execute();

    assertSame(hostSpec, plugin.slot.getHostSpec());
  }

  @Test
  void execute_identificationError_surfacesAsSqlException() throws SQLException {
    when(rdsUtils.identifyRdsType(any())).thenReturn(RdsUrlType.RDS_WRITER_CLUSTER);
    final SQLException cause = new SQLException("identify failed");
    when(pluginService.identifyConnection(connection, hostSpec)).thenThrow(cause);

    final SQLException thrown = assertThrows(SQLException.class, this::execute);

    assertSame(cause, thrown);
    verify(sqlFunction, never()).call();
    verify(monitorService, never()).register(any());
    assertNull(plugin.slot);
  }

  @Test
  void execute_rebuildsTheSlot_whenThePhysicalConnectionChanges() throws SQLException {
    execute();
    final MonitoredConnectionSlot first = plugin.slot;

    when(pluginService.getCurrentConnection()).thenReturn(otherConnection);
    execute();
    final MonitoredConnectionSlot second = plugin.slot;

    assertNotSame(first, second);
    assertSame(otherConnection, second.getConnection());
    verify(monitorService).unregister(first);
    verify(monitorService).register(second);
  }

  @Test
  void notifyConnectionChanged_dropsTheSlot_onlyForRelevantChanges() throws SQLException {
    execute();
    final MonitoredConnectionSlot slot = plugin.slot;

    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION));
    assertSame(slot, plugin.slot);

    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.NODE_CHANGED));
    assertNull(plugin.slot);
    verify(monitor).unregister(slot);

    execute();
    final MonitoredConnectionSlot rebuilt = plugin.slot;
    plugin.notifyConnectionChanged(EnumSet.of(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
    assertNull(plugin.slot);
    verify(monitor).unregister(rebuilt);
  }

  @Test
  void releaseResources_unregistersTheSlot() throws SQLException {
    execute();
    final MonitoredConnectionSlot slot = plugin.slot;

    plugin.releaseResources();

    assertNull(plugin.slot);
    verify(monitor).unregister(slot);
    assertNull(plugin.monitorService);
  }

  @Test
  void execute_registrationFailure_isSurfacedAndTheSlotIsLeftIdle() throws SQLException {
    final SQLException cause = new SQLException("monitor unavailable");
    when(monitorService.register(any(MonitoredConnectionSlot.class))).thenThrow(cause);

    final SQLException thrown = assertThrows(SQLException.class, this::execute);

    assertSame(cause, thrown);
    verify(sqlFunction, never()).call();
    assertEquals(MonitoredConnectionSlot.IDLE, plugin.slot.getInFlightSinceNanos());
  }

  @Test
  void execute_wrapsSqlExceptionForOtherExceptionClasses() throws SQLException {
    when(rdsUtils.identifyRdsType(any())).thenReturn(RdsUrlType.RDS_WRITER_CLUSTER);
    when(pluginService.identifyConnection(connection, hostSpec)).thenThrow(new SQLException("identify failed"));

    final RuntimeException thrown = assertThrows(RuntimeException.class, () -> plugin.execute(
        ResultSet.class, RuntimeException.class, connection, MONITORED_METHOD,
        () -> resultSet, EMPTY_ARGS));

    assertTrue(thrown.getCause() instanceof SQLException);
    verify(rdsUtils).identifyRdsType(eq(hostSpec.getHost()));
  }
}
