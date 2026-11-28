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

package software.amazon.jdbc.plugin.gdbfailover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class GlobalDbFailoverConnectionPluginTest {

  private static final HostSpec OHIO_1 = host("global-ohio-mysql-instance-1.abc.us-east-2.rds.amazonaws.com",
      HostRole.READER);
  private static final HostSpec OHIO_1_WRITER = host(OHIO_1.getHost(), HostRole.WRITER);
  private static final HostSpec OHIO_2 = host("global-ohio-mysql-instance-2.abc.us-east-2.rds.amazonaws.com",
      HostRole.READER);
  private static final HostSpec OREGON_1 = host("global-oregon-mysql-instance-1.xyz.us-west-2.rds.amazonaws.com",
      HostRole.WRITER);

  private final AtomicReference<List<HostSpec>> topology = new AtomicReference<>();
  private PluginService pluginService;
  private FullServicesContainer servicesContainer;

  private static HostSpec host(final String host, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(host).port(3306).role(role).build();
  }

  @BeforeEach
  void setUp() throws SQLException {
    this.pluginService = mock(PluginService.class);
    this.servicesContainer = mock(FullServicesContainer.class);
    final TargetDriverDialect targetDriverDialect = mock(TargetDriverDialect.class);
    when(this.servicesContainer.getPluginService()).thenReturn(this.pluginService);
    when(this.pluginService.getTelemetryFactory()).thenReturn(mock(TelemetryFactory.class));
    when(this.pluginService.getTargetDriverDialect()).thenReturn(targetDriverDialect);
    when(targetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(Collections.emptySet());
    when(this.pluginService.forceRefreshHostList(anyBoolean(), anyLong())).thenReturn(true);
    when(this.pluginService.getAllHosts()).thenAnswer(i -> this.topology.get());
    when(this.pluginService.getHosts()).thenAnswer(i -> this.topology.get());
  }

  /** Records the attempts and lets each test decide what happens during them. */
  private static class TestPlugin extends GlobalDbFailoverConnectionPlugin {
    final List<String> attempts = new ArrayList<>();
    @Nullable Runnable duringReaderAttempt;
    @Nullable Runnable afterReaderAttemptAbandoned;
    @Nullable Runnable onTopologyWait;
    boolean readerAttemptAbandoned;
    boolean readerAttemptHadAbandonCondition;
    int topologyWaits;

    TestPlugin(final FullServicesContainer servicesContainer) {
      super(servicesContainer, new Properties());
      this.homeRegion = "us-east-2";
      this.activeHomeFailoverMode = GlobalDbFailoverMode.STRICT_WRITER;
      this.inactiveHomeFailoverMode = GlobalDbFailoverMode.STRICT_HOME_READER;
    }

    @Override
    protected void failoverToWriter(
        final HostSpec writerCandidate, final long failoverEndNano, final @Nullable BooleanSupplier abandonWhen)
        throws SQLException {
      this.attempts.add("writer:" + writerCandidate.getHost());
      throw new FailoverSuccessSQLException();
    }

    @Override
    protected void failoverToAllowedHost(
        final Supplier<Set<HostSpec>> allowedHosts,
        final @Nullable HostRole verifyRole,
        final long failoverEndNano,
        final @Nullable BooleanSupplier abandonWhen) throws SQLException {
      this.attempts.add("reader:" + verifyRole);
      this.readerAttemptHadAbandonCondition = abandonWhen != null;
      if (this.duringReaderAttempt != null) {
        this.duringReaderAttempt.run();
      }
      this.readerAttemptAbandoned = abandonWhen != null && abandonWhen.getAsBoolean();
      if (this.readerAttemptAbandoned) {
        if (this.afterReaderAttemptAbandoned != null) {
          this.afterReaderAttemptAbandoned.run();
        }
        return;
      }
      throw new FailoverSuccessSQLException();
    }

    @Override
    protected void waitForTopologyChange() {
      // No sleeping in unit tests; the test changes the topology here.
      this.topologyWaits++;
      if (this.onTopologyWait != null) {
        this.onTopologyWait.run();
      }
    }
  }

  @Test
  void testWriterMovingIntoHomeRegionDuringReaderFailoverSwitchesToWriter() {
    // Failover starts while the writer is in the other region: home reader. While it waits, the switchover makes
    // the home region primary, so the reader attempt is abandoned and the failover connects to the new writer.
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1, OHIO_2));
    final TestPlugin plugin = new TestPlugin(this.servicesContainer);
    plugin.duringReaderAttempt = () -> this.topology.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));

    assertThrows(FailoverSuccessSQLException.class, plugin::failover);

    assertTrue(plugin.readerAttemptAbandoned);
    assertEquals(Arrays.asList("reader:READER", "writer:" + OHIO_1.getHost()), plugin.attempts);
  }

  @Test
  void testReaderFailoverIsNotAbandonedWhileWriterStaysOutOfHomeRegion() {
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1, OHIO_2));
    final TestPlugin plugin = new TestPlugin(this.servicesContainer);

    assertThrows(FailoverSuccessSQLException.class, plugin::failover);

    assertFalse(plugin.readerAttemptAbandoned);
    assertEquals(Collections.singletonList("reader:READER"), plugin.attempts);
  }

  @Test
  void testReaderFailoverIsNotAbandonedWhenTopologyHasNoWriter() {
    // A topology without a writer (mid-switchover) is not a region change.
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1, OHIO_2));
    final TestPlugin plugin = new TestPlugin(this.servicesContainer);
    plugin.duringReaderAttempt = () -> this.topology.set(Arrays.asList(OHIO_1, OHIO_2));

    assertThrows(FailoverSuccessSQLException.class, plugin::failover);

    assertFalse(plugin.readerAttemptAbandoned);
    assertEquals(Collections.singletonList("reader:READER"), plugin.attempts);
  }

  @Test
  void testWriterFailoverInHomeRegionGoesStraightToWriter() {
    this.topology.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));
    final TestPlugin plugin = new TestPlugin(this.servicesContainer);

    assertThrows(FailoverSuccessSQLException.class, plugin::failover);

    assertEquals(Collections.singletonList("writer:" + OHIO_1.getHost()), plugin.attempts);
  }

  @Test
  void testSameActiveAndInactiveModesNeverAbandonAnAttempt() {
    // A region change can't change the outcome when both modes are the same, so there is no abandon condition
    // even though the writer moves into the home region during the attempt.
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1, OHIO_2));
    final TestPlugin plugin = new TestPlugin(this.servicesContainer);
    plugin.activeHomeFailoverMode = GlobalDbFailoverMode.STRICT_HOME_READER;
    plugin.inactiveHomeFailoverMode = GlobalDbFailoverMode.STRICT_HOME_READER;
    plugin.duringReaderAttempt = () -> this.topology.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));

    assertThrows(FailoverSuccessSQLException.class, plugin::failover);

    assertFalse(plugin.readerAttemptHadAbandonCondition);
    assertEquals(Collections.singletonList("reader:READER"), plugin.attempts);
  }

  @Test
  void testNoWriterAfterAbandonedAttemptWaitsForOne() {
    // The reader attempt is abandoned because the writer moved to Ohio, but by the time the mode is chosen again
    // the topology has no writer (switchover still in progress). The failover waits for a writer instead of failing.
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1, OHIO_2));
    final TestPlugin plugin = new TestPlugin(this.servicesContainer);
    plugin.duringReaderAttempt = () -> this.topology.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));
    plugin.afterReaderAttemptAbandoned = () -> this.topology.set(Arrays.asList(OHIO_1, OHIO_2));
    plugin.onTopologyWait = () -> this.topology.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));

    assertThrows(FailoverSuccessSQLException.class, plugin::failover);

    assertTrue(plugin.readerAttemptAbandoned);
    assertEquals(1, plugin.topologyWaits);
    assertEquals(Arrays.asList("reader:READER", "writer:" + OHIO_1.getHost()), plugin.attempts);
  }

  @Test
  void testNoWriterOnFirstPassFailsImmediately() {
    this.topology.set(Arrays.asList(OHIO_1, OHIO_2));
    final TestPlugin plugin = new TestPlugin(this.servicesContainer);

    assertThrows(FailoverFailedSQLException.class, plugin::failover);

    assertEquals(0, plugin.topologyWaits);
    assertTrue(plugin.attempts.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testAbandonedAttemptIsNotCountedAsTriggered() throws SQLException {
    // Real failoverToAllowedHost and failoverToWriter with a mocked connection path. The abandoned reader attempt
    // records no reader metric; the writer attempt that follows records one triggered and one success.
    final TelemetryFactory telemetryFactory = mock(TelemetryFactory.class);
    final TelemetryCounter readerTriggered = mock(TelemetryCounter.class);
    final TelemetryCounter readerSuccess = mock(TelemetryCounter.class);
    final TelemetryCounter readerFailed = mock(TelemetryCounter.class);
    final TelemetryCounter writerTriggered = mock(TelemetryCounter.class);
    final TelemetryCounter writerSuccess = mock(TelemetryCounter.class);
    final TelemetryCounter writerFailed = mock(TelemetryCounter.class);
    when(telemetryFactory.createCounter("readerFailover.triggered.count")).thenReturn(readerTriggered);
    when(telemetryFactory.createCounter("readerFailover.completed.success.count")).thenReturn(readerSuccess);
    when(telemetryFactory.createCounter("readerFailover.completed.failed.count")).thenReturn(readerFailed);
    when(telemetryFactory.createCounter("writerFailover.triggered.count")).thenReturn(writerTriggered);
    when(telemetryFactory.createCounter("writerFailover.completed.success.count")).thenReturn(writerSuccess);
    when(telemetryFactory.createCounter("writerFailover.completed.failed.count")).thenReturn(writerFailed);
    when(this.pluginService.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(this.pluginService.getHostSpecBuilder())
        .thenAnswer(i -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
    when(this.pluginService.getHostSpecByStrategy(anyList(), any(), anyString()))
        .thenAnswer(i -> ((List<HostSpec>) i.getArgument(0)).get(0));
    final Connection connection = mock(Connection.class);
    when(this.pluginService.connect(any(HostSpec.class), any(Properties.class), any(ConnectionPlugin.class)))
        .thenReturn(connection);
    // The writer moves to Ohio while the reader connection is being opened: the reader attempt is abandoned after
    // its connection has been verified. The next pass connects to the writer.
    when(this.pluginService.getHostRole(connection)).thenAnswer(i -> {
      final HostSpec writer = Utils.getWriter(this.topology.get());
      if (writer == OREGON_1) {
        this.topology.set(Arrays.asList(OHIO_1_WRITER, OHIO_2));
        return HostRole.READER;
      }
      return HostRole.WRITER;
    });
    this.topology.set(Arrays.asList(OREGON_1, OHIO_1, OHIO_2));
    final GlobalDbFailoverConnectionPlugin plugin = new GlobalDbFailoverConnectionPlugin(
        this.servicesContainer, new Properties()) {
      {
        this.homeRegion = "us-east-2";
        this.activeHomeFailoverMode = GlobalDbFailoverMode.STRICT_WRITER;
        this.inactiveHomeFailoverMode = GlobalDbFailoverMode.STRICT_HOME_READER;
      }
    };

    assertThrows(FailoverSuccessSQLException.class, plugin::failover);

    verify(readerTriggered, never()).inc();
    verify(readerSuccess, never()).inc();
    verify(readerFailed, never()).inc();
    verify(writerTriggered).inc();
    verify(writerSuccess).inc();
    verify(writerFailed, never()).inc();
  }
}
