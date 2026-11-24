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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
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
    boolean readerAttemptAbandoned;

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
      if (this.duringReaderAttempt != null) {
        this.duringReaderAttempt.run();
      }
      this.readerAttemptAbandoned = abandonWhen != null && abandonWhen.getAsBoolean();
      if (this.readerAttemptAbandoned) {
        return;
      }
      throw new FailoverSuccessSQLException();
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
}
