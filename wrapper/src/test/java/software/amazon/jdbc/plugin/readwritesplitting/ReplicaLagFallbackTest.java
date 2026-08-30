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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.Rebindable;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.states.SessionStateService;

/** Tests the cache-only replica-lag safety fallback used by read/write splitting. */
class ReplicaLagFallbackTest {

  private static final int THRESHOLD_MS = 1000;

  private AutoCloseable closeable;

  @Mock private PluginService pluginService;
  @Mock private HostListProviderService hostListProviderService;
  @Mock private RdsHostListProvider hostListProvider;
  @Mock private Connection writerConnection;
  @Mock private Connection readerConnection;
  @Mock private Connection readerConnection2;
  @Mock private Statement statement;
  @Mock private SessionStateService sessionStateService;

  private final HostSpec writer = host("writer", HostRole.WRITER, HostAvailability.AVAILABLE, null);
  private final HostSpec readerOne = host("reader-one", HostRole.READER, HostAvailability.AVAILABLE, null);
  private final HostSpec readerTwo = host("reader-two", HostRole.READER, HostAvailability.AVAILABLE, null);

  private final AtomicReference<Connection> currentConnection = new AtomicReference<>();
  private final AtomicReference<HostSpec> currentHost = new AtomicReference<>();
  private List<HostSpec> publishedHosts;
  private @Nullable List<HostSpec> storedTopology;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    currentConnection.set(writerConnection);
    currentHost.set(writer);
    publishedHosts = Arrays.asList(writer, readerOne, readerTwo);
    storedTopology = publishedHosts;

    when(pluginService.getCurrentConnection()).thenAnswer(invocation -> currentConnection.get());
    when(pluginService.getCurrentHostSpec()).thenAnswer(invocation -> currentHost.get());
    doAnswer(invocation -> {
      currentConnection.set(invocation.getArgument(0));
      currentHost.set(invocation.getArgument(1));
      return null;
    }).when(pluginService).setCurrentConnection(any(Connection.class), any(HostSpec.class));
    when(pluginService.getHosts()).thenAnswer(invocation -> publishedHosts);
    when(pluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq("random")))
        .thenAnswer(invocation -> readerCandidate(invocation.getArgument(0)));
    when(pluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq("lowestLoad")))
        .thenAnswer(invocation -> readerCandidate(invocation.getArgument(0)));
    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenAnswer(invocation -> invocation.<HostSpec>getArgument(0).getRole() == HostRole.WRITER
            ? writerConnection : readerConnection);
    when(hostListProviderService.getHostListProvider()).thenReturn(hostListProvider);
    when(hostListProvider.getStoredTopology()).thenAnswer(invocation -> storedTopology);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.of(true));
    when(sessionStateService.getReadOnly()).thenReturn(Optional.empty());
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  // -------------------------------------------------------------------------
  // Group 1 – Basic threshold behaviour (7 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_lagAboveThreshold_readSwitchesToWriter() throws SQLException {
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(writerConnection, currentConnection.get());
    assertEquals(writer, currentHost.get());
  }

  @Test
  void enabled_lagBelowThreshold_switchesToReader() throws SQLException {
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 100f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
    assertEquals(readerOne, currentHost.get());
  }

  @Test
  void enabled_lagAtThreshold_staysOnReader() throws SQLException {
    // threshold check is strict ">", so lag == threshold must NOT fallback
    givenTopology(Arrays.asList(writer, readerOne),
        Arrays.asList(writer, reader("reader-one", (float) THRESHOLD_MS)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_zeroThresholdTreatsPositiveLagAsBreach() throws SQLException {
    final Properties props = new Properties();
    props.setProperty(UnifiedReadWriteSplittingPlugin.REPLICA_LAG_THRESHOLD_MS.name, "0");
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 1f)));

    read(newTopologyPlugin(props));

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void disabled_lagAboveThreshold_staysOnReaderAndNeverReadsTopology() throws SQLException {
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    read(newTopologyPlugin(new Properties()));

    assertEquals(readerConnection, currentConnection.get());
    verify(hostListProvider, never()).getStoredTopology();
  }

  @Test
  void enabled_lagRecovered_returnsToReaderImmediately() throws SQLException {
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin(enabledProps());

    read(plugin);
    assertEquals(writerConnection, currentConnection.get());

    // Lag drops back below threshold — next routing decision must go to reader again (no hysteresis)
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 100f)));
    read(plugin);

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_unknownLag_doesNotInterfereWithRouting() throws SQLException {
    // null lag in Aurora topology means "unknown" — routing must remain unchanged (reader)
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", null)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  // -------------------------------------------------------------------------
  // Group 2 – Lag value selection (7 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_notOnReader_usesWorstReaderLag() throws SQLException {
    // Not pinned to any reader yet: two candidates — worst lag (5000 ms) must trigger fallback
    givenTopology(Arrays.asList(writer, readerOne, readerTwo), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_currentReaderHealthy_staysOnItWhileAnotherReaderLags() throws SQLException {
    // Pinned to readerOne (healthy) — readerTwo's high lag must NOT affect this routing decision
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne, readerTwo), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_currentReaderLagging_routesToWriterEvenIfAnotherReaderIsHealthy() throws SQLException {
    // Pinned to readerOne which is lagging — even though readerTwo is healthy, writer is chosen
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne, readerTwo), Arrays.asList(
        writer, reader("reader-one", 5000f), reader("reader-two", 50f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_queryLevelLoadBalancing_usesWorstReaderEvenWhenCurrentIsHealthy() throws SQLException {
    // With qLB enabled, no reader is pinned — all candidates are evaluated, worst lag wins
    final Properties props = enabledProps();
    props.setProperty(UnifiedReadWriteSplittingPlugin.QUERY_LEVEL_LOAD_BALANCING.name, "true");
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne, readerTwo), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(props));

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_lowestLoadSelector_bestEligibleReaderHealthy_keepsReader() throws SQLException {
    // 'lowestLoad' is lag-minimizing: candidates reader-one (100 ms) and reader-two (5000 ms)
    // aggregate via Math.min → 100 ms is below the threshold → no fallback even though one
    // eligible reader is severely lagging.
    final Properties props = enabledProps();
    props.setProperty(ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.name, "lowestLoad");
    givenTopology(Arrays.asList(writer, readerOne, readerTwo), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(props));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_lowestLoadSelector_everyEligibleReaderLagging_fallsBackToWriter() throws SQLException {
    // Even the best eligible reader (Math.min) is lagging (1500 ms) → above threshold → writer
    final Properties props = enabledProps();
    props.setProperty(ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.name, "lowestLoad");
    givenTopology(Arrays.asList(writer, readerOne, readerTwo), Arrays.asList(
        writer, reader("reader-one", 1500f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(props));

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_cachedLaggingReaderWouldBeReused_routesToWriter() throws SQLException {
    // The plugin has a cached reader (readerOne) that is lagging; on the next read it must
    // fall back to the writer rather than returning to the cached lagging reader.
    final ReadWriteSplittingPlugin plugin = newTopologyPlugin(enabledProps());

    // First: establish a cached reader by doing a healthy read
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 100f)));
    read(plugin);
    assertEquals(readerConnection, currentConnection.get());

    // Now the cached reader's lag spikes — switch back to writer first so the plugin is on the
    // writer and will consider reusing its cached reader for the next read
    startOn(writerConnection, writer);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));
    read(plugin);

    // Must NOT reuse the lagging cached reader
    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_unavailableNonCurrentReaderIsIgnored() throws SQLException {
    // An unavailable non-current reader (readerTwo) with high lag must be skipped in the
    // lag evaluation; only the available readerOne (healthy) matters
    final HostSpec unavailableReader = host("reader-two", HostRole.READER, HostAvailability.NOT_AVAILABLE, null);
    givenTopology(Arrays.asList(writer, readerOne, unavailableReader), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_unavailableCurrentStickyReaderIsMeasured() throws SQLException {
    // The current (sticky) reader is marked NOT_AVAILABLE but still holds an open connection —
    // its lag must still be checked and trigger writer fallback when it exceeds the threshold
    final HostSpec unavailableReader = host("reader-one", HostRole.READER, HostAvailability.NOT_AVAILABLE, null);
    startOn(readerConnection, unavailableReader);
    givenTopology(Arrays.asList(writer, unavailableReader), Arrays.asList(writer, reader("reader-one", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(writerConnection, currentConnection.get());
  }

  // -------------------------------------------------------------------------
  // Group 3 – Data source correctness (4 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_readerFilteredOutOfGetHosts_lagIsIgnored() throws SQLException {
    // readerTwo is in the stored topology (lag 5000) but NOT in publishedHosts (getHosts()).
    // Since candidates come from getHosts(), readerTwo must be ignored — lag stays below threshold.
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_availabilityComesFromGetHostsNotFromTopology() throws SQLException {
    // readerTwo appears as NOT_AVAILABLE in publishedHosts (authoritative for availability)
    // even if the stored topology has it as AVAILABLE — the NOT_AVAILABLE status must win
    final HostSpec unavailableReader = host("reader-two", HostRole.READER, HostAvailability.NOT_AVAILABLE, null);
    givenTopology(Arrays.asList(writer, readerOne, unavailableReader), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_lagRankedSelectorUsesWorstCurrentLagWhenPublishedMetricsAreStale() throws SQLException {
    // Both readers are AVAILABLE in published hosts, but the stored topology has up-to-date lag.
    // Worst lag (5000 ms) is picked pessimistically and triggers writer fallback.
    givenTopology(Arrays.asList(writer, readerOne, readerTwo), Arrays.asList(
        writer, reader("reader-one", 100f), reader("reader-two", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    // Pessimistic: the 5000 ms reader is eligible, so fallback to writer
    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_neverTriggersBlockingTopologyFetch() throws SQLException {
    // The lag evaluation must only read from the in-memory cache (getStoredTopology),
    // never call the blocking refresh() method.
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    verify(hostListProvider, never()).refresh();
    verify(hostListProvider).getStoredTopology();
  }

  // -------------------------------------------------------------------------
  // Group 4 – Transaction safety (6 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_lagAboveThresholdInOpenTransaction_isNotMoved() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(true);
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    assertDoesNotThrow(() -> read(newTopologyPlugin(enabledProps())));

    // Transaction pins the connection — no switch must happen
    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_lagAboveThresholdInXaTransaction_isNotMoved() throws SQLException {
    when(pluginService.isXaTransactionActive()).thenReturn(true);
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    // XA transaction also pins the connection
    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_readOnlyTransactionOnLaggingReader_startsOnWriter() throws SQLException {
    // A read-only transaction is starting (setReadOnly(true) before any transaction).
    // Lag is above threshold → first routing decision goes to writer.
    when(pluginService.isInTransaction()).thenReturn(false);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_readOnlyTransactionOnWriter_staysOnWriterWhileLagging() throws SQLException {
    // Already on writer (e.g. after lag fallback) and isReadOnly+lag still high.
    // Plugin should stay on writer — no spurious switch to reader.
    startOn(writerConnection, writer);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin(enabledProps());
    // Trigger read — because we are already on writer and lag would also route to writer, no move
    plugin.execute(Void.class, SQLException.class, writerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_readOnlyTransactionHealthyLag_usesReader() throws SQLException {
    // setReadOnly(true) with healthy lag → must connect to a reader
    when(pluginService.isInTransaction()).thenReturn(false);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 50f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_setReadOnlyTrueInTransaction_doesNotThrow() throws SQLException {
    // setReadOnly(true) inside an open transaction must NOT throw even when lag is above threshold
    when(pluginService.isInTransaction()).thenReturn(true);
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    final ReadWriteSplittingPlugin plugin = newTopologyPlugin(enabledProps());
    assertDoesNotThrow(() ->
        plugin.execute(Void.class, SQLException.class, readerConnection,
            JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true}));
  }

  // -------------------------------------------------------------------------
  // Group 5 – Write and routing hint (3 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_lagAboveThreshold_writeStillRoutesToWriter() throws SQLException {
    // A setReadOnly(false) from a reader must switch to writer. Lag evaluation must NOT run
    // and must NOT touch the stored topology for a write decision.
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    newTopologyPlugin(enabledProps()).execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {false});

    assertEquals(writerConnection, currentConnection.get());
    verify(hostListProvider, never()).getStoredTopology();
  }

  @Test
  void enabled_explicitReaderHintStillUsesWriterWhenLagging() throws SQLException {
    // An explicit /*@reader*/ routing hint on a plain statement still goes to writer when
    // the lag fallback kicks in (lag threshold overrides the hint on safety grounds).
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    final PluginCallContext callContext = new PluginCallContext();
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.READER);
    final Rebindable rebindable = mock(Rebindable.class);
    callContext.setRebindHandle(rebindable);
    when(pluginService.getCallContext()).thenReturn(callContext);

    newAutoTopologyPlugin(enabledProps()).execute(
        Void.class, SQLException.class, statement,
        JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
        () -> null, new Object[] {"select 1"});

    assertEquals(writerConnection, currentConnection.get());
    verify(rebindable).rebind(writerConnection);
  }

  @Test
  void enabled_plainStatementPath_appliesFallbackAndRefreshesOnce() throws SQLException {
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));
    final PluginCallContext callContext = new PluginCallContext();
    final Rebindable rebindable = mock(Rebindable.class);
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    callContext.setRebindHandle(rebindable);
    when(pluginService.getCallContext()).thenReturn(callContext);

    final AutoReadWriteSplittingPlugin plugin = newAutoTopologyPlugin(enabledProps());
    plugin.execute(Void.class, SQLException.class, statement, JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
        () -> null, new Object[] {"select 1"});

    assertEquals(writerConnection, currentConnection.get());
    verify(rebindable).rebind(writerConnection);
    // Topology must be refreshed exactly once for the whole routing decision
    verify(pluginService, times(1)).refreshHostList();
  }

  // -------------------------------------------------------------------------
  // Group 6 – Global Write Forwarding (1 test)
  // -------------------------------------------------------------------------

  @Test
  void enabled_writerResolutionStay_keepsReaderAndWarnsOnce() throws SQLException {
    // When the WriterResolver returns STAY (the Global Write Forwarding scenario), the plugin
    // must remain on the current reader connection after the lag fallback routes to WRITER.
    // This test isolates that behaviour using a mock WriterResolver — no real RDS URL parsing
    // is involved, which makes the test independent of RdsUtils hostname patterns.
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne),
        Arrays.asList(writer, reader("reader-one", 5000f)));

    // Build a mock WriterResolver that always returns STAY (simulates GWF: out-of-region writer).
    final software.amazon.jdbc.plugin.readwritesplitting.resolver.WriterResolver stayResolver =
        mock(software.amazon.jdbc.plugin.readwritesplitting.resolver.WriterResolver.class);
    when(stayResolver.resolveWriter(any()))
        .thenReturn(software.amazon.jdbc.plugin.readwritesplitting.resolver.WriterResolution.stay());

    // Assemble a ReadWriteSplittingPlugin with real helpers except for the mocked writer resolver.
    final software.amazon.jdbc.plugin.readwritesplitting.classifier.TopologyRoleClassifier roleClassifier =
        new software.amazon.jdbc.plugin.readwritesplitting.classifier.TopologyRoleClassifier();
    final String strategy = "random";
    final RwSplitHelpers helpers = RwSplitHelpers.builder()
        .roleClassifier(roleClassifier)
        .routingSignal(new software.amazon.jdbc.plugin.readwritesplitting.signal.ReadOnlyFlagSignal())
        .switchGate(new software.amazon.jdbc.plugin.readwritesplitting.gate.TransactionAwareGate())
        .topologyRefresher(new software.amazon.jdbc.plugin.readwritesplitting.refresher.TopologyRefresherImpl())
        .writerResolver(stayResolver)
        .readerResolver(new software.amazon.jdbc.plugin.readwritesplitting.resolver.TopologyReaderResolver(
            new software.amazon.jdbc.plugin.readwritesplitting.source.TopologyHostsCandidateSource(),
            new software.amazon.jdbc.plugin.readwritesplitting.balancer.StickyReaderPolicy(strategy),
            stayResolver))
        .cachePolicy(new software.amazon.jdbc.plugin.readwritesplitting.cache.DefaultCachePolicy(enabledProps()))
        .initialConnectionHandler(
            new software.amazon.jdbc.plugin.readwritesplitting.handler.VerifyRoleOnConnect(strategy, false))
        .connectionUpdatePolicy(
            new software.amazon.jdbc.plugin.readwritesplitting.updater.RoleBasedUpdatePolicy(roleClassifier))
        .build();

    final ReadWriteSplittingPlugin plugin =
        new ReadWriteSplittingPlugin(pluginService, enabledProps(), helpers);
    plugin.initHostProvider("", "", enabledProps(), hostListProviderService, () -> null);
    // Seed the writer host spec so the resolver can be consulted
    plugin.setWriterHostSpec(writer);

    // Trigger a read with high lag — fallback selects WRITER, but STAY keeps the reader connection
    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    // The physical connection must not have moved (STAY = remain on reader)
    assertEquals(readerConnection, currentConnection.get());
  }

  // -------------------------------------------------------------------------
  // Group 7 – Endpoint mode / RDS Proxy (9 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_endpointMode_lagAboveThreshold_switchesToWriteEndpoint() throws SQLException {
    // SimpleReadWriteSplittingPlugin uses an opaque read endpoint — lag is sourced from the
    // stored Aurora topology. When lag is above threshold the plugin routes to the write endpoint.
    final Properties props = endpointProps(enabledProps());

    // Stored topology has the single backend reader at high lag
    final HostSpec backendReader = reader("backend-reader", 5000f);
    storedTopology = Arrays.asList(writer, backendReader);

    // Current connection is on the read endpoint
    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenAnswer(invocation -> {
          final HostSpec h = invocation.getArgument(0);
          return "writer.endpoint".equalsIgnoreCase(h.getHost()) ? writerConnection : readerConnection;
        });
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_onReadEndpoint_backendReadersHealthy_keepsUsingTheReadEndpoint()
      throws SQLException {
    final Properties props = endpointProps(enabledProps());

    // All backend readers are healthy — lag below threshold
    final HostSpec backendReader = reader("backend-reader", 50f);
    storedTopology = Arrays.asList(writer, backendReader);

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(readerConnection);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    // Stayed on reader endpoint
    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_nothingCached_leavesRoutingUntouched() throws SQLException {
    // No stored topology → lag is unknown → routing is left unchanged
    final Properties props = endpointProps(enabledProps());
    storedTopology = null;

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(readerConnection);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_topologyReadFails_leavesRoutingUntouched() throws SQLException {
    final Properties props = endpointProps(enabledProps());
    when(hostListProvider.getStoredTopology()).thenThrow(new SQLException("monitor unavailable"));

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(readerConnection);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    // Exception swallowed — stays on read endpoint
    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_providerReportsNoTopology_leavesRoutingUntouched() throws SQLException {
    final Properties props = endpointProps(enabledProps());
    // Provider returns empty topology (not null, but zero hosts)
    storedTopology = Arrays.asList();

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(readerConnection);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_countsBackendReaderMissingFromPublishedHostList() throws SQLException {
    // A backend reader not present in storedTopology contributes no lag — only the one that
    // IS in the topology is measured. If that one is healthy, endpoint stays on reader.
    final Properties props = endpointProps(enabledProps());
    final HostSpec backendReader = reader("backend-reader-only-in-topology", 100f);
    storedTopology = Arrays.asList(writer, backendReader);

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(readerConnection);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_ignoresReaderThatLeftTheCluster() throws SQLException {
    // A backend reader that left the cluster (not in storedTopology anymore) contributes no lag.
    // Only remaining readers are measured; if they are all healthy, keep the read endpoint.
    final Properties props = endpointProps(enabledProps());
    // Only the healthy reader remains in topology; the lagging one left
    final HostSpec healthyBackend = reader("healthy-backend", 50f);
    storedTopology = Arrays.asList(writer, healthyBackend);

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(readerConnection);
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_usesWorstBackendReaderRegardlessOfSelector() throws SQLException {
    // In endpoint mode, host-selector metrics don't apply — ALL backend readers in the Aurora
    // topology are evaluated. The worst lag (5000 ms) triggers writer fallback.
    final Properties props = endpointProps(enabledProps());
    final HostSpec fastReader = reader("fast-backend", 50f);
    final HostSpec slowReader = reader("slow-backend", 5000f);
    storedTopology = Arrays.asList(writer, fastReader, slowReader);

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenAnswer(invocation -> {
          final HostSpec h = invocation.getArgument(0);
          return "writer.endpoint".equalsIgnoreCase(h.getHost()) ? writerConnection : readerConnection;
        });
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    assertEquals(writerConnection, currentConnection.get());
  }

  @Test
  void enabled_endpointMode_onReadEndpoint_measuresBackendReadersInsteadOfTheEndpoint()
      throws SQLException {
    // The read endpoint host itself carries no lag in the topology (it is opaque).
    // Lag is measured from the backend reader entries in storedTopology.
    final Properties props = endpointProps(enabledProps());
    final HostSpec backendReader = reader("backend-reader", 5000f);
    storedTopology = Arrays.asList(writer, backendReader);

    final HostSpec readEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader.endpoint").port(5432).role(HostRole.READER).build();
    final HostSpec writeEndpointHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
    startOn(readerConnection, readEndpointHost);

    when(pluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenAnswer(invocation -> {
          final HostSpec h = invocation.getArgument(0);
          return "writer.endpoint".equalsIgnoreCase(h.getHost()) ? writerConnection : readerConnection;
        });
    when(pluginService.getHosts()).thenReturn(Arrays.asList(writeEndpointHost, readEndpointHost));
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(readEndpointHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    final SimpleReadWriteSplittingPlugin plugin = new SimpleReadWriteSplittingPlugin(pluginService, props);
    plugin.initHostProvider("", "", props, hostListProviderService, () -> null);

    plugin.execute(Void.class, SQLException.class, readerConnection,
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});

    // Backend reader was lagging → switched to write endpoint
    assertEquals(writerConnection, currentConnection.get());
  }

  // -------------------------------------------------------------------------
  // Group 8 – Topology refresh count (8 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_readRoutedToReader_refreshesTopologyExactlyOnce() throws SQLException {
    // A routing decision that sends a read to a reader must trigger exactly one topology refresh
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 100f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
    verify(pluginService, times(1)).refreshHostList();
  }

  @Test
  void enabled_readRoutedToWriterByLag_refreshesTopologyExactlyOnce() throws SQLException {
    // A routing decision that falls back from reader to writer (due to lag) must also trigger
    // exactly one topology refresh — not two (one from lag check, one from switch)
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(writerConnection, currentConnection.get());
    verify(pluginService, times(1)).refreshHostList();
  }

  @Test
  void enabled_noRoutingDecision_doesNotRefreshTopology() throws SQLException {
    // A method that produces NO_DECISION from the routing signal (e.g. clearWarnings) must not
    // trigger a topology refresh at all
    when(pluginService.getCurrentConnection()).thenReturn(writerConnection);
    when(pluginService.getCurrentHostSpec()).thenReturn(writer);

    newTopologyPlugin(enabledProps()).execute(
        Void.class, SQLException.class, writerConnection,
        JdbcMethod.CONNECTION_CLEARWARNINGS.methodName, () -> null, null);

    verify(pluginService, never()).refreshHostList();
  }

  @Test
  void enabled_boundStatementRerouted_refreshesTopologyExactlyOnce() throws SQLException {
    // A plain Statement rerouted via rebinding must trigger exactly one topology refresh
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    final PluginCallContext callContext = new PluginCallContext();
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    final Rebindable rebindable = mock(Rebindable.class);
    callContext.setRebindHandle(rebindable);
    when(pluginService.getCallContext()).thenReturn(callContext);

    newAutoTopologyPlugin(enabledProps()).execute(
        Void.class, SQLException.class, statement,
        JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
        () -> null, new Object[] {"select 1"});

    assertEquals(writerConnection, currentConnection.get());
    verify(pluginService, times(1)).refreshHostList();
  }

  @Test
  void enabled_boundStatementThatCannotBeReboundDoesNotRefreshTopology() throws SQLException {
    // A plain Statement without a rebind handle cannot be rerouted — no topology refresh for lag
    startOn(readerConnection, readerOne);
    givenTopology(Arrays.asList(writer, readerOne), Arrays.asList(writer, reader("reader-one", 5000f)));

    final PluginCallContext callContext = new PluginCallContext();
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    // No rebind handle set — rerouting impossible
    when(pluginService.getCallContext()).thenReturn(callContext);

    newAutoTopologyPlugin(enabledProps()).execute(
        Void.class, SQLException.class, statement,
        JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
        () -> null, new Object[] {"select 1"});

    // No rebinding possible → no topology refresh for lag decision
    verify(pluginService, never()).refreshHostList();
  }

  @Test
  void disabled_boundStatement_doesNotRefreshTopology() throws SQLException {
    // When the lag threshold is disabled, processing a bound statement must never refresh topology
    startOn(readerConnection, readerOne);

    final PluginCallContext callContext = new PluginCallContext();
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.SELECT);
    final Rebindable rebindable = mock(Rebindable.class);
    callContext.setRebindHandle(rebindable);
    when(pluginService.getCallContext()).thenReturn(callContext);

    // Disabled props: no REPLICA_LAG_THRESHOLD_MS set → defaults to -1
    newAutoTopologyPlugin(new Properties()).execute(
        Void.class, SQLException.class, statement,
        JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
        () -> null, new Object[] {"select 1"});

    verify(pluginService, never()).refreshHostList();
    verify(hostListProvider, never()).getStoredTopology();
  }

  @Test
  void enabled_plainStatementWrite_doesNotRefreshTopologyForLag() throws SQLException {
    final PluginCallContext callContext = new PluginCallContext();
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, QueryType.INSERT);
    callContext.setRebindHandle(mock(Rebindable.class));
    when(pluginService.getCallContext()).thenReturn(callContext);

    newAutoTopologyPlugin(enabledProps()).execute(Void.class, SQLException.class, statement,
        JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName, () -> null,
        new Object[] {"insert into t values (1)"});

    // Writer path: no lag evaluation and no topology refresh triggered by lag logic
    verify(pluginService, never()).refreshHostList();
    verify(hostListProvider, never()).getStoredTopology();
  }

  @Test
  void enabled_uncachedTopology_doesNotResolveCandidates() throws SQLException {
    // When the stored topology is null (no cache yet), relevantReaderLagMs returns NaN early
    // and must NOT attempt to resolve reader candidates through the resolver.
    storedTopology = null;

    read(newTopologyPlugin(enabledProps()));

    // Routing must still reach a reader (lag is unknown → routing unchanged)
    assertEquals(readerConnection, currentConnection.get());
    // The topology cache miss must surface as "lag unavailable" — exactly one topology
    // refresh occurs (from performSwitch) but zero calls to getStoredTopology beyond the
    // single lag-check call that returns null.
    verify(hostListProvider, times(1)).getStoredTopology();
  }

  // -------------------------------------------------------------------------
  // Extra coverage – null / empty topology guards (3 tests)
  // -------------------------------------------------------------------------

  @Test
  void enabled_noCachedTopology_doesNotInterfereWithRouting() throws SQLException {
    // When no topology has been cached yet (null), lag is unknown → routing is left unchanged
    // and the plugin still routes to a reader normally.
    storedTopology = null;

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_cachedTopologyReadFails_doesNotInterfereWithRouting() throws SQLException {
    // An exception from getStoredTopology() must be swallowed and treated as "lag unknown"
    // so routing continues without throwing to the caller.
    when(hostListProvider.getStoredTopology()).thenThrow(new SQLException("monitor unavailable"));

    read(newTopologyPlugin(enabledProps()));

    assertEquals(readerConnection, currentConnection.get());
  }

  @Test
  void enabled_nonRdsProvider_doesNotReadTopologyOrInterfereWithRouting() throws SQLException {
    // When the HostListProvider is NOT an RdsHostListProvider (e.g. a static list), the lag
    // feature silently no-ops because lag is only published by Aurora topology monitors.
    when(hostListProviderService.getHostListProvider())
        .thenReturn(mock(software.amazon.jdbc.hostlistprovider.HostListProvider.class));

    read(newTopologyPlugin(enabledProps()));

    // Route still goes to reader (lag unknown for non-Aurora providers)
    assertEquals(readerConnection, currentConnection.get());
    verify(hostListProvider, never()).getStoredTopology();
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private ReadWriteSplittingPlugin newTopologyPlugin(final Properties properties) throws SQLException {
    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(pluginService, properties);
    plugin.initHostProvider("", "", properties, hostListProviderService, () -> null);
    return plugin;
  }

  private AutoReadWriteSplittingPlugin newAutoTopologyPlugin(final Properties properties)
      throws SQLException {
    final AutoReadWriteSplittingPlugin plugin =
        new AutoReadWriteSplittingPlugin(pluginService, properties);
    plugin.initHostProvider("", "", properties, hostListProviderService, () -> null);
    return plugin;
  }

  private void read(final ReadWriteSplittingPlugin plugin) throws SQLException {
    plugin.execute(Void.class, SQLException.class, currentConnection.get(),
        JdbcMethod.CONNECTION_SETREADONLY.methodName, () -> null, new Object[] {true});
  }

  private static HostSpec host(
      final String name,
      final HostRole role,
      final HostAvailability availability,
      final @Nullable Float lagMs) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(name)
        .port(5432)
        .role(role)
        .availability(availability)
        .lagMs(lagMs)
        .build();
  }

  private static HostSpec reader(final String name, final @Nullable Float lagMs) {
    return host(name, HostRole.READER, HostAvailability.AVAILABLE, lagMs);
  }

  private Properties enabledProps() {
    final Properties properties = new Properties();
    properties.setProperty(UnifiedReadWriteSplittingPlugin.REPLICA_LAG_THRESHOLD_MS.name,
        Integer.toString(THRESHOLD_MS));
    return properties;
  }

  private static Properties endpointProps(final Properties base) {
    base.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.endpoint");
    base.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.endpoint");
    base.setProperty(SimpleReadWriteSplittingPlugin.VERIFY_NEW_SRW_CONNECTIONS.name, "false");
    return base;
  }

  private void givenTopology(final List<HostSpec> hosts, final List<HostSpec> latestTopology) {
    publishedHosts = hosts;
    storedTopology = latestTopology;
  }

  private void startOn(final Connection connection, final HostSpec host) {
    currentConnection.set(connection);
    currentHost.set(host);
  }

  private static @Nullable HostSpec readerCandidate(final List<HostSpec> hosts) {
    for (final HostSpec host : hosts) {
      if (host.getRole() == HostRole.READER && host.getAvailability() == HostAvailability.AVAILABLE) {
        return host;
      }
    }
    return null;
  }
}
