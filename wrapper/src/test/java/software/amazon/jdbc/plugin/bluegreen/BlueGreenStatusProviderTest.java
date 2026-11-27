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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.OpenedConnectionTracker;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.storage.StorageService;

class BlueGreenStatusProviderTest {

  private static final String BGD_ID = "1";
  private static final String CLUSTER_ID = "cluster-id";

  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private PluginService mockPluginService;
  @Mock private StorageService mockStorageService;
  @Mock private EventPublisher mockEventPublisher;

  /**
   * A plain dialect that does NOT implement {@link software.amazon.jdbc.dialect.BlueGreenDialect}. The
   * provider constructor only starts its monitor threads for a Blue/Green capable dialect, so this
   * keeps the unit under test free of background threads and real connections.
   */
  @Mock private Dialect mockDialect;

  private Properties props;
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getStorageService()).thenReturn(mockStorageService);
    when(mockServicesContainer.getEventPublisher()).thenReturn(mockEventPublisher);
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private BlueGreenStatusProvider newProvider() {
    return new BlueGreenStatusProvider(mockServicesContainer, props, BGD_ID, CLUSTER_ID);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "0")
  void defaultOrExplicitZeroGraceDropsBlueImmediately(final String graceMs) {
    BlueGreenConnectionPlugin.BG_DROP_BLUE_CONNECTIONS_GRACE_MS.set(props, graceMs);
    final BlueGreenStatusProvider provider = newProvider();
    final OpenedConnectionTracker tracker = mock(OpenedConnectionTracker.class);
    provider.tracker = tracker;
    final BlueGreenInterimStatus source = collectedInterimStatus(BlueGreenPhase.IN_PROGRESS, "blue", false);
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = source;

    provider.dropBlueConnections();
    verify(tracker).invalidateAllConnections(source.startTopology.get(0));
    verifyNoMoreInteractions(tracker);
  }

  @Test
  void positiveGraceSchedulesOnceAndAllowsPhaseChanges() {
    BlueGreenConnectionPlugin.BG_DROP_BLUE_CONNECTIONS_GRACE_MS.set(props, "200");
    final TestableProvider provider = newTestableProvider();
    final OpenedConnectionTracker tracker = mock(OpenedConnectionTracker.class);
    provider.tracker = tracker;
    final BlueGreenInterimStatus source = collectedInterimStatus(BlueGreenPhase.IN_PROGRESS, "blue", false);
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = source;

    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;
    provider.updateSummaryStatus(BlueGreenRole.SOURCE, source);
    assertTrue(provider.phaseTimeNano.containsKey("Blue connection grace period started"));
    assertFalse(provider.summaryStatus.getExecuteRouting().isEmpty());
    provider.updateSummaryStatus(BlueGreenRole.SOURCE, source);
    provider.latestStatusPhase = BlueGreenPhase.POST;
    provider.updateSummaryStatus(BlueGreenRole.SOURCE, source);
    assertEquals(BlueGreenPhase.POST, provider.summaryStatus.getCurrentPhase());
    provider.latestStatusPhase = BlueGreenPhase.COMPLETED;
    provider.blueDnsUpdateCompleted = true;
    provider.greenDnsRemoved = true;
    provider.updateSummaryStatus(BlueGreenRole.SOURCE, source);
    assertEquals(BlueGreenPhase.COMPLETED, provider.summaryStatus.getCurrentPhase());

    verify(tracker).scheduleInvalidation(source.startTopology, 200L);
    verifyNoMoreInteractions(tracker);
  }

  @Test
  void disabledDropSkipsGracePeriod() {
    BlueGreenStatusProvider.BG_DROP_BLUE_CONNECTIONS.set(props, "false");
    BlueGreenConnectionPlugin.BG_DROP_BLUE_CONNECTIONS_GRACE_MS.set(props, "200");
    final BlueGreenStatusProvider provider = newProvider();
    final OpenedConnectionTracker tracker = mock(OpenedConnectionTracker.class);
    provider.tracker = tracker;
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
        collectedInterimStatus(BlueGreenPhase.IN_PROGRESS, "blue", false);
    provider.dropBlueConnections();
    verifyNoInteractions(tracker);
  }

  @Test
  void rejectsNegativeDropGrace() {
    BlueGreenConnectionPlugin.BG_DROP_BLUE_CONNECTIONS_GRACE_MS.set(props, "-1");
    assertThrows(IllegalArgumentException.class, () -> new BlueGreenConnectionPlugin(mockServicesContainer, props));
    assertThrows(IllegalArgumentException.class, this::newProvider);
  }

  private BlueGreenInterimStatus interimStatus(final BlueGreenPhase phase) {
    return interimStatus(phase, false);
  }

  private BlueGreenInterimStatus interimStatus(
      final BlueGreenPhase phase, final boolean endpointUnreachable) {
    return new BlueGreenInterimStatus(
        phase,
        "1.0",
        5432,
        new ArrayList<>(),
        new ArrayList<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashSet<>(),
        false,
        false,
        false,
        endpointUnreachable);
  }

  /**
   * An interim status that looks like a monitor which has successfully collected everything: a
   * topology and a host name. Used for the readiness assertions, which require both roles to have
   * reported collected data.
   */
  private BlueGreenInterimStatus collectedInterimStatus(
      final BlueGreenPhase phase, final String host, final boolean endpointUnreachable) {
    return new BlueGreenInterimStatus(
        phase,
        "1.0",
        5432,
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host(host).port(5432).build()),
        new ArrayList<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashSet<>(Collections.singletonList(host)),
        false,
        false,
        false,
        endpointUnreachable);
  }

  /**
   * A provider with the monitor lifecycle and the clock replaced. {@code initMonitoring} is counted
   * instead of starting real monitor threads, which is what lets the deletion tests assert that the
   * context reset actually restarts monitoring.
   *
   * <p>Note that neither field uses an explicit initializer: {@code initMonitoring()} can be called
   * from the superclass constructor, and a subclass field initializer would run afterwards and wipe
   * the recorded count.
   */
  private static class TestableProvider extends BlueGreenStatusProvider {

    private long nanoTime;
    private int initMonitoringCount;

    TestableProvider(
        final FullServicesContainer servicesContainer,
        final Properties props,
        final String bgdId,
        final String clusterId) {
      super(servicesContainer, props, bgdId, clusterId);
    }

    @Override
    protected void initMonitoring() {
      this.initMonitoringCount++;
    }

    @Override
    protected long getNanoTime() {
      return this.nanoTime;
    }

    void advanceMs(final long ms) {
      this.nanoTime += TimeUnit.MILLISECONDS.toNanos(ms);
    }
  }

  private TestableProvider newTestableProvider() {
    return new TestableProvider(mockServicesContainer, props, BGD_ID, CLUSTER_ID);
  }

  /** Grace period used by the deletion tests, kept short so the intent is obvious. */
  private static final long GRACE_MS = 30_000;

  /**
   * The regression this guards (issue #2096): a target driver property restricting which node may be
   * connected to must not reach the monitoring connections. The green node is a replica until it is
   * promoted, so inheriting {@code targetServerType=primary} stopped the green monitor from ever
   * connecting, and the plugin never observed the switchover finishing.
   */
  @Test
  void monitoringPropertiesDropNodeSelectionRestrictions() {
    props.setProperty("targetServerType", "primary");
    props.setProperty("socketTimeout", "3000");
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());

    final Properties monitoringProps = newProvider().getMonitoringProperties();

    assertNull(monitoringProps.getProperty("targetServerType"));
    assertEquals("3000", monitoringProps.getProperty("socketTimeout"),
        "Unrelated properties must still be inherited by monitoring connections.");
  }

  /**
   * The documented workaround for the above on released versions: the monitoring prefix overrides the
   * inherited value, so the restriction can be neutralized without touching application connections.
   */
  @Test
  void monitoringPrefixOverridesInheritedProperty() {
    props.setProperty("someProperty", "applicationValue");
    props.setProperty("blue-green-monitoring-someProperty", "monitoringValue");
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());

    final Properties monitoringProps = newProvider().getMonitoringProperties();

    assertEquals("monitoringValue", monitoringProps.getProperty("someProperty"));
    assertNull(monitoringProps.getProperty("blue-green-monitoring-someProperty"),
        "The prefixed key itself must not be passed to the target driver.");
  }

  /**
   * Removing inherited restrictions must not defeat an explicit monitoring override. Someone who has
   * deliberately set a monitoring value for a node-selection property means it, so the removal has to
   * happen before the prefixed values are applied, not after.
   */
  @Test
  void explicitMonitoringOverrideSurvivesRemoval() {
    props.setProperty("targetServerType", "primary");
    props.setProperty("blue-green-monitoring-targetServerType", "preferSecondary");
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());

    final Properties monitoringProps = newProvider().getMonitoringProperties();

    assertEquals("preferSecondary", monitoringProps.getProperty("targetServerType"),
        "An explicit monitoring override must win over both the inherited value and the removal.");
  }

  @Test
  void inProgressSuspendsTrafficWhileBlueDnsStillPointsAtOldTopology() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = false;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.IN_PROGRESS, status.getCurrentPhase());
    assertFalse(status.getExecuteRouting().isEmpty(),
        "Traffic should still be suspended while the blue endpoints resolve to the old topology.");
  }

  /**
   * The regression this guards: reading a status after SWITCHOVER_IN_PROGRESS is not guaranteed, so
   * once the blue endpoints resolve to new IP addresses the provider must stop holding traffic instead
   * of waiting for the switchover timer to expire.
   */
  @Test
  void inProgressReleasesTrafficOnceBlueDnsResolvesToNewTopology() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = true;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.POST, status.getCurrentPhase());
    assertTrue(status.getExecuteRouting().isEmpty(),
        "Held JDBC calls should be released once the blue endpoints point at the new topology.");
  }

  @Test
  void inProgressKeepsSuspendingTrafficWhenReleaseOnBlueDnsUpdateIsDisabled() {
    BlueGreenStatusProvider.BG_RELEASE_ON_BLUE_DNS_UPDATE.set(props, "false");
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = true;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.IN_PROGRESS, status.getCurrentPhase());
  }

  @Test
  void inProgressKeepsSuspendingTrafficDuringRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = true;
    provider.rollback = true;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.IN_PROGRESS, status.getCurrentPhase());
  }

  /**
   * The regression this guards: a monitor reports NOT_CREATED both when the deployment is genuinely
   * absent and when its status simply can't be read, which is expected once a switchover tears the
   * deployment down. Treating that as a rollback would reinstate the pre-switchover routing and send
   * traffic back to the old topology.
   */
  @Test
  void notCreatedAfterSwitchoverIsNotTreatedAsRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        interimStatus(BlueGreenPhase.IN_PROGRESS);

    provider.updatePhase(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.NOT_CREATED));

    assertFalse(provider.rollback, "NOT_CREATED must not be mistaken for a rollback.");
    assertEquals(BlueGreenPhase.IN_PROGRESS, provider.latestStatusPhase,
        "An unreadable status must not move the phase backwards.");
  }

  @Test
  void deploymentReportedAvailableAgainIsTreatedAsRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        interimStatus(BlueGreenPhase.IN_PROGRESS);

    provider.updatePhase(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.CREATED));

    assertTrue(provider.rollback, "A deployment reported as CREATED again is a genuine rollback.");
    assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase);
  }

  @Test
  void phaseDoesNotMoveBackwardsWithoutRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
        interimStatus(BlueGreenPhase.IN_PROGRESS);

    // Only TARGET drives rollback detection, so a SOURCE regression must be ignored outright.
    provider.updatePhase(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.CREATED));

    assertFalse(provider.rollback);
    assertEquals(BlueGreenPhase.IN_PROGRESS, provider.latestStatusPhase);
  }

  @Test
  void nullPhaseLeavesTheCurrentPhaseUntouched() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;

    // A monitor reports a null phase when the status table returned no rows at all.
    provider.updatePhase(BlueGreenRole.TARGET, interimStatus(null));

    assertFalse(provider.rollback);
    assertEquals(BlueGreenPhase.IN_PROGRESS, provider.latestStatusPhase);
  }

  @Test
  void unsupportedDialectDoesNotStartMonitoring() {
    final BlueGreenStatusProvider provider = newProvider();

    // No Blue/Green capable dialect means no monitors, so the provider stays in its initial state.
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
    assertEquals(Collections.emptyMap(), provider.correspondingNodes);
  }

  // ---------------------------------------------------------------------------------------------
  // Blue/Green Deployment deleted (and possibly recreated) while the application keeps running.
  //
  // These cannot be covered by the integration tests: reproducing them requires deleting a
  // deployment and creating a second one against a still-running JVM, and the failure only shows up
  // in state that survives across deployments.
  // ---------------------------------------------------------------------------------------------

  /**
   * Puts the provider into the state it reaches once a deployment has been discovered and then
   * deleted: a known CREATED deployment, stale mappings collected from the green environment, and a
   * source monitor that no longer sees any status information.
   */
  private TestableProvider providerWithDeletedDeployment() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        collectedInterimStatus(BlueGreenPhase.CREATED, "green-node", false);
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(null);
    provider.roleByHost.put("green-node", BlueGreenRole.TARGET);
    provider.correspondingNodes.put("blue-node", Pair.create(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("blue-node").build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("green-node").build()));
    return provider;
  }

  @Test
  void absentDeploymentIsNotActedOnBeforeTheGracePeriodExpires() {
    final TestableProvider provider = providerWithDeletedDeployment();

    // First observation only starts the grace period.
    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS - 1);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount,
        "An empty status result can be transient and must not immediately discard the context.");
    assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase);
  }

  /**
   * The bug this guards: a deployment that is deleted and recreated left the target monitor pinned to
   * the deleted green endpoint, and left the blue to green mapping pointing at nodes that no longer
   * exist, until the application was restarted. Resetting the context and restarting monitoring is
   * what allows the new green endpoint to be discovered.
   */
  @Test
  void deletedDeploymentResetsContextAndRestartsMonitoring() {
    final TestableProvider provider = providerWithDeletedDeployment();

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();

    assertEquals(1, provider.initMonitoringCount, "Monitoring must be restarted from scratch.");
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
    assertTrue(provider.correspondingNodes.isEmpty(),
        "The blue to green mapping refers to nodes that no longer exist and must be discarded.");
    assertTrue(provider.roleByHost.isEmpty());
    assertNull(provider.interimStatuses[BlueGreenRole.SOURCE.getValue()]);
    assertNull(provider.interimStatuses[BlueGreenRole.TARGET.getValue()]);
  }

  /**
   * The regression this guards: deleting a deployment does not necessarily make the green endpoint
   * unreachable. The green instance can still be running, or its name can still resolve, so the
   * target monitor keeps connecting successfully to a node that is no longer part of any deployment
   * and reports no status for its role. Detection cannot rely on the source monitor alone, because
   * once a replacement deployment exists the source monitor reports it as present again.
   */
  @Test
  void orphanedGreenReportedByTargetAloneResetsContext() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;
    // The blue database reports a deployment, so the source monitor sees nothing wrong.
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(BlueGreenPhase.CREATED);
    // The green node answers but no longer reports itself as part of a deployment.
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        collectedInterimStatus(null, "green-node", false);
    // It did report the deployment earlier, so this is a loss rather than a slow provisioning.
    provider.targetDeploymentSeen = true;

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();

    assertEquals(1, provider.initMonitoringCount,
        "A green node that no longer reports the deployment must trigger rediscovery.");
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  /**
   * The regression this guards: RDS provisions the deployment metadata asynchronously and not
   * necessarily on both environments at once, so a green node that has never reported a status is
   * most likely still waiting for its own metadata. Treating that as a deletion would reset the
   * context, rediscover the same silent green node, and reset again on a loop, churning monitor
   * threads for as long as the metadata took to appear.
   */
  @Test
  void greenThatHasNeverReportedTheDeploymentIsNotTreatedAsDeleted() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(BlueGreenPhase.CREATED);
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        collectedInterimStatus(null, "green-node", false);
    // Never confirmed its membership, which is the difference from the test above.
    provider.targetDeploymentSeen = false;

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS * 10);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount,
        "Metadata that has not been provisioned yet must not be mistaken for a deleted deployment.");
    assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase);
  }

  @Test
  void greenReportingItsPhaseRecordsThatTheDeploymentWasSeen() {
    final TestableProvider provider = newTestableProvider();
    assertFalse(provider.targetDeploymentSeen);

    provider.prepareStatus(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.CREATED));

    assertTrue(provider.targetDeploymentSeen,
        "A green environment reporting a real phase must be remembered.");
  }

  @Test
  void greenReportingNoPhaseDoesNotRecordThatTheDeploymentWasSeen() {
    final TestableProvider provider = newTestableProvider();

    provider.prepareStatus(BlueGreenRole.TARGET, interimStatus(null));

    assertFalse(provider.targetDeploymentSeen);
  }

  /**
   * The regression this guards, observed against a real deployment: resetContext() runs on one
   * monitor's thread while the other monitor can already be inside prepareStatus, blocked on
   * processStatusLock. Stopping it does not interrupt that wait, so it resumes after the reset and
   * writes the deleted deployment's topology into the freshly cleared context. roleByHost and
   * hostIpAddresses are never pruned, so the old green node then survived into the next deployment and
   * showed up in its readiness report.
   */
  @Test
  void statusFromADiscardedMonitorIsIgnored() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;

    final BlueGreenStatusMonitor current = mock(BlueGreenStatusMonitor.class);
    final BlueGreenStatusMonitor discarded = mock(BlueGreenStatusMonitor.class);
    provider.monitors[BlueGreenRole.TARGET.getValue()] = current;

    provider.onMonitorStatusChanged(discarded, BlueGreenRole.TARGET,
        collectedInterimStatus(BlueGreenPhase.CREATED, "old-green-node", false));

    assertTrue(provider.roleByHost.isEmpty(),
        "A discarded monitor must not write its topology into the current context.");
    assertNull(provider.interimStatuses[BlueGreenRole.TARGET.getValue()]);
    assertFalse(provider.targetDeploymentSeen);
  }

  @Test
  void statusFromTheCurrentMonitorIsProcessed() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;

    final BlueGreenStatusMonitor current = mock(BlueGreenStatusMonitor.class);
    provider.monitors[BlueGreenRole.TARGET.getValue()] = current;

    provider.onMonitorStatusChanged(current, BlueGreenRole.TARGET,
        collectedInterimStatus(BlueGreenPhase.CREATED, "green-node", false));

    assertTrue(provider.roleByHost.containsKey("green-node"));
    assertNotNull(provider.interimStatuses[BlueGreenRole.TARGET.getValue()]);
    assertTrue(provider.targetDeploymentSeen);
  }

  /**
   * The green environment also reports no status from time to time during a switchover, so the target
   * signal is subject to the same restriction as the source one.
   */
  @Test
  void absentStatusFromTargetIsIgnoredOnceSwitchoverHasBegun() {
    for (final BlueGreenPhase phase : new BlueGreenPhase[] {
        BlueGreenPhase.PREPARATION, BlueGreenPhase.IN_PROGRESS, BlueGreenPhase.POST}) {

      final TestableProvider provider = newTestableProvider();
      provider.latestStatusPhase = phase;
      provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
          interimStatus(BlueGreenPhase.IN_PROGRESS);
      provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(null);
      provider.targetDeploymentSeen = true;

      provider.checkDeploymentRemoved();
      provider.advanceMs(GRACE_MS * 10);
      provider.checkDeploymentRemoved();

      assertEquals(0, provider.initMonitoringCount,
          "A target gap during " + phase + " must not reset the context.");
      assertEquals(phase, provider.latestStatusPhase);
    }
  }

  /**
   * A status result that comes back before the grace period expires must restart the clock, otherwise
   * a brief blip would eventually be enough to discard a healthy deployment.
   */
  @Test
  void deploymentReappearingBeforeTheGracePeriodExpiresRestartsTheGracePeriod() {
    final TestableProvider provider = providerWithDeletedDeployment();

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS - 1);

    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(BlueGreenPhase.CREATED);
    provider.checkDeploymentRemoved();

    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(null);
    provider.advanceMs(GRACE_MS - 1);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount,
        "The grace period must be measured from the most recent uninterrupted absence.");
  }

  /**
   * Some engines report a deleted deployment as an error rather than as an empty result, which the
   * monitor maps to NOT_CREATED. Both have to be treated as the deployment being gone; handling only
   * the empty result would leave the fix inert on those engines.
   */
  @Test
  void notCreatedReportedBySourceIsTreatedAsDeletedDeployment() {
    final TestableProvider provider = providerWithDeletedDeployment();
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
        interimStatus(BlueGreenPhase.NOT_CREATED);

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();

    assertEquals(1, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  /**
   * The regression this guards, caught by the MySQL Aurora switchover integration test: absent status
   * information is normal from PREPARATION onwards. collectStatus() filters out endpoints of the
   * separated old1 cluster, so once the old blue cluster is renamed the source monitor reads an empty
   * result as an ordinary part of the switchover. Resetting the context then discards the blue to
   * green corresponding node map mid-switchover, and new connections go to the old blue cluster,
   * which is exactly what the switchover routing exists to prevent. A stalled switchover is covered
   * by bgSwitchoverTimeoutMs instead.
   */
  @Test
  void absentStatusDuringSwitchoverDoesNotResetContext() {
    for (final BlueGreenPhase phase : new BlueGreenPhase[] {
        BlueGreenPhase.PREPARATION, BlueGreenPhase.IN_PROGRESS, BlueGreenPhase.POST}) {

      final TestableProvider provider = providerWithDeletedDeployment();
      provider.latestStatusPhase = phase;

      provider.checkDeploymentRemoved();
      provider.advanceMs(GRACE_MS * 10);
      provider.checkDeploymentRemoved();

      assertEquals(0, provider.initMonitoringCount,
          "Absent status during " + phase + " is normal and must not reset the context.");
      assertEquals(phase, provider.latestStatusPhase);
    }
  }

  /**
   * A completed switchover legitimately removes the status information, and that path is owned by
   * resetContextWhenCompleted(). Treating it as a deletion here would replace the switchover summary
   * with a deletion reset.
   */
  @Test
  void completedSwitchoverIsNotTreatedAsDeletedDeployment() {
    final TestableProvider provider = providerWithDeletedDeployment();
    provider.latestStatusPhase = BlueGreenPhase.COMPLETED;

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS * 10);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.COMPLETED, provider.latestStatusPhase);
  }

  /**
   * After the reset the monitors report no deployment for as long as none exists. That steady state
   * must not be mistaken for a fresh deletion, or the provider would reset itself in a loop and never
   * keep a monitoring connection long enough to discover a new deployment.
   */
  @Test
  void steadyStateWithNoDeploymentDoesNotResetContextRepeatedly() {
    final TestableProvider provider = providerWithDeletedDeployment();

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();
    assertEquals(1, provider.initMonitoringCount);

    // Replacement monitors come up and keep reporting that there is no deployment.
    for (int i = 0; i < 5; i++) {
      provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(null);
      provider.advanceMs(GRACE_MS * 2);
      provider.checkDeploymentRemoved();
    }

    assertEquals(1, provider.initMonitoringCount,
        "Absence of a deployment is only actionable once, when a known deployment disappears.");
  }

  /**
   * The regression this guards: prepareStatus() skips all processing when the interim status hash is
   * unchanged, and a deleted deployment reports an identical status on every tick. The deletion check
   * therefore has to run on that short-circuit path too. A counter of consecutive observations, or a
   * check placed only on the change path, would never fire.
   */
  @Test
  void deletedDeploymentIsDetectedEvenAfterTheInterimStatusStopsChanging() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;

    final BlueGreenInterimStatus absentStatus = interimStatus(null);

    // First call goes through the change path and starts the grace period.
    provider.prepareStatus(BlueGreenRole.SOURCE, absentStatus);
    assertEquals(0, provider.initMonitoringCount);

    // Every later call reports exactly the same status, so prepareStatus short-circuits.
    provider.advanceMs(GRACE_MS);
    provider.prepareStatus(BlueGreenRole.SOURCE, absentStatus);

    assertEquals(1, provider.initMonitoringCount,
        "The deletion check must keep running once the interim status stops changing.");
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  // ---------------------------------------------------------------------------------------------
  // A context that joins a switchover late. resetContextWhenCompleted() starts fresh monitors right
  // after a switchover, and they can still read the finished deployment's POST or COMPLETED status.
  // Such a context never collected the start-of-switchover IP addresses and topology, so it treats
  // that status as already finished. The switchover timer bounds the remaining cases.
  // ---------------------------------------------------------------------------------------------

  private static final long SWITCHOVER_TIMEOUT_MS =
      Long.parseLong(BlueGreenStatusProvider.BG_SWITCHOVER_TIMEOUT_MS.defaultValue);

  /**
   * The regression this guards, observed on Aurora MySQL with 4.6.0: pods whose fresh context first
   * read a lingering POST status reported POST forever, never logged a warning, and ignored both the
   * deletion of the old deployment and the creation of a new one. Such a status must be ignored
   * without arming the switchover timer, and without leaving data behind that would affect the next
   * deployment.
   */
  @Test
  void finishedSwitchoverStatusIsIgnoredByContextThatNeverSawTheDeployment() {
    for (final BlueGreenPhase phase : new BlueGreenPhase[] {
        BlueGreenPhase.POST, BlueGreenPhase.COMPLETED}) {

      final TestableProvider provider = newTestableProvider();
      final BlueGreenInterimStatus sourceStatus = collectedInterimStatus(phase, "old-blue-node", false);
      final BlueGreenInterimStatus targetStatus = collectedInterimStatus(phase, "old-green-node", false);

      provider.prepareStatus(BlueGreenRole.SOURCE, sourceStatus);
      provider.prepareStatus(BlueGreenRole.TARGET, targetStatus);

      assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase, phase.name());
      assertNull(provider.summaryStatus, phase.name());
      assertEquals(0, provider.postStatusEndTimeNano, "The switchover timer must not be armed.");
      assertNull(provider.interimStatuses[BlueGreenRole.SOURCE.getValue()]);
      assertNull(provider.interimStatuses[BlueGreenRole.TARGET.getValue()]);
      assertTrue(provider.roleByHost.isEmpty(), "Hosts of a finished deployment must not be kept.");

      // The status stays readable for longer than the switchover timeout.
      provider.advanceMs(SWITCHOVER_TIMEOUT_MS * 2);
      provider.prepareStatus(BlueGreenRole.SOURCE, sourceStatus);
      provider.prepareStatus(BlueGreenRole.TARGET, targetStatus);
      assertEquals(0, provider.initMonitoringCount, "Ignoring the status must not cycle the context.");

      // A new deployment is created. Had the TARGET POST been stored, this would look like a rollback.
      provider.prepareStatus(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.CREATED));
      provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.CREATED));

      assertFalse(provider.rollback, phase.name());
      assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase, phase.name());
      assertEquals(BlueGreenPhase.CREATED, provider.summaryStatus.getCurrentPhase(), phase.name());
    }
  }

  /**
   * A context that saw CREATED has the start-of-switchover data that POST routing needs, so it keeps
   * adopting POST, including when a fast switchover hides PREPARATION and IN_PROGRESS from it.
   */
  @Test
  void contextThatSawCreatedAdoptsPost() {
    final TestableProvider provider = newTestableProvider();

    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.CREATED));
    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.POST));

    assertEquals(BlueGreenPhase.POST, provider.latestStatusPhase);
    assertEquals(BlueGreenPhase.POST, provider.summaryStatus.getCurrentPhase());
    assertTrue(provider.postStatusEndTimeNano > 0,
        "The switchover timer must be armed even though PREPARATION was not observed.");
  }

  /**
   * The case the switchover timer exists for: a switchover observed from PREPARATION that stalls in
   * IN_PROGRESS, with a status that stops changing. On expiry the forced COMPLETED status is published,
   * which releases the held traffic, and the context is reset as it would be on the next status change.
   */
  @Test
  void switchoverStalledInProgressIsCompletedAndResetOnTimeout() {
    final TestableProvider provider = newTestableProvider();
    final BlueGreenInterimStatus inProgressStatus = interimStatus(BlueGreenPhase.IN_PROGRESS);

    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.PREPARATION));
    provider.prepareStatus(BlueGreenRole.SOURCE, inProgressStatus);
    assertEquals(BlueGreenPhase.IN_PROGRESS, provider.summaryStatus.getCurrentPhase());

    provider.advanceMs(SWITCHOVER_TIMEOUT_MS - 1);
    provider.prepareStatus(BlueGreenRole.SOURCE, inProgressStatus);
    assertEquals(0, provider.initMonitoringCount, "The context must be kept until the timeout.");

    provider.advanceMs(2);
    provider.prepareStatus(BlueGreenRole.SOURCE, inProgressStatus);

    verify(mockStorageService).set(eq(BGD_ID),
        argThat((BlueGreenStatus status) -> status.getCurrentPhase() == BlueGreenPhase.COMPLETED));
    assertEquals(1, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  /**
   * The COMPLETED branch of checkSwitchoverTimerExpiry(): the deployment reports COMPLETED but the DNS
   * updates are never observed, so getStatusOfCompleted() keeps reporting POST. If the status stops
   * changing, only the timer ends it.
   */
  @Test
  void completedWaitingForDnsUpdatesIsCompletedAndResetOnTimeout() {
    final TestableProvider provider = newTestableProvider();
    final BlueGreenInterimStatus completedStatus = interimStatus(BlueGreenPhase.COMPLETED);

    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.CREATED));
    provider.prepareStatus(BlueGreenRole.SOURCE, completedStatus);
    assertEquals(BlueGreenPhase.POST, provider.summaryStatus.getCurrentPhase(),
        "COMPLETED is reported as POST while the DNS updates have not been observed.");

    provider.advanceMs(SWITCHOVER_TIMEOUT_MS + 1);
    provider.prepareStatus(BlueGreenRole.SOURCE, completedStatus);

    verify(mockStorageService).set(eq(BGD_ID),
        argThat((BlueGreenStatus status) -> status.getCurrentPhase() == BlueGreenPhase.COMPLETED));
    assertEquals(1, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  private BlueGreenInterimStatus switchoverStatus(
      final BlueGreenPhase phase,
      final boolean allStartTopologyIpChanged,
      final boolean allStartTopologyEndpointsRemoved) {
    return new BlueGreenInterimStatus(
        phase,
        "1.0",
        5432,
        new ArrayList<>(),
        new ArrayList<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashSet<>(),
        allStartTopologyIpChanged,
        allStartTopologyEndpointsRemoved,
        false,
        false);
  }

  /**
   * Replays the whole lifecycle seen in the 4.6.0 case, with the finished deployment's status staying
   * readable for a varying time after the switchover. In that case it decided which pods got stuck:
   * pods whose fresh monitors read before the status disappeared stayed in POST, the rest recovered.
   * The outcome must not depend on it, including when the status outlives the switchover timeout.
   *
   * @param lingerMs how long the fresh context keeps reading the finished deployment's POST status.
   *     240000 is longer than the 180000 default of bgSwitchoverTimeoutMs.
   */
  @ParameterizedTest
  @ValueSource(longs = {0, 3_000, 240_000})
  void finishedSwitchoverStatusLingeringAfterResetDoesNotAffectTheNextDeployment(final long lingerMs) {
    final TestableProvider provider = newTestableProvider();

    // The switchover, observed from the start by the original context.
    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.CREATED));
    provider.prepareStatus(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.CREATED));
    provider.advanceMs(1_000);
    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.PREPARATION));
    provider.advanceMs(3_500);
    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.IN_PROGRESS));
    provider.advanceMs(4_500);
    provider.prepareStatus(BlueGreenRole.SOURCE, switchoverStatus(BlueGreenPhase.POST, false, false));
    provider.advanceMs(20_000);
    provider.prepareStatus(BlueGreenRole.SOURCE, switchoverStatus(BlueGreenPhase.POST, true, false));
    provider.advanceMs(15_000);
    provider.prepareStatus(BlueGreenRole.TARGET, switchoverStatus(BlueGreenPhase.POST, false, true));
    provider.prepareStatus(BlueGreenRole.SOURCE, switchoverStatus(BlueGreenPhase.COMPLETED, true, false));

    assertEquals(1, provider.initMonitoringCount, "The completed switchover must reset the context.");
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);

    // The fresh context. Its monitors keep reading the finished deployment's status for lingerMs.
    final BlueGreenInterimStatus lingeringStatus = interimStatus(BlueGreenPhase.POST);
    for (long elapsedMs = 0; elapsedMs < lingerMs; elapsedMs += 1_000) {
      provider.prepareStatus(BlueGreenRole.SOURCE, lingeringStatus);
      provider.prepareStatus(BlueGreenRole.TARGET, lingeringStatus);
      provider.advanceMs(1_000);
    }

    // The status rows disappear, and the deployment is deleted some time later.
    final BlueGreenInterimStatus absentStatus = interimStatus(null);
    for (int i = 0; i < 60; i++) {
      provider.prepareStatus(BlueGreenRole.SOURCE, absentStatus);
      provider.prepareStatus(BlueGreenRole.TARGET, absentStatus);
      provider.advanceMs(10_000);
    }

    assertEquals(0, provider.postStatusEndTimeNano, "The switchover timer must not be armed.");
    assertEquals(1, provider.initMonitoringCount, "The fresh context must not be reset.");
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);

    // A new deployment is created.
    provider.prepareStatus(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.CREATED));
    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.CREATED));

    assertFalse(provider.rollback);
    assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase);
    assertEquals(BlueGreenPhase.CREATED, provider.summaryStatus.getCurrentPhase());
  }

  /** A switchover observed from PREPARATION keeps the deadline that PREPARATION set. */
  @Test
  void switchoverTimerStartedInPreparationIsNotRestartedByLaterPhases() {
    final TestableProvider provider = newTestableProvider();

    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.PREPARATION));
    final long deadline = provider.postStatusEndTimeNano;
    assertTrue(deadline > 0);

    provider.advanceMs(10_000);
    provider.prepareStatus(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.POST));

    assertEquals(deadline, provider.postStatusEndTimeNano);
  }

  // ---------------------------------------------------------------------------------------------
  // Green environment unreachable. A green environment the driver cannot reach blocks a switchover,
  // because the blue to green mapping needed to route traffic is never built.
  // ---------------------------------------------------------------------------------------------

  @Test
  void unreachableGreenReArmsTheReadinessEvent() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, true);
    // The deployment had previously been reported as ready for switchover.
    provider.greenTopologyRecognizedLogged.set(true);

    provider.checkGreenReachability();

    assertTrue(provider.greenUnreachableLogged.get());
    assertFalse(provider.greenTopologyRecognizedLogged.get(),
        "Regaining reachability has to be reported, not just the outage.");
  }

  @Test
  void unreachableGreenIsReportedOncePerOutage() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, true);

    provider.checkGreenReachability();
    // If the warning fired again it would re-arm the readiness event a second time.
    provider.greenTopologyRecognizedLogged.set(true);
    provider.checkGreenReachability();

    assertTrue(provider.greenTopologyRecognizedLogged.get(),
        "A sustained outage must not be reported on every status update.");
  }

  @Test
  void reachableGreenClearsTheUnreachableState() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, true);
    provider.checkGreenReachability();
    assertTrue(provider.greenUnreachableLogged.get());

    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, false);
    provider.checkGreenReachability();

    assertFalse(provider.greenUnreachableLogged.get(),
        "A recovered green environment must allow the next outage to be reported again.");
  }

  /**
   * Readiness has to account for reachability. The target monitor keeps reporting the last topology
   * it managed to collect, so the collected data still looks complete after the green environment has
   * gone away; without the reachability check the deployment would be reported as ready for
   * switchover when it is not.
   */
  @Test
  void unreachableGreenIsNotReportedAsReadyForSwitchover() {
    final BlueGreenStatusProvider provider = providerReadyForSwitchover(true);

    provider.logGreenTopologyRecognized();

    assertFalse(provider.greenTopologyRecognizedLogged.get(),
        "An unreachable green environment is not ready for switchover.");
  }

  @Test
  void reachableGreenWithCollectedTopologyIsReportedAsReadyForSwitchover() {
    final BlueGreenStatusProvider provider = providerReadyForSwitchover(false);

    provider.logGreenTopologyRecognized();

    assertTrue(provider.greenTopologyRecognizedLogged.get(),
        "The readiness event must still be emitted when the green environment is reachable.");
  }

  /**
   * The regression this guards: a green node that is up and answering, but detached from the
   * deployment, leaves the collected topology and the corresponding node map looking complete. Being
   * reachable is not sufficient, so readiness also has to require that green still reports itself as
   * part of the deployment, or the driver would advertise a mapping no switchover can use.
   */
  @Test
  void reachableGreenThatNoLongerReportsTheDeploymentIsNotReadyForSwitchover() {
    final BlueGreenStatusProvider provider = providerReadyForSwitchover(false);
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        collectedInterimStatus(null, "green-node", false);

    provider.logGreenTopologyRecognized();

    assertFalse(provider.greenTopologyRecognizedLogged.get(),
        "A green node detached from the deployment is not ready for switchover.");
  }

  /**
   * A provider whose collected data satisfies every readiness condition, so that reachability is the
   * only variable left.
   */
  private BlueGreenStatusProvider providerReadyForSwitchover(final boolean greenUnreachable) {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
        collectedInterimStatus(BlueGreenPhase.CREATED, "blue-node", false);
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        collectedInterimStatus(BlueGreenPhase.CREATED, "green-node", greenUnreachable);
    provider.correspondingNodes.put("blue-node", Pair.create(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("blue-node").build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("green-node").build()));
    return provider;
  }
}
